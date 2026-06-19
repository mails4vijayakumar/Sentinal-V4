"""
agents/Agent-7-Rca/main.py
============================
Agent 7: Root-Cause Analysis, deployments check, rollback detection,
resolution monitoring. Final agent in the pipeline.

Uses LLM to synthesize all enrichment data into a structured RCAResult.
Stores the resolution to feedback.resolutions.
Updates SNOW incident to Resolved (primary flow).
Publishes pipeline_complete SSE.
"""
from __future__ import annotations
import asyncio, json, logging, os, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import UUID

import httpx, uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

import sys; sys.path.insert(0, "/app")
from shared.models import (
    EnrichedIncident, IncidentFlow, OrchestratorEvent, RCAResult,
    ResolutionStep, Severity, SSEEventType, ConfluenceKBHit,
)
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget
from shared.snow_auth import get_snow_token

log        = logging.getLogger(__name__)
PORT       = int(os.getenv("AGENT_7_PORT", "8007"))
SNOW_BASE  = os.getenv("SNOW_BASE_URL", "").rstrip("/")
LLM_PROV   = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MDL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
ANT_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
ANT_MDL    = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
OAI_KEY    = os.getenv("OPENAI_API_KEY", "")
OAI_MDL    = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
AGENT_NAME = "rca"
AGENT_NUM  = 7

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    yield

app = FastAPI(title="Sentinel Agent 7 — RCA", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_NAME}

@app.post("/feedback")
async def post_feedback(body: dict):
    """Accept human feedback on resolution quality."""
    rc = get_routing_client()
    fire_and_forget(rc.write_enrichment(body.get("run_id", ""), {
        "agent_num": 7, "source": "human_feedback", "data": body,
    }))
    return {"ok": True}

async def worker_loop():
    redis = await get_redis()
    while True:
        run_id = await redis.dequeue(AGENT_NUM)
        if run_id:
            asyncio.create_task(process_run(run_id))

async def process_run(run_id: str):
    redis = await get_redis()
    rc    = get_routing_client()
    t0    = time.monotonic()
    await redis.publish_event({"event": SSEEventType.AGENT_START, "run_id": run_id,
        "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
        "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)

    ctx = await redis.get_context(run_id)
    if not ctx: return

    event_d = ctx.get("event", {})
    enrichments = ctx.get("enrichments", {})

    try:
        rca = await _generate_rca(run_id, event_d, enrichments)

        # Resolve SNOW incident (primary flow)
        flow = ctx.get("flow", "primary")
        snow_sys_id = enrichments.get("servicenow", {}).get("snow_sys_id")
        if flow == "primary" and snow_sys_id:
            await _resolve_snow(snow_sys_id, rca)

        # Flush final work note
        if snow_sys_id:
            await _flush_final_work_note(snow_sys_id, rca, enrichments)

        dur_ms = int((time.monotonic() - t0) * 1000)
        fire_and_forget(rc.record_step(run_id, {"agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "status": "completed", "duration_ms": dur_ms,
            "summary": f"RCA complete. Root cause: {rca.root_cause_category}. Confidence: {rca.confidence:.0%}"}))
        fire_and_forget(rc.complete_run(run_id, "completed", dur_ms))

        # Store resolution to feedback schema
        fire_and_forget(rc._http.post("/admin/feedback", json={
            "run_id": run_id, "incident_id": str(rca.incident_id),
            "root_cause": rca.root_cause, "root_cause_cat": rca.root_cause_category,
            "resolution_steps": [s.model_dump() for s in rca.resolution_steps],
            "confidence": int(rca.confidence * 100),
            "llm_provider": rca.llm_provider, "llm_model": rca.llm_model,
            "tokens_used": rca.tokens_used,
        }))

        await redis.publish_event({"event": SSEEventType.PIPELINE_COMPLETE, "run_id": run_id,
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "root_cause": rca.root_cause, "confidence": rca.confidence,
                "steps": len(rca.resolution_steps), "duration_ms": dur_ms,
                "flow": flow, "severity": event_d.get("severity"),
                "rollback_required": rca.rollback_required,
            }}, run_id=run_id)

    except Exception as exc:
        log.exception("Agent 7 RCA error: %s", exc)
        fire_and_forget(rc.complete_run(run_id, "failed"))
        await redis.publish_event({"event": SSEEventType.PIPELINE_ERROR, "run_id": run_id,
            "data": {"error": str(exc)}}, run_id=run_id)


async def _generate_rca(run_id: str, event_d: dict, enrichments: dict) -> RCAResult:
    from uuid import uuid4
    severity    = event_d.get("severity", "P3")
    title       = event_d.get("title", "Unknown incident")
    source      = event_d.get("source", "unknown")
    splunk      = enrichments.get("splunk", {})
    confluence  = enrichments.get("confluence", {})
    snow        = enrichments.get("servicenow", {})
    pd          = enrichments.get("pagerduty", {})

    kb_hits = [ConfluenceKBHit(**h) for h in confluence.get("hits", [])[:3]]
    kb_text = "\n".join(f"- {h.title} ({h.score:.2f}): {h.excerpt}" for h in kb_hits) or "No KB matches."

    prompt = f"""You are a healthcare IT incident responder performing root-cause analysis.

INCIDENT
  Title: {title}
  Severity: {severity}
  Source: {source}

LOG ANALYSIS (Splunk)
  Errors: {splunk.get('error_count', 0)}, Warns: {splunk.get('warn_count', 0)}
  Classification: {splunk.get('classification', 'unknown')}
  Summary: {splunk.get('llm_summary', 'No log data available')}

KNOWLEDGE BASE
{kb_text}

SERVICEOW
  INC: {snow.get('snow_number', 'N/A')} | CI: {snow.get('ci_name', 'N/A')}

Respond ONLY with valid JSON matching this schema (no markdown, no preamble):
{{
  "root_cause": "one-sentence root cause statement",
  "root_cause_category": "db_connection|memory|deployment|network|config|unknown",
  "confidence": 0.0-1.0,
  "rollback_required": true|false,
  "rollback_target": null or "version/commit/tag",
  "resolution_steps": [
    {{"step_num": 1, "action": "...", "owner": "...", "tool": "...", "command": null, "rationale": "..."}}
  ]
}}"""

    raw_json, provider, model, tokens = await _llm_complete(prompt)

    try:
        d = json.loads(raw_json)
    except json.JSONDecodeError:
        # Fallback
        d = {"root_cause": "Unable to determine root cause automatically.",
             "root_cause_category": "unknown", "confidence": 0.3,
             "rollback_required": False, "rollback_target": None, "resolution_steps": []}

    steps = [ResolutionStep(**s) if isinstance(s, dict) else ResolutionStep(step_num=i+1, action=str(s))
             for i, s in enumerate(d.get("resolution_steps", []))]

    return RCAResult(
        run_id=uuid4(), incident_id=uuid4(),
        external_id=event_d.get("external_id", ""),
        severity=Severity(severity),
        flow=IncidentFlow(event_d.get("flow", "primary")),
        root_cause=d.get("root_cause", ""),
        root_cause_category=d.get("root_cause_category"),
        confidence=float(d.get("confidence", 0.5)),
        resolution_steps=steps,
        rollback_required=bool(d.get("rollback_required", False)),
        rollback_target=d.get("rollback_target"),
        supporting_kb=kb_hits,
        log_evidence=splunk.get("llm_summary"),
        llm_provider=provider, llm_model=model, tokens_used=tokens,
    )

async def _llm_complete(prompt: str):
    if LLM_PROV == "anthropic" and ANT_KEY:
        return await _call_anthropic(prompt)
    if LLM_PROV == "openai" and OAI_KEY:
        return await _call_openai(prompt)
    return await _call_ollama(prompt)

async def _call_ollama(prompt: str):
    async with httpx.AsyncClient(base_url=OLLAMA_URL, timeout=60) as c:
        resp = await c.post("/api/generate", json={"model": OLLAMA_MDL, "prompt": prompt, "stream": False, "format": "json"})
        resp.raise_for_status()
        d = resp.json()
        return d["response"], "ollama", OLLAMA_MDL, d.get("eval_count", 0)

async def _call_anthropic(prompt: str):
    async with httpx.AsyncClient(base_url="https://api.anthropic.com", timeout=60) as c:
        resp = await c.post("/v1/messages",
            headers={"x-api-key": ANT_KEY, "anthropic-version": "2023-06-01"},
            json={"model": ANT_MDL, "max_tokens": 1024,
                  "messages": [{"role": "user", "content": prompt}]})
        resp.raise_for_status()
        d = resp.json()
        text = d["content"][0]["text"]
        tokens = d.get("usage", {}).get("output_tokens", 0)
        return text, "anthropic", ANT_MDL, tokens

async def _call_openai(prompt: str):
    async with httpx.AsyncClient(base_url="https://api.openai.com", timeout=60) as c:
        resp = await c.post("/v1/chat/completions",
            headers={"Authorization": f"Bearer {OAI_KEY}"},
            json={"model": OAI_MDL, "response_format": {"type": "json_object"},
                  "messages": [{"role": "user", "content": prompt}]})
        resp.raise_for_status()
        d = resp.json()
        text   = d["choices"][0]["message"]["content"]
        tokens = d.get("usage", {}).get("completion_tokens", 0)
        return text, "openai", OAI_MDL, tokens

async def _resolve_snow(sys_id: str, rca: RCAResult):
    if not SNOW_BASE: return
    try:
        token = await get_snow_token()
        async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=10) as c:
            await c.patch(f"/api/now/table/incident/{sys_id}",
                headers={"Authorization": token, "Content-Type": "application/json"},
                json={"state": "6", "close_code": "Solved (Permanently)",
                      "close_notes": f"[Sentinel] Root cause: {rca.root_cause}"})
    except Exception as exc:
        log.warning("SNOW resolve failed: %s", exc)

async def _flush_final_work_note(sys_id: str, rca: RCAResult, enrichments: dict):
    steps_text = "\n".join(f"{s.step_num}. {s.action}" for s in rca.resolution_steps[:5])
    note = (f"[Sentinel Final RCA]\nRoot cause: {rca.root_cause}\n"
            f"Category: {rca.root_cause_category}\nConfidence: {rca.confidence:.0%}\n"
            f"Rollback required: {'YES — target: ' + (rca.rollback_target or 'latest') if rca.rollback_required else 'No'}\n\n"
            f"Resolution steps:\n{steps_text}")
    if not SNOW_BASE: return
    try:
        token = await get_snow_token()
        async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=10) as c:
            await c.patch(f"/api/now/table/incident/{sys_id}",
                headers={"Authorization": token, "Content-Type": "application/json"},
                json={"work_notes": note})
    except Exception as exc:
        log.warning("Final work note failed: %s", exc)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
