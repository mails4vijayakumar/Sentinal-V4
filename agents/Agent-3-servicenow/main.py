"""
agents/Agent-3-servicenow/main.py
===================================
Agent 3: ServiceNow INC create/bind + work-note flush.

Primary flow: Creates a new SNOW incident and links it to the pipeline run.
Secondary flow: Binds to the existing SNOW ticket (already created by customer support).
Both flows: Flushes a structured work-note with enrichment evidence at the end.
"""
from __future__ import annotations
import asyncio, json, logging, os, time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional

import httpx, uvicorn
from fastapi import FastAPI

import sys; sys.path.insert(0, "/app")
from shared.models import ServiceNowEnrichment, SSEEventType
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget
from shared.snow_auth import get_snow_token

log         = logging.getLogger(__name__)
PORT        = int(os.getenv("AGENT_3_PORT", "8003"))
SNOW_BASE   = os.getenv("SNOW_BASE_URL", "").rstrip("/")
AGENT_NAME  = "servicenow"
AGENT_NUM   = 3
_INC_TABLE  = "incident"

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    yield

app = FastAPI(title="Sentinel Agent 3 — ServiceNow", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_NAME}

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

    ctx  = await redis.get_context(run_id)
    if not ctx:
        return
    flow      = ctx.get("flow", "primary")
    event     = ctx.get("event", {})
    severity  = event.get("severity", "P3")
    title     = event.get("title", "Automated Incident")
    ext_id    = event.get("external_id", "")
    host      = event.get("host") or ""
    splunk    = ctx.get("enrichments", {}).get("splunk", {})

    try:
        token = await get_snow_token()
        headers = {"Authorization": token, "Content-Type": "application/json",
                   "Accept": "application/json"}
        enrichment: ServiceNowEnrichment
        if flow == "primary":
            enrichment = await _create_incident(headers, severity, title, ext_id, host, splunk)
        else:
            enrichment = await _bind_incident(headers, ext_id)

        # Flush work note with current evidence
        if enrichment.snow_sys_id:
            await _post_work_note(headers, enrichment.snow_sys_id, ctx)

        ctx.setdefault("enrichments", {})["servicenow"] = enrichment.model_dump()
        await redis.store_context(run_id, ctx)
        fire_and_forget(rc.write_enrichment(run_id, {"agent_num": AGENT_NUM, "source": "servicenow", "data": enrichment.model_dump()}))
        fire_and_forget(rc.record_step(run_id, {"agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "status": "completed", "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary": f"INC {enrichment.action}: {enrichment.snow_number or 'N/A'}"}))
        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
            "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)
        await redis.enqueue(4, run_id)

    except Exception as exc:
        log.exception("Agent 3 error: %s", exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
            "agent_num": AGENT_NUM, "agent_name": AGENT_NAME, "data": {"error": str(exc)}}, run_id=run_id)
        await redis.enqueue(4, run_id)   # non-fatal

_IMPACT = {"P1": "1", "P2": "2", "P3": "3", "P4": "4", "P5": "5"}
_URGENCY = {"P1": "1", "P2": "2", "P3": "3", "P4": "4", "P5": "5"}

async def _create_incident(headers, severity, title, ext_id, host, splunk) -> ServiceNowEnrichment:
    if not SNOW_BASE:
        return ServiceNowEnrichment(action="skipped")
    async with httpx.AsyncClient(base_url=SNOW_BASE, headers=headers, timeout=15) as c:
        body = {
            "short_description": title,
            "impact": _IMPACT.get(severity, "3"),
            "urgency": _URGENCY.get(severity, "3"),
            "category": "Software",
            "caller_id": os.getenv("SNOW_CALLER_ID", "sentinel.agent"),
            "description": f"Automated incident from Sentinel. DT ID: {ext_id}\nAffected host: {host}",
        }
        resp = await c.post(f"/api/now/table/{_INC_TABLE}", json=body)
        resp.raise_for_status()
        rec = resp.json()["result"]
    return ServiceNowEnrichment(
        snow_number=rec["number"], snow_sys_id=rec["sys_id"],
        ci_name=host, action="created",
    )

async def _bind_incident(headers, snow_number: str) -> ServiceNowEnrichment:
    if not SNOW_BASE or not snow_number.startswith("INC"):
        return ServiceNowEnrichment(action="skipped")
    async with httpx.AsyncClient(base_url=SNOW_BASE, headers=headers, timeout=15) as c:
        resp = await c.get(f"/api/now/table/{_INC_TABLE}", params={
            "sysparm_query": f"number={snow_number}", "sysparm_limit": "1",
        })
        resp.raise_for_status()
        recs = resp.json().get("result", [])
    if not recs:
        return ServiceNowEnrichment(action="skipped")
    rec = recs[0]
    return ServiceNowEnrichment(
        snow_number=rec["number"], snow_sys_id=rec["sys_id"],
        ci_name=rec.get("cmdb_ci", {}).get("display_value"),
        owner_group=rec.get("assignment_group", {}).get("display_value"),
        action="bound",
    )

async def _post_work_note(headers, sys_id: str, ctx: dict):
    splunk  = ctx.get("enrichments", {}).get("splunk", {})
    note    = (
        f"[Sentinel Auto-Triage]\n"
        f"Splunk: {splunk.get('error_count', 0)} errors / {splunk.get('warn_count', 0)} warns "
        f"in last {splunk.get('time_range', 'N/A')}.\n"
        f"Classification: {splunk.get('classification', 'unknown')}\n"
        f"Summary: {splunk.get('llm_summary', 'N/A')}"
    )
    if not SNOW_BASE:
        return
    async with httpx.AsyncClient(base_url=SNOW_BASE, headers=headers, timeout=15) as c:
        await c.patch(f"/api/now/table/{_INC_TABLE}/{sys_id}",
                      json={"work_notes": note})

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
