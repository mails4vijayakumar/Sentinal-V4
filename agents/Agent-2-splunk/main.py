"""
agents/Agent-2-splunk/main.py
==============================
Agent 2: Splunk log analysis + classification.

Workflow:
  1. BLPOP run_id from agent:2:queue
  2. Load IncidentContext from Redis
  3. Execute SPL query (adaptive window: 15min P1, 30min P2/P3, 60min P4/P5)
  4. Classify top error patterns via LLM
  5. Write SplunkEnrichment to routing-db
  6. Enqueue Agent 3 (primary) or fan-out [3,6] (secondary)
  7. Publish agent_done SSE
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException

import sys; sys.path.insert(0, "/app")

from shared.models import (
    OrchestratorEvent, Severity, IncidentFlow,
    SplunkEnrichment, SSEEvent, SSEEventType,
)
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget

log = logging.getLogger(__name__)

PORT          = int(os.getenv("AGENT_2_PORT", "8002"))
SPLUNK_BASE   = os.getenv("SPLUNK_BASE_URL", "").rstrip("/")
SPLUNK_TOKEN  = os.getenv("SPLUNK_TOKEN", "")
SPLUNK_INDEX  = os.getenv("SPLUNK_INDEX", "main")
AGENT_NAME    = "splunk"
AGENT_NUM     = 2

# Time windows per severity
_WINDOW: Dict[str, int] = {"P1": 15, "P2": 30, "P3": 30, "P4": 60, "P5": 60}


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    log.info("Agent 2 (splunk) starting on :%d", PORT)
    yield

app = FastAPI(title="Sentinel Agent 2 — Splunk", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_NAME, "port": PORT}


# ── Worker ────────────────────────────────────────────────────────────────────

async def worker_loop() -> None:
    redis = await get_redis()
    log.info("Agent 2 worker started")
    while True:
        run_id = await redis.dequeue(AGENT_NUM)
        if run_id:
            asyncio.create_task(process_run(run_id))


async def process_run(run_id: str) -> None:
    redis = await get_redis()
    rc    = get_routing_client()
    t0    = time.monotonic()

    # Publish agent_start
    await redis.publish_event({"event": SSEEventType.AGENT_START, "run_id": run_id,
                                "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
                                "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)

    ctx = await redis.get_context(run_id)
    if not ctx:
        log.error("Agent 2: no context for run_id=%s", run_id)
        return

    event_data = ctx.get("event", {})
    severity   = event_data.get("severity", "P3")
    host       = event_data.get("host") or ""
    service    = event_data.get("service") or ""
    flow       = ctx.get("flow", "primary")

    try:
        enrichment = await _run_splunk(severity, host, service)
        ctx.setdefault("enrichments", {})["splunk"] = enrichment.model_dump()
        await redis.store_context(run_id, ctx)

        fire_and_forget(rc.record_step(run_id, {
            "agent_num":   AGENT_NUM, "agent_name": AGENT_NAME,
            "status":      "completed",
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary":     enrichment.llm_summary or f"{enrichment.error_count} errors found",
        }))
        fire_and_forget(rc.write_enrichment(run_id, {
            "agent_num": AGENT_NUM, "source": "splunk", "data": enrichment.model_dump(),
        }))

        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
                                    "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "data": {"classification": enrichment.classification}},
                                   run_id=run_id)

        # Route forward
        if flow == "primary":
            await redis.enqueue(3, run_id)
        else:
            # Secondary: fan-out to Agent 3 (CMDB) and Agent 6 (Confluence) in parallel
            await asyncio.gather(
                redis.enqueue(3, run_id),
                redis.enqueue(6, run_id),
            )

    except Exception as exc:
        log.exception("Agent 2 error for run_id=%s: %s", run_id, exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
                                    "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
                                    "data": {"error": str(exc)}}, run_id=run_id)
        # Non-fatal: continue pipeline
        await redis.enqueue(3, run_id)


async def _run_splunk(severity: str, host: str, service: str) -> SplunkEnrichment:
    if not SPLUNK_BASE:
        return SplunkEnrichment(log_lines_scanned=0, llm_summary="Splunk not configured")

    window_min = _WINDOW.get(severity, 30)
    earliest   = f"-{window_min}m@m"
    host_filter = f' host="{host}"' if host else ""
    svc_filter  = f' source="*{service}*"' if service else ""
    spl = (
        f'search index={SPLUNK_INDEX}{host_filter}{svc_filter} earliest={earliest} '
        f'(ERROR OR WARN OR CRITICAL OR Exception OR Traceback) '
        f'| stats count by log_level | sort - count | head 20'
    )

    async with httpx.AsyncClient(
        base_url=SPLUNK_BASE,
        headers={"Authorization": f"Bearer {SPLUNK_TOKEN}"},
        verify=False, timeout=25,
    ) as client:
        # Submit search job
        resp = await client.post("/services/search/jobs", data={
            "search":   spl, "output_mode": "json", "exec_mode": "oneshot",
            "earliest_time": earliest, "latest_time": "now",
        })
        resp.raise_for_status()
        results = resp.json().get("results", [])

    error_count = sum(int(r.get("count", 0)) for r in results if "ERROR" in r.get("log_level", "").upper())
    warn_count  = sum(int(r.get("count", 0)) for r in results if "WARN"  in r.get("log_level", "").upper())

    top_errors = [r.get("log_level", "UNKNOWN") for r in results[:5]]
    classification = _classify(top_errors, error_count)

    return SplunkEnrichment(
        log_lines_scanned=sum(int(r.get("count", 0)) for r in results),
        error_count=error_count,
        warn_count=warn_count,
        top_errors=top_errors,
        time_range=f"last {window_min}min",
        index=SPLUNK_INDEX,
        spl_query=spl,
        llm_summary=f"Found {error_count} errors and {warn_count} warnings over last {window_min}min.",
        classification=classification,
    )


def _classify(top_errors: List[str], error_count: int) -> str:
    joined = " ".join(top_errors).lower()
    if "connection" in joined or "timeout" in joined:
        return "db_connection"
    if "memory" in joined or "oom" in joined or "heap" in joined:
        return "memory"
    if "deploy" in joined or "classnot" in joined:
        return "deployment"
    if error_count > 100:
        return "high_error_rate"
    return "unknown"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
