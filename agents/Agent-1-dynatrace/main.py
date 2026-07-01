"""
agents/Agent-1-dynatrace/main.py
=================================
Agent 1: Dynatrace webhook ingestion, dedup, severity classification, and flow routing.

Responsibilities:
  - Accept DT problem webhooks (HMAC-SHA256 verified)
  - Accept SNOW outbound webhooks (HMAC-SHA256 verified)
  - Map DT severity → P1/P2/P3 and SNOW priority → P1/P5
  - Deduplicate via Redis distributed lock on (external_id)
  - Assign flow: primary (P1–P3) or secondary (P4–P5)
  - Persist incident to routing-db
  - Enqueue run_id on Agent 2 work queue
  - Publish pipeline_started SSE event
  - Expose /sse/dashboard and /sse/run/{run_id} for the React webapp
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import uvicorn
from fastapi import FastAPI, Header, Request, status
from fastapi.responses import StreamingResponse

# Adjust import path when running as a container
import sys
sys.path.insert(0, "/app")

from shared.models import (
    OrchestratorEvent, PipelineRun,
    SSEEvent, SSEEventType,
)
from shared.redis_client import STREAM_DASHBOARD, STREAM_RUN_PREFIX, get_redis
from shared.routing_client import get_routing_client, fire_and_forget

from ._common import (
    DT_SECRET,
    _DT_SEVERITY_MAP,
)
from .intake import dt as dt_intake
from .intake import snow as snow_intake

log = logging.getLogger(__name__)

PORT             = int(os.getenv("AGENT_1_PORT", "8001"))
AGENT_NAME       = "dynatrace"


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Agent 1 (dynatrace) starting on :%d", PORT)
    yield
    log.info("Agent 1 shutdown")


app = FastAPI(title="Sentinel Agent 1 — Dynatrace Ingestion", lifespan=lifespan)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_NAME, "port": PORT}


@app.get("/ready")
async def ready():
    redis = await get_redis()
    ok = await redis.ping()
    rc = get_routing_client()
    routing_ok = await rc.ping()
    if not (ok and routing_ok):
        raise HTTPException(status_code=503, detail="Dependencies not ready")
    return {"status": "ready"}


# ── DT webhook ────────────────────────────────────────────────────────────────

@app.post("/api/webhook/dynatrace", status_code=202)
async def dynatrace_webhook(
    request:        Request,
    x_dt_signature: str | None = Header(None, alias="X-DT-Signature"),
):
    return await dt_intake.handle(request, x_dt_signature, _ingest)


# ── SNOW webhook ──────────────────────────────────────────────────────────────

@app.post("/api/webhook/servicenow", status_code=202)
async def servicenow_webhook(
    request:          Request,
    x_snow_signature: str | None = Header(None, alias="X-SNOW-Signature"),
):
    return await snow_intake.handle(request, x_snow_signature, _ingest)


# ── Core ingestion logic ──────────────────────────────────────────────────────

async def _ingest(event: OrchestratorEvent) -> dict:
    redis = await get_redis()

    # Idempotency — one active run per external_id
    async with redis.lock(event.dedup_key or event.external_id) as acquired:
        if not acquired:
            log.info("Duplicate event skipped: %s", event.external_id)
            return {"accepted": False, "deduplicated": True, "external_id": event.external_id}

        # Persist incident to routing-db (fire-and-forget)
        rc = get_routing_client()
        inc_data = event.model_dump(mode="json")
        fire_and_forget(rc.upsert_incident({
            "external_id": event.external_id,
            "source":      event.source.value,
            "severity":    event.severity.value,
            "flow":        event.flow.value,
            "title":       event.title,
            "host":        event.host,
            "service":     event.service,
            "raw_payload": event.raw_payload,
        }))

        # Build pipeline run object
        from uuid import uuid4
        run = PipelineRun(
            event=event,
            flow=event.flow,
        )
        run_id = str(run.run_id)

        # Store run context in Redis
        await redis.store_context(run_id, run.model_dump(mode="json"))

        # Persist run to routing-db
        fire_and_forget(rc.create_run({
            "incident_id": str(run.incident_id),
            "flow":        event.flow.value,
        }))

        # Publish SSE: pipeline_started
        sse = SSEEvent(
            event=SSEEventType.PIPELINE_STARTED,
            run_id=run_id,
            data={
                "external_id": event.external_id,
                "severity":    event.severity.value,
                "flow":        event.flow.value,
                "title":       event.title,
                "source":      event.source.value,
            },
        )
        await redis.publish_event(sse.model_dump(mode="json"), run_id=run_id)

        # Enqueue Agent 2
        await redis.enqueue(2, run_id)

        log.info(
            "Ingested %s %s → run_id=%s flow=%s severity=%s",
            event.source.value, event.external_id, run_id, event.flow.value, event.severity.value,
        )

    return {
        "accepted":    True,
        "run_id":      run_id,
        "external_id": event.external_id,
        "severity":    event.severity.value,
        "flow":        event.flow.value,
    }


# ── SSE endpoints ─────────────────────────────────────────────────────────────

@app.get("/sse/dashboard")
async def sse_dashboard(request: Request):
    """Global SSE stream — all pipeline events."""
    return StreamingResponse(
        _sse_stream(request, STREAM_DASHBOARD),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@app.get("/sse/run/{run_id}")
async def sse_run(run_id: str, request: Request, last_id: str = "0"):
    """Per-run SSE stream with replay support via last_id."""
    stream = f"{STREAM_RUN_PREFIX}{run_id}"
    return StreamingResponse(
        _sse_stream(request, stream, last_id=last_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


async def _sse_stream(
    request:  Request,
    stream:   str,
    last_id:  str = "0",
) -> AsyncGenerator[str, None]:
    redis = await get_redis()
    current_id = last_id
    # Send immediate heartbeat
    yield "event: heartbeat\ndata: {}\n\n"

    while not await request.is_disconnected():
        try:
            entries = await redis.read_stream(stream, last_id=current_id, count=50, block_ms=4000)
            for entry_id, fields in entries:
                current_id = entry_id
                payload = {k: _try_json(v) for k, v in fields.items()}
                yield f"data: {json.dumps(payload)}\n\n"
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("SSE read error: %s", exc)
            yield "event: error\ndata: {}\n\n"
            await asyncio.sleep(2)


def _try_json(v: str):
    try:
        return json.loads(v)
    except (ValueError, TypeError):
        return v


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
