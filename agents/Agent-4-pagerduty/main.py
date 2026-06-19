"""
agents/Agent-4-pagerduty/main.py
==================================
Agent 4: PagerDuty on-call resolution, SNOW assignment, SLA enforcement.

Primary flow: Creates or updates a PD incident and finds on-call engineer.
Updates SNOW with on-call assignment. Calculates SLA breach time.
Secondary flow: Skipped for P4/P5.
"""
from __future__ import annotations
import asyncio, logging, os, time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import httpx, uvicorn
from fastapi import FastAPI

import sys; sys.path.insert(0, "/app")
from shared.models import PagerDutyEnrichment, Severity, SSEEventType
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget
from shared.snow_auth import get_snow_token

log       = logging.getLogger(__name__)
PORT      = int(os.getenv("AGENT_4_PORT", "8004"))
PD_KEY    = os.getenv("PD_API_KEY", "")
PD_SVC_ID = os.getenv("PD_SERVICE_ID", "")
PD_EMAIL  = os.getenv("PD_FROM_EMAIL", "sentinel@example.com")
SNOW_BASE = os.getenv("SNOW_BASE_URL", "").rstrip("/")
AGENT_NAME = "pagerduty"
AGENT_NUM  = 4

_SLA_MINUTES = {"P1": 15, "P2": 60, "P3": 240, "P4": 0, "P5": 0}

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    yield

app = FastAPI(title="Sentinel Agent 4 — PagerDuty", lifespan=lifespan)

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

    ctx      = await redis.get_context(run_id)
    if not ctx: return
    flow     = ctx.get("flow", "primary")
    severity = ctx.get("event", {}).get("severity", "P3")

    try:
        if flow == "secondary" or severity in ("P4", "P5"):
            enr = PagerDutyEnrichment(action="skipped")
        else:
            enr = await _trigger_pd(run_id, ctx, severity)
            # Update SNOW assignment if we have both PD contact and SNOW record
            snow_sys_id = ctx.get("enrichments", {}).get("servicenow", {}).get("snow_sys_id")
            if snow_sys_id and enr.on_call_name:
                await _update_snow_assignment(snow_sys_id, enr.on_call_name, enr.on_call_email)

        ctx.setdefault("enrichments", {})["pagerduty"] = enr.model_dump()
        await redis.store_context(run_id, ctx)
        fire_and_forget(rc.write_enrichment(run_id, {"agent_num": AGENT_NUM, "source": "pagerduty", "data": enr.model_dump()}))
        fire_and_forget(rc.record_step(run_id, {"agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "status": "completed", "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary": f"PD {enr.action}: on-call={enr.on_call_name or 'N/A'}"}))
        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
            "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)
        # Fan-out: Agent 5 (notifications) and Agent 6 (confluence) in parallel
        await asyncio.gather(redis.enqueue(5, run_id), redis.enqueue(6, run_id))

    except Exception as exc:
        log.exception("Agent 4 error: %s", exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
            "agent_num": AGENT_NUM, "data": {"error": str(exc)}}, run_id=run_id)
        await asyncio.gather(redis.enqueue(5, run_id), redis.enqueue(6, run_id))

async def _trigger_pd(run_id: str, ctx: dict, severity: str) -> PagerDutyEnrichment:
    if not (PD_KEY and PD_SVC_ID):
        return PagerDutyEnrichment(action="skipped")
    title = ctx.get("event", {}).get("title", "Sentinel Alert")
    sla_m = _SLA_MINUTES.get(severity, 0)
    sla_breach = datetime.utcnow() + timedelta(minutes=sla_m) if sla_m else None

    headers = {"Authorization": f"Token token={PD_KEY}", "From": PD_EMAIL,
               "Content-Type": "application/json", "Accept": "application/json"}
    body = {
        "incident": {
            "type": "incident",
            "title": title,
            "service": {"id": PD_SVC_ID, "type": "service_reference"},
            "urgency": "high" if severity in ("P1", "P2") else "low",
            "incident_key": f"sentinel-{run_id}",
            "body": {"type": "incident_body", "details": f"Severity: {severity}\nRun: {run_id}"},
        }
    }
    async with httpx.AsyncClient(base_url="https://api.pagerduty.com", headers=headers, timeout=15) as c:
        resp = await c.post("/incidents", json=body)
        resp.raise_for_status()
        inc = resp.json()["incident"]
        inc_id = inc["id"]

        # Get on-call
        oncall_resp = await c.get("/oncalls", params={"escalation_policy_ids[]": inc.get("escalation_policy", {}).get("id", ""), "limit": 1})
        oncalls = oncall_resp.json().get("oncalls", [])
        on_call = oncalls[0].get("user", {}) if oncalls else {}

    return PagerDutyEnrichment(
        pd_incident_id=inc_id,
        pd_incident_key=inc.get("incident_key"),
        on_call_name=on_call.get("name"),
        on_call_email=on_call.get("email"),
        sla_minutes=sla_m,
        action="alerted",
    )

async def _update_snow_assignment(sys_id: str, on_call_name: str, on_call_email: Optional[str]):
    if not SNOW_BASE: return
    token = await get_snow_token()
    async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=10) as c:
        await c.patch(f"/api/now/table/incident/{sys_id}",
            headers={"Authorization": token, "Content-Type": "application/json"},
            json={"assigned_to": on_call_name or "",
                  "work_notes": f"[Sentinel] PagerDuty on-call: {on_call_name} <{on_call_email}>"})

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
