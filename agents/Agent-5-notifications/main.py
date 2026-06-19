"""
agents/Agent-5-notifications/main.py
======================================
Agent 5: Multi-channel notification delivery.
Sends to: Microsoft Teams webhook, Email (SMTP), SMS (optional).
Primary flow only. Secondary flow is skipped.
"""
from __future__ import annotations
import asyncio, logging, os, smtplib, time
from contextlib import asynccontextmanager
from datetime import datetime
from email.mime.text import MIMEText
from typing import List

import httpx, uvicorn
from fastapi import FastAPI

import sys; sys.path.insert(0, "/app")
from shared.models import NotificationEnrichment, SSEEventType
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget

log         = logging.getLogger(__name__)
PORT        = int(os.getenv("AGENT_5_PORT", "8005"))
TEAMS_URL   = os.getenv("TEAMS_WEBHOOK_URL", "")
SMTP_HOST   = os.getenv("SMTP_HOST", "")
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER   = os.getenv("SMTP_USER", "")
SMTP_PASS   = os.getenv("SMTP_PASS", "")
NOTIFY_TO   = os.getenv("NOTIFY_EMAIL_TO", "")
AGENT_NAME  = "notifications"
AGENT_NUM   = 5

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    yield

app = FastAPI(title="Sentinel Agent 5 — Notifications", lifespan=lifespan)

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
    title    = ctx.get("event", {}).get("title", "Alert")
    snow_num = ctx.get("enrichments", {}).get("servicenow", {}).get("snow_number", "N/A")
    pd_id    = ctx.get("enrichments", {}).get("pagerduty", {}).get("pd_incident_id", "N/A")
    on_call  = ctx.get("enrichments", {}).get("pagerduty", {}).get("on_call_name", "N/A")
    classification = ctx.get("enrichments", {}).get("splunk", {}).get("classification", "unknown")

    try:
        channels: List[str] = []
        teams_ok = email_ok = False
        if flow == "secondary" or severity in ("P4", "P5"):
            enr = NotificationEnrichment(channels_notified=[])
        else:
            if TEAMS_URL:
                teams_ok = await _send_teams(severity, title, snow_num, pd_id, on_call, run_id)
                if teams_ok: channels.append("teams")
            if SMTP_HOST and NOTIFY_TO:
                email_ok = await _send_email(severity, title, snow_num, on_call, classification, run_id)
                if email_ok: channels.append("email")
            enr = NotificationEnrichment(channels_notified=channels, teams_ok=teams_ok, email_ok=email_ok)

        ctx.setdefault("enrichments", {})["notifications"] = enr.model_dump()
        await redis.store_context(run_id, ctx)
        fire_and_forget(rc.record_step(run_id, {"agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "status": "completed", "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary": f"Notified: {', '.join(channels) or 'none'}"}))
        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
            "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)
        # After notifications, Agent 6 runs (if not already running from Agent 4 fan-out)
        # Agent 6 is already enqueued by Agent 4 — no double-enqueue needed here

    except Exception as exc:
        log.exception("Agent 5 error: %s", exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
            "agent_num": AGENT_NUM, "data": {"error": str(exc)}}, run_id=run_id)

async def _send_teams(severity, title, snow_num, pd_id, on_call, run_id) -> bool:
    COLOR = {"P1": "FF0000", "P2": "FF6B00", "P3": "FFC300", "P4": "00C176", "P5": "666666"}
    card = {
        "@type": "MessageCard", "@context": "http://schema.org/extensions",
        "themeColor": COLOR.get(severity, "666666"),
        "summary": f"[{severity}] {title}",
        "sections": [{"activityTitle": f"🚨 [{severity}] {title}",
                      "facts": [{"name": "SNOW", "value": snow_num},
                                 {"name": "PagerDuty", "value": pd_id},
                                 {"name": "On-Call", "value": on_call},
                                 {"name": "Run ID", "value": run_id}]}]
    }
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            resp = await c.post(TEAMS_URL, json=card)
            return resp.status_code == 200
    except Exception as exc:
        log.warning("Teams notification failed: %s", exc)
        return False

async def _send_email(severity, title, snow_num, on_call, classification, run_id) -> bool:
    body = (
        f"Sentinel Alert — {severity}: {title}\n\n"
        f"SNOW Incident: {snow_num}\n"
        f"On-Call: {on_call}\n"
        f"Classification: {classification}\n"
        f"Run ID: {run_id}\n"
    )
    msg = MIMEText(body)
    msg["Subject"] = f"[{severity}] Sentinel: {title}"
    msg["From"]    = SMTP_USER
    msg["To"]      = NOTIFY_TO
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [NOTIFY_TO], msg.as_string())
        return True
    except Exception as exc:
        log.warning("Email failed: %s", exc)
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
