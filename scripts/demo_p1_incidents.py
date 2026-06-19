#!/usr/bin/env python3
"""
scripts/demo_p1_incidents.py
=============================
Fire real P1 incidents end-to-end through the live pipeline.
Use this for demos — it creates a SIGNED webhook and watches the SSE stream.

Usage:
    python scripts/demo_p1_incidents.py [--count 2] [--interval 15]
"""
import argparse, asyncio, base64, hashlib, hmac, json, logging, os, time, uuid
import httpx
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

AGENT1_URL = os.getenv("AGENT1_URL", "http://localhost:8001")
DT_SECRET  = os.getenv("DT_WEBHOOK_SECRET", "")

DEMO_PROBLEMS = [
    ("AVAILABILITY", "EHR Application — Zero health checks passing on api-gateway-prod-01", ["app:ehr-application","env:prod","tier:critical"]),
    ("PERFORMANCE",  "PACS Image Service — P95 latency exceeding 8s SLA threshold",         ["app:pacs","env:prod"]),
    ("ERROR",        "HL7 Interface — Unhandled exception in ADT message processor",         ["app:hl7-engine","env:prod"]),
]

def _sign(body: bytes) -> str:
    if not DT_SECRET: return "NOSIG"
    return base64.b64encode(hmac.new(DT_SECRET.encode(), body, hashlib.sha256).digest()).decode()

async def fire_one(idx: int) -> str:
    sev, title, tags = DEMO_PROBLEMS[idx % len(DEMO_PROBLEMS)]
    prob_id = f"P-DEMO-{uuid.uuid4().hex[:8].upper()}"
    payload = {
        "eventType": "PERFORMANCE_EVENT", "severity": sev, "status": "OPEN",
        "problemId": prob_id, "displayName": title, "tags": tags,
        "impactedEntities": [{"entityId": f"SERVICE-DEMO{idx}", "name": f"demo-svc-{idx}", "type": "SERVICE"}],
        "deploymentEvent": False,
    }
    body = json.dumps(payload).encode()
    async with httpx.AsyncClient(timeout=10) as c:
        resp = await c.post(f"{AGENT1_URL}/api/webhook/dynatrace",
            content=body, headers={"Content-Type": "application/json", "X-DT-Signature": _sign(body)})
        resp.raise_for_status()
        d = resp.json()
        log.info("Fired %s %s → run_id=%s", sev, prob_id, d.get("run_id","?")[:8])
        return d.get("run_id", "")

async def watch_sse(run_id: str, timeout: int = 90):
    log.info("Watching SSE for run %s…", run_id[:8])
    deadline = time.time() + timeout
    async with httpx.AsyncClient(timeout=timeout+5) as c:
        async with c.stream("GET", f"{AGENT1_URL}/sse/run/{run_id}") as resp:
            async for line in resp.aiter_lines():
                if time.time() > deadline: break
                if not line.startswith("data:"): continue
                e = json.loads(line[5:].strip())
                evt = e.get("event","")
                if evt in ("agent_done","agent_start","pipeline_complete","pipeline_error"):
                    log.info("  [%s] agent=%s", evt, e.get("agent_num","—"))
                if evt in ("pipeline_complete","pipeline_error"):
                    log.info("  ✓ Pipeline %s in %sms", evt, e.get("data",{}).get("duration_ms","?"))
                    return

async def main(count: int, interval: int):
    run_ids = []
    for i in range(count):
        if i > 0: await asyncio.sleep(interval)
        run_id = await fire_one(i)
        if run_id: run_ids.append(run_id)
    await asyncio.gather(*[watch_sse(rid) for rid in run_ids])

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--count",    type=int, default=2)
    p.add_argument("--interval", type=int, default=10)
    asyncio.run(main(**vars(p.parse_args())))
