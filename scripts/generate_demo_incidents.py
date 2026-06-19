#!/usr/bin/env python3
"""
scripts/generate_demo_incidents.py
====================================
Generates synthetic demo incidents (DB records only — no live pipeline).
Useful for seeding the Reports view and chatbot with realistic history.
"""
import asyncio, json, logging, os, random, sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

import httpx

ROUTING_BASE = os.getenv("ROUTING_DB_URL", "http://localhost:8000")
ADMIN_TOKEN  = os.getenv("ROUTING_DB_ADMIN_TOKEN", "")

SCENARIOS = [
    ("P1", "primary", "EHR Application — Database connection pool exhausted on api-gateway-prod-01", 42_000),
    ("P2", "primary", "HL7 Interface Engine — Channel reconnection failure after pod restart",     28_000),
    ("P2", "primary", "PACS Image Service — NFS mount point unavailable on radiology-prod-02",    55_000),
    ("P3", "primary", "Pharmacy System — Slow query timeout on medication-lookup service",         18_000),
    ("P4", "secondary","Storage Controller — SAN volume nearing 85% capacity threshold",           12_000),
    ("P5", "secondary","Lab System — Certificate expiry warning (30 days)",                         8_000),
    ("P1", "primary", "EHR API Gateway — OOMKilled: heap exhausted on ehr-api-prod-03",           38_000),
    ("P3", "primary", "Billing Service — Stripe webhook timeout, invoice processing delayed",      22_000),
]

async def main(count: int = 30):
    headers = {"X-Admin-Token": ADMIN_TOKEN, "Content-Type": "application/json"}
    async with httpx.AsyncClient(base_url=ROUTING_BASE, headers=headers, timeout=10) as c:
        for i in range(count):
            scenario = SCENARIOS[i % len(SCENARIOS)]
            sev, flow, title, dur_ms = scenario
            offset_h  = random.randint(1, 72)
            started   = datetime.now(timezone.utc) - timedelta(hours=offset_h)
            completed = started + timedelta(milliseconds=dur_ms + random.randint(-5000, 5000))

            ext_id = f"DEMO-{uuid4().hex[:8].upper()}"
            inc_resp = await c.post("/admin/incidents", json={
                "external_id": ext_id, "source": "dynatrace",
                "severity": sev, "flow": flow, "title": title,
            })
            if inc_resp.status_code not in (200, 201): continue
            inc_id = inc_resp.json().get("id", str(uuid4()))

            run_resp = await c.post("/admin/runs", json={"incident_id": inc_id, "flow": flow})
            if run_resp.status_code not in (200, 201): continue
            run_id = run_resp.json()["run_id"]

            await c.patch(f"/admin/runs/{run_id}", json={
                "status": "completed", "duration_ms": dur_ms,
                "completed_at": completed.isoformat(),
            })
            log.info("Created demo run %s  %s  %s", run_id[:8], sev, title[:50])

    log.info("Generated %d demo incidents.", count)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=30)
    asyncio.run(main(p.parse_args().count))
