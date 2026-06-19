#!/usr/bin/env python3
"""
scripts/inject_fidelis_portal_demo_logs.py
===========================================
Inject realistic demo log events for the Fidelis Portal into Splunk.
Used for demo runs where real portal traffic isn't available.

Usage:
    SPLUNK_BASE_URL=... SPLUNK_TOKEN=... python scripts/inject_fidelis_portal_demo_logs.py
"""
import asyncio, httpx, logging, os, time, json, random
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

SPLUNK_BASE  = os.getenv("SPLUNK_BASE_URL", "").rstrip("/")
SPLUNK_TOKEN = os.getenv("SPLUNK_TOKEN", "")
SPLUNK_INDEX = os.getenv("SPLUNK_INDEX", "main")

DEMO_EVENTS = [
    "ERROR fidelis-portal JDBC timeout after 30000ms — unable to acquire connection from pool",
    "ERROR fidelis-portal NullPointerException in PatientService.getAppointments()",
    "WARN  fidelis-portal Redis session eviction: cache miss for session-id=sess_a8f2c",
    "ERROR fidelis-portal Connection refused to ehr-api.internal:8080 after 3 retries",
    "ERROR fidelis-portal HTTP 503 from EHR API — downstream timeout",
    "WARN  fidelis-portal Response time 6.2s exceeds SLA threshold of 3s",
]

async def main(count: int = 20):
    if not (SPLUNK_BASE and SPLUNK_TOKEN):
        log.error("SPLUNK_BASE_URL and SPLUNK_TOKEN required"); return
    headers = {"Authorization": f"Bearer {SPLUNK_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(base_url=SPLUNK_BASE, headers=headers, verify=False, timeout=10) as c:
        for i in range(count):
            event = random.choice(DEMO_EVENTS)
            ts    = int(time.time()) - random.randint(0, 600)
            resp  = await c.post("/services/collector/event", json={
                "time": ts, "host": "portal-prod-01",
                "source": "fidelis-portal", "sourcetype": "java_app",
                "index": SPLUNK_INDEX, "event": event,
            })
            if resp.status_code == 200:
                log.info("[%d/%d] Injected: %s", i+1, count, event[:60])
            else:
                log.warning("  Failed [%d]: %s", resp.status_code, resp.text[:80])
    log.info("Injection complete.")

if __name__ == "__main__":
    asyncio.run(main(count=20))
