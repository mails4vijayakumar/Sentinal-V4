#!/usr/bin/env python3
"""
scripts/snow_poller.py
=======================
Polling fallback for environments where SNOW Business Rules cannot post
outbound webhooks (e.g., restricted SNOW instance, no internet egress from SNOW).

Polls the SNOW incident table every POLL_INTERVAL_S seconds for NEW incidents
and fires them at Agent 1 as synthetic webhooks.

Usage:
    SNOW_BASE_URL=... SNOW_USERNAME=... SNOW_PASSWORD=... python scripts/snow_poller.py
"""
import asyncio, hashlib, json, logging, os, time
from datetime import datetime, timedelta, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snow_poller")

SNOW_BASE      = os.getenv("SNOW_BASE_URL", "").rstrip("/")
SNOW_USER      = os.getenv("SNOW_USERNAME", "")
SNOW_PASS      = os.getenv("SNOW_PASSWORD", "")
AGENT1_URL     = os.getenv("AGENT1_URL", "http://localhost:8001")
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL_S", "30"))
LOOKBACK_MINS  = int(os.getenv("LOOKBACK_MINS", "5"))
PRIORITIES     = os.getenv("SNOW_POLL_PRIORITIES", "1,2,3,4,5").split(",")
_seen: set[str] = set()

async def poll_once(client: httpx.AsyncClient) -> None:
    since = (datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINS)).strftime("%Y-%m-%d %H:%M:%S")
    resp = await client.get(f"{SNOW_BASE}/api/now/table/incident", params={
        "sysparm_query": f"sys_created_on>={since}^priority^IN{','.join(PRIORITIES)}",
        "sysparm_fields": "number,priority,short_description,caller_id,cmdb_ci,state,category",
        "sysparm_limit": "20",
    })
    resp.raise_for_status()
    for rec in resp.json().get("result", []):
        number = rec["number"]
        if number in _seen:
            continue
        _seen.add(number)
        payload = {
            "number": number,
            "priority": rec.get("priority", {}).get("value", "4"),
            "short_description": rec.get("short_description", ""),
            "caller_id": rec.get("caller_id", {}).get("display_value", ""),
            "cmdb_ci": rec.get("cmdb_ci", {}).get("display_value", ""),
            "state": rec.get("state", {}).get("value", "1"),
            "category": rec.get("category", ""),
        }
        fwd = await client.post(f"{AGENT1_URL}/api/webhook/servicenow",
            json=payload, headers={"Content-Type": "application/json"})
        log.info("Forwarded %s → %s", number, fwd.status_code)

async def main():
    if not (SNOW_BASE and SNOW_USER):
        log.error("SNOW_BASE_URL and SNOW_USERNAME must be set"); return
    async with httpx.AsyncClient(
        base_url=SNOW_BASE,
        auth=(SNOW_USER, SNOW_PASS),
        timeout=15,
    ) as client:
        log.info("Snow poller started (interval=%ds)", POLL_INTERVAL)
        while True:
            try:
                await poll_once(client)
            except Exception as exc:
                log.warning("Poll error: %s", exc)
            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
