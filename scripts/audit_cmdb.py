#!/usr/bin/env python3
"""
scripts/audit_cmdb.py
======================
Pre-go-live: SNOW CI gap report for the entity catalogue.
Checks whether Dynatrace-monitored services have matching CMDB records.

Usage:
    python scripts/audit_cmdb.py --dt-base https://xxx.live.dynatrace.com --dt-token dt0c01....
"""
import argparse, asyncio, httpx, json, logging, os, sys
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

async def main(args):
    dt_headers  = {"Authorization": f"Api-Token {args.dt_token}"}
    snow_auth   = (os.getenv("SNOW_USERNAME",""), os.getenv("SNOW_PASSWORD",""))

    async with httpx.AsyncClient(timeout=20) as c:
        # Fetch DT services
        resp = await c.get(f"{args.dt_base}/api/v2/entities",
            headers=dt_headers,
            params={"entitySelector": "type(SERVICE)", "fields": "displayName,tags", "pageSize": 500})
        resp.raise_for_status()
        dt_services = {e["displayName"]: e for e in resp.json().get("entities", [])}

        # Check each against SNOW CMDB
        found = missing = 0
        for svc_name, entity in dt_services.items():
            snow_resp = await c.get(f"{os.getenv('SNOW_BASE_URL','')}/api/now/table/cmdb_ci_service",
                auth=snow_auth,
                params={"sysparm_query": f"name={svc_name}", "sysparm_limit": 1})
            if snow_resp.status_code == 200 and snow_resp.json().get("result"):
                found += 1
                log.info("  ✓ %s", svc_name)
            else:
                missing += 1
                log.warning("  ✗ NOT IN CMDB: %s", svc_name)

    log.info("\nSummary: %d found, %d missing in CMDB", found, missing)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dt-base",  default=os.getenv("DT_BASE_URL",""))
    p.add_argument("--dt-token", default=os.getenv("DT_API_TOKEN",""))
    asyncio.run(main(p.parse_args()))
