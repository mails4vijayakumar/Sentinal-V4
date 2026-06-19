#!/usr/bin/env python3
"""
scripts/verify_snow_schema.py
==============================
Pre-go-live check: confirms all custom u_* fields used by Agent 3 exist on the
SNOW incident table. Run this before first deployment.

Usage:
    python scripts/verify_snow_schema.py
"""
import httpx, os, sys, logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

SNOW_BASE = os.getenv("SNOW_BASE_URL", "").rstrip("/")
SNOW_USER = os.getenv("SNOW_USERNAME", "")
SNOW_PASS = os.getenv("SNOW_PASSWORD", "")

REQUIRED_FIELDS = [
    "number", "priority", "state", "short_description", "description",
    "category", "subcategory", "caller_id", "cmdb_ci",
    "assignment_group", "assigned_to", "impact", "urgency",
    "work_notes", "close_code", "close_notes",
    # Custom Sentinel fields (optional — verify they exist)
    "u_service_tier", "u_hipaa_flag",
]

def main():
    if not (SNOW_BASE and SNOW_USER):
        log.error("Set SNOW_BASE_URL and SNOW_USERNAME"); sys.exit(1)
    with httpx.Client(base_url=SNOW_BASE, auth=(SNOW_USER, SNOW_PASS), timeout=15) as c:
        resp = c.get("/api/now/table/incident", params={"sysparm_limit": "1"})
        resp.raise_for_status()
        if not resp.json().get("result"):
            log.error("Could not fetch incident table"); sys.exit(1)
        sample = resp.json()["result"][0]

    missing = [f for f in REQUIRED_FIELDS if f not in sample]
    present = [f for f in REQUIRED_FIELDS if f in sample]

    log.info("SNOW schema check:")
    for f in present: log.info("  ✓ %s", f)
    for f in missing: log.warning("  ✗ MISSING: %s", f)

    if any(f.startswith("u_") for f in missing):
        log.warning("\nSome custom u_* fields are missing. Ask your SNOW admin to add them.")
    elif not missing:
        log.info("\n✓ All required fields present. Ready for Agent 3.")
    sys.exit(0 if not missing else 2)

if __name__ == "__main__":
    main()
