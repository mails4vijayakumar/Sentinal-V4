#!/usr/bin/env python3
"""
scripts/create_confluence_runbooks.py
======================================
Seed sample runbook pages in Confluence.
Useful for demo environments when no real Confluence KB exists.
Creates pages in CONFLUENCE_SPACE_KEY (default: RUNBOOKS).

Usage:
    CONFLUENCE_BASE_URL=... CONFLUENCE_TOKEN=... python scripts/create_confluence_runbooks.py
"""
import asyncio, httpx, logging, os
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CONF_BASE  = os.getenv("CONFLUENCE_BASE_URL", "").rstrip("/")
CONF_TOKEN = os.getenv("CONFLUENCE_TOKEN", "")
SPACE_KEY  = os.getenv("CONFLUENCE_SPACE_KEY", "RUNBOOKS")

RUNBOOKS = [
    ("EHR High Memory", "Heap > 85% — Steps: 1. POST /admin/gc 2. Check sessions 3. Rolling restart"),
    ("DB Connection Pool Exhaustion", "Check pgbouncer SHOW POOLS. Kill long-running queries. Raise pool_size."),
    ("HL7 Interface Restart", "Notify clinical informatics. Drain channels. systemctl restart mirth."),
    ("PACS Recovery", "Check Orthanc service. Verify NFS mount. Test DICOM port 4242."),
    ("Kubernetes OOMKilled", "kubectl describe pod. Review limits. Increase memory 25%. File Jira."),
]

async def main():
    if not (CONF_BASE and CONF_TOKEN):
        log.error("CONFLUENCE_BASE_URL and CONFLUENCE_TOKEN required"); return
    headers = {"Authorization": f"Bearer {CONF_TOKEN}", "Content-Type": "application/json"}
    async with httpx.AsyncClient(base_url=CONF_BASE, headers=headers, timeout=15) as c:
        for title, body in RUNBOOKS:
            resp = await c.post("/rest/api/content", json={
                "type": "page", "title": title,
                "space": {"key": SPACE_KEY},
                "body": {"storage": {"value": f"<p>{body}</p>", "representation": "storage"}},
            })
            if resp.status_code in (200, 201):
                log.info("✓ Created: %s", title)
            elif resp.status_code == 400 and "title" in resp.text:
                log.info("  Already exists: %s", title)
            else:
                log.warning("  Failed [%d]: %s", resp.status_code, title)

if __name__ == "__main__":
    asyncio.run(main())
