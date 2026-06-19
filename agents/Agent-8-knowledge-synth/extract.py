from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger("agent8.extract")

REQUIRED_FIELDS = (
    "number,short_description,description,close_notes,close_code,"
    "assignment_group,category,subcategory,closed_at,u_source_tool"
)


async def snow_extract_closed(
    *,
    client: httpx.AsyncClient,
    window_start: date,
    window_end: date,
    page_size: int,
    access_token: str,
) -> list[dict[str, Any]]:
    """Page through the SNOW incident table for closed incidents in the window."""
    results: list[dict[str, Any]] = []
    offset = 0
    query = (
        f"state=7"
        f"^closed_at>=javascript:gs.dateGenerate('{window_start.isoformat()}','00:00:00')"
        f"^closed_at<=javascript:gs.dateGenerate('{window_end.isoformat()}','23:59:59')"
    )
    while True:
        resp = await client.get(
            "/api/now/table/incident",
            params={
                "sysparm_query": query,
                "sysparm_fields": REQUIRED_FIELDS,
                "sysparm_limit": page_size,
                "sysparm_offset": offset,
            },
            headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
        )
        resp.raise_for_status()
        batch = resp.json().get("result", [])
        if not batch:
            break
        results.extend(batch)
        logger.info("snow_extract_page", extra={"offset": offset, "batch_size": len(batch)})
        offset += page_size
    return results
