"""
shared/dt_client.py
===================
Dynatrace entity resolution via the Entities API.

Stub for future DT Entities API name lookup — see 2026-06-30 AM intake spec §8.
"""
from __future__ import annotations

from typing import Optional


async def resolve_entity_by_name(name: str) -> Optional[str]:
    """
    Stub for DT Entities API name lookup — see 2026-06-30 AM intake spec §8.

    Args:
        name: Entity name (e.g. 'checkout' service).

    Returns:
        Entity ID if found, else None.
    """
    return None
