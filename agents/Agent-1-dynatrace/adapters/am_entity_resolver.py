"""Hybrid AM-label → DT-entity resolver with negative cache."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

from shared.redis_client import get_redis
from shared.routing_client import get_routing_client
from shared import dt_client

NEGATIVE_CACHE_TTL = 600  # seconds; tune from am_entity_resolved_total{source="unresolved"}


class EntityRef(BaseModel):
    entity_id:   Optional[str] = None
    entity_name: Optional[str] = None
    source:      Literal["label", "routing-db", "dt-name", "unresolved", "unresolved-cached"]


async def resolve(labels: dict[str, str]) -> EntityRef:
    if eid := labels.get("dt_entity_id"):
        return EntityRef(entity_id=eid, source="label")

    key = labels.get("service") or labels.get("job")
    name = labels.get("service") or labels.get("instance") or labels.get("job")

    redis = await get_redis()

    if key:
        if await redis.get(f"am_entity_miss:{key}"):
            return EntityRef(entity_name=name, source="unresolved-cached")

        rc = get_routing_client()
        if mapped := await rc.get_am_entity(key):
            return EntityRef(entity_id=mapped.entity_id, source="routing-db")

    if name and (eid := await dt_client.resolve_entity_by_name(name)):
        return EntityRef(entity_id=eid, entity_name=name, source="dt-name")

    if key:
        await redis.set(f"am_entity_miss:{key}", "1", ex=NEGATIVE_CACHE_TTL)
    return EntityRef(entity_name=name, source="unresolved")
