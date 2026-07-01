"""SNOW webhook intake — extracted verbatim from main.py during PR1 refactor."""
from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import HTTPException, Request
from pydantic import ValidationError

from .._common import (
    SNOW_SECRET,
    IncidentFlow,
    IncidentSource,
    OrchestratorEvent,
    ServiceNowPayload,
    Severity,
    _SNOW_PRIORITY_MAP,
    verify_hmac_signature,
)


def verify(header_value: str | None, body: bytes) -> None:
    if SNOW_SECRET:
        if not header_value or not verify_hmac_signature(body, header_value, SNOW_SECRET):
            raise HTTPException(status_code=401, detail="Invalid SNOW signature")


def parse(body: bytes) -> ServiceNowPayload:
    try:
        return ServiceNowPayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def to_events(payload: ServiceNowPayload) -> list[OrchestratorEvent]:
    severity = _SNOW_PRIORITY_MAP.get(str(payload.priority), Severity.INFO)
    flow     = IncidentFlow.PRIMARY if severity in (
        Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM
    ) else IncidentFlow.SECONDARY

    return [OrchestratorEvent(
        source=IncidentSource.SERVICENOW,
        external_id=payload.number,
        severity=severity,
        flow=flow,
        title=payload.short_description,
        raw_payload=payload.model_dump(),
        host=payload.cmdb_ci,
        service=payload.cmdb_ci,
        dedup_key=f"snow:{payload.number}",
    )]


async def handle(
    request: Request,
    header_value: str | None,
    _ingest: Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve: Callable[[str], Awaitable[dict]] | None = None,
) -> dict:
    body = await request.body()
    verify(header_value, body)
    payload = parse(body)
    events = to_events(payload)
    return await _ingest(events[0])
