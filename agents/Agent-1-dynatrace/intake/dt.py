"""DT webhook intake — extracted verbatim from main.py during PR1 refactor."""
from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import HTTPException, Request
from pydantic import ValidationError

from .._common import (
    DT_SECRET,
    DynatracePayload,
    IncidentFlow,
    IncidentSource,
    OrchestratorEvent,
    Severity,
    _DT_SEVERITY_MAP,
    verify_hmac_signature,
)


def verify(header_value: str | None, body: bytes) -> None:
    if DT_SECRET:
        if not header_value or not verify_hmac_signature(body, header_value, DT_SECRET):
            raise HTTPException(status_code=401, detail="Invalid DT signature")


def parse(body: bytes) -> DynatracePayload:
    try:
        return DynatracePayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def to_events(payload: DynatracePayload) -> list[OrchestratorEvent]:
    severity = _DT_SEVERITY_MAP.get(payload.severity.upper(), Severity.INFO)
    flow     = IncidentFlow.PRIMARY if severity in (
        Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM
    ) else IncidentFlow.SECONDARY

    host    = payload.impactedEntities[0]["name"] if payload.impactedEntities else None
    service = next(
        (t.split(":", 1)[1] for t in payload.tags if t.startswith("app:")),
        None,
    )

    return [OrchestratorEvent(
        source=IncidentSource.DYNATRACE,
        external_id=payload.problemId,
        severity=severity,
        flow=flow,
        title=payload.displayName,
        raw_payload=payload.model_dump(),
        host=host,
        service=service,
        dedup_key=f"dt:{payload.problemId}",
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

    if payload.status.upper() == "RESOLVED":
        return {"accepted": False, "reason": "RESOLVED events are ignored"}

    events = to_events(payload)
    # DT is always single-alert; keep the dispatch as direct call to preserve
    # current return shape (the dict from _ingest, not a list).
    return await _ingest(events[0])
