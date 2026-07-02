"""
agents/Agent-1-dynatrace/intake/alertmanager.py
================================================
Alertmanager webhook intake — accepts AM v4 payloads, routes per-alert.

Auth:       Bearer token (AM_WEBHOOK_TOKEN).  Empty env → 503 fail-closed.
Feature:    AM_INTAKE_ENABLED=false → 404.
Routing:    severity critical/warning → Flow A (PRIMARY);
            info/unknown   → Flow B (SECONDARY).
            sentinel_flow label overrides severity mapping.
Race guard: Phase 1 ingests all firing events; Phase 2 resolves same-batch
            flap fingerprints sequentially; Phase 3 resolves decoupled
            resolved events concurrently.

Adjustment #1 (SNOW client): _resolve() uses inline httpx.AsyncClient
    mirroring Agent 3's pattern — no shared get_snow_client() wrapper.
    Tests inject via `_snow_client_factory` module attribute.

Adjustment #2 (metrics): prometheus_client Counters are deferred.
    All metric increments are replaced with structured log lines prefixed
    `am_metric event=...` for future Prometheus/OTEL integration.
"""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Literal

import httpx
from fastapi import HTTPException, Request
from pydantic import BaseModel, ValidationError

from shared.models import (
    IncidentFlow,
    IncidentSource,
    OrchestratorEvent,
    SSEEvent,
    SSEEventType,
    Severity,
)
from shared.redis_client import get_redis
from shared.snow_auth import get_snow_token

log = logging.getLogger(__name__)

SNOW_BASE  = os.getenv("SNOW_BASE_URL", "").rstrip("/")
_INC_TABLE = "incident"

# Severity label → internal Severity
SEV_MAP: dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "warning":  Severity.HIGH,
    "info":     Severity.LOW,
}


# ── Pydantic models ───────────────────────────────────────────────────────────

class AMAlert(BaseModel):
    status:       Literal["firing", "resolved"]
    labels:       dict[str, str]
    annotations:  dict[str, str] = {}
    startsAt:     datetime
    endsAt:       datetime | None = None
    fingerprint:  str
    generatorURL: str | None = None


class AMPayload(BaseModel):
    version:     str          # tolerate non-"4"; warn but parse
    groupKey:    str
    status:      Literal["firing", "resolved"]
    receiver:    str
    externalURL: str
    alerts:      list[AMAlert]


# ── Auth ──────────────────────────────────────────────────────────────────────

def verify(authorization: str | None) -> None:
    token = os.getenv("AM_WEBHOOK_TOKEN", "")
    if not token:
        log.warning("am_metric event=token_missing reason=env_unset")
        raise HTTPException(status_code=503, detail="AM intake not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    presented = authorization[len("Bearer "):]
    if not hmac.compare_digest(presented, token):
        raise HTTPException(status_code=401, detail="Invalid AM token")


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse(body: bytes) -> AMPayload:
    try:
        payload = AMPayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if payload.version != "4":
        log.warning("am_metric event=unexpected_version received=%s", payload.version)
    return payload


# ── Event conversion ──────────────────────────────────────────────────────────

def _to_event(alert: AMAlert, group_key: str) -> OrchestratorEvent | None:
    labels = alert.labels
    if "alertname" not in labels:
        log.warning(
            "am_metric event=alerts_dropped reason=missing_alertname fingerprint=%s",
            alert.fingerprint,
        )
        return None

    raw_sev = labels.get("severity", "").lower()
    sev = SEV_MAP.get(raw_sev)
    if sev is None:
        log.debug(
            "am_unmapped_severity value=%s alertname=%s",
            raw_sev, labels["alertname"],
        )
        log.info(
            "am_metric event=severity_defaulted reason=unmapped_severity fingerprint=%s",
            alert.fingerprint,
        )
        sev = Severity.INFO

    sentinel_flow = labels.get("sentinel_flow", "").lower()
    if sentinel_flow in ("a", "b"):
        flow = IncidentFlow.PRIMARY if sentinel_flow == "a" else IncidentFlow.SECONDARY
    else:
        flow = (
            IncidentFlow.PRIMARY
            if sev in (Severity.CRITICAL, Severity.HIGH)
            else IncidentFlow.SECONDARY
        )

    log.info(
        "am_metric event=alerts_received status=%s fingerprint=%s flow=%s",
        alert.status, alert.fingerprint, flow.value,
    )

    return OrchestratorEvent(
        source       = IncidentSource.ALERTMANAGER,
        external_id  = f"am-{alert.fingerprint}",
        severity     = sev,
        flow         = flow,
        title        = labels["alertname"],
        host         = labels.get("instance"),
        service      = labels.get("service") or labels.get("job"),
        environment  = labels.get("environment", os.getenv("AM_DEFAULT_ENV", "prod")),
        am_group_key = group_key,
        raw_payload  = alert.model_dump(mode="json"),
        dedup_key    = f"am:{alert.fingerprint}",
    )


# ── Main handler ──────────────────────────────────────────────────────────────

async def handle(
    request:      Request,
    authorization: str | None,
    _ingest:      Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve_cb:  Callable[[str], Awaitable[dict]],
) -> dict:
    if os.getenv("AM_INTAKE_ENABLED", "true").lower() != "true":
        raise HTTPException(status_code=404, detail="AM intake disabled")

    verify(authorization)
    body = await request.body()
    payload = parse(body)

    firing:       list[OrchestratorEvent] = []
    resolved_fps: list[str]               = []
    dropped = 0

    for alert in payload.alerts:
        if alert.status == "resolved":
            resolved_fps.append(f"am-{alert.fingerprint}")
            continue
        event = _to_event(alert, payload.groupKey)
        if event is None:
            dropped += 1
            continue
        firing.append(event)

    firing_fps         = {e.external_id for e in firing}
    same_batch_flap    = [fp for fp in resolved_fps if fp     in firing_fps]
    decoupled_resolved = [fp for fp in resolved_fps if fp not in firing_fps]

    # Phase 1: ingest all firing events concurrently
    ingest_results = await asyncio.gather(
        *(_ingest(e) for e in firing), return_exceptions=True
    )

    # Phase 2: process same-batch flap resolves SEQUENTIALLY after ingest
    flap_results: list[Any] = []
    for fp in same_batch_flap:
        flap_results.append(await _resolve_cb(fp))

    # Phase 3: process decoupled resolves concurrently
    decoupled_results = await asyncio.gather(
        *(_resolve_cb(fp) for fp in decoupled_resolved),
        return_exceptions=True,
    )

    accepted   = sum(
        1 for r in ingest_results
        if isinstance(r, dict) and r.get("accepted")
    )
    dedup_n    = sum(
        1 for r in ingest_results
        if isinstance(r, dict) and r.get("deduplicated")
    )
    resolved_n = sum(
        1 for r in (*flap_results, *decoupled_results)
        if isinstance(r, dict) and r.get("resolved")
    )
    errors     = sum(
        1 for r in (*ingest_results, *flap_results, *decoupled_results)
        if isinstance(r, Exception)
    )

    log.info(
        "am_metric event=batch_done accepted=%d dedup=%d resolved=%d dropped=%d errors=%d",
        accepted, dedup_n, resolved_n, dropped, errors,
    )

    return {
        "accepted":     accepted,
        "deduplicated": dedup_n,
        "resolved":     resolved_n,
        "dropped":      dropped,
        "errors":       errors,
    }


# ── SNOW client factory (injectable for tests) ────────────────────────────────

@asynccontextmanager
async def _default_snow_client_factory(*args: Any, **kwargs: Any):
    """Default factory: builds an inline httpx.AsyncClient mirroring Agent 3."""
    token   = await get_snow_token()
    headers = {
        "Authorization": token,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }
    async with httpx.AsyncClient(base_url=SNOW_BASE, headers=headers, timeout=15) as c:
        yield c


# Module-level reference — tests override this attribute with monkeypatch
_snow_client_factory = _default_snow_client_factory


# ── Resolved handler ──────────────────────────────────────────────────────────

async def _resolve(external_id: str) -> dict:
    """
    Auto-close the SNOW INC bound to `external_id` when AM reports resolved.

    Pattern mirrors Agent 3: inline httpx.AsyncClient, same env vars,
    same graceful degradation when SNOW_BASE_URL is unset.
    """
    if not SNOW_BASE:
        log.warning("am_resolve_skipped reason=snow_disabled external_id=%s", external_id)
        return {"resolved": False, "reason": "snow_disabled"}

    redis = await get_redis()
    raw   = await redis._redis.get(f"snow_incident:{external_id}")
    if not raw:
        log.info("am_metric event=resolve outcome=no_binding external_id=%s", external_id)
        return {"resolved": False, "reason": "no_binding"}

    binding = json.loads(raw)

    async with _snow_client_factory() as c:
        # Guard: only close INCs that Alertmanager created
        resp = await c.get(
            f"/api/now/table/{_INC_TABLE}/{binding['incident_sys_id']}",
            params={"sysparm_fields": "u_source_tool"},
        )
        resp.raise_for_status()
        inc = resp.json().get("result", {})

        if inc.get("u_source_tool") != "Alertmanager":
            log.warning(
                "am_metric event=resolve outcome=source_mismatch "
                "source=%s external_id=%s",
                inc.get("u_source_tool"), external_id,
            )
            return {"resolved": False, "reason": "source_mismatch"}

        fp = external_id[3:]  # strip "am-"
        ts = datetime.now(timezone.utc).isoformat()
        patch_resp = await c.patch(
            f"/api/now/table/{_INC_TABLE}/{binding['incident_sys_id']}",
            json={
                "state":       "7",
                "close_code":  "Solved (Permanently)",
                "close_notes": (
                    f"Auto-closed by Sentinel — Alertmanager reported "
                    f"status=resolved at {ts}. fingerprint={fp}"
                ),
                "work_notes": _resolve_worknote(external_id),
            },
        )
        patch_resp.raise_for_status()

    # Clean up Redis bindings
    await redis._redis.delete(f"snow_incident:{external_id}")
    await redis._redis.delete(f"problem_id:{binding['incident_number']}")

    # SSE: pipeline_complete (PIPELINE_COMPLETE, not PIPELINE_RESOLVED)
    run_id = binding.get("run_id")
    sse = SSEEvent(
        event=SSEEventType.PIPELINE_COMPLETE,
        run_id=run_id,
        data={
            "external_id": external_id,
            "source":      "alertmanager",
            "outcome":     "auto_resolved",
        },
    )
    await redis.publish_event(sse.model_dump(mode="json"), run_id=run_id)

    log.info(
        "am_metric event=resolve outcome=success "
        "incident_number=%s external_id=%s",
        binding["incident_number"], external_id,
    )

    return {"resolved": True, "incident_number": binding["incident_number"]}


def _resolve_worknote(external_id: str) -> str:
    fp = external_id[3:]  # strip "am-"
    ts = datetime.now(timezone.utc).isoformat() + "Z"
    return (
        "=== RESOLUTION — Alertmanager ===\n"
        f"Timestamp : {ts}\n"
        "Status    : auto-closed\n"
        f"Source    : alertmanager fingerprint={fp}\n"
        "Note      : External monitoring reported condition cleared.\n"
    )
