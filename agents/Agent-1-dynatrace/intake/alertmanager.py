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
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Literal, Optional

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

    # Phase 1: ingest all firing events concurrently.
    # return_exceptions=False is safe — _ingest_with_dlq catches and enqueues.
    ingest_results = await asyncio.gather(
        *(_ingest_with_dlq(e, _ingest) for e in firing), return_exceptions=False,
    )

    # Phase 2: process same-batch flap resolves SEQUENTIALLY after ingest
    flap_results: list[Any] = []
    for fp in same_batch_flap:
        flap_results.append(await _resolve_with_dlq(fp, _resolve_cb))

    # Phase 3: process decoupled resolves concurrently.
    # return_exceptions=False is safe — _resolve_with_dlq catches and enqueues.
    decoupled_results = await asyncio.gather(
        *(_resolve_with_dlq(fp, _resolve_cb) for fp in decoupled_resolved),
        return_exceptions=False,
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


# ── Dead-Letter Queue (DLQ) ───────────────────────────────────────────────────
#
# Redis-backed retry envelope for AM dispatch failures.
# Keys: am_dlq:{external_id}  TTL: 24 h
# Sweep cadence: 5 min (loop runs in main.py lifespan via _sweep_loop).
# Max retries: 5. Exhausted entries are left in place until TTL expiry so
# operators can inspect them; they are logged at ERROR level.
#
# Adjustment #1 (private Redis API): uses redis._redis.{set,get,delete,scan_iter}
# directly — consistent with PR2's _resolve() pattern. Public helpers
# (.get/.set/.delete/.scan_iter) will be added to RedisClient in a follow-up
# ticket and both _resolve() and this module will be updated together.

DLQ_TTL_SECONDS = 86400   # 24 h
DLQ_MAX_ATTEMPTS = 5

# Module-level reference to the ingest function. None in production until
# sweep_am_dlq() resolves it via lazy import; monkeypatched in tests.
_ingest: Optional[Callable] = None


async def _enqueue_dlq(external_id: str, payload: dict, kind: str, error: Exception) -> None:
    """
    Persist a failed dispatch to the Redis DLQ for later retry.

    Stores expires_at on first creation (only when the key doesn't already exist)
    to enforce 24h TTL from creation, not from last retry.
    """
    redis = await get_redis()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(seconds=DLQ_TTL_SECONDS)
    entry = {
        "event":           payload,
        "kind":            kind,
        "first_failed_at": now.isoformat(),
        "expires_at":      expires_at.isoformat(),
        "attempts":        0,
        "last_error":      str(error)[:500],
    }
    await redis._redis.set(
        f"am_dlq:{external_id}",
        json.dumps(entry),
        ex=DLQ_TTL_SECONDS,
    )
    log.info(
        "am_metric event=dlq_enqueued kind=%s external_id=%s",
        kind, external_id,
    )


async def _ingest_with_dlq(event: OrchestratorEvent, ingest_fn: Callable) -> dict:
    """
    Call ingest_fn(event). On failure, enqueue to DLQ and return {"dlq": True}.
    Ensures asyncio.gather(..., return_exceptions=False) never sees a raised
    exception from this wrapper — the 202 contract with AM is preserved.
    """
    try:
        return await ingest_fn(event)
    except Exception as exc:
        try:
            await _enqueue_dlq(
                event.external_id,
                event.model_dump(mode="json"),
                "ingest",
                exc,
            )
        except Exception as dlq_exc:
            log.error(
                "am_metric event=dlq_enqueue_failed external_id=%s error=%s",
                event.external_id, dlq_exc,
            )
        log.error(
            "am_dlq_enqueued external_id=%s kind=ingest error=%s",
            event.external_id, exc,
        )
        return {"dlq": True}


async def _resolve_with_dlq(external_id: str, resolve_fn: Callable) -> dict:
    """
    Call resolve_fn(external_id). On failure, enqueue to DLQ and return {"dlq": True}.
    """
    try:
        return await resolve_fn(external_id)
    except Exception as exc:
        try:
            await _enqueue_dlq(
                external_id,
                {"external_id": external_id},
                "resolve",
                exc,
            )
        except Exception as dlq_exc:
            log.error(
                "am_metric event=dlq_enqueue_failed external_id=%s error=%s",
                external_id, dlq_exc,
            )
        log.error(
            "am_dlq_enqueued external_id=%s kind=resolve error=%s",
            external_id, exc,
        )
        return {"dlq": True}


async def sweep_am_dlq() -> None:
    """
    Scan Redis for am_dlq:* keys and retry each entry.

    - Entries with attempts >= DLQ_MAX_ATTEMPTS are logged at ERROR and left
      in place (Adjustment #4 — exhausted entries kept for operator inspection;
      24h TTL handles cleanup).
    - Successful retries: key deleted.
    - Failed retries: attempts incremented, updated entry re-written with
      remaining TTL (DLQ_TTL_SECONDS reset — 24h from now).

    Called by _sweep_loop() in main.py every 5 minutes (Adjustment #3).
    The lazy import of the production _ingest function avoids a circular
    import at module-load time (intake.alertmanager ↔ main).
    """
    global _ingest
    # Resolve the ingest function: use the module-level override (test monkeypatch)
    # or fall back to the lazy import from main (production).
    ingest_fn = _ingest
    if ingest_fn is None:
        from main import _ingest as _main_ingest  # lazy: avoids circular import at load
        ingest_fn = _main_ingest

    redis = await get_redis()
    async for key in redis._redis.scan_iter("am_dlq:*"):
        raw = await redis._redis.get(key)
        if not raw:
            continue
        entry = json.loads(raw)

        if entry["attempts"] >= DLQ_MAX_ATTEMPTS:
            log.error(
                "am_metric event=dlq_exhausted key=%s last_error=%s",
                key, entry["last_error"],
            )
            # Key left in place — TTL (24h) will clean up (Adjustment #4).
            continue

        try:
            if entry["kind"] == "ingest":
                await ingest_fn(OrchestratorEvent(**entry["event"]))
            else:
                await _resolve(entry["event"]["external_id"])
            await redis._redis.delete(key)
            log.info(
                "am_metric event=dlq_swept outcome=success external_id=%s",
                entry.get("event", {}).get("external_id", key),
            )
        except Exception as exc:
            entry["attempts"] += 1
            entry["last_error"] = str(exc)[:500]
            # Compute remaining TTL from persisted expires_at (24h-from-creation semantic)
            try:
                expires_at = datetime.fromisoformat(entry["expires_at"])
                remaining_seconds = max(1, int((expires_at - datetime.now(timezone.utc)).total_seconds()))
            except (KeyError, ValueError):
                # Legacy entry (pre-fix) — assume 24h from now as a bounded migration path
                log.debug("am_legacy_dlq_entry key=%s missing_or_invalid_expires_at", key)
                remaining_seconds = DLQ_TTL_SECONDS
            await redis._redis.set(key, json.dumps(entry), ex=remaining_seconds)
            log.warning(
                "am_dlq_retry_failed key=%s attempts=%d error=%s",
                key, entry["attempts"], exc,
            )
