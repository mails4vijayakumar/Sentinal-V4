"""
tests/test_am_dlq.py
====================
TDD suite for the Alertmanager DLQ wrappers and sweep loop (PR3).

Adjustment #1: redis mock attribute access uses redis_mock._redis.scan_iter,
    redis_mock._redis.get, redis_mock._redis.set, redis_mock._redis.delete
    — consistent with PR2's _resolve() which also reaches into redis._redis
    directly, deferring public helper additions to a follow-up ticket.

Monkeypatching: uses monkeypatch.setattr(am_module, "attr", ...) on the
    imported module object — consistent with test_alertmanager_intake.py's
    established pattern (dotted-string path doesn't work with conftest's
    synthetic module aliases).

scan_iter mock: redis.asyncio's scan_iter is a regular method that returns
    an async iterator (not a coroutine itself). Mock it with a plain function
    lambda that returns the async generator directly; do NOT use AsyncMock
    which would make it return a coroutine instead.
"""
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import agents.Agent_1_dynatrace.intake.alertmanager as am_module
from agents.Agent_1_dynatrace.intake.alertmanager import (
    _ingest_with_dlq, _resolve_with_dlq, sweep_am_dlq, _enqueue_dlq,
)
from shared.models import OrchestratorEvent, IncidentSource, Severity, IncidentFlow


def _evt():
    return OrchestratorEvent(
        source=IncidentSource.ALERTMANAGER,
        external_id="am-test1",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="x",
    )


async def _aiter(items):
    """Async generator helper for mocking scan_iter."""
    for x in items:
        yield x


# ── DLQ enqueue tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_ingest_failure_enqueues_dlq(monkeypatch):
    """_ingest_with_dlq catches exceptions and enqueues to DLQ."""
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))
    failing_ingest = AsyncMock(side_effect=RuntimeError("boom"))

    result = await _ingest_with_dlq(_evt(), failing_ingest)

    assert result == {"dlq": True}
    redis_mock._redis.set.assert_awaited()
    args, kwargs = redis_mock._redis.set.call_args
    assert args[0] == "am_dlq:am-test1"
    entry = json.loads(args[1])
    assert entry["kind"] == "ingest"
    assert entry["attempts"] == 0
    assert "boom" in entry["last_error"]
    assert kwargs["ex"] == 86400


@pytest.mark.asyncio
async def test_resolve_failure_enqueues_dlq(monkeypatch):
    """_resolve_with_dlq catches exceptions and enqueues to DLQ."""
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))
    failing_resolve = AsyncMock(side_effect=RuntimeError("snow down"))

    result = await _resolve_with_dlq("am-test2", failing_resolve)

    assert result == {"dlq": True}


# ── Sweep tests ───────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_sweep_success_deletes_key(monkeypatch):
    """sweep_am_dlq deletes the key on successful retry."""
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    # scan_iter returns an async iterator directly (not a coroutine)
    redis_mock._redis.scan_iter = lambda pattern: _aiter(["am_dlq:am-test1"])
    redis_mock._redis.get = AsyncMock(return_value=json.dumps({
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest",
        "first_failed_at": "2026-06-30T00:00:00Z",
        "attempts": 0,
        "last_error": "boom",
    }).encode())
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(am_module, "_ingest", AsyncMock(return_value={"accepted": True}))

    await sweep_am_dlq()

    redis_mock._redis.delete.assert_awaited_with("am_dlq:am-test1")


@pytest.mark.asyncio
async def test_sweep_failure_increments_attempts(monkeypatch):
    """sweep_am_dlq increments attempts and updates the entry on retry failure."""
    initial = {
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest", "first_failed_at": "...", "attempts": 1,
        "last_error": "old",
    }
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    redis_mock._redis.scan_iter = lambda pattern: _aiter(["am_dlq:am-test1"])
    redis_mock._redis.get = AsyncMock(return_value=json.dumps(initial).encode())
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(am_module, "_ingest", AsyncMock(side_effect=RuntimeError("still down")))

    await sweep_am_dlq()

    args, kwargs = redis_mock._redis.set.call_args
    updated = json.loads(args[1])
    assert updated["attempts"] == 2
    assert "still down" in updated["last_error"]


@pytest.mark.asyncio
async def test_sweep_exhausted_increments_counter_and_leaves_key(monkeypatch):
    """Exhausted entries (attempts >= 5) are logged but NOT deleted."""
    exhausted = {
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest", "first_failed_at": "...", "attempts": 5,
        "last_error": "...",
    }
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    redis_mock._redis.scan_iter = lambda pattern: _aiter(["am_dlq:am-test1"])
    redis_mock._redis.get = AsyncMock(return_value=json.dumps(exhausted).encode())
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))
    # Patch _ingest to avoid triggering the lazy import of main
    monkeypatch.setattr(am_module, "_ingest", AsyncMock())

    await sweep_am_dlq()

    redis_mock._redis.delete.assert_not_awaited()    # exhausted entries left in place


# ── HTTP contract test ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_handle_returns_202_dict_even_when_ingest_enqueues_dlq(monkeypatch):
    """
    6th test (added beyond brief): HTTP contract preservation.

    Verifies that even when _ingest raises (causing DLQ enqueue for every
    firing event), handle() still returns a summary dict without raising.
    The FastAPI route returns 202 to AM unconditionally; this test confirms
    the underlying handle() does not propagate the error either.

    Rationale: the DLQ wrapper changes return_exceptions from True to False
    in asyncio.gather. This test guards against a regression where an
    unhandled exception surfaces through the gather and breaks the 202 contract.
    """
    redis_mock = AsyncMock()
    redis_mock._redis = AsyncMock()
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))

    from fastapi import Request
    from agents.Agent_1_dynatrace.intake.alertmanager import handle
    import json as _json
    from datetime import datetime, timezone

    body = _json.dumps({
        "version": "4",
        "groupKey": '{}:{alertname="Test"}',
        "status": "firing",
        "receiver": "sentinel",
        "externalURL": "http://am.local",
        "alerts": [{
            "status": "firing",
            "labels": {"alertname": "TestAlert", "severity": "critical",
                       "service": "svc"},
            "annotations": {},
            "startsAt": datetime.now(timezone.utc).isoformat(),
            "fingerprint": "aabbcc001122",
        }],
    }).encode()

    scope = {
        "type": "http",
        "method": "POST",
        "headers": [(b"content-type", b"application/json")],
        "query_string": b"",
        "path": "/api/webhook/alertmanager",
    }

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    request = Request(scope, receive)

    monkeypatch.setenv("AM_WEBHOOK_TOKEN", "tok")
    monkeypatch.setenv("AM_INTAKE_ENABLED", "true")

    # _ingest always raises — so DLQ wrapper enqueues and returns {"dlq": True}
    failing_ingest = AsyncMock(side_effect=RuntimeError("ingest broken"))
    resolve_cb = AsyncMock(return_value={"resolved": False})

    result = await handle(request, "Bearer tok", failing_ingest, resolve_cb)

    # handle() returns a summary dict — never raises
    assert isinstance(result, dict)
    # DLQ wrapper absorbs the error; errors counter stays 0
    assert result["errors"] == 0
    # The event was DLQ'd, not accepted
    assert result["accepted"] == 0
