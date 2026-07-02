"""
tests/test_alertmanager_intake.py
==================================
TDD suite for the Alertmanager webhook intake module.

Adjustment #1:  _resolve() uses inline httpx.AsyncClient (mirrors Agent 3).
                Tests that need to exercise _resolve with a SNOW call inject
                a fake httpx.AsyncClient via the `httpx_client_factory` param.

Adjustment #4:  Weak-assertion tests are STRENGTHENED — each patches main._ingest
                with an AsyncMock and asserts on call_args so the flow/severity
                routing is actually verified, not just the HTTP status code.
"""
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import asynccontextmanager

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# We import app from main — the conftest sys.path aliasing covers this.
from agents.Agent_1_dynatrace.main import app
from fastapi.testclient import TestClient

AM_TOKEN = "test-am-token"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AM_WEBHOOK_TOKEN", AM_TOKEN)
    monkeypatch.setenv("AM_INTAKE_ENABLED", "true")
    monkeypatch.setenv("AM_DEFAULT_ENV", "prod")


@pytest.fixture
def client(mock_redis, mock_routing):
    """TestClient with mocked Redis + routing to avoid real connections."""
    return TestClient(app)


# ── Payload helpers ───────────────────────────────────────────────────────────

def _am_payload(alerts):
    return {
        "version": "4",
        "groupKey": '{}:{alertname="HighCPU"}',
        "status": "firing",
        "receiver": "sentinel",
        "externalURL": "http://am.local",
        "alerts": alerts,
    }


def _alert(status="firing", alertname="HighCPU", severity="critical",
           service="checkout", fingerprint="deadbeef01", labels_extra=None):
    return {
        "status": status,
        "labels": {"alertname": alertname, "severity": severity,
                   "service": service, **(labels_extra or {})},
        "annotations": {"summary": "High CPU"},
        "startsAt": datetime.now(timezone.utc).isoformat(),
        "fingerprint": fingerprint,
    }


# ── Auth tests ────────────────────────────────────────────────────────────────

def test_auth_missing_header_returns_401(client):
    r = client.post("/api/webhook/alertmanager",
                    json=_am_payload([_alert()]))
    assert r.status_code == 401


def test_auth_wrong_token_returns_401(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": "Bearer wrong"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 401


def test_auth_empty_env_returns_503(client, monkeypatch):
    monkeypatch.setenv("AM_WEBHOOK_TOKEN", "")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 503


def test_feature_flag_off_returns_404(client, monkeypatch):
    monkeypatch.setenv("AM_INTAKE_ENABLED", "false")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 404


# ── Happy-path tests ──────────────────────────────────────────────────────────

def test_valid_v4_payload_returns_202(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 202
    body = r.json()
    assert body["accepted"] >= 1


def test_unexpected_version_proceeds_with_warning(client, caplog):
    import logging
    caplog.set_level(logging.WARNING)
    payload = _am_payload([_alert()])
    payload["version"] = "5"
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=payload)
    assert r.status_code == 202
    # parse() emits: "am_metric event=unexpected_version received=5"
    assert any("unexpected_version" in m for m in caplog.messages)


def test_malformed_body_returns_422(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}",
                             "Content-Type": "application/json"},
                    content=b"{not json")
    assert r.status_code == 422


def test_alert_missing_alertname_is_dropped_rest_proceed(client):
    bad = _alert(fingerprint="bad")
    bad["labels"].pop("alertname")
    good = _alert(fingerprint="good")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([bad, good]))
    assert r.status_code == 202
    body = r.json()
    assert body["dropped"] == 1
    assert body["accepted"] == 1


# ── Routing tests (Adjustment #4: strengthened with _ingest mock) ─────────────

def test_severity_critical_routes_to_flow_a(mock_redis, mock_routing, monkeypatch):
    """Critical severity → IncidentFlow.PRIMARY."""
    from agents.Agent_1_dynatrace import main as agent1_main
    from shared.models import IncidentFlow

    captured = []

    async def fake_ingest(event):
        captured.append(event)
        return {"accepted": True, "run_id": "r1", "external_id": event.external_id,
                "severity": event.severity.value, "flow": event.flow.value}

    monkeypatch.setattr(agent1_main, "_ingest", fake_ingest)

    with TestClient(app) as c:
        r = c.post("/api/webhook/alertmanager",
                   headers={"Authorization": f"Bearer {AM_TOKEN}"},
                   json=_am_payload([_alert(severity="critical")]))
    assert r.status_code == 202
    assert len(captured) == 1
    assert captured[0].flow == IncidentFlow.PRIMARY


def test_severity_info_routes_to_flow_b(mock_redis, mock_routing, monkeypatch):
    """Info severity → IncidentFlow.SECONDARY."""
    from agents.Agent_1_dynatrace import main as agent1_main
    from shared.models import IncidentFlow

    captured = []

    async def fake_ingest(event):
        captured.append(event)
        return {"accepted": True, "run_id": "r1", "external_id": event.external_id,
                "severity": event.severity.value, "flow": event.flow.value}

    monkeypatch.setattr(agent1_main, "_ingest", fake_ingest)

    with TestClient(app) as c:
        r = c.post("/api/webhook/alertmanager",
                   headers={"Authorization": f"Bearer {AM_TOKEN}"},
                   json=_am_payload([_alert(severity="info")]))
    assert r.status_code == 202
    assert len(captured) == 1
    assert captured[0].flow == IncidentFlow.SECONDARY


def test_sentinel_flow_label_overrides_severity(mock_redis, mock_routing, monkeypatch):
    """sentinel_flow=a label forces PRIMARY even when severity=info."""
    from agents.Agent_1_dynatrace import main as agent1_main
    from shared.models import IncidentFlow

    captured = []

    async def fake_ingest(event):
        captured.append(event)
        return {"accepted": True, "run_id": "r1", "external_id": event.external_id,
                "severity": event.severity.value, "flow": event.flow.value}

    monkeypatch.setattr(agent1_main, "_ingest", fake_ingest)

    with TestClient(app) as c:
        r = c.post("/api/webhook/alertmanager",
                   headers={"Authorization": f"Bearer {AM_TOKEN}"},
                   json=_am_payload([_alert(
                       severity="info",
                       labels_extra={"sentinel_flow": "a"},
                   )]))
    assert r.status_code == 202
    assert len(captured) == 1
    assert captured[0].flow == IncidentFlow.PRIMARY


def test_unmapped_severity_falls_to_info_with_debug_log(client, caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(severity="emergency")]))
    assert r.status_code == 202
    assert any("am_unmapped_severity" in m for m in caplog.messages)


def test_fingerprint_becomes_external_id(mock_redis, mock_routing, monkeypatch):
    """fingerprint=abc123 → OrchestratorEvent.external_id='am-abc123'."""
    from agents.Agent_1_dynatrace import main as agent1_main

    captured = []

    async def fake_ingest(event):
        captured.append(event)
        return {"accepted": True, "run_id": "r1", "external_id": event.external_id,
                "severity": event.severity.value, "flow": event.flow.value}

    monkeypatch.setattr(agent1_main, "_ingest", fake_ingest)

    with TestClient(app) as c:
        r = c.post("/api/webhook/alertmanager",
                   headers={"Authorization": f"Bearer {AM_TOKEN}"},
                   json=_am_payload([_alert(fingerprint="abc123")]))
    assert r.status_code == 202
    assert len(captured) == 1
    assert captured[0].external_id == "am-abc123"


# ── Race-guard test ───────────────────────────────────────────────────────────

def test_same_fingerprint_firing_and_resolved_processed_sequentially(
    mock_redis, mock_routing, monkeypatch
):
    """
    Race guard: resolved for same fingerprint as a firing alert in one batch
    must wait until the firing ingest is complete before _resolve is called.

    We verify call order by recording an ordered log and asserting
    the ingest entry appears before the resolve entry for flap-1.
    """
    from agents.Agent_1_dynatrace import main as agent1_main
    import agents.Agent_1_dynatrace.intake.alertmanager as am_module

    call_order = []

    async def fake_ingest(event):
        call_order.append(("ingest", event.external_id))
        return {"accepted": True, "run_id": "r1", "external_id": event.external_id,
                "severity": event.severity.value, "flow": event.flow.value}

    async def fake_resolve(external_id):
        call_order.append(("resolve", external_id))
        return {"resolved": False, "reason": "no_binding"}

    monkeypatch.setattr(agent1_main, "_ingest", fake_ingest)
    monkeypatch.setattr(am_module, "_resolve", fake_resolve)

    alerts = [
        _alert(status="firing",   fingerprint="flap-1"),
        _alert(status="resolved", fingerprint="flap-1"),
    ]

    with TestClient(app) as c:
        r = c.post("/api/webhook/alertmanager",
                   headers={"Authorization": f"Bearer {AM_TOKEN}"},
                   json=_am_payload(alerts))
    assert r.status_code == 202

    # Ingest for flap-1 must appear before resolve for flap-1
    ingest_idx  = next(i for i, (op, fp) in enumerate(call_order)
                       if op == "ingest" and fp == "am-flap-1")
    resolve_idx = next(i for i, (op, fp) in enumerate(call_order)
                       if op == "resolve" and fp == "am-flap-1")
    assert ingest_idx < resolve_idx, f"Expected ingest before resolve: {call_order}"


# ── _resolve unit tests (async, patch redis + httpx) ─────────────────────────

@pytest.mark.asyncio
async def test_resolve_no_binding_returns_no_binding(monkeypatch):
    import agents.Agent_1_dynatrace.intake.alertmanager as am_module
    from agents.Agent_1_dynatrace.intake.alertmanager import _resolve

    # Ensure SNOW_BASE is set so we don't short-circuit with "snow_disabled"
    monkeypatch.setattr(am_module, "SNOW_BASE", "https://snow.test")

    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))

    result = await _resolve("am-orphan")
    assert result == {"resolved": False, "reason": "no_binding"}


@pytest.mark.asyncio
async def test_resolve_source_tool_mismatch_refuses_close(monkeypatch):
    """
    _resolve must refuse to close an INC whose u_source_tool != 'Alertmanager'.

    Adjustment #1: We inject a fake httpx.AsyncClient factory rather than
    patching a non-existent get_snow_client(). The factory is the
    `_snow_client_factory` module attribute, overridden via monkeypatch.
    """
    import json as _json
    import agents.Agent_1_dynatrace.intake.alertmanager as am_module
    from agents.Agent_1_dynatrace.intake.alertmanager import _resolve

    # Ensure SNOW_BASE is set so we don't short-circuit with "snow_disabled"
    monkeypatch.setattr(am_module, "SNOW_BASE", "https://snow.test")

    # Redis: binding exists
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=_json.dumps({
        "incident_sys_id": "abc",
        "incident_number": "INC1",
        "run_id": "r1",
    }).encode())
    monkeypatch.setattr(am_module, "get_redis", AsyncMock(return_value=redis_mock))

    # Fake httpx GET response → u_source_tool = "Dynatrace" (mismatch)
    get_resp = MagicMock()
    get_resp.raise_for_status = MagicMock()
    get_resp.json = MagicMock(return_value={
        "result": {"u_source_tool": "Dynatrace"}
    })

    patch_resp = MagicMock()
    patch_resp.raise_for_status = MagicMock()

    fake_client = AsyncMock()
    fake_client.get = AsyncMock(return_value=get_resp)
    fake_client.patch = AsyncMock(return_value=patch_resp)

    @asynccontextmanager
    async def fake_async_client(*args, **kwargs):
        yield fake_client

    monkeypatch.setattr(am_module, "_snow_client_factory", fake_async_client)

    result = await _resolve("am-flap")
    assert result == {"resolved": False, "reason": "source_mismatch"}
    # patch must NOT have been called
    fake_client.patch.assert_not_awaited()
