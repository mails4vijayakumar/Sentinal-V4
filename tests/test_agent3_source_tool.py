"""
tests/test_agent3_source_tool.py
==================================
Targeted unit tests for Agent 3's u_source_tool branching.

Verifies that _create_incident sets u_source_tool="Alertmanager" and
u_source_alert_id (stripped of "am-" prefix) for AM-sourced incidents,
and u_source_tool="Dynatrace" for the default DT path.

Insertion point: agents/Agent-3-servicenow/main.py — _create_incident(),
the `body` dict assembly, guarded by `if source == "alertmanager"`.

The test loads _create_incident directly via importlib to avoid the
hyphen-directory naming issue (no conftest alias for Agent 3).
"""
import importlib.util
import sys
import pytest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
AGENT3_MAIN = ROOT / "agents" / "Agent-3-servicenow" / "main.py"


def _load_agent3():
    """Load agent3 main module, stubbing heavy shared deps."""
    mod_name = f"_agent3_main_{id(object())}"  # unique name per call
    spec = importlib.util.spec_from_file_location(mod_name, AGENT3_MAIN)
    mod = importlib.util.module_from_spec(spec)

    # Stub shared imports that need a real stack
    for key, stub in [
        ("shared.models",        _fake_shared_models()),
        ("shared.redis_client",  MagicMock()),
        ("shared.routing_client",MagicMock()),
        ("shared.snow_auth",     MagicMock(get_snow_token=AsyncMock(return_value="Bearer tok"))),
    ]:
        sys.modules.setdefault(key, stub)

    spec.loader.exec_module(mod)
    return mod


def _fake_shared_models():
    """Return a MagicMock that behaves enough like shared.models."""
    import shared.models as real_models
    return real_models


@pytest.mark.asyncio
async def test_create_incident_alertmanager_sets_source_tool(monkeypatch):
    """source='alertmanager' → u_source_tool='Alertmanager', u_source_alert_id stripped."""
    monkeypatch.setenv("SNOW_BASE_URL", "https://snow.example.com")
    monkeypatch.setenv("SNOW_CALLER_ID", "sentinel.agent")

    captured_body: dict = {}

    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.json = MagicMock(return_value={
        "result": {"number": "INC001", "sys_id": "abc123"}
    })

    async def fake_post(url, json=None, **kw):
        captured_body.update(json or {})
        return fake_resp

    fake_client = AsyncMock()
    fake_client.post = AsyncMock(side_effect=fake_post)

    @asynccontextmanager
    async def fake_client_cm(base_url=None, headers=None, timeout=None):
        yield fake_client

    mod = _load_agent3()

    with patch.object(mod.httpx, "AsyncClient", side_effect=fake_client_cm):
        result = await mod._create_incident(
            headers={"Authorization": "Bearer tok"},
            severity="P1",
            title="Test AM alert",
            ext_id="am-deadbeef",
            host="host-1",
            splunk={},
            source="alertmanager",
        )

    assert captured_body.get("u_source_tool") == "Alertmanager", (
        f"Expected u_source_tool='Alertmanager', got {captured_body.get('u_source_tool')!r}"
    )
    assert captured_body.get("u_source_alert_id") == "deadbeef", (
        f"Expected u_source_alert_id='deadbeef', got {captured_body.get('u_source_alert_id')!r}"
    )
    assert result.action == "created"


@pytest.mark.asyncio
async def test_create_incident_default_sets_dynatrace_source_tool(monkeypatch):
    """Default path (source='dynatrace') → u_source_tool='Dynatrace', no u_source_alert_id."""
    monkeypatch.setenv("SNOW_BASE_URL", "https://snow.example.com")
    monkeypatch.setenv("SNOW_CALLER_ID", "sentinel.agent")

    captured_body: dict = {}

    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.json = MagicMock(return_value={
        "result": {"number": "INC002", "sys_id": "def456"}
    })

    async def fake_post(url, json=None, **kw):
        captured_body.update(json or {})
        return fake_resp

    fake_client = AsyncMock()
    fake_client.post = AsyncMock(side_effect=fake_post)

    @asynccontextmanager
    async def fake_client_cm(base_url=None, headers=None, timeout=None):
        yield fake_client

    mod = _load_agent3()

    with patch.object(mod.httpx, "AsyncClient", side_effect=fake_client_cm):
        result = await mod._create_incident(
            headers={"Authorization": "Bearer tok"},
            severity="P2",
            title="DT problem",
            ext_id="PROBLEM-999",
            host="host-dt",
            splunk={},
            source="dynatrace",
        )

    assert captured_body.get("u_source_tool") == "Dynatrace"
    assert "u_source_alert_id" not in captured_body
    assert result.action == "created"
