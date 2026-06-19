"""tests/test_pipeline.py — Agent 1 webhook and routing tests"""
import json, pytest
from httpx import AsyncClient, ASGITransport
from tests.conftest import dt_payload, dt_sig, snow_payload, snow_sig

@pytest.fixture(scope="module")
def agent1_app(mock_redis, mock_routing):
    from agents.Agent_1_dynatrace.main import app  # noqa
    return app

@pytest.mark.asyncio
async def test_health_returns_ok(agent1_app):
    async with AsyncClient(transport=ASGITransport(agent1_app), base_url="http://test") as c:
        r = await c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

@pytest.mark.asyncio
@pytest.mark.parametrize("severity,expected_flow", [
    ("AVAILABILITY", "primary"), ("PERFORMANCE", "primary"),
    ("ERROR", "primary"), ("CUSTOM", "secondary"),
])
async def test_dt_severity_routing(severity, expected_flow, agent1_app, mock_redis, mock_routing):
    body = json.dumps(dt_payload(severity=severity)).encode()
    async with AsyncClient(transport=ASGITransport(agent1_app), base_url="http://test") as c:
        r = await c.post("/api/webhook/dynatrace", content=body,
            headers={"Content-Type":"application/json", "X-DT-Signature": dt_sig(body)})
    assert r.status_code == 202
    assert r.json()["flow"] == expected_flow

@pytest.mark.asyncio
async def test_missing_dt_signature_rejected(agent1_app):
    body = json.dumps(dt_payload()).encode()
    async with AsyncClient(transport=ASGITransport(agent1_app), base_url="http://test") as c:
        r = await c.post("/api/webhook/dynatrace", content=body, headers={"Content-Type":"application/json"})
    assert r.status_code == 401

@pytest.mark.asyncio
async def test_resolved_dt_event_ignored(agent1_app, mock_redis, mock_routing):
    p = dt_payload(); p["status"] = "RESOLVED"
    body = json.dumps(p).encode()
    async with AsyncClient(transport=ASGITransport(agent1_app), base_url="http://test") as c:
        r = await c.post("/api/webhook/dynatrace", content=body,
            headers={"Content-Type":"application/json", "X-DT-Signature": dt_sig(body)})
    assert r.status_code == 200
    assert r.json()["accepted"] is False

@pytest.mark.asyncio
@pytest.mark.parametrize("priority,flow", [("4","secondary"),("5","secondary"),("2","primary")])
async def test_snow_priority_routing(priority, flow, agent1_app, mock_redis, mock_routing):
    body = json.dumps(snow_payload(priority=priority)).encode()
    async with AsyncClient(transport=ASGITransport(agent1_app), base_url="http://test") as c:
        r = await c.post("/api/webhook/servicenow", content=body,
            headers={"Content-Type":"application/json", "X-SNOW-Signature": snow_sig(body)})
    assert r.status_code == 202
    assert r.json()["flow"] == flow
