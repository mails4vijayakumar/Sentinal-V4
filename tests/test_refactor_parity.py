"""
Parity contract for Agent 1's DT and SNOW webhook endpoints.

These tests define the observable HTTP contract: status codes, response
body shapes, and error details. Any change to response status, body
shape, or error detail requires deliberate review — do not modify these
tests casually to make them pass.
"""
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from agents.Agent_1_dynatrace.main import app
# Import the signing functions from conftest (available via pytest's conftest fixtures)
from conftest import dt_sig, snow_sig


@pytest.fixture
def client(mock_redis):
    """TestClient with mocked Redis to avoid actual connections."""
    return TestClient(app)


@pytest.fixture
def dt_payload_open():
    return {
        "problemId":       "PROBLEM-12345",
        "displayName":     "High CPU on host-x",
        "severity":        "ERROR",
        "status":          "OPEN",
        "eventType":       "PERFORMANCE_EVENT",
        "impactedEntities": [{"name": "host-x", "entityId": "HOST-ABC"}],
        "tags":            ["app:checkout"],
        "deploymentEvent": False,
    }


@pytest.fixture
def snow_payload():
    return {
        "number":            "INC0010001",
        "short_description": "Manual P4 ticket",
        "priority":          "4",
        "cmdb_ci":           "svc-checkout",
        "sys_created_on":    datetime.now(timezone.utc).isoformat(),
    }


@pytest.mark.unit
def test_dt_valid_payload_returns_202_and_run_id(client, dt_payload_open):
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": dt_sig(body)})
    assert r.status_code == 202
    j = r.json()
    assert j["accepted"] is True
    assert j["external_id"] == "PROBLEM-12345"
    assert "run_id" in j


@pytest.mark.unit
def test_dt_invalid_signature_returns_401(client, dt_payload_open):
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": "deadbeef"})
    assert r.status_code == 401


@pytest.mark.unit
def test_dt_malformed_body_returns_422(client):
    body = b"not json"
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": dt_sig(body)})
    assert r.status_code == 422


@pytest.mark.unit
def test_dt_resolved_status_is_ignored(client, dt_payload_open):
    dt_payload_open["status"] = "RESOLVED"
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": dt_sig(body)})
    assert r.status_code == 202
    assert r.json() == {"accepted": False, "reason": "RESOLVED events are ignored"}


@pytest.mark.unit
def test_snow_valid_payload_returns_202_and_run_id(client, snow_payload):
    body = json.dumps(snow_payload).encode()
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": snow_sig(body)})
    assert r.status_code == 202
    j = r.json()
    assert j["accepted"] is True
    assert j["external_id"] == "INC0010001"


@pytest.mark.unit
def test_snow_invalid_signature_returns_401(client, snow_payload):
    body = json.dumps(snow_payload).encode()
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": "deadbeef"})
    assert r.status_code == 401


@pytest.mark.unit
def test_snow_malformed_body_returns_422(client):
    body = b"{"
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": snow_sig(body)})
    assert r.status_code == 422
