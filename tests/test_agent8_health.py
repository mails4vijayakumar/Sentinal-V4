import pytest
from fastapi.testclient import TestClient
from agents.Agent_8_knowledge_synth.main import app


@pytest.mark.unit
def test_health_returns_200():
    with TestClient(app) as client:
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok", "agent": "knowledge-synth", "version": "0.1.0"}
