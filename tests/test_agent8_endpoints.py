from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from agents.Agent_8_knowledge_synth.main import app
from agents.Agent_8_knowledge_synth.schemas import RunCounts


@pytest.mark.unit
def test_synthesize_endpoint_requires_admin_token(monkeypatch):
    monkeypatch.setenv("SYNTH_ADMIN_TOKEN", "secret")
    with TestClient(app) as client:
        r = client.post(
            "/jobs/synthesize",
            json={"window_start": "2026-05-01", "window_end": "2026-05-31"},
        )
        assert r.status_code == 401


@pytest.mark.unit
def test_synthesize_endpoint_returns_counts_when_authorised(monkeypatch):
    monkeypatch.setenv("SYNTH_ADMIN_TOKEN", "secret")
    with patch(
        "agents.Agent_8_knowledge_synth.main._run_synthesis_now",
        new=AsyncMock(return_value=RunCounts(extracted=10, created=2)),
    ):
        with TestClient(app) as client:
            r = client.post(
                "/jobs/synthesize",
                json={"window_start": "2026-05-01", "window_end": "2026-05-31"},
                headers={"X-Synth-Admin-Token": "secret"},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["counts"]["extracted"] == 10
            assert body["counts"]["created"] == 2
