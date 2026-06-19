import pytest
from agents.Agent_8_knowledge_synth.config import Settings


@pytest.mark.unit
def test_settings_defaults(monkeypatch):
    monkeypatch.setenv("SNOW_BASE_URL", "https://x.service-now.com")
    monkeypatch.setenv("SNOW_CLIENT_ID", "cid")
    monkeypatch.setenv("SNOW_CLIENT_SECRET", "csec")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/d")
    s = Settings()
    assert s.synth_min_cluster_size == 5
    assert s.synth_quality_score_floor == 0.40
    assert s.synth_dedup_update_threshold == 0.92
    assert s.synth_publish_confluence is True
    assert s.synth_admin_token is None


@pytest.mark.unit
def test_settings_overrides(monkeypatch):
    monkeypatch.setenv("SNOW_BASE_URL", "https://x.service-now.com")
    monkeypatch.setenv("SNOW_CLIENT_ID", "cid")
    monkeypatch.setenv("SNOW_CLIENT_SECRET", "csec")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/d")
    monkeypatch.setenv("SYNTH_MIN_CLUSTER_SIZE", "10")
    monkeypatch.setenv("SYNTH_PUBLISH_CONFLUENCE", "false")
    s = Settings()
    assert s.synth_min_cluster_size == 10
    assert s.synth_publish_confluence is False
