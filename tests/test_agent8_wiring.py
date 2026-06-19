import pytest

from agents.Agent_8_knowledge_synth.wiring import build_deps


@pytest.mark.unit
def test_build_deps_returns_callable_dependencies(monkeypatch):
    monkeypatch.setenv("SNOW_BASE_URL", "https://x.service-now.com")
    monkeypatch.setenv("SNOW_CLIENT_ID", "cid")
    monkeypatch.setenv("SNOW_CLIENT_SECRET", "csec")
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/d")

    deps = build_deps()
    # Each side-effect is a callable
    for attr in [
        "extract", "embed_batch", "cluster_per_team", "apply_quality_gate",
        "pick_representatives", "synthesize_one", "find_similar_article",
        "upsert_article_versioned", "insert_decision", "publish_to_confluence",
        "retire_low_feedback_articles", "create_run", "finalize_run",
    ]:
        assert callable(getattr(deps, attr)), f"{attr} is not callable"
