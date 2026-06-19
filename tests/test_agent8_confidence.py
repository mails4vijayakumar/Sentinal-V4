import math

import pytest

from agents.Agent_8_knowledge_synth.upsert import compute_confidence_score


@pytest.mark.unit
def test_confidence_score_combines_all_inputs():
    score = compute_confidence_score(
        cluster_cohesion=0.80,
        source_incident_count=17,
        llm_self_rating=0.85,
        rolling_feedback_score=0.70,
    )
    # 0.30*0.80 + 0.20*min(1, log10(18)/1.5) + 0.20*0.85 + 0.30*0.70
    # = 0.24 + 0.167 + 0.17 + 0.21
    assert math.isclose(score, 0.787, abs_tol=0.01)


@pytest.mark.unit
def test_confidence_score_clamped_to_unit_interval():
    score = compute_confidence_score(1.0, 10_000, 1.0, 1.0)
    assert 0.0 <= score <= 1.0


@pytest.mark.unit
def test_confidence_score_without_feedback_uses_neutral():
    score = compute_confidence_score(0.8, 17, 0.85, rolling_feedback_score=None)
    # rolling_feedback_score=None → neutral 0.5 contribution
    score_with_neutral = compute_confidence_score(0.8, 17, 0.85, 0.5)
    assert math.isclose(score, score_with_neutral, abs_tol=1e-6)
