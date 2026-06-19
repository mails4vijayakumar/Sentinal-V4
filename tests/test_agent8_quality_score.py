import pytest
from agents.Agent_8_knowledge_synth.normalize import quality_score


def _inc(close_notes="x" * 300, close_code="Solved (Permanently)",
         has_step_markers=True, has_sentinel_attribution=False):
    notes = close_notes
    if has_step_markers:
        notes = "Step 1: do X\nStep 2: do Y\n" + close_notes
    return {
        "number": "INC1",
        "short_description": "x",
        "description": "y",
        "close_notes": notes,
        "close_code": close_code,
        "assignment_group": "g",
        "category": None,
        "subcategory": None,
        "closed_at_iso": "2026-05-12T14:23:00",
        "has_sentinel_attribution": has_sentinel_attribution,
    }


@pytest.mark.unit
def test_quality_score_high_with_long_notes_and_steps():
    score = quality_score(_inc(has_sentinel_attribution=True))
    assert score >= 0.9


@pytest.mark.unit
def test_quality_score_low_for_short_notes():
    score = quality_score(_inc(close_notes="ok", has_step_markers=False))
    assert score < 0.4


@pytest.mark.unit
def test_quality_score_penalises_non_permanent_close_code():
    score = quality_score(_inc(close_code="Solved (Workaround)"))
    assert score < quality_score(_inc(close_code="Solved (Permanently)"))
