import pytest
from pydantic import ValidationError
from agents.Agent_8_knowledge_synth.schemas import (
    ResolutionStep, SynthesizedArticle, RunCounts,
)


@pytest.mark.unit
def test_resolution_step_requires_step_and_action():
    s = ResolutionStep(step=1, action="Restart the pod")
    assert s.step == 1 and s.command is None


@pytest.mark.unit
def test_synthesized_article_rejects_empty_resolution_steps():
    with pytest.raises(ValidationError):
        SynthesizedArticle(
            title="x", problem_summary="y", resolution_steps=[],
            keywords=["k"], assignment_group="t", confidence_self_rating=0.5,
        )


@pytest.mark.unit
def test_synthesized_article_clamps_self_rating():
    with pytest.raises(ValidationError):
        SynthesizedArticle(
            title="x", problem_summary="y",
            resolution_steps=[ResolutionStep(step=1, action="a")],
            keywords=[], assignment_group="t", confidence_self_rating=1.5,
        )


@pytest.mark.unit
def test_run_counts_defaults_to_zero():
    c = RunCounts()
    assert c.extracted == 0 and c.created == 0
