import pytest

from agents.Agent_8_knowledge_synth.dedup import classify_dedup_decision


@pytest.mark.unit
@pytest.mark.parametrize(
    "sim,expected",
    [
        (0.95, "update"),
        (0.92, "update"),
        (0.86, "review"),
        (0.80, "review"),
        (0.79, "create"),
        (None, "create"),
    ],
)
def test_classify_dedup_decision(sim, expected):
    assert (
        classify_dedup_decision(sim, update_threshold=0.92, review_threshold=0.80)
        == expected
    )
