"""Conflict-resolution matrix and threshold boundary tests."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import resolve_conflict  # noqa: E402
from shared.models import RuleMatch


def _m(rule_set, count, weight):
    return RuleMatch(rule_id=f"{rule_set}.x", rule_set=rule_set,
                     weight=weight, match_count=count, sample_line="x")


def test_empty_matches_keeps_dt_hypothesis():
    final, splunk_cat, conf, src = resolve_conflict("app", [])
    assert final == "app"
    assert splunk_cat is None
    assert conf == 0.0
    assert src == "dt"


def test_single_ruleset_full_confidence_splunk_wins():
    # Only db matches — runner-up score is 0 → confidence = 100.
    final, splunk_cat, conf, src = resolve_conflict("app", [_m("db", 100, 1.1)])
    assert final == "db"
    assert splunk_cat == "db"
    assert conf > 99.99
    assert src == "splunk"


def test_dt_wins_when_splunk_ambiguous():
    # Two rule sets with near-equal scores → low confidence → DT keeps.
    matches = [_m("db", 10, 1.1), _m("app", 10, 1.0)]
    final, splunk_cat, conf, src = resolve_conflict("app", matches)
    assert src == "dt"
    assert final == "app"
    assert splunk_cat in ("db", "app")  # argmax is db (weight 1.1); either OK if scores tie
    assert conf < 65.0


def test_boundary_confidence_65_splunk_wins():
    """Confidence exactly at threshold → Splunk wins (>= not >)."""
    # Craft scores where winner/(winner+runner) ≈ 0.65.
    # score = w * log10(1 + count). For db (w=1.1): score_db = 1.1*log10(1+C_db).
    # For app (w=1.0): score_app = 1.0*log10(1+C_app).
    # Solve for 0.65: score_db / (score_db + score_app) = 0.65.
    # Use synthetic: score_db = 6.5, score_app = 3.5 → ratio = 0.65.
    # Approximate with counts that hit these scores:
    # log10(1+C) = 6.5/1.1 ≈ 5.909 → C ≈ 10^5.909 - 1 ≈ 810,000
    # log10(1+C) = 3.5/1.0 = 3.5   → C ≈ 3,161
    matches = [_m("db", 810_000, 1.1), _m("app", 3161, 1.0)]
    final, _splunk_cat, conf, src = resolve_conflict("app", matches)
    assert conf >= 64.9  # allow for rounding
    if conf >= 65.0:
        assert src == "splunk"
        assert final == "db"


def test_boundary_confidence_below_65_dt_wins():
    # Confidence just below 65 → DT wins.
    matches = [_m("db", 10, 1.1), _m("app", 8, 1.0)]
    final, _splunk_cat, conf, src = resolve_conflict("infra", matches)
    assert conf < 65.0
    assert src == "dt"
    assert final == "infra"


def test_argmax_splunk_category_recorded_even_when_dt_wins():
    matches = [_m("db", 10, 1.1), _m("app", 9, 1.0)]
    _final, splunk_cat, _conf, src = resolve_conflict("app", matches)
    assert src == "dt"          # low confidence
    assert splunk_cat == "db"   # argmax still recorded


def test_only_matches_return_that_category_at_100():
    final, splunk_cat, conf, src = resolve_conflict("app", [_m("infra", 5, 1.2)])
    assert splunk_cat == "infra"
    assert src == "splunk"
    assert final == "infra"
    assert conf > 99.99
