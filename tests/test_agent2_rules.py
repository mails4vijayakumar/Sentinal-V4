"""Structural checks on the hand-curated rules module."""
import re
import sys
from pathlib import Path

# Make agents/Agent-2-splunk/ importable without touching PYTHONPATH globally.
_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import rules  # noqa: E402


def test_window_min_covers_all_severities():
    for sev in ("P1", "P2", "P3", "P4", "P5"):
        assert sev in rules.WINDOW_MIN
    assert rules.WINDOW_MIN["P1"] == 15
    assert rules.WINDOW_MIN["P2"] == 30
    assert rules.WINDOW_MIN["P4"] == 60


def test_dt_hypothesis_map_shape():
    # Spec §5.1 requires eleven rows in v1.
    assert len(rules.DT_HYPOTHESIS_MAP) == 11
    assert rules.DT_HYPOTHESIS_MAP[("FAILURE_RATE_INCREASED", "SERVICE")] == "app"
    assert rules.DT_HYPOTHESIS_MAP[("CPU_SATURATED", "HOST")] == "infra"
    assert rules.DT_HYPOTHESIS_MAP[("DATABASE_CONNECTION_FAILURE", "SERVICE")] == "db"


def test_rule_sets_use_expected_weights():
    assert all(w == 1.0 for _, _, w in rules.APP_RULES)
    assert all(w == 1.2 for _, _, w in rules.INFRA_RULES)
    assert all(w == 1.1 for _, _, w in rules.DB_RULES)


def test_rule_sets_have_shippable_v1_size():
    # Spec §5.4 shippable v1: 5 app + 4 infra + 4 db.
    assert len(rules.APP_RULES) == 5
    assert len(rules.INFRA_RULES) == 4
    assert len(rules.DB_RULES) == 4


def test_rules_use_compiled_regex():
    for rule_set in (rules.APP_RULES, rules.INFRA_RULES, rules.DB_RULES):
        for rule_id, pattern, weight in rule_set:
            assert isinstance(pattern, re.Pattern), f"{rule_id} not compiled"


def test_category_routing_covers_three_categories():
    for cat in ("app", "infra", "db"):
        row = rules.CATEGORY_ROUTING[cat]
        assert row["team"]
        assert row["queue"]
        assert row["snow_category"]
        assert row["snow_subcategory"]


def test_error_keywords_is_spl_clause():
    assert rules.ERROR_KEYWORDS.startswith("(") and rules.ERROR_KEYWORDS.endswith(")")
    assert "ERROR" in rules.ERROR_KEYWORDS
    assert "Exception" in rules.ERROR_KEYWORDS
