"""Routing pre-compute: category → SNOW fields."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import pytest
from classifier import precompute_routing  # noqa: E402


@pytest.mark.parametrize("category,team,queue,cat,sub", [
    ("app",   "app-support", "APP_SUPPORT", "Application",    "Runtime Error"),
    ("infra", "infra-ops",   "INFRA_OPS",   "Infrastructure", "Resource"),
    ("db",    "dba",         "DB_ADMIN",    "Database",       "Query/Connection"),
])
def test_precompute_routing_known(category, team, queue, cat, sub):
    r = precompute_routing(category)
    assert r["team"] == team
    assert r["queue"] == queue
    assert r["snow_category"] == cat
    assert r["snow_subcategory"] == sub


def test_precompute_routing_unknown_returns_empty():
    assert precompute_routing("unknown-cat") == {}
