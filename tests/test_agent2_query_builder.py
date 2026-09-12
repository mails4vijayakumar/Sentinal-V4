"""Query builder shape + dedup tests for classifier.build_tiered_queries."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import build_tiered_queries  # noqa: E402


def test_full_tiers_when_host_and_service_present():
    q = build_tiered_queries(host="db-01", service="payments", severity="P2", index="prod")
    assert len(q) == 3
    t1, t2, t3 = q
    # T1: has both host + service filters
    assert 'host="db-01"' in t1
    assert 'source="*payments*"' in t1
    # T2: drops service
    assert 'host="db-01"' in t2
    assert 'source="*payments*"' not in t2
    # T3: drops host
    assert 'host=' not in t3
    # All: index + time window + keywords + head 500
    for t in q:
        assert "index=prod" in t
        assert "-30m@m" in t  # P2 window
        assert "(ERROR OR WARN OR CRITICAL OR Exception OR Traceback OR FATAL)" in t
        assert t.endswith("| head 500")


def test_p1_window_15_min():
    q = build_tiered_queries("h", "s", "P1", "prod")
    for t in q:
        assert "-15m@m" in t


def test_p4_window_60_min():
    q = build_tiered_queries("h", "s", "P4", "prod")
    for t in q:
        assert "-60m@m" in t


def test_unknown_severity_defaults_to_30_min():
    q = build_tiered_queries("h", "s", "PX", "prod")
    for t in q:
        assert "-30m@m" in t


def test_empty_service_dedups_t1_and_t2():
    q = build_tiered_queries(host="db-01", service="", severity="P2", index="prod")
    assert len(q) == 2  # T1 collapses into T2
    # First query is host-only (T1==T2 shape); second is index-wide
    assert 'host="db-01"' in q[0]
    assert 'host=' not in q[1]


def test_empty_host_dedups_t2_and_t3():
    q = build_tiered_queries(host="", service="payments", severity="P2", index="prod")
    # T2 (host-only) collapses into T3 (index-wide); T1 still has service filter.
    # Result: T1 with service filter, then T3.
    assert len(q) == 2
    assert 'source="*payments*"' in q[0]
    assert 'source=' not in q[1]


def test_empty_host_and_service_yields_one_query():
    q = build_tiered_queries(host="", service="", severity="P3", index="prod")
    assert len(q) == 1
    assert 'host=' not in q[0]
    assert 'source=' not in q[0]
