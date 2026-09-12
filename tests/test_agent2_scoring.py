"""Scoring — regex matches, counts, sample lines, weights."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import score  # noqa: E402


def _rows(*lines):
    return [{"_raw": ln} for ln in lines]


def test_score_pure_app():
    rows = _rows(
        "java.lang.NullPointerException at Foo.bar",
        "Uncaught: something",
    )
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "app.null_pointer" in ids
    assert "app.unhandled_ex" in ids
    for m in matches:
        assert m.rule_set == "app"
        assert m.weight == 1.0


def test_score_pure_infra_case_insensitive():
    rows = _rows("java.lang.OutOfMemoryError: Java heap space",
                 "OOMKilled")
    matches = score(rows)
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert oom.match_count == 2
    assert oom.weight == 1.2
    assert oom.rule_set == "infra"


def test_score_pure_db():
    rows = _rows(
        "HikariCP - Connection pool exhausted after 30000ms",
        "org.postgresql.util.PSQLException: statement timeout",
    )
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "db.conn_pool" in ids
    assert "db.query_timeout" in ids
    for m in matches:
        assert m.rule_set == "db"
        assert m.weight == 1.1


def test_score_mixed_60_40():
    rows = _rows(
        # 3 db
        "connection pool exhausted A",
        "connection pool timeout B",
        "connection pool exhausted C",
        # 2 app
        "NullPointerException here",
        "Uncaught exception",
    )
    matches = score(rows)
    db_conn = next(m for m in matches if m.rule_id == "db.conn_pool")
    assert db_conn.match_count == 3
    app_hits = [m for m in matches if m.rule_set == "app"]
    assert sum(m.match_count for m in app_hits) == 2


def test_score_empty_returns_empty():
    assert score([]) == []


def test_score_row_without_raw_uses_str():
    rows = [{"message": "OutOfMemory here"}]
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "infra.oom" in ids


def test_sample_line_truncated_to_200_chars():
    long_line = "OutOfMemory " + "x" * 500
    matches = score(_rows(long_line))
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert len(oom.sample_line) == 200


def test_sample_line_is_first_match():
    rows = _rows("OutOfMemory first", "OutOfMemory second")
    matches = score(rows)
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert oom.sample_line == "OutOfMemory first"
    assert oom.match_count == 2
