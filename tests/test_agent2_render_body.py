"""render_evidence_body — §4.6 body content only (header is added by snow_notes)."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import render_evidence_body  # noqa: E402
from shared.models import SplunkEnrichment, RuleMatch


def test_render_body_with_matches():
    e = SplunkEnrichment(
        tier_used=1, log_lines_scanned=342,
        splunk_category="db", confidence=78.0,
        dt_hypothesis="app", hypothesis_source="splunk",
        error_category="db",
        spl_queries=["search index=prod host=\"db-01\" earliest=-30m@m ..."],
        rule_matches=[
            RuleMatch(rule_id="db.conn_pool", rule_set="db", weight=1.1,
                      match_count=42, sample_line="HikariCP - Connection pool exhausted"),
            RuleMatch(rule_id="db.query_timeout", rule_set="db", weight=1.1,
                      match_count=8, sample_line="statement timeout"),
        ],
    )
    body = render_evidence_body(e)
    assert "Category (Splunk-scored): db" in body
    assert "confidence 78%" in body
    assert "DT hypothesis was: app" in body
    assert "Tier: 1" in body
    assert "Log lines scanned: 342" in body
    assert "db.conn_pool" in body
    assert "42 matches" in body
    assert "HikariCP - Connection pool exhausted" in body
    assert "SPL:" in body


def test_render_body_empty_evidence():
    e = SplunkEnrichment(
        log_lines_scanned=0, error_category="app",
        dt_hypothesis="app", hypothesis_source="dt",
        confidence=0.0, rule_matches=[],
        time_range="last 30min",
    )
    body = render_evidence_body(e)
    assert "No matching log evidence" in body
    assert "using DT hypothesis: app" in body
