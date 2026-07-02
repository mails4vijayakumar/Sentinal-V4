"""Backward-compat and new-field checks for SplunkEnrichment."""
from shared.models import SplunkEnrichment, RuleMatch


def test_rule_match_shape():
    m = RuleMatch(rule_id="db.conn_pool", rule_set="db", weight=1.1,
                  match_count=3, sample_line="HikariCP timeout")
    assert m.rule_id == "db.conn_pool"
    assert m.rule_set == "db"
    assert m.weight == 1.1
    assert m.match_count == 3
    assert m.sample_line == "HikariCP timeout"


def test_splunk_enrichment_backward_compat_defaults():
    """Existing readers (Agents 6, 7) rely on these fields with these defaults."""
    e = SplunkEnrichment()
    assert e.log_lines_scanned == 0
    assert e.error_count == 0
    assert e.warn_count == 0
    assert e.top_errors == []
    assert e.classification is None or isinstance(e.classification, str)


def test_splunk_enrichment_new_fields_defaults():
    e = SplunkEnrichment()
    assert e.tier_used is None
    assert e.spl_queries == []
    assert e.rule_matches == []
    assert e.dt_hypothesis is None
    assert e.splunk_category is None
    assert e.confidence == 0.0
    assert e.hypothesis_source == "dt"
    assert e.error_category is None
    assert e.assigned_team is None
    assert e.assigned_queue is None
    assert e.snow_category is None
    assert e.snow_subcategory is None


def test_splunk_enrichment_roundtrip():
    e = SplunkEnrichment(
        tier_used=1, confidence=78.0, hypothesis_source="splunk",
        error_category="db", assigned_team="dba",
        rule_matches=[RuleMatch(rule_id="db.conn_pool", rule_set="db",
                                weight=1.1, match_count=42,
                                sample_line="HikariCP - Connection pool exhausted")],
    )
    dumped = e.model_dump()
    restored = SplunkEnrichment(**dumped)
    assert restored.tier_used == 1
    assert restored.confidence == 78.0
    assert restored.rule_matches[0].rule_id == "db.conn_pool"
