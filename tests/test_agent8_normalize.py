import pytest
from agents.Agent_8_knowledge_synth.normalize import normalize_incident


@pytest.mark.unit
def test_normalize_strips_html_and_collapses_whitespace():
    raw = {
        "number": "INC1",
        "short_description": "  App  down  ",
        "description": "<p>Service <b>app-svc</b> returns 503</p>",
        "close_notes": "Restarted pods.\n\n\nPool size raised.",
        "close_code": "Solved (Permanently)",
        "assignment_group": "App-Backend",
        "category": "Application",
        "subcategory": "DB",
        "closed_at": "2026-05-12 14:23:00",
    }
    out = normalize_incident(raw)
    assert out["short_description"] == "App down"
    assert out["description"] == "Service app-svc returns 503"
    assert out["close_notes"] == "Restarted pods.\nPool size raised."
    assert out["closed_at_iso"] == "2026-05-12T14:23:00"


@pytest.mark.unit
def test_normalize_drops_excluded_close_codes():
    raw = {
        "number": "INC2", "short_description": "x", "description": "y",
        "close_notes": "fix", "close_code": "Duplicate",
        "assignment_group": "g", "category": None, "subcategory": None,
        "closed_at": "2026-05-12 14:23:00",
    }
    assert normalize_incident(raw) is None


@pytest.mark.unit
def test_normalize_drops_empty_resolution():
    raw = {
        "number": "INC3", "short_description": "x", "description": "y",
        "close_notes": "  ", "close_code": "Solved (Permanently)",
        "assignment_group": "g", "category": None, "subcategory": None,
        "closed_at": "2026-05-12 14:23:00",
    }
    assert normalize_incident(raw) is None
