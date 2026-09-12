"""Table-driven checks for classifier.preclassify."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import pytest
from classifier import preclassify  # noqa: E402


def _event(event_type: str = "", entity_type: str = "") -> dict:
    return {"raw_payload": {"eventType": event_type, "entityType": entity_type}}


@pytest.mark.parametrize("event_type,entity_type,expected", [
    ("FAILURE_RATE_INCREASED",            "SERVICE",       "app"),
    ("RESPONSE_TIME_DEGRADATION",         "SERVICE",       "app"),
    ("APPLICATION_ERROR",                 "APPLICATION",   "app"),
    ("PROCESS_UNAVAILABLE",               "PROCESS_GROUP", "infra"),
    ("PROCESS_MEMORY_RESOURCE_EXHAUSTED", "PROCESS_GROUP", "infra"),
    ("CPU_SATURATED",                     "HOST",          "infra"),
    ("MEMORY_SATURATED",                  "HOST",          "infra"),
    ("HOST_DISK_QUEUE_LENGTH_HIGH",       "HOST",          "infra"),
    ("HOST_NOT_AVAILABLE",                "HOST",          "infra"),
    ("DATABASE_CONNECTION_FAILURE",       "SERVICE",       "db"),
    ("SLOW_DB_QUERIES",                   "SERVICE",       "db"),
])
def test_preclassify_known_combos(event_type, entity_type, expected):
    assert preclassify(_event(event_type, entity_type)) == expected


def test_preclassify_unmapped_defaults_to_app():
    assert preclassify(_event("NEW_UNKNOWN_TYPE", "SERVICE")) == "app"


def test_preclassify_missing_raw_payload_defaults_to_app():
    assert preclassify({}) == "app"


def test_preclassify_missing_fields_defaults_to_app():
    assert preclassify({"raw_payload": {}}) == "app"
