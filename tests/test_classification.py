"""tests/test_classification.py — Splunk error classification unit tests"""
import pytest, sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from agents.Agent_2_splunk.main import _classify

@pytest.mark.parametrize("errors,count,expected", [
    (["ConnectionError", "TimeoutError"], 50, "db_connection"),
    (["OutOfMemoryError", "HeapDump"],     20, "memory"),
    (["ClassNotFoundException"],           5,  "deployment"),
    (["ERROR","WARN"],                     200, "high_error_rate"),
    (["INFO"],                             1,  "unknown"),
])
def test_classify(errors, count, expected):
    assert _classify(errors, count) == expected
