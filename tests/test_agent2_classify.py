"""Full classify() orchestration: short-circuit, degradation, backward compat."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import httpx
import pytest
import classifier  # noqa: E402


def _event(host="db-01", service="payments", event_type="DATABASE_CONNECTION_FAILURE",
           entity_type="SERVICE"):
    return {
        "host": host, "service": service,
        "raw_payload": {"eventType": event_type, "entityType": entity_type},
    }


def _make_responder(t1_rows, t2_rows=None, t3_rows=None):
    """Sequence Splunk responses so T1 returns t1_rows, T2 returns t2_rows, T3 returns t3_rows."""
    tier_rows = [t1_rows, t2_rows or [], t3_rows or []]
    tier_idx = {"n": 0}

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            sid = f"SID-{tier_idx['n']}"
            tier_idx["n"] += 1
            return httpx.Response(201, json={"sid": sid})
        if "/jobs/" in req.url.path and req.method == "GET" and req.url.path.endswith(("SID-0","SID-1","SID-2")):
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            # Which tier's results are we returning?
            # Match on the sid segment in the URL.
            path = req.url.path
            for i, rows in enumerate(tier_rows):
                if f"SID-{i}/results" in path:
                    return httpx.Response(200, json={"results": rows})
            return httpx.Response(200, json={"results": []})
        return httpx.Response(500)
    return responder, tier_idx


async def test_classify_t1_short_circuits_when_ge_20_rows(monkeypatch):
    """T1 returns 21 rows → T2/T3 must not be executed."""
    monkeypatch.setenv("SPLUNK_TIER1_SHORTCIRCUIT_MIN", "20")
    # Force classifier to see Splunk configured.
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    t1 = [{"_raw": "HikariCP - Connection pool exhausted"} for _ in range(21)]
    responder, tier_idx = _make_responder(t1)
    _orig_async_client = httpx.AsyncClient
    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: _orig_async_client(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")

    assert tier_idx["n"] == 1  # only T1 submitted
    assert enr.tier_used == 1
    assert enr.error_category == "db"
    assert enr.classification == "db"  # backward compat
    assert enr.hypothesis_source == "splunk"
    assert enr.confidence > 65
    assert enr.assigned_team == "dba"


async def test_classify_falls_through_all_tiers_when_short_returns(monkeypatch):
    monkeypatch.setenv("SPLUNK_TIER1_SHORTCIRCUIT_MIN", "20")
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    t1 = [{"_raw": "connection pool exhausted"}]  # only 1 row
    t3 = [{"_raw": "connection pool exhausted"} for _ in range(5)]
    responder, tier_idx = _make_responder(t1, [], t3)
    _orig_async_client = httpx.AsyncClient
    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: _orig_async_client(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")
    assert tier_idx["n"] == 3  # T1 + T2 + T3 all submitted
    assert enr.tier_used == 3  # last tier that produced usable results


async def test_classify_degrades_when_splunk_unconfigured(monkeypatch):
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "", raising=False)
    enr = await classifier.classify(_event(), severity="P2")
    assert enr.hypothesis_source == "dt"
    assert enr.confidence == 0.0
    assert enr.error_category == "db"  # from DT hypothesis
    assert enr.classification == "db"
    assert "not configured" in (enr.llm_summary or "").lower()


async def test_classify_degrades_on_splunk_exception(monkeypatch):
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    def responder(req):
        return httpx.Response(500, text="splunk down")

    _orig_async_client = httpx.AsyncClient
    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: _orig_async_client(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")
    assert enr.hypothesis_source == "dt"
    assert enr.error_category == "db"
    assert "failed" in (enr.llm_summary or "").lower()
