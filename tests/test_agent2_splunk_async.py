"""Async Splunk submit/poll/fetch pattern tests."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import httpx
import pytest
from classifier import run_splunk_async, SplunkPollTimeout  # noqa: E402


class _MockTransport(httpx.MockTransport):
    def __init__(self, responder):
        super().__init__(responder)


async def test_run_splunk_async_happy_path():
    calls = []

    def responder(req: httpx.Request) -> httpx.Response:
        calls.append(f"{req.method} {req.url.path}")
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-42"})
        if req.url.path.endswith("/SID-42") and req.method == "GET":
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={"results": [{"_raw": "line1"}, {"_raw": "line2"}]})
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")

    assert rows == [{"_raw": "line1"}, {"_raw": "line2"}]
    assert calls[0].startswith("POST /services/search/jobs")
    assert any("/SID-42" in call and "GET" in call for call in calls)
    assert any(call.endswith("/results") for call in calls)


async def test_run_splunk_async_polls_until_done(monkeypatch):
    """First poll returns RUNNING, second returns DONE."""
    import asyncio as _asyncio
    _orig_sleep = _asyncio.sleep
    monkeypatch.setattr(_asyncio, "sleep", lambda _s: _orig_sleep(0))  # no real wait
    poll_count = {"n": 0}

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-1"})
        if req.url.path.endswith("/SID-1") and req.method == "GET":
            poll_count["n"] += 1
            state = "RUNNING" if poll_count["n"] < 2 else "DONE"
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": state}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={"results": []})
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")

    assert rows == []
    assert poll_count["n"] == 2


async def test_run_splunk_async_poll_timeout_raises(monkeypatch):
    import asyncio as _asyncio
    _orig_sleep = _asyncio.sleep
    monkeypatch.setattr(_asyncio, "sleep", lambda _s: _orig_sleep(0))

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-2"})
        # Always RUNNING — never reaches DONE.
        return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "RUNNING"}}]})

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        with pytest.raises(SplunkPollTimeout):
            await run_splunk_async(c, "search index=prod")


async def test_run_splunk_async_empty_results_when_key_missing():
    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-3"})
        if req.url.path.endswith("/SID-3") and req.method == "GET":
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={})  # No "results" key.
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")
    assert rows == []
