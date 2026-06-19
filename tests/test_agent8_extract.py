import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from agents.Agent_8_knowledge_synth.extract import snow_extract_closed

FIXTURES = Path(__file__).parent / "fixtures" / "agent8"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_paginates_until_empty():
    pages = [
        json.loads((FIXTURES / "snow_response_page1.json").read_text()),
        json.loads((FIXTURES / "snow_response_page2.json").read_text()),
    ]
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = pages[call_count["n"]]
        call_count["n"] += 1
        return httpx.Response(200, json=body)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://x.service-now.com") as client:
        results = await snow_extract_closed(
            client=client,
            window_start=date(2026, 5, 1),
            window_end=date(2026, 5, 31),
            page_size=1000,
            access_token="dummy",
        )
    assert len(results) == 2
    assert results[0]["number"] == "INC0010001"
    assert call_count["n"] == 2  # one page with data, one empty signals stop
