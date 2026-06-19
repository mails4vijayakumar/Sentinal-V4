from unittest.mock import AsyncMock

import pytest

from agents.Agent_8_knowledge_synth.embed import build_embedding_text, embed_batch


@pytest.mark.unit
def test_build_embedding_text_concatenates_fields():
    inc = {
        "short_description": "Pods crash",
        "description": "503 errors",
        "close_notes": "Restarted",
    }
    t = build_embedding_text(inc)
    assert "Pods crash" in t and "503 errors" in t and "Restarted" in t


@pytest.mark.unit
@pytest.mark.asyncio
async def test_embed_batch_calls_client_with_chunks():
    fake_client = AsyncMock()

    async def _embed(batch):
        return [[0.1] * 768] * len(batch)

    fake_client.embed = AsyncMock(side_effect=_embed)

    texts = ["t"] * 70
    out = await embed_batch(fake_client, texts, batch_size=32)
    assert len(out) == 70
    # 70 / 32 → 3 calls (32, 32, 6)
    assert fake_client.embed.await_count == 3
