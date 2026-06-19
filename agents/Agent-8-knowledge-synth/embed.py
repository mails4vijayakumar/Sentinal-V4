from __future__ import annotations

from typing import Any, Protocol


class EmbeddingClient(Protocol):
    async def embed(self, texts: list[str]) -> list[list[float]]: ...


def build_embedding_text(inc: dict[str, Any]) -> str:
    parts = [
        inc.get("short_description") or "",
        inc.get("description") or "",
        f"Resolution: {inc.get('close_notes') or ''}",
    ]
    return "\n\n".join(p for p in parts if p)


async def embed_batch(
    client: EmbeddingClient, texts: list[str], *, batch_size: int = 32
) -> list[list[float]]:
    results: list[list[float]] = []
    for i in range(0, len(texts), batch_size):
        chunk = texts[i : i + batch_size]
        out = await client.embed(chunk)
        results.extend(out)
    return results
