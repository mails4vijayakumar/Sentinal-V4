"""
shared/embedding_client.py
==========================
Shared embedding generation for all pgvector RAG operations.

Always uses Ollama nomic-embed-text (768 dims) locally — embeddings are
never sent to external cloud APIs regardless of which LLM completion
provider is active. This keeps PHI-adjacent text on-premises.

Usage:
    from shared.embedding_client import embed_text, embed_batch
    vector = await embed_text("EHR service is timing out on all connections")
    vectors = await embed_batch(["text 1", "text 2"])
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import List, Optional

import httpx

log = logging.getLogger(__name__)

OLLAMA_BASE  = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
EMBED_MODEL  = os.getenv("EMBED_MODEL", "nomic-embed-text")
EMBED_DIM    = 768
EMBED_TIMEOUT = 30.0   # seconds
BATCH_SIZE   = 32      # max texts per Ollama batch call


class EmbeddingClient:
    """Async client for Ollama embedding API."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=OLLAMA_BASE,
            timeout=EMBED_TIMEOUT,
            limits=httpx.Limits(max_connections=10),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def embed(self, text: str) -> List[float]:
        """Embed a single text string. Returns a 768-float vector."""
        if not text or not text.strip():
            return [0.0] * EMBED_DIM
        try:
            resp = await self._client.post(
                "/api/embeddings",
                json={"model": EMBED_MODEL, "prompt": text.strip()},
            )
            resp.raise_for_status()
            embedding = resp.json()["embedding"]
            if len(embedding) != EMBED_DIM:
                log.warning(
                    "Unexpected embedding dim %d (expected %d)", len(embedding), EMBED_DIM
                )
            return embedding
        except Exception as exc:
            log.error("Embedding failed for text[:%d]: %s", len(text), exc)
            return [0.0] * EMBED_DIM

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed multiple texts concurrently in batches."""
        results: List[List[float]] = []
        for i in range(0, len(texts), BATCH_SIZE):
            chunk = texts[i: i + BATCH_SIZE]
            batch_results = await asyncio.gather(
                *[self.embed(t) for t in chunk], return_exceptions=True
            )
            for r in batch_results:
                if isinstance(r, Exception):
                    log.error("Batch embedding error: %s", r)
                    results.append([0.0] * EMBED_DIM)
                else:
                    results.append(r)
        return results

    async def ping(self) -> bool:
        """Verify Ollama is available and the embed model is loaded."""
        try:
            resp = await self._client.get("/api/tags")
            if resp.status_code != 200:
                return False
            models = [m["name"].split(":")[0] for m in resp.json().get("models", [])]
            return EMBED_MODEL.split(":")[0] in models
        except Exception:
            return False


# ── Module-level singleton ────────────────────────────────────────────────────
_client: Optional[EmbeddingClient] = None


def _get_client() -> EmbeddingClient:
    global _client
    if _client is None:
        _client = EmbeddingClient()
    return _client


async def embed_text(text: str) -> List[float]:
    """Convenience function: embed a single text."""
    return await _get_client().embed(text)


async def embed_batch(texts: List[str]) -> List[List[float]]:
    """Convenience function: embed multiple texts."""
    return await _get_client().embed_batch(texts)
