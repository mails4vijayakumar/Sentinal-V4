"""
shared/vector_client.py
=======================
pgvector query helpers used by Agent 6 (Confluence KB search) and Agent 7 (RCA).

Provides:
  - cosine similarity search over kb.chunks
  - upsert helpers to keep the KB fresh after each resolved incident
  - chunk-and-index helper for new documents

Requires asyncpg + pgvector installed on the DB.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

import asyncpg

log = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sentinel:changeme@pgbouncer:6432/sentinel"
)
VECTOR_SCHEMA   = "kb"
EMBED_DIM       = 768
TOP_K_DEFAULT   = 5
MIN_SCORE       = 0.60    # drop hits below this cosine similarity


# ── Connection pool ───────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=30,
            init=_register_vector_codec,
        )
    return _pool


async def _register_vector_codec(conn: asyncpg.Connection) -> None:
    """Register the pgvector type codec so asyncpg can handle vector[] columns."""
    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    await conn.set_type_codec(
        "vector",
        encoder=_encode_vector,
        decoder=_decode_vector,
        schema="pg_catalog",
        format="text",
    )


def _encode_vector(v: List[float]) -> str:
    return "[" + ",".join(str(x) for x in v) + "]"


def _decode_vector(s: str) -> List[float]:
    return [float(x) for x in s.strip("[]").split(",")]


# ── Cosine similarity search ──────────────────────────────────────────────────

async def search_kb(
    query_vector: List[float],
    top_k: int = TOP_K_DEFAULT,
    min_score: float = MIN_SCORE,
    source_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Perform ANN cosine similarity search over kb.chunks.

    Returns a list of dicts with keys:
        chunk_id, document_id, title, source_url, content, score
    Sorted by score descending.
    """
    pool = await _get_pool()
    source_filter = "AND d.source_type = $3" if source_type else ""
    params: list = [_encode_vector(query_vector), top_k]
    if source_type:
        params.append(source_type)

    sql = f"""
        SELECT
            c.id          AS chunk_id,
            d.id          AS document_id,
            d.title,
            d.source_url,
            c.content,
            1 - (c.embedding <=> $1::vector)  AS score
        FROM {VECTOR_SCHEMA}.chunks c
        JOIN {VECTOR_SCHEMA}.documents d ON d.id = c.document_id
        WHERE 1 - (c.embedding <=> $1::vector) >= {min_score}
        {source_filter}
        ORDER BY c.embedding <=> $1::vector
        LIMIT $2
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)

    return [
        {
            "chunk_id":    str(row["chunk_id"]),
            "document_id": str(row["document_id"]),
            "title":       row["title"],
            "source_url":  row["source_url"],
            "content":     row["content"],
            "score":       round(float(row["score"]), 4),
        }
        for row in rows
    ]


# ── Document upsert ───────────────────────────────────────────────────────────

async def upsert_document(
    title:       str,
    content:     str,
    source_type: str,
    source_id:   str,
    source_url:  Optional[str] = None,
    metadata:    Optional[Dict[str, Any]] = None,
) -> UUID:
    """
    Insert or update a document and return its ID.
    Deduplicates by (source_type, source_id).
    """
    import json
    pool = await _get_pool()
    content_hash = hashlib.sha256(content.encode()).hexdigest()
    meta_json = json.dumps(metadata or {})

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO kb.documents (id, source_type, source_id, source_url, title, content, content_hash, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (source_type, source_id)
            DO UPDATE SET
                title        = EXCLUDED.title,
                content      = EXCLUDED.content,
                content_hash = EXCLUDED.content_hash,
                metadata     = EXCLUDED.metadata,
                indexed_at   = NOW()
            RETURNING id
        """, uuid4(), source_type, source_id, source_url, title, content, content_hash, meta_json)
        return row["id"]


async def upsert_chunk(
    document_id: UUID,
    chunk_index: int,
    content:     str,
    embedding:   List[float],
) -> UUID:
    """Insert or update a chunk and its embedding."""
    pool = await _get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO kb.chunks (id, document_id, chunk_index, content, embedding, token_count)
            VALUES ($1, $2, $3, $4, $5::vector, $6)
            ON CONFLICT (document_id, chunk_index)
            DO UPDATE SET
                content     = EXCLUDED.content,
                embedding   = EXCLUDED.embedding,
                token_count = EXCLUDED.token_count
            RETURNING id
        """, uuid4(), document_id, chunk_index, content,
            _encode_vector(embedding),
            len(content.split()))
        return row["id"]


# ── Chunking helper ───────────────────────────────────────────────────────────

def chunk_text(text: str, max_tokens: int = 400, overlap: int = 50) -> List[str]:
    """
    Split text into overlapping token windows.
    Approximate — uses whitespace tokens, not a real tokenizer.
    """
    words = text.split()
    if len(words) <= max_tokens:
        return [text]
    chunks = []
    start = 0
    while start < len(words):
        end = min(start + max_tokens, len(words))
        chunks.append(" ".join(words[start:end]))
        start = end - overlap
    return chunks
