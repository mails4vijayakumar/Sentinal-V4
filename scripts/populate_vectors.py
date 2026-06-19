#!/usr/bin/env python3
"""
scripts/populate_vectors.py
============================
Backfill pgvector RAG tables from existing kb.documents rows.
Run after restore or schema migration to ensure all documents have embeddings.

Usage:
    DATABASE_URL=... OLLAMA_BASE_URL=... python scripts/populate_vectors.py
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

from shared.embedding_client import embed_batch
from shared.vector_client import _get_pool, chunk_text, upsert_chunk

async def main():
    pool = await _get_pool()
    async with pool.acquire() as conn:
        docs = await conn.fetch("""
            SELECT d.id, d.title, d.content
            FROM kb.documents d
            WHERE NOT EXISTS (
                SELECT 1 FROM kb.chunks c WHERE c.document_id = d.id
            )
        """)
    if not docs:
        log.info("All documents already have embeddings. Nothing to do."); return

    log.info("Backfilling %d documents…", len(docs))
    for doc in docs:
        chunks = chunk_text(doc["content"])
        embeddings = await embed_batch(chunks)
        for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            await upsert_chunk(doc["id"], i, chunk, emb)
        log.info("  ✓ %s (%d chunks)", doc["title"], len(chunks))
    log.info("Done.")

if __name__ == "__main__":
    asyncio.run(main())
