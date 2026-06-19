#!/usr/bin/env python3
"""
scripts/diagnose_agent6_cql.py
================================
Diagnose Agent 6 vector search issues.
Tests the pgvector cosine search with a sample query and prints scores.

Usage:
    DATABASE_URL=... OLLAMA_BASE_URL=... python scripts/diagnose_agent6_cql.py "EHR memory issue"
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO)
from shared.embedding_client import embed_text
from shared.vector_client import search_kb

async def main(query: str):
    print(f"Query: {query!r}")
    vec  = await embed_text(query)
    hits = await search_kb(vec, top_k=5, min_score=0.0)
    print(f"\nResults ({len(hits)} hits):")
    for h in hits:
        print(f"  [{h['score']:.3f}] {h['title']}")
        print(f"         {h['content'][:120]}…")

if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "EHR database connection timeout"
    asyncio.run(main(q))
