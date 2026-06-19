#!/usr/bin/env python3
"""
scripts/verify_agent6_fix.py
=============================
Verify that the Agent 6 KB search returns non-zero results.
Fires a synthetic run context through Agent 6's search logic.

Usage:
    DATABASE_URL=... OLLAMA_BASE_URL=... python scripts/verify_agent6_fix.py
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
from shared.embedding_client import embed_text, _get_client
from shared.vector_client import search_kb

QUERIES = [
    "EHR application high memory heap exhausted",
    "database connection pool timeout JDBC",
    "HL7 interface engine restart procedure",
    "kubernetes OOMKilled pod memory limit",
    "PACS NFS mount unavailable",
]

async def main():
    log.info("Checking Ollama embedding service…")
    ok = await _get_client().ping()
    log.info("  Ollama ping: %s", "✓" if ok else "✗ FAIL")

    log.info("\nRunning %d test queries:", len(QUERIES))
    total_hits = 0
    for q in QUERIES:
        vec  = await embed_text(q)
        hits = await search_kb(vec, top_k=3, min_score=0.5)
        total_hits += len(hits)
        status = "✓" if hits else "✗"
        log.info("  %s [%d hits] %r", status, len(hits), q[:60])

    if total_hits == 0:
        log.error("\n✗ No KB hits for any query. Check that vectors are populated (run populate_vectors.py).")
        sys.exit(1)
    log.info("\n✓ Agent 6 KB search working (%d total hits across %d queries).", total_hits, len(QUERIES))

if __name__ == "__main__":
    asyncio.run(main())
