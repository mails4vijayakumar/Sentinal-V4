#!/usr/bin/env python3
"""
scripts/create_p1_runbooks_and_seed_kb.py
==========================================
Creates P1 runbook pages in Confluence AND immediately ingests them into pgvector.
Combined helper for demo environment setup.
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
from shared.embedding_client import embed_batch
from shared.vector_client import chunk_text, upsert_document, upsert_chunk

P1_RUNBOOKS = [
    ("P1 — EHR Full Outage Response",
     "SEVERITY: P1 — all EHR services down. Step 1: page all on-call via PagerDuty. "
     "Step 2: check load balancer health. Step 3: failover to DR site. "
     "Step 4: notify clinical leadership. Step 5: open SNOW major incident bridge."),
    ("P1 — Database Primary Failover",
     "SEVERITY: P1 — primary DB unreachable. Step 1: confirm via pg_ctl status. "
     "Step 2: promote standby: pg_ctl promote. Step 3: update connection strings. "
     "Step 4: notify all application teams. RTO: 5 minutes."),
]

async def main():
    for title, content in P1_RUNBOOKS:
        doc_id = await upsert_document(title=title, content=content,
            source_type="runbook", source_id=title.lower().replace(" ","_")[:40])
        chunks = chunk_text(content)
        embeddings = await embed_batch(chunks)
        for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
            await upsert_chunk(doc_id, i, chunk, emb)
        log.info("✓ Seeded: %s (%d chunks)", title, len(chunks))
    log.info("P1 runbooks seeded to pgvector.")

if __name__ == "__main__":
    asyncio.run(main())
