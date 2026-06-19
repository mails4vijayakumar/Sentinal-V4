#!/usr/bin/env python3
"""
scripts/create_fidelis_portal_runbook.py
=========================================
Seeds the Fidelis Patient Portal runbook into pgvector.
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO)
from shared.embedding_client import embed_batch
from shared.vector_client import chunk_text, upsert_document, upsert_chunk

CONTENT = """Fidelis Patient Portal — Incident Runbook
Symptoms: Patients unable to login, appointment booking failing, results not loading.
Step 1: Check portal health endpoint: GET https://portal.fidelis.org/health
Step 2: Check underlying EHR API connectivity from portal host
Step 3: Review portal application logs: tail -200 /var/log/fidelis-portal/app.log
Step 4: Check Redis session store: redis-cli -h redis.fidelis.org ping
Step 5: If EHR API timeout: escalate to EHR on-call immediately (patient safety risk)
Step 6: If session store down: portal restart with in-memory fallback
Compliance note: Patient data must not be cached in logs."""

async def main():
    doc_id = await upsert_document(title="Fidelis Patient Portal Runbook",
        content=CONTENT, source_type="runbook", source_id="fidelis_portal_runbook")
    chunks = chunk_text(CONTENT)
    embeddings = await embed_batch(chunks)
    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        await upsert_chunk(doc_id, i, chunk, emb)
    print(f"✓ Fidelis Portal runbook seeded ({len(chunks)} chunks)")

if __name__ == "__main__":
    asyncio.run(main())
