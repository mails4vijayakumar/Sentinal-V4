#!/usr/bin/env python3
"""
scripts/create_cas_runbook.py
==============================
Seeds the CAS (Clinical Application Services) runbook into pgvector.
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO)
from shared.embedding_client import embed_batch
from shared.vector_client import chunk_text, upsert_document, upsert_chunk

CAS_CONTENT = """CAS Runbook — Clinical Application Services Recovery
Symptoms: Login failures, session timeouts, SSO not working.
Step 1: Check CAS service: systemctl status cas-server
Step 2: Verify LDAP connectivity: ldapsearch -h ldap.hospital.org -b dc=hospital,dc=org
Step 3: Clear Tomcat session cache: rm -rf /var/cas/sessions/*
Step 4: Restart: systemctl restart cas-server
Step 5: Verify login page response: curl https://sso.hospital.org/cas/login
Escalation: Page IAM team if LDAP unreachable."""

async def main():
    doc_id = await upsert_document(title="CAS — Clinical SSO Recovery Runbook",
        content=CAS_CONTENT, source_type="runbook", source_id="cas_sso_runbook")
    chunks     = chunk_text(CAS_CONTENT)
    embeddings = await embed_batch(chunks)
    for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
        await upsert_chunk(doc_id, i, chunk, emb)
    print(f"✓ CAS runbook seeded ({len(chunks)} chunks)")

if __name__ == "__main__":
    asyncio.run(main())
