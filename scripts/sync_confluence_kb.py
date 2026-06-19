#!/usr/bin/env python3
"""
scripts/sync_confluence_kb.py
==============================
Ingest Confluence KB space into pgvector.
Fetches all pages in CONFLUENCE_SPACE_KEY, chunks them, embeds, and upserts.

Usage:
    CONFLUENCE_BASE_URL=... CONFLUENCE_TOKEN=... CONFLUENCE_SPACE_KEY=RUNBOOKS \
    python scripts/sync_confluence_kb.py
"""
import asyncio, logging, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)

import httpx
from shared.embedding_client import embed_batch
from shared.vector_client import chunk_text, upsert_document, upsert_chunk

CONF_BASE  = os.getenv("CONFLUENCE_BASE_URL", "").rstrip("/")
CONF_TOKEN = os.getenv("CONFLUENCE_TOKEN", "")
SPACE_KEY  = os.getenv("CONFLUENCE_SPACE_KEY", "RUNBOOKS")

async def main():
    if not (CONF_BASE and CONF_TOKEN):
        log.error("CONFLUENCE_BASE_URL and CONFLUENCE_TOKEN required"); return

    headers = {"Authorization": f"Bearer {CONF_TOKEN}", "Accept": "application/json"}
    async with httpx.AsyncClient(base_url=CONF_BASE, headers=headers, timeout=30) as c:
        start = 0
        while True:
            resp = await c.get("/rest/api/content", params={
                "spaceKey": SPACE_KEY, "type": "page", "expand": "body.storage",
                "limit": 25, "start": start,
            })
            resp.raise_for_status()
            data = resp.json()
            pages = data.get("results", [])
            if not pages: break

            for page in pages:
                page_id  = page["id"]
                title    = page["title"]
                body     = page.get("body", {}).get("storage", {}).get("value", "")
                # Strip HTML tags (simple)
                import re
                content = re.sub(r"<[^>]+>", " ", body).strip()
                if not content: continue

                url = f"{CONF_BASE}/pages/{page_id}"
                doc_id = await upsert_document(
                    title=title, content=content,
                    source_type="confluence", source_id=page_id, source_url=url,
                )
                chunks     = chunk_text(content)
                embeddings = await embed_batch(chunks)
                for i, (chunk, emb) in enumerate(zip(chunks, embeddings)):
                    await upsert_chunk(doc_id, i, chunk, emb)
                log.info("✓ %s (%d chunks)", title, len(chunks))

            if data.get("size", 0) < 25: break
            start += 25

    log.info("Confluence sync complete.")

if __name__ == "__main__":
    asyncio.run(main())
