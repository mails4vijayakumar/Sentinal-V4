"""
agents/Agent-6-confluence/main.py
===================================
Agent 6: Confluence KB search + scoring + attach to SNOW.

Uses pgvector RAG to find relevant runbooks and procedures.
Scores each hit and attaches the best match to the SNOW incident as a knowledge article.
Enqueues Agent 7 (RCA) when done.
"""
from __future__ import annotations
import asyncio, logging, os, time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Optional

import httpx, uvicorn
from fastapi import FastAPI

import sys; sys.path.insert(0, "/app")
from shared.models import ConfluenceEnrichment, ConfluenceKBHit, SSEEventType
from shared.redis_client import get_redis
from shared.routing_client import get_routing_client, fire_and_forget
from shared.embedding_client import embed_text
from shared.vector_client import search_kb
from shared.snow_auth import get_snow_token

log           = logging.getLogger(__name__)
PORT          = int(os.getenv("AGENT_6_PORT", "8006"))
CONFLUENCE_BASE = os.getenv("CONFLUENCE_BASE_URL", "").rstrip("/")
CONFLUENCE_TOK  = os.getenv("CONFLUENCE_TOKEN", "")
SNOW_BASE       = os.getenv("SNOW_BASE_URL", "").rstrip("/")
AGENT_NAME      = "confluence"
AGENT_NUM       = 6
MIN_ATTACH_SCORE = 0.80   # only attach KB articles above this threshold

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(worker_loop())
    yield

app = FastAPI(title="Sentinel Agent 6 — Confluence", lifespan=lifespan)

@app.get("/health")
async def health():
    return {"status": "ok", "agent": AGENT_NAME}

async def worker_loop():
    redis = await get_redis()
    while True:
        run_id = await redis.dequeue(AGENT_NUM)
        if run_id:
            asyncio.create_task(process_run(run_id))

async def process_run(run_id: str):
    redis = await get_redis()
    rc    = get_routing_client()
    t0    = time.monotonic()
    await redis.publish_event({"event": SSEEventType.AGENT_START, "run_id": run_id,
        "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
        "timestamp": datetime.utcnow().isoformat()}, run_id=run_id)

    ctx  = await redis.get_context(run_id)
    if not ctx: return
    title    = ctx.get("event", {}).get("title", "")
    splunk   = ctx.get("enrichments", {}).get("splunk", {})
    service  = ctx.get("event", {}).get("service") or ""
    classification = splunk.get("classification", "")

    # Build a rich search query
    query = " ".join(filter(None, [title, service, classification, splunk.get("llm_summary", "")]))

    try:
        embedding = await embed_text(query)
        raw_hits  = await search_kb(embedding, top_k=5, min_score=0.60)

        hits: List[ConfluenceKBHit] = []
        for h in raw_hits:
            hits.append(ConfluenceKBHit(
                page_id=h["document_id"],
                title=h["title"],
                url=h.get("source_url") or "",
                score=h["score"],
                excerpt=h["content"][:300],
            ))

        best = hits[0] if hits else None
        kb_attached = False

        if best and best.score >= MIN_ATTACH_SCORE:
            snow_sys_id = ctx.get("enrichments", {}).get("servicenow", {}).get("snow_sys_id")
            if snow_sys_id:
                kb_attached = await _attach_kb_to_snow(snow_sys_id, best)

        enr = ConfluenceEnrichment(query=query, hits=hits,
                                    top_score=best.score if best else 0.0,
                                    kb_attached=kb_attached)
        ctx.setdefault("enrichments", {})["confluence"] = enr.model_dump()
        await redis.store_context(run_id, ctx)

        fire_and_forget(rc.write_enrichment(run_id, {"agent_num": AGENT_NUM, "source": "confluence", "data": enr.model_dump()}))
        fire_and_forget(rc.record_step(run_id, {"agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "status": "completed", "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary": f"{len(hits)} KB hits (top={best.score:.2f if best else 0}), attached={kb_attached}"}))

        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
            "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
            "timestamp": datetime.utcnow().isoformat(),
            "data": {"hits": len(hits), "top_score": best.score if best else 0}}, run_id=run_id)

        await redis.enqueue(7, run_id)

    except Exception as exc:
        log.exception("Agent 6 error: %s", exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
            "agent_num": AGENT_NUM, "data": {"error": str(exc)}}, run_id=run_id)
        await redis.enqueue(7, run_id)

async def _attach_kb_to_snow(sys_id: str, hit: ConfluenceKBHit) -> bool:
    if not SNOW_BASE: return False
    try:
        token = await get_snow_token()
        note  = f"[Sentinel KB] Relevant runbook: {hit.title}\n{hit.url}\nScore: {hit.score:.2f}\n\nExcerpt:\n{hit.excerpt}"
        async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=10) as c:
            resp = await c.patch(f"/api/now/table/incident/{sys_id}",
                headers={"Authorization": token, "Content-Type": "application/json"},
                json={"work_notes": note})
            return resp.status_code == 200
    except Exception as exc:
        log.warning("KB attach failed: %s", exc)
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
