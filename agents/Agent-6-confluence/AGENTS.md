# Agent 6 — Confluence KB Search & Attach

**Port:** `8006` · **Queue:** `agent:6:queue` · **Enqueues:** `agent:7:queue`

## Role

Performs Retrieval-Augmented Generation (RAG) over the knowledge base. Builds a
search query from the incident context, runs a pgvector cosine-similarity search,
scores the hits, and — when a hit is confident enough — attaches the matching
runbook to the SNOW incident as a work-note. Always enqueues Agent 7 (RCA).

## Query Construction

Concatenates the most informative signals into one search string:

```
{title} {service} {classification} {splunk.llm_summary}
```

## Vector Search

1. `embed_text(query)` → 768-dim vector via Ollama `nomic-embed-text` (local only)
2. `search_kb(vector, top_k=5, min_score=0.60)` → cosine search over `kb.chunks`
3. Results mapped to `ConfluenceKBHit` (page_id, title, url, score, excerpt)

## KB Attach Threshold

Only the top hit, and only if `score >= 0.80`, is attached to the SNOW incident:

```
[Sentinel KB] Relevant runbook: <title>
<url>
Score: 0.87

Excerpt:
<first 300 chars>
```

## Outputs

- Writes `ConfluenceEnrichment` (`query`, `hits[]`, `top_score`, `kb_attached`)
- Records step + enrichment in routing-db
- Publishes `agent_start` / `agent_done` (data includes hit count + top score)
- Enqueues Agent 7

## Embedding Privacy

Embeddings are **always** generated locally via Ollama, never sent to a cloud
LLM provider — regardless of `LLM_PROVIDER`. This keeps PHI-adjacent incident
text on-premises.

## Failure Behaviour

Non-fatal — emits `agent_error` and still enqueues Agent 7. A missing or empty
KB simply yields zero hits (`kb_attached: false`).

## Diagnostics

- `scripts/diagnose_agent6_cql.py "query"` — inspect raw search scores
- `scripts/verify_agent6_fix.py` — verify embeddings populated and search works
- `scripts/populate_vectors.py` — backfill embeddings for un-chunked documents

## Key Env Vars

`CONFLUENCE_BASE_URL`, `CONFLUENCE_TOKEN`, `SNOW_BASE_URL`, `OLLAMA_BASE_URL`,
`EMBED_MODEL`, `DATABASE_URL`, `AGENT_6_PORT`
