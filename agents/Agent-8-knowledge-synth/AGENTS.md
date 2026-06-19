# Agent 8 — Knowledge Synthesizer

**Port:** 8008 · **Trigger:** monthly cron (`SYNTH_SCHEDULE_CRON`, default day 1 02:00) + `POST /jobs/synthesize` for ad-hoc runs · **Off critical path** (no 60 s SLA).

## What it does

Extracts closed ServiceNow incidents from the previous month, scrubs PHI, clusters them per `assignment_group` using HDBSCAN, and synthesises versioned KB articles via the configured LLM. Articles land in `sentinel_synthesized_kb` (pgvector) and optionally in the `AUTO_KB` Confluence space. Consumers (Agents 2/6, chatbot) read pgvector directly — Agent 8 never pushes to them.

## Pipeline stages

```
extract → normalize → phi_scrub → embed → cluster → quality-gate
       → synthesize (LLM) → dedup → upsert → publish (Confluence) → retire
```

`orchestrator.py` is the only file that knows the full sequence. Each stage has its own module under `agents/Agent-8-knowledge-synth/`. `queries.py` is the only file with raw SQL. `main.py` is the only file with FastAPI routes + APScheduler.

## Endpoints

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `GET` | `/health` | none | liveness probe (returns `{"status":"ok","agent":"knowledge-synth","version":"0.1.0"}`) |
| `POST` | `/jobs/synthesize` | `X-Synth-Admin-Token` | run synthesis for a custom `{window_start, window_end}` window (used for backfill + debugging) |

## Configuration

See `.env.example` for the full `SYNTH_*` list. Key knobs:

- `SYNTH_MIN_CLUSTER_SIZE` (default 5) — lower for low-volume teams; raise to suppress noise on high-volume teams.
- `SYNTH_MIN_CLUSTER_COHESION` (default 0.65) — HDBSCAN cluster cohesion floor. Clusters below this are dropped.
- `SYNTH_QUALITY_SCORE_FLOOR` (default 0.40) — incidents below this quality score are filtered before clustering.
- `SYNTH_DEDUP_UPDATE_THRESHOLD` (0.92) and `SYNTH_DEDUP_REVIEW_THRESHOLD` (0.80) — cosine similarity bands. Above update → in-place version bump. Between thresholds → `decision='review'`. Below → new article.
- `SYNTH_LLM_MODEL` — default `llama3.1:70b`; override per provider (`claude-sonnet-4-...`, `gpt-4o`, …).
- `SYNTH_LLM_MAX_TOKENS_PER_RUN` (500k) — Reserved for a future cost-ceiling feature; current implementation does not enforce a per-run token budget. Configure your LLM provider's quota at the provider level for now.
- `SYNTH_PUBLISH_CONFLUENCE` — flip to `false` to disable Confluence side-effects while iterating on prompts.
- `SYNTH_CONFLUENCE_SPACE` (default `AUTO_KB`) — target space key for synthesised articles.
- `SYNTH_RETIRE_LOW_FEEDBACK` — when true, articles with low feedback signals are flagged for retirement at the end of each run.
- `SYNTH_ADMIN_TOKEN` — bearer token for `/jobs/synthesize`. Empty disables the endpoint.
- `SYNTH_MAX_CONCURRENT_SYNTHESIZE` (10) — semaphore limit for LLM fan-out within a run.

## Operational runbook

**Trigger an ad-hoc run:**
```bash
curl -X POST http://localhost:8008/jobs/synthesize \
  -H "X-Synth-Admin-Token: $SYNTH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"window_start":"2026-05-01","window_end":"2026-05-31"}'
```

**Backfill the previous N months** (uses the same endpoint, one window per month):
```bash
SYNTH_ADMIN_TOKEN="$(grep '^SYNTH_ADMIN_TOKEN=' .env | cut -d= -f2)" \
  python scripts/backfill_synthesized_kb.py --months 3
```

**Inspect the latest runs:**
```sql
SELECT run_id, status, counts, stage_durations, started_at, finished_at
FROM kb_synthesis_runs
ORDER BY started_at DESC
LIMIT 5;
```

**Inspect decisions for a specific run:**
```sql
SELECT decision, cluster_signature, article_id, similarity_score, notes
FROM kb_synthesis_decisions
WHERE run_id = '<uuid>';
```

**Review articles flagged for human attention:**
```sql
SELECT d.cluster_signature, d.similarity_score, kb.title, kb.assignment_group
FROM kb_synthesis_decisions d
LEFT JOIN sentinel_synthesized_kb kb ON kb.id = d.article_id
WHERE d.decision = 'review'
  AND d.created_at > NOW() - INTERVAL '60 days';
```

**Retire an article manually:**
```sql
UPDATE sentinel_synthesized_kb
SET is_active = FALSE, retired_at = NOW()
WHERE id = '<uuid>';
```

**Count active articles by assignment group:**
```sql
SELECT assignment_group, COUNT(*) AS active_articles
FROM sentinel_synthesized_kb
WHERE is_active = TRUE
GROUP BY assignment_group
ORDER BY active_articles DESC;
```

## Failure modes

- **LLM provider unreachable** — per-cluster `synthesize_one` returns `None`, decision row written with `decision='skip'`, run finalises as `succeeded` with `counts.skipped` populated. Re-run the window manually after the provider recovers.
- **SNOW throttle** — extraction retries via `shared/http_client.py` retry contract (2/4/8 s backoff). If the final attempt still fails, run finalises as `failed` with `error_message` set; no partial articles created.
- **Confluence publish failure** — article remains in pgvector with `is_active = TRUE` and `confluence_page_id = NULL`. The next run picks up unpublished articles and retries publication.
- **pgvector full-table scan slow at scale** — rebuild ivfflat indexes after every ~10× growth:
  ```sql
  REINDEX INDEX CONCURRENTLY idx_skb_title_vec;
  REINDEX INDEX CONCURRENTLY idx_skb_body_vec;
  ```
- **Token budget** — `SYNTH_LLM_MAX_TOKENS_PER_RUN` is reserved for a future cost-ceiling feature; current implementation does not enforce a per-run token budget. Configure your LLM provider's quota at the provider level for now.
- **APScheduler missed-fire** — APScheduler is configured with `misfire_grace_time=3600`; if Agent 8 was down at 02:00 and comes up by 03:00, the run still fires. Beyond that, trigger the missed window via the admin endpoint.

## Security

- `POST /jobs/synthesize` enforces `hmac.compare_digest` on the `X-Synth-Admin-Token` header.
- The PHI scrub layer (`phi_scrub.py`) is applied **before** any text crosses the LLM boundary. Patterns mirror Agent 1's chatbot scrubber (CLAUDE.md §10.6). Synthesised articles are scrubbed again on the way back from the LLM as a defence-in-depth pass.
- `SYNTH_ADMIN_TOKEN` lives in a secrets manager in production. `.env` is dev-only.

## See also

- Spec: [`docs/superpowers/specs/2026-06-19-agent-8-knowledge-synthesizer-design.md`](../../docs/superpowers/specs/2026-06-19-agent-8-knowledge-synthesizer-design.md)
- Plan: [`docs/superpowers/plans/2026-06-19-agent-8-knowledge-synthesizer.md`](../../docs/superpowers/plans/2026-06-19-agent-8-knowledge-synthesizer.md)
- Migration: [`migrations/003_synthesized_kb.sql`](./migrations/003_synthesized_kb.sql)
- Backfill script: [`../../scripts/backfill_synthesized_kb.py`](../../scripts/backfill_synthesized_kb.py)
