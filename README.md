# Sentinel — Healthcare IT Incident Orchestrator

Fully automated incident triage pipeline. Dynatrace alerts flow through 8 FastAPI microservices — 7 on the realtime critical path plus a monthly batch **Knowledge Synthesizer** — get enriched via Splunk / ServiceNow / PagerDuty / Confluence, and reach LLM-powered root-cause resolution in under 60 seconds. Agent 8 mines closed incidents each month and writes versioned KB articles into pgvector for Agents 2/6 and the chatbot to consume on subsequent runs.

---

## Architecture

```
           ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────────┐
DT ──────► │ Agent 1 │──►│ Agent 2 │──►│ Agent 3 │──►│   Agent 4   │──► [5 ∥ 6] ──► 7
SNOW ─────►│ Ingest  │   │ Splunk  │   │  SNOW   │   │  PagerDuty  │
           └─────────┘   └─────────┘   └─────────┘   └─────────────┘
                │
                └── routing-db (:8000) — pipeline state, SSE, admin API

           ┌──────────────────────────────────────┐
cron ────► │ Agent 8 — Knowledge Synthesizer :8008│ ──► pgvector (sentinel_synthesized_kb) + Confluence AUTO_KB
(monthly)  │ extract → scrub → cluster → LLM → KB │
           └──────────────────────────────────────┘
```

### Two realtime flows + one batch agent

| Flow / Agent | Trigger                         | Agents                       |
|--------------|---------------------------------|------------------------------|
| Primary      | DT P1/P2/P3                     | 1→2→3→4→[5∥6]→7              |
| Secondary    | SNOW P4/P5                      | 1→2→[3∥6]→7                  |
| Batch        | Monthly cron (default 1st @ 02:00) or `POST /jobs/synthesize` | 8 (off the 60-s SLA; reads closed SNOW incidents, writes pgvector + Confluence) |

---

## Quick Start

```bash
# 1. Configure
cp .env.example .env
# edit .env — set passwords, DT token, SNOW credentials

# 2. Start (with local Ollama LLM)
docker compose -f docker/docker-compose.yml --profile ollama up -d

# 3. Seed demo KB
python scripts/create_p1_runbooks_and_seed_kb.py

# 4. Test
python scripts/test_snow_webhook.py --priority 4

# 5. Open dashboard
open http://localhost:3000

# 6. (Optional) Backfill the previous 3 months of synthesised KB articles via Agent 8
SYNTH_ADMIN_TOKEN="$(grep '^SYNTH_ADMIN_TOKEN=' .env | cut -d= -f2)" \
  python scripts/backfill_synthesized_kb.py --months 3
```

---

## Stack

Python 3.12 · FastAPI · asyncio · httpx · Redis 7 · React 18 · Vite · TypeScript · PostgreSQL 16 + pgvector · Docker Compose · Ollama / Anthropic / OpenAI (pluggable)

---

## Key Files

| File | Purpose |
|------|---------|
| `CLAUDE.md` | Architecture spec and agent reference (canonical) |
| `.env.example` | All environment variables (incl. `SYNTH_*` for Agent 8) |
| `docker/docker-compose.yml` | Full stack orchestration (8 agents + routing-db + Postgres + Redis + webapp + Ollama) |
| `shared/` | Shared Python library (all agents) |
| `routing-db/` | Pipeline state service (:8000) |
| `webapp/` | React dashboard (:3000) — live, reports, chat |
| `agents/Agent-8-knowledge-synth/AGENTS.md` | Knowledge Synthesizer operational runbook |
| `scripts/backfill_synthesized_kb.py` | Agent 8: backfill N prior months on first deploy |
| `scripts/` | Setup, demo, diagnostic utilities |

---

## Security Notes

- HMAC-SHA256 verification on all inbound webhooks (DT + SNOW)
- All SNOW API calls use OAuth2 with auto-refresh via `shared/snow_auth.py`
- `routing-db` write endpoints require `X-Admin-Token`
- Agent 8 `/jobs/synthesize` requires `X-Synth-Admin-Token` (empty value disables the endpoint)
- PHI-adjacent data: embeddings always via local Ollama (never sent to cloud); Agent 8 scrubs PHI **before** any incident text crosses the LLM boundary
- Healthcare environment: see `CLAUDE.md` §10.6 for the security best-practices checklist

---

## Development

```bash
# Unit tests
pytest tests/ -m "not integration"

# Integration tests (requires running stack)
pytest tests/ -m integration

# Individual agent dev (hot-reload) — port = 8000 + agent number; Agent 8 = 8008
cd agents/Agent-1-dynatrace            # or Agent-2-splunk, ..., Agent-8-knowledge-synth
uvicorn main:app --reload --port 8001

# Agent 8 — ad-hoc synthesis run for a custom window
curl -X POST http://localhost:8008/jobs/synthesize \
  -H "X-Synth-Admin-Token: $SYNTH_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"window_start":"2026-05-01","window_end":"2026-05-31"}'
```
