# Sentinel — Healthcare IT Incident Orchestrator

Fully automated incident triage pipeline. Dynatrace alerts flow through 7 FastAPI microservices, get enriched via Splunk / ServiceNow / PagerDuty / Confluence, and reach LLM-powered root-cause resolution in under 60 seconds.

---

## Architecture

```
           ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────────┐
DT ──────► │ Agent 1 │──►│ Agent 2 │──►│ Agent 3 │──►│   Agent 4   │──► [5 ∥ 6] ──► 7
SNOW ─────►│ Ingest  │   │ Splunk  │   │  SNOW   │   │  PagerDuty  │
           └─────────┘   └─────────┘   └─────────┘   └─────────────┘
                │
                └── routing-db (:8000) — pipeline state, SSE, admin API
```

### Two flows

| Flow      | Trigger            | Agents |
|-----------|--------------------|--------|
| Primary   | DT P1/P2/P3        | 1→2→3→4→[5∥6]→7 |
| Secondary | SNOW P4/P5         | 1→2→[3∥6]→7 |

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
```

---

## Stack

Python 3.12 · FastAPI · asyncio · httpx · Redis 7 · React 18 · Vite · TypeScript · PostgreSQL 16 + pgvector · Docker Compose · Ollama / Anthropic / OpenAI (pluggable)

---

## Key Files

| File | Purpose |
|------|---------|
| `AGENTS.md` | Architecture spec and agent reference |
| `.env.example` | All environment variables |
| `docker/docker-compose.yml` | Full stack orchestration |
| `shared/` | Shared Python library (all agents) |
| `routing-db/` | Pipeline state service (:8000) |
| `webapp/` | React dashboard (:3000) |
| `scripts/` | Setup, demo, diagnostic utilities |

---

## Security Notes

- HMAC-SHA256 verification on all inbound webhooks (DT + SNOW)
- All SNOW API calls use OAuth2 with auto-refresh via `shared/snow_auth.py`
- `routing-db` write endpoints require `X-Admin-Token`
- PHI-adjacent data: embeddings always via local Ollama (never sent to cloud)
- Healthcare environment: see `AGENTS.md` §Compliance for BAA guidance

---

## Development

```bash
# Unit tests
pytest tests/ -m "not integration"

# Integration tests (requires running stack)
pytest tests/ -m integration

# Individual agent dev (hot-reload)
cd agents/Agent-1-dynatrace
uvicorn main:app --reload --port 8001
```
