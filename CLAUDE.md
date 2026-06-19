# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Sentinel — Master Specification

Fully automated healthcare IT incident triage pipeline. Dynatrace alerts flow
through 7 FastAPI microservices, enriched via DynaTrace / Splunk / ServiceNow / PagerDuty /
Confluence / RCA, resolved by LLM-powered RCA in under 60 seconds. A React dashboard renders the pipeline live over SSE.
The pipeline has **two entry points**. The primary flow (DT-originated, P1 / P2 / P3) runs the full 7-agent chain. The secondary flow (SNOW-originated, P4 / P5 manually created by customer support) runs an enrichment-only fan-out — no incident creation, no notification, just evidence-gathering work notes posted back to the existing SNOW record.

This system is deployed in a healthcare environment and processes incident data that may contain sensitive operational and patient-context information. This file is the canonical project guide. Read §1–4 to orient, §5–9 when implementing or debugging, §10–12 for VS Code.

Regulatory compliance policy (HIPAA Privacy Rule, Security Rule, BAA requirements) is maintained separately by your organisation's Privacy Officer and legal team. This file contains the **engineering best practices** that make the system secure by default — they are good practice regardless of regulatory context.

**Doc hierarchy.** `README.md` is the canonical quick-start. This `CLAUDE.md` is the architecture spec. Each component carries its own deep-dive in `agents/<Agent-N-name>/AGENTS.md`, `routing-db/AGENTS.md`, `webapp/AGENTS.md`, and `webapp/CHATBOT.md` — read those when working inside a single component. Personal overrides live in `Codex.local.md` (gitignored).

---



## Contents

1. Tech Stack
2. Repository Layout
3. Quick Start (VS Code)
4. Pipeline Architecture (4.1 Flow A · 4.2 Flow B · 4.3 Routing · 4.4 Agents · 4.5 Event Contract · 4.6 Work-note rule · 4.7 Webapp routes · 4.8 Chatbot)
5. Per-Agent Reference
6. Inter-Agent Contract
7. Shared State (7.1 Redis · 7.2 SNOW lifecycle · 7.3 Routing database)
8. External Integrations
9. Configuration Master Table (9.1 Required vars · 9.2 Tuning constants · 9.3 Chatbot vars)
10. Cross-Cutting Concerns
11. VS Code Workspace
12. Plugins
13. Coding Conventions
14. Implementation Notes & Open Questions

---

## 1. Tech Stack

Python 3.12 · FastAPI · asyncio · httpx · Redis · React 18 · Vite · Docker Compose · PostgreSQL (single `sentinel` DB — routing tables, pgvector RAG, Agent 6 feedback, chat history) · LLM provider: Anthropic Claude / OpenAI / Ollama (pluggable via `LLM_PROVIDER`)

---

## 2. Repository Layout

```
sentinel/
├── agents/
│   ├── Agent-1-dynatrace/                # Agent 1 — :8001 — webhook ingest, dedup, severity, flow router
│   ├── Agent-2-splunk/                   # Agent 2 — :8002 — log analysis + classification
│   ├── Agent-3-servicenow/               # Agent 3 — :8003 — INC create / bind, work-note flush
│   ├── Agent-4-pagerduty/                # Agent 4 — :8004 — on-call resolution, SNOW assignment, SLA
│   ├── Agent-5-notifications/            # Agent 5 — :8005 — Teams / Email / PD trigger / SMS
│   ├── Agent-6-confluence/               # Agent 6 — :8006 — KB search + scoring + attach
│   ├── Agent-7-Rca/                      # Agent 7 — :8007 — RCA, deployments, rollback, resolution monitor
│   └── requirements.txt                  # shared Python deps
│                                         # Each agent has its own AGENTS.md inside its directory
├── routing-db/                   # Routing service — :8000 — owns PostgreSQL routing tables, migrations, admin API
│   ├── app/
│   │   ├── api/{reads,admin}.py  # GET reads, POST admin (separate token)
│   │   ├── core/{config,logging,security}.py
│   │   ├── db/{connection,migrations,queries}.py
│   │   ├── models.py
│   │   └── main.py
│   ├── migrations/001_initial.sql
│   ├── seed/dev_seed.sql
│   ├── docker/Dockerfile
│   ├── requirements.txt
│   ├── .env.example
│   └── AGENTS.md
├── shared/
│   ├── models.py                 # OrchestratorEvent → … → RCAResult
│   ├── redis_client.py           # async Redis pool
│   ├── http_client.py            # shared httpx.AsyncClient
│   ├── auth.py                   # X-Agent{N}-Token validation
│   ├── snow_auth.py              # ServiceNow OAuth2 token cache (used by Agents 1, 3–7)
│   ├── routing_client.py         # HTTP client for routing-db
│   ├── embedding_client.py       # shared embedding generation for pgvector RAG
│   └── vector_client.py          # pgvector query helpers
├── webapp/                       # React dashboard — :3000
│   ├── src/
│   │   ├── routes/                # LiveView.tsx · ReportsView.tsx · ChatPage.tsx
│   │   ├── components/            # pipeline/ · reports/ · chat/ · shared/
│   │   ├── hooks/                 # useSSE · usePipelineRun · useReports · useChatStream · useCitationPane
│   │   ├── styles/                # tokens.css (design tokens + glow vars) · globals.css
│   │   └── lib/                   # api · events · formatters · chat
│   ├── package.json
│   ├── vite.config.ts
│   ├── AGENTS.md                  # dashboard guide (three routes, SSE, tokens, build)
│   └── CHATBOT.md                 # chatbot guide (full-page /chat, pluggable LLM, tools, v2 sketch)
├── tests/
│   ├── conftest.py
│   ├── test_pipeline.py
│   ├── test_classification.py
│   └── test_rca_scoring.py
├── scripts/
│   ├── test_snow_webhook.py            # fire test SNOW webhook (Flow B) into Agent 1
│   ├── snow_poller.py                  # polling fallback for environments where SNOW Business Rules can't post outbound
│   ├── verify_snow_schema.py           # pre-go-live: confirm Agent 3's required u_* fields exist on SNOW incident table
│   ├── audit_cmdb.py                   # pre-go-live: SNOW CI gap report for the entity catalogue
│   ├── demo_p1_incidents.py            # generate demo P1 incidents end-to-end
│   ├── generate_demo_incidents.py      # demo data generator
│   ├── populate_vectors.py             # backfill pgvector RAG tables
│   ├── sync_confluence_kb.py           # ingest Confluence KB into pgvector
│   ├── create_confluence_runbooks.py   # seed sample runbook pages
│   ├── create_p1_runbooks_and_seed_kb.py
│   ├── create_cas_runbook.py
│   ├── create_fidelis_portal_runbook.py
│   ├── inject_fidelis_portal_demo_logs.py
│   ├── diagnose_agent6_cql.py
│   ├── verify_agent6_fix.py
│   ├── migrate_sqlite_to_pg.py         # one-off: migrate routing data SQLite → Postgres
│   └── redis_server.py                 # local Redis dev launcher
├── docker/
│   ├── docker-compose.yml
│   └── docker-compose.override.yml
├── .vscode/                      # workspace settings (see §11)
├── .env.example
├── README.md                    # project overview (architecture, agents, quick start)
├── CLAUDE.md                    # this file
├── Codex.local.md               # personal overrides (gitignored)
└── pytest.ini                   # asyncio_mode=auto, markers: unit | integration
```

Per-agent layout (flat — consistent across all 7):

```
agents/Agent-<N>-<name>/
├── main.py                      # FastAPI app + routes + lifespan in one file
├── Dockerfile                   # built from repo root with this Dockerfile path
└── AGENTS.md                    # per-agent deep dive
```

No per-agent `app/`, `tests/`, or `.env.example`. Tests live at the top level
under `tests/`. Smoke / seed / diagnostic scripts live at top-level `scripts/`,
not under each agent. Shared code (event models, Redis/HTTP/auth clients,
routing-db client, vector helpers) is imported from `shared/` — every agent
runs with `PYTHONPATH` rooted at the repo root.


---

## 3. Quick Start (VS Code)

### Stack
```bash
# From repo root (preferred — matches README and CI):
docker compose -f docker/docker-compose.yml --profile ollama up -d
docker compose -f docker/docker-compose.yml logs -f
docker compose -f docker/docker-compose.yml down       # preserve volumes
docker compose -f docker/docker-compose.yml down -v    # drop ALL data
```

The `--profile ollama` flag starts a local Ollama container alongside the
7 agents + routing-db + Redis + Postgres + webapp. The default LLM provider in
`docker-compose.yml` is `LLM_PROVIDER=ollama` with `OLLAMA_MODEL=llama3.1:8b`
and `EMBED_MODEL=nomic-embed-text`. **Embeddings always run locally via Ollama**
regardless of which chat provider is selected — never sent to a cloud LLM. To
swap chat to Anthropic or OpenAI, set `LLM_PROVIDER` + the matching
`*_API_KEY` / `*_MODEL` env vars; embeddings stay local.

On first `up`, the `routing-db` service applies migrations and seeds defaults inside its own startup lifespan. Its `/health` returns 503 until both steps complete; agents that depend on it (Agents 1, 6, 7) wait for `service_healthy` before booting. Subsequent `up`s re-apply migrations idempotently and skip seeding because `entity_project_map` is non-empty.

### Routing DB import (bulk)
```bash
# Convert your catalogue CSV to JSON and POST to the admin endpoint
jq -n --slurpfile rows <(csvjson entities.csv) \
     '{rows: $rows[0], override_admin: false}' \
  | curl -X POST http://localhost:8000/v1/admin/import \
         -H "X-Routing-Admin-Token: $ROUTING_DB_ADMIN_TOKEN" \
         -H "Content-Type: application/json" \
         -d @-
```

By default this preserves rows whose `source = 'admin'` (runtime overrides made via the same admin endpoint). Pass `override_admin: true` when the catalogue is meant to be authoritative. See `routing-db/AGENTS.md` for the precedence rules.

### Single agent (dev — hot reload)
```bash
# Each agent is a flat single-file FastAPI app — `main.py`, not `app.main`.
cd agents/Agent-1-dynatrace            # or Agent-2-splunk, …, Agent-7-Rca (capital R)
pip install -r ../requirements.txt
export PYTHONPATH=$(pwd)/../..         # shared/ must be importable
uvicorn main:app --reload --port 8001  # port = 8000 + agent number; 8000 is routing-db
```

### Webapp (dev)
```bash
cd webapp
npm install
npm run dev                     # Vite on :3000
```

### Tests
```bash
pytest tests/ -m "not integration"                          # unit-only, no stack needed
pytest tests/ -m integration                                # requires `docker compose ... up -d`
pytest tests/test_pipeline.py::test_all_agents_healthy -v   # single test by node id
pytest tests/test_classification.py -v                      # Agent 2 scoring
pytest tests/test_rca_scoring.py -v                         # Agent 7 RCA weights
```

Markers (`unit`, `integration`) are declared in `pytest.ini`. There are no
per-agent `tests/` directories — all tests live at the repo root.

### Seed and smoke
```bash
# Seed Confluence-backed runbooks into pgvector (do this once after first `up`):
python scripts/create_p1_runbooks_and_seed_kb.py

# SNOW (Flow B, P4/P5) smoke test:
python scripts/test_snow_webhook.py --priority 4

# End-to-end P1 demo:
python scripts/demo_p1_incidents.py

# Pre-go-live checks:
python scripts/verify_snow_schema.py    # confirms u_* fields exist on SNOW incident table
python scripts/audit_cmdb.py            # SNOW CI gap report
```

---

## 4. Pipeline Architecture

The system has two flows triggered by two entry points. Both run on the same 7 agents — the second flow is a subset.

### 4.1 Flow A — DT-originated (P1 / P2 / P3)

End-to-end flow with per-stage time budget. Total wall-clock target: **<60 s**. P1 PagerDuty page lands at the **~47 s** mark (Agent 5).

```
Dynatrace Webhook ─POST /webhook/dynatrace──┐
                                            ▼
[1] Ingest      :8001  <2s   ─OrchestratorEvent───┐
    │ + SNOW pre-existence check (8h window)      │
    ▼                                             ▼
[2] Classify    :8002  <20s  ─ClassifiedEvent─────┐
                                                  ▼
[3] Create INC  :8003  <5s   ─IncidentCreatedEvent┐
    │ (or BIND to DT-auto-created INC if found)   │
    ▼                                             ▼
[4] Assign      :8004  <5s   ─AssignedEvent───────┐
                                                  ▼
[5] Notify      :8005  <5s   ─NotifiedEvent───────┐  ◄── PD page (P1/P2/P3, ~47s for P1)
                                                  ▼
[6] KB          :8006  <10s  ─KBEnrichedEvent─────┐
                                                  ▼
[7] RCA         :8007  <15s  ─SNOW final RCA + rollback
```

Splunk tier-1 in Agent 2 dominates the critical path (up to 20 s). Agents 3–5 must stay tight to keep the page under 47 s. Agents 6–7 run after the page is delivered, so they can be slower without breaching the user-facing SLA.

### 4.2 Flow B — SNOW-originated (P4 / P5)

Customer support manually creates a SNOW incident at P4 or P5 (no DT alert exists). Agent 1 receives a SNOW webhook on incident creation, identifies the `u_source_tool ≠ Dynatrace` case, and fans out an enrichment-only request to Agents 2, 6, and 7 in parallel. No new INC is created, no notification fires, no team reassignment happens — support already owns the ticket.

```
SNOW Incident Created (manual, P3–P5)
        │
        ▼  POST /webhook/snow-incident
        │  X-SNOW-Token
        ▼
[1] Ingest :8001  <2s   ─ManualIncidentEvent (synthetic)
        │
        ├──fan-out──┬──────────────┬──────────────┐
        ▼           ▼              ▼              ▼
   [2] Splunk   [6] Confluence  [7] rca      [7] DT Logs
   evidence     KB articles     deployments   matching events
        │           │              │              │
        └────────┬──┴──────────────┴──────────────┘
                 ▼  combined work note
            ServiceNow incident updated
            (no INC create, no PD, no Teams)
```

Each enrichment agent posts its own attributed work note. There is no terminal RCA verdict — the engineer reads the four work notes and decides. Total target: <30 s.

### 4.3 Routing decision (Agent 1)

Agent 1 inspects the entry point and the incoming priority/source to choose the flow:

| Source             | Priority    | `u_source_tool` | Action                                                          |
|--------------------|-------------|-----------------|-----------------------------------------------------------------|
| DT webhook         | P1, P2, P3  | n/a             | Flow A (full pipeline). Pre-check SNOW for INC in last 8 h.     |
| SNOW webhook       | P1, P2, P3  | `Dynatrace`     | DT auto-created the INC. Bind Redis to existing INC; suppress duplicate. |
| SNOW webhook       | P1, P2, P3  | not Dynatrace   | Reject / route to ops review (unexpected manual entry at high priority). |
| SNOW webhook       | P4, P5      | not Dynatrace   | Flow B (enrichment-only fan-out).                              |
| SNOW webhook       | P4, P5      | `Dynatrace`     | Anomaly — DT shouldn't auto-create at P4/P5. Log + ops review.  |

### 4.4 Agents

| # | Agent       | Port | Responsibility                                                                                  |
|---|-------------|------|------------------------------------------------------------------------------------------------|
| 1 | dynatrace   | 8001 | DT webhook + SNOW webhook; HMAC; schema; dedup; SNOW pre-existence; severity map; DT enrich; flow router |
| 2 | splunk      | 8002 | Splunk 3-tier search, regex scoring, conflict resolution; enrichment-only mode for Flow B       |
| 3 | servicenow  | 8003 | SNOW Table API + CMDB; idempotent INC create; **bind-to-existing** when DT auto-created the INC |
| 4 | pagerduty   | 8004 | Routing matrix, PD on-call schedule, SNOW PATCH (assignment + SLA); team-lead fallback (Flow A only) |
| 5 | notifications | 8005 | Parallel Teams/Email/PD trigger/SMS; PD for P1/P2/P3; P4+ suppressed (Flow A only)            |
| 6 | confluence  | 8006 | Confluence 3-tier CQL, 6-factor scoring, KB attach; same in both flows                          |
| 7 | rca         | 8007 | Rca + Splunk + Davis + DT Logs; 4-signal RCA in Flow A; deployment+log scan in Flow B        |
| – | webapp      | 3000 | React dashboard — routes: `/` live view, `/reports` aggregates, `/chat` full-page assistant |

The agent **named** for a vendor is not the only consumer of that vendor's API. SNOW, for instance, is written to by agents 3, 4, 5, 6, and 7 — only agent 3 is named for it because it owns the create/bind. PagerDuty Schedules is queried by agent 4; PagerDuty Events v2 is fired by agent 5. The naming reflects primary ownership, not exclusivity. See §8 for the full integration matrix.

### 4.5 Event Contract Chain (`shared/models.py`)

| Stage | Type                       | Added by | Key new fields |
|-------|----------------------------|----------|----------------|
| 1     | `OrchestratorEvent`        | 1        | `priority_*`, `splunk_index`, `entity_*`, `host_name`, `pipeline_run_id`, `flow="A"` |
| 1     | `ManualIncidentEvent`      | 1        | `incident_number`, `incident_sys_id`, `short_description`, `cmdb_ci`, `flow="B"` |
| 2     | `ClassifiedEvent`          | 2        | `error_category`, `assigned_team`, `assigned_queue`, `snow_*`, `classification_confidence`, `matched_log_lines` |
| 3     | `IncidentCreatedEvent`     | 3        | `incident_number`, `incident_sys_id`, `incident_url`, `incident_created_at`, `was_bound=true/false` |
| 4     | `AssignedIncidentEvent`    | 4        | `assignee_*`, `assignment_group_*`, `on_call_source`, `assignment_work_note` |
| 5     | `NotifiedIncidentEvent`    | 5        | `channels_attempted/delivered/failed`, `pagerduty_dedup_key`, `p1_escalation_scheduled` |
| 6     | `KBEnrichedIncidentEvent`  | 6        | top-3 article objects with score, tier, extracted steps + code blocks |
| 7     | (terminal — writes SNOW)   | 7        | RCA verdict, confidence breakdown, rollback command, resolution monitor handle |

Nothing is removed between stages. Adding fields is backward-compatible; renaming or removing is breaking and requires the next agent to update first.

### 4.6 Work-note-on-create rule

**Every agent must post an attributed work note to the SNOW incident as soon as the incident exists** (i.e., from Agent 3 onward in Flow A; from Agent 1 dispatch onward in Flow B). The work note documents what the agent did, how long it took, what evidence it found, and what it forwarded downstream.

Work-note format (consistent across all agents):

```
=== <STAGE NAME> — Agent <N> ===
Timestamp : <ISO-8601>
Status    : <success | degraded | failed>
Duration  : <ms>
Pipeline  : <pipeline_run_id>

<stage-specific body>
```

Agents 1 and 2 run before the SNOW INC exists in Flow A. Their work-note payloads are buffered in Redis under `pending_worknote:{problemId}` and flushed by Agent 3 immediately after `POST /api/now/table/incident` returns.

### 4.7 Webapp — three routes

The React dashboard lives at `:3000` and talks **only to Agent 1**. No agent tokens in the browser, no direct calls to routing-db or to Agents 2–7. All traffic is `X-Dashboard-Token` gated at Agent 1.

| Route | Purpose | Data source |
|-------|---------|------------|
| `/` | Live pipeline visualization — SSE problem snapshots, both Flow A and B | Redis `problem_snapshot:{problemId}` via `GET /api/events` on Agent 1 |
| `/reports` | Historical aggregates — MTTR, volume by priority, classification matrix, KB effectiveness | Postgres via Agent 1 reports API |
| `/chat` | Full-page conversational assistant — read-only queries on runs, reports, runbooks | Agent 1 chat orchestrator + LLM provider |

**Dashboard API endpoints** (implemented in `agents/Agent-1-dynatrace/app/api/dashboard.py`):

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| `GET` | `/api/events` | `?token=<DASHBOARD_TOKEN>` (query param — EventSource limitation) | SSE stream; polls Redis every 2 s; pushes `{ problems: [...], ts: "..." }` |
| `GET` | `/api/pipeline/{problem_id}` | `X-Dashboard-Token` header | Full snapshot for one problem; enriched with SNOW binding |

**problem_snapshot Redis key** — written by Agent 1 (`deduplication.store_problem_snapshot`) after publishing to Agent 2, cleared on RESOLVED. Contains only safe structural fields (§10.6): `problem_id`, `entity_id`, `entity_name`, `entity_type`, `severity`, `priority`, `current_stage`, `pipeline_run_id`, `started_at`. No free-text alert titles or descriptions.

**Why query-param auth for SSE.** The browser `EventSource` API does not support setting custom request headers. Agent 1's `/api/events` therefore accepts the dashboard token as a `?token=` query parameter. The `X-Dashboard-Token` header is accepted on all other dashboard endpoints. Do not expose the token in logs — it appears in the URL query string which may be logged by nginx; configure nginx to strip query strings from access logs for this path.

### 4.8 Chatbot — architecture and scope

The chatbot at `/chat` is a read-only research interface. It answers questions about pipeline runs, reports, and runbooks. It never takes actions (no acknowledge, no reassign, no rollback in v1 — see `webapp/CHATBOT.md` for the v2 approval-gate sketch).

**The chatbot endpoint lives on Agent 1** (`POST /api/chat/stream`). This keeps the webapp's single-service contract intact and reuses Agent 1's auth and rate-limiting. Agent 1 acts as the LLM orchestrator: it validates the token, builds the conversation context, calls the configured LLM provider with tools, dispatches tool calls to internal services, and streams token events back to the browser via SSE.

**Tool surface (all read-only):**

| Tool | Calls | What it returns |
|------|-------|----------------|
| `get_run(run_id)` | Redis `pipeline_run:{runId}` | Full event sequence + RCA + work-note attribution |
| `search_runs(service?, priority?, since_hours, flow?)` | Postgres | Completed runs matching filters |
| `search_kb(query, team?)` | Agent 6 (Confluence 3-tier CQL) | Top-N KB articles with scores and resolution steps |
| `get_reports_kpi(metric, since_hours)` | Postgres | MTTR, p95 duration, classification accuracy, verdict distribution |
| `get_routing_info(entity_id?, entity_name?)` | routing-db `GET /v1/entities` | Entity → GitLab project mapping |

**Pluggable LLM provider.** All three providers are equal citizens — none is privileged. Switch with `LLM_PROVIDER=anthropic|openai|ollama`. The `LLMProvider` Protocol in `agents/dynatrace/app/services/llm/` has three implementations: `anthropic_provider.py`, `openai_provider.py`, `ollama_provider.py`. The orchestrator code never sees provider-specific JSON shapes; each adapter maps to the internal `StreamEvent` type. The provider contract test suite (`tests/chat/test_provider_contract.py`) runs identical scenarios against all three on every CI run.

**Grounding rules.** The system prompt enforces three rules: (1) no answer without a tool call when one applies — the model cannot answer incident questions from training data alone, (2) every factual claim carries a citation (run ID, KB article URL, deployment SHA) that the browser renders as a link, (3) when data is missing (expired Redis key, unknown entity), the bot says so explicitly rather than guessing.

**Conversation persistence.** Chat history is persisted in Postgres (`chat_conversations` + `chat_messages` tables in Agent 1's migrations). Conversations are keyed on the user's dashboard token identity. `CHAT_PERSISTENCE_BACKEND=none` falls back to session-only memory for lightweight environments.

**SSE event types for chat** (same pipe as pipeline events, different vocabulary):

```
text_delta    → token from the model
tool_use      → model invoking a tool (renders ToolUseIndicator in UI)
tool_result   → tool output returned
citation      → factual reference (run_id, kb_id, label, preview payload)
message_done  → end of response (includes provider name + token count)
error         → tool loop exceeded, provider error, rate limit
heartbeat     → keep-alive every 15s
```

**Full detail in `webapp/CHATBOT.md`.** That file covers the full-page UI layout (two-column: conversation 60% / citation pane 40%), keyboard shortcuts, provider contract tests, security and abuse model, conversation persistence schema, local config, and the v2 action-gate sketch.

---

## 5. Per-Agent Reference

### 5.1 Agent 1 — Dynatrace (`:8001`)

Pipeline entry. Two intake endpoints: `POST /webhook/dynatrace` (Flow A) and `POST /webhook/snow-incident` (Flow B). Both must always return HTTP 200 to their caller (DT and SNOW both retry-storm on non-200). Only exception is `401` on auth failure.

**Flow A stages (DT webhook):** HMAC auth → schema validate → state filter (`OPEN`/`RESOLVED`/`MERGED`) → Redis dedup (`SET NX EX`, 8 h) → **SNOW pre-existence check** (query `incident` table for `u_source_alert_id = problemId` with `sys_created_on > now-8h`) → severity map → DT Entities enrichment (graceful 404 fallback) → Splunk index derivation → publish to Agent 2.

**Flow B stages (SNOW webhook):** HMAC auth → schema validate → priority filter (P4/P5 only; reject P1/P2/P3 with non-DT source) → check `u_source_tool` (must NOT be Dynatrace) → build `ManualIncidentEvent` from SNOW record (entity inferred from `cmdb_ci`, error keywords from `short_description`) → fan-out POST to Agents 2, 6, 7 with header `X-Flow-Mode: enrichment-only`.

**SNOW pre-existence check (Flow A).** Before publishing to Agent 2, Agent 1 queries SNOW for any incident with `u_source_alert_id = problemId` created in the last 8 hours and not in state `Closed` or `Resolved`. Three outcomes:

| Pre-existence result            | Action                                                                              |
|---------------------------------|-------------------------------------------------------------------------------------|
| No matching INC                 | Normal flow — Agent 3 will create the INC                                           |
| Matching INC, source=Dynatrace  | Bind: write `snow_incident:{problemId}` to Redis with the existing INC; Agent 3 will detect the binding and skip create, fall through to enrichment work notes |
| Matching INC, other source      | Log warning, proceed to create anyway; ops review                                   |

Failure-mode contract:

| Failure                  | Response  | Downstream effect |
|--------------------------|-----------|-------------------|
| Bad/missing auth token   | 401       | DT or SNOW shows auth error |
| Schema invalid           | 200 + DLQ | no processing |
| Redis down               | 200 + log | possible duplicate INC |
| DT Entities API 404/500  | 200       | enrichment skipped, defaults used |
| SNOW pre-existence query fails | 200 | proceed without binding; Agent 3 will catch dup via its own Redis check |
| Agent 2 unreachable      | 200 + DLQ | pipeline halted at boundary |

### 5.2 Agent 2 — Splunk (`:8002`)

Brain of the routing decision in Flow A. Six phases: pre-classify (DT eventType + entityType → hypothesis) → 3-tier query build → Splunk async submit/poll/fetch → regex scoring (3 rule sets, weights 1.0/1.2/1.1 for app/infra/db) → conflict resolution (Splunk wins iff confidence ≥ 65%) → routing pre-compute.

Splunk async pattern: `POST /search/jobs` → poll `/jobs/{sid}` until `dispatchState=DONE` (max 10×2s) → `GET /jobs/{sid}/results`. Tier 1 short-circuits if it returns enough evidence.

The 65% threshold matters: a database failure surfacing as `FAILURE_RATE_INCREASED` in DT is re-routed to the DBA team only when Splunk evidence is unambiguous. Below 65% the DT hypothesis is preserved.

**Flow B (enrichment-only mode).** When called with header `X-Flow-Mode: enrichment-only` from Agent 1, Agent 2 skips routing pre-compute and instead writes a Splunk-evidence work note directly to the SNOW incident: matching log lines, error patterns detected, and the Splunk query used. No downstream forward to Agent 3.

### 5.3 Agent 3 — ServiceNow (`:8003`)

First write to a system of record. **Idempotent by design** — checks `snow_incident:{problemId}` in Redis before every attempt, including retries.

Eight stages: dedup check → CMDB sys_id resolve (1 h cache) → impact/urgency map → short/long description build → `POST /api/now/table/incident` → write Redis bindings (forward + reverse, 24 h TTL) → exponential backoff retry (2/4/8 s) → flush buffered work notes from Agents 1 and 2.

**Bind-to-existing path.** If the dedup check finds `snow_incident:{problemId}` already populated by Agent 1's SNOW pre-existence check (DT auto-created the INC), Agent 3 skips the POST entirely. It instead patches the existing INC with the structured long-description block (DT problem details, classification, log evidence) as the first work note, then forwards the `IncidentCreatedEvent` (with `was_bound=true`) to Agent 4. Downstream agents cannot tell the difference.

SNOW custom fields required on the incident table: `u_source_tool`, `u_source_alert_id`, `u_dynatrace_url`, `u_classification_confidence`. Run `scripts/audit_cmdb.py` pre-go-live to identify missing CIs — gaps cause soft failure (work note posted, pipeline continues).

### 5.4 Agent 4 — PagerDuty (`:8004`)

Routing executor. Seven stages: routing matrix lookup → PagerDuty Schedules API → email → SNOW sys_user → PATCH `assignment_group` + `assigned_to` + `state=2` + start SLA → P1 escalation flag → routing work note.

PagerDuty schedule failure falls back to a static team-lead email per team. Configure these before go-live or P1 incidents during a PD outage route to no one.

### 5.5 Agent 5 — Notifications (`:8005`)

Where the 47-second guarantee lives. PagerDuty and notifications are restricted to P1, P2, and P3. P4 and P5 get nothing — those are SNOW-originated manual tickets that customer support already owns.

| Priority | Category    | Teams | Email | PD  | SMS |
|----------|-------------|-------|-------|-----|-----|
| P1       | infra       | ✓     | ✓     | ✓   | ✓   |
| P1       | app/db      | ✓     | ✓     | ✓   | —   |
| P2       | any         | ✓     | ✓     | ✓   | —   |
| P3       | any         | ✓     | ✓     | ✓   | —   |
| P4–P5    | any         | —     | —     | —   | —   |

(Agent 5 is not invoked at all in Flow B. P4/P5 incidents are SNOW-originated and bypass this agent entirely.)

All channels deliver in parallel via `asyncio.gather`. Per-channel retry (3×, exponential) is independent — a Teams failure doesn't block email. Teams success is response body `"1"` literal, not JSON. PagerDuty `dedup_key = problemId` enables auto-resolve when DT closes the problem.

P1 ack-check: scheduled 5 min after delivery; if not acknowledged, manager paged via email + SMS and escalation work note posted to SNOW. P2 and P3 do not trigger ack-check — only the on-call notification is sent.

### 5.6 Agent 6 — Confluence (`:8006`)

Confluence search + extraction + scoring. Six-factor model:

| Factor              | Weight | Signal |
|---------------------|--------|--------|
| `term_match`        | 30%    | search terms in title (×2) and body |
| `structural_match`  | 20%    | runbook/troubleshooting label + resolution headings |
| `tier_bonus`        | 15%    | 1.0 / 0.6 / 0.3 for tiers 1 / 2 / 3 |
| `recency`           | 15%    | step function on last-update age |
| `completeness`      | 10%    | ≥3 ordered steps + code block + ≥150 words |
| `space_relevance`   | 10%    | article in correct team's Confluence space |

Articles >18 months stale are flagged but still surfaced. Step-signature dedup collapses re-titled runbooks. Top 3 attached to SNOW via `kb_use` table; recommendation + (later) feedback recorded in `kb_recommendations` Postgres table for monthly weight tuning.

**Flow B (enrichment-only mode).** Same scoring, same attachment behaviour, but the search terms are derived from the manual incident's `short_description` and `cmdb_ci` rather than from a `ClassifiedEvent`. Agent 6 posts its own work note and does not forward to Agent 7.

### 5.7 Agent 7 — Rca (`:8007`)

Four-signal weighted confidence:

```
confidence = temporal × 35  +  file_match × 30  +  log_timeline × 20  +  davis/100 × 15
```

| Confidence | Verdict                          | Action |
|------------|----------------------------------|--------|
| ≥ 75%      | `deployment_regression`          | rollback recommended; prepended ahead of KB steps |
| ≥ 50%      | `probable_deployment_regression` | investigate before rolling back |
| ≥ 25%      | `uncertain`                      | flagged for review |
| < 25%      | `non_deployment`                 | KB steps stand alone |

Lookback adapts to category (24 h app / 48 h db / 72 h infra). Error onset uses precise DT Problems API `startTime`, not webhook timestamp — Davis confirmation makes the webhook several minutes late.

Resolution monitor: polls `/api/v2/problems/{id}` every 5 min for up to 6 h; on `CLOSED`, writes `state=7` to SNOW and clears Redis bindings.

The `entity_project_map` table (§7.3) maps Dynatrace entity IDs (preferred) and names to GitLab project paths, served by the routing-db service. Agent 7 prefers `entity_id` lookup because Dynatrace entity names can change on rename — the ID is stable. Without a mapping entry, temporal/file signals collapse to 0 and the verdict becomes `non_deployment` regardless of ground truth. New services need an entry added via the routing-db admin API (`POST /v1/admin/import` from CI, or `POST /v1/admin/entities` for runtime overrides) before Agent 7 can correlate their deployments.

**Flow B (enrichment-only mode).** When called by Agent 1 directly with a `ManualIncidentEvent`, Agent 7 runs two queries instead of the full 4-signal model: (1) **GitLab recent deployments** for the inferred service in a 24-hour lookback, and (2) **Dynatrace Grail DQL** (`POST /platform/storage/query/v1/query:execute`) for matching log events around the incident's `opened_at`. The DQL adapter lives at [`agents/Agent-7-Rca/dt_grail_client.py`](agents/Agent-7-Rca/dt_grail_client.py) — OAuth2 client-credentials grant against `DT_OAUTH_TOKEN_URL` with scope `storage:logs:read`, bearer cached in-memory until expiry minus 60 s. Posts a single work note: "Recent deployments: …; matching DT log events: …". No confidence verdict, no rollback recommendation — the engineer interprets the evidence themselves. On trial tenants without a Platform OAuth client, set `DT_LOGS_ENABLED=false` and the deployments-only path runs (degradable per §10.2).

---

## 6. Inter-Agent Contract

### 6.1 Auth secret naming — read carefully

Every hop has **one shared secret** living under **two names** depending on which agent's `.env` you're reading:

| Hop          | Sender's `.env` key | Receiver's `.env` key   | Header sent                |
|--------------|---------------------|-------------------------|----------------------------|
| DT  → Ag 1   | (Dynatrace config)  | `DT_WEBHOOK_SECRET`     | `X-Dynatrace-Token`        |
| Ag 1 → Ag 2  | `AGENT2_SECRET`     | `AGENT1_SHARED_SECRET`  | `X-Agent1-Token`           |
| Ag 2 → Ag 3  | `AGENT3_SECRET`     | `AGENT2_SHARED_SECRET`  | `X-Agent2-Token`           |
| Ag 3 → Ag 4  | `AGENT4_SECRET`     | `AGENT3_SHARED_SECRET`  | `X-Agent3-Token`           |
| Ag 4 → Ag 5  | `AGENT5_SECRET`     | `AGENT4_SHARED_SECRET`  | `X-Agent4-Token`           |
| Ag 5 → Ag 6  | `AGENT6_SECRET`     | `AGENT5_SHARED_SECRET`  | `X-Agent5-Token`           |
| Ag 6 → Ag 7  | `AGENT7_SECRET`     | `AGENT6_SHARED_SECRET`  | `X-Agent6-Token`           |

Convention: the **sender** holds `AGENT{N+1}_SECRET` (next hop's secret); the **receiver** holds `AGENT{N}_SHARED_SECRET` (its own intake secret). For each pair the values must be byte-identical. All comparisons use `hmac.compare_digest`.

### 6.2 Intake endpoints

| Agent | Method | Path                              | Auth Header        | Notes                       |
|-------|--------|-----------------------------------|--------------------|-----------------------------|
| 1     | POST   | `/webhook/dynatrace`              | `X-Dynatrace-Token`| Flow A entry                |
| 1     | POST   | `/webhook/snow-incident`          | `X-SNOW-Token`     | Flow B entry (or P1/P2/P3 bind)|
| 2     | POST   | `/intake/orchestrator-event`      | `X-Agent1-Token`   | Flow A                      |
| 2     | POST   | `/intake/enrichment`              | `X-Agent1-Token`   | Flow B; `X-Flow-Mode: enrichment-only` required |
| 2     | POST   | `/intake/resolve`                 | `X-Agent1-Token`   |                             |
| 3     | POST   | `/intake/classified-event`        | `X-Agent2-Token`   | Flow A; bind path also enters here |
| 4     | POST   | `/intake/incident-created-event`  | `X-Agent3-Token`   | Flow A only                 |
| 5     | POST   | `/intake/assigned-incident-event` | `X-Agent4-Token`   | Flow A only; gated on P1/P2/P3 |
| 6     | POST   | `/intake/notified-incident-event` | `X-Agent5-Token`   | Flow A                      |
| 6     | POST   | `/intake/enrichment`              | `X-Agent1-Token`   | Flow B fan-out target       |
| 7     | POST   | `/intake/kb-enriched-event`       | `X-Agent6-Token`   | Flow A                      |
| 7     | POST   | `/intake/enrichment`              | `X-Agent1-Token`   | Flow B fan-out target       |
| 1     | GET    | `/api/events`                     | `?token=` query param | Dashboard SSE stream (EventSource limitation — no header support) |
| 1     | GET    | `/api/pipeline/{problem_id}`      | `X-Dashboard-Token`| Dashboard problem detail    |
| all   | GET    | `/health`                         | none               |                             |
| all   | GET    | `/metrics`                        | none               |                             |

### 6.3 Retry & idempotency

Every agent uses the same retry contract for outbound calls:

| Attempt | Wait | Retried on    |
|---------|------|---------------|
| 1       | 0 s  | —             |
| 2       | 2 s  | 5xx, timeouts |
| 3       | 4 s  | 5xx, timeouts |
| 4       | 8 s  | 5xx, timeouts |

After attempt 4, failure routes to DLQ. **4xx is never retried** — it's a contract bug, not transient.

Idempotency is explicit only in Agent 3 (Redis check before SNOW POST) but the property holds end-to-end: every agent keys off `problem_id` / `incident_number` and writes deterministic Redis bindings. Replays of the full pipeline for the same `problem_id` find existing bindings and short-circuit.

---

## 7. Shared State

### 7.1 Redis key schema

| Key                                | Writer  | Readers      | TTL  | Purpose |
|------------------------------------|---------|--------------|------|---------|
| `active_problem:{problemId}`       | Ag 1    | Ag 1         | 8 h  | Dedup of in-flight DT problems (`SET NX EX`) |
| `snow_incident:{problemId}`        | Ag 1, 3 | Ag 4–7       | 24 h | INC↔problem binding `{incident_number, incident_sys_id, incident_url, was_bound}`. Ag 1 writes when SNOW pre-existence finds a DT-auto-created INC; Ag 3 writes on create. |
| `problem_id:{incidentNumber}`      | Ag 3    | Ag 7 (close) | 24 h | Reverse lookup for resolution close |
| `cmdb_ci:{entityName}`             | Ag 3    | Ag 3         | 1 h  | CMDB sys_id cache |
| `pipeline_run:{runId}`             | all     | dashboard    | 24 h | Per-stage event store for SSE |
| `pd_ack:{problemId}`               | Ag 5    | Ag 5         | 1 h  | P1 ack-check scheduling state |
| `pending_worknote:{problemId}`     | Ag 1, 2 | Ag 3         | 1 h  | Buffered work notes for stages that ran before the SNOW INC existed; flushed by Agent 3 |
| `flow_b_run:{incidentNumber}`      | Ag 1    | dashboard    | 24 h | Tracking record for SNOW-originated enrichment runs (no problemId exists) |

Pool: `shared/redis_client.py` exposes a single async client. Connect once; reuse across requests.

### 7.2 ServiceNow incident lifecycle — who writes what

**Flow A — DT-originated:**

| When        | Field / Action                                                                                        | Written by |
|-------------|-------------------------------------------------------------------------------------------------------|------------|
| Create (or bind) | `category`, `subcategory`, `impact`, `urgency`, `cmdb_ci`, `short/long description`, `u_*`, `state=1` | Ag 3 (or DT directly when auto-created) |
| Immediately after create | Buffered work notes from Ag 1 (ingest) + Ag 2 (classification)                              | Ag 3 (flushes `pending_worknote:{problemId}`) |
| +T(1–3 s)   | `assignment_group`, `assigned_to`, `state=2`, `business_stc=1`, routing work note                     | Ag 4       |
| +T(3–8 s)   | Notification work note (Teams/Email/PD/SMS receipt) — only for P1/P2/P3                              | Ag 5       |
| +T(8–18 s)  | KB attachments (`kb_use` table) + KB work note                                                        | Ag 6       |
| +T(18–35 s) | RCA work note + rollback command + unified plan                                                       | Ag 7       |
| When DT closes | `state=7`, `close_code="Solved (Permanently)"`                                                     | Ag 7 (resolution monitor) |

**Flow B — SNOW-originated (P4–P5):** Incident already exists. The four enrichment agents post work notes in parallel; whichever finishes first writes first.

| When        | Field / Action                                                | Written by |
|-------------|---------------------------------------------------------------|------------|
| +T(0 s)     | "Enrichment dispatched" stub note                             | Ag 1       |
| +T(2–5 s)   | Splunk evidence work note                                     | Ag 2       |
| +T(2–10 s)  | KB articles + work note                                       | Ag 6       |
| +T(2–15 s)  | GitLab recent deployments + DT log events work note           | Ag 7       |
| +T(15 s)    | (optional) Aggregated summary note linking the four above     | Ag 1       |

**Work-note-on-create rule.** Every agent that runs after the incident exists must write an attributed work note. In Flow A, Agents 1 and 2 run before Agent 3 creates the INC, so they buffer to Redis (`pending_worknote:{problemId}`) and Agent 3 flushes the buffer immediately after create. The format in §4.6 is mandatory.

### 7.3 Routing database (standalone service)

The routing database is a standalone HTTP service (`routing-db`, port 8000) — peer infrastructure to Redis and the shared Sentinel PostgreSQL instance, not a pipeline agent. It owns three PostgreSQL lookup tables and exposes them over HTTP. Agents 1, 6, and 7 read from it through `shared/routing_client.py`. The service has its own `routing-db/CLAUDE.md`; this section covers what crosses agent boundaries.

| Table                   | Reader(s) | Replaces                                            | Key                              |
|-------------------------|-----------|-----------------------------------------------------|----------------------------------|
| `entity_project_map`    | Ag 7      | `ENTITY_PROJECT_MAP` env var                        | `entity_id` (PK), `entity_name` (UNIQUE) |
| `splunk_index_map`      | Ag 1      | hardcoded table in pre-classifier                   | `(environment, entity_type)` composite   |
| `confluence_space_map`  | Ag 6      | `CONFLUENCE_SPACE_{APP,INFRA,DBA}` env vars         | `team`                                   |
| `schema_migrations`     | service   | —                                                   | `version`                                |

**Why a service rather than a shared file.** Earlier iterations had agents mounting a SQLite file directly via Docker volume. That coupled the volume mount permissions matrix to the agent set, made the admin endpoint Agent 7's responsibility, and made backup the volume's problem. The service model — now backed by PostgreSQL — keeps one component owning the data, agents talking over HTTP, and backup handled by the database server.

**API.** Read endpoints (`GET /v1/entities/{id}`, `GET /v1/splunk-index?env=…&type=…`, `GET /v1/spaces/{team}`) require `X-Routing-Token`. Admin endpoints (`POST /v1/admin/entities`, `POST /v1/admin/import`) require a separate `X-Routing-Admin-Token`. The two tokens have different distributions: read tokens go in agent env (`ROUTING_DB_READ_TOKEN`); admin tokens are held by humans and the deploy-time CSV importer only.

**Bootstrap.** The service's FastAPI lifespan applies migrations on every boot (idempotent — versioned in `schema_migrations`) and seeds defaults only when `entity_project_map` is empty. Its `/health` returns 503 until both steps complete; agents that need the routing DB declare `depends_on: routing-db: condition: service_healthy` so they never start querying a half-initialised database.

**Population.** Three input methods, fixed precedence order: `seed < csv < admin`.

1. **Seed file** (`routing-db/seed/dev_seed.sql`) — applied only on first bootstrap of a fresh DB. Contains baseline Splunk indices, Confluence spaces, sample entities.
2. **Bulk import** (`POST /v1/admin/import`) — deploy-time path, typically called from CI with catalogue-derived JSON. Sets each row's `source = 'csv'`. Skips rows whose existing `source = 'admin'` unless `override_admin: true` is passed.
3. **Admin upsert** (`POST /v1/admin/entities`) — runtime hot-add for emergencies. Sets `source = 'admin'`, which protects the row from the next CSV import.

The `source` column is the audit trail. `updated_at` and `updated_by` let you trace who changed what.

**Read pattern.** `RoutingClient` (in `shared/routing_client.py`) is a thin async HTTP wrapper with a 5-minute in-process cache. Agents call:

- Agent 1: `client.get_splunk_index(environment=..., entity_type=...)` once per alert
- Agent 6: `client.get_confluence_space(team=...)` once per recommendation cycle
- Agent 7: `client.get_gitlab_project(entity_id=...)` once per RCA — preferring `entity_id` (stable) over `entity_name` (renames break it)

The 5-minute cache eliminates almost all the latency cost of the network hop. Cache invalidation is not implemented — config changes via deploy or admin endpoint can wait that long.

**Failure mode.** If `routing-db` is unreachable, the client's `get_*()` methods return `None`. Each agent already handles `None` as a soft fail: Agent 1 falls back to `prod` as the Splunk index, Agent 6 falls back to a configured default space, Agent 7 produces a `non_deployment` verdict. None of the three agents block the pipeline on routing-db unavailability. Setting `ROUTING_DB_FALLBACK_ENABLED=false` makes this fail loud instead — useful in test environments where soft fail would mask real bugs.

---

## 8. External Integrations

| Integration              | Agents          | Scope / API |
|--------------------------|-----------------|-------------|
| Dynatrace Entities API   | 1               | `entities.read` for host/zone/tags enrichment |
| Dynatrace Problems API   | 1, 7            | `problems.read` (1: webhook payloads; 7: precise startTime + Davis) |
| Dynatrace Grail DQL      | 7               | Platform OAuth client (`storage:logs:read`, `storage:events:read`) → `POST /platform/storage/query/v1/query:execute`. Agent 7 Flow B log enrichment around the manual incident's `opened_at`. Trial tenants set `DT_LOGS_ENABLED=false` (deployments-only fallback per §10.2). |
| Splunk REST              | 2, 7            | search jobs API (2: classify; 7: pre/post-deploy log timeline) |
| ServiceNow Table API     | 1, 3, 4, 5, 6, 7| OAuth2 client credentials, cached token; `itil` role required. Ag 1 reads for pre-existence check; Ag 3+ write |
| ServiceNow CMDB          | 3               | `cmdb_ci` lookup (graceful degradation if missing) |
| ServiceNow Business Rule | 1               | Outbound webhook on `incident.insert` → `POST /webhook/snow-incident` (Flow B trigger) |
| PagerDuty Schedules      | 4               | resolves on-call user → email → SNOW sys_id |
| PagerDuty Events v2      | 5               | trigger + ack polling; `dedup_key = problemId`. P1/P2/P3 only |
| Microsoft Teams          | 5               | webhook per team; success = response body `"1"` literal |
| Email backend            | 5               | SendGrid or AWS SES |
| SMS backend              | 5               | Twilio or AWS SNS (P1-infra only; managers on escalation) |
| Confluence Cloud REST    | 6               | 3-tier CQL search + page body storage format |
| PostgreSQL (`sentinel`)  | all agents (RAG), 6, 1 (chat) | Single shared DB. pgvector tables (`sentinel_kb_articles`, `sentinel_incident_patterns`, `sentinel_incidents`, `sentinel_rca_history`) via `VECTOR_DB_URL`. Agent 6: `kb_recommendations` feedback. Agent 1: `chat_conversations` + `chat_messages`. |
| GitLab API               | 7               | `read_api`: deployments, commits, diffs, pipelines (rollback POST) |
| Routing DB service       | 1, 6, 7         | internal HTTP service (port 8000); see §7.3 and `routing-db/CLAUDE.md` |
| LLM provider (chatbot)   | 1 (chat only)   | Anthropic Claude API, OpenAI API, or Ollama (local). Configured via `LLM_PROVIDER`. All calls originate at Agent 1's chat orchestrator — never from the browser or from other agents |

Stateful components: Redis (dedup, bindings), PostgreSQL (routing-db tables + Agent 6 feedback + chat history). Each needs its own backup story before go-live; use `pg_dump` for logical PostgreSQL backups or your cloud provider's automated snapshot feature.

---

## 9. Configuration Master Table

Copy `.env.example` → `.env`. Personal overrides go in `Codex.local.md` (gitignored).

### 9.1 Required environment variables

This table is the **canonical list of what `.env.example` and `docker/docker-compose.yml` actually expose today**. Anything below labeled "aspirational" appears in the design but is not yet wired through compose — the agent code falls back to a default. Treat aspirational rows as a TODO when adding the feature.

| Group          | Variable                       | Used by | Notes |
|----------------|--------------------------------|---------|-------|
| Postgres       | `POSTGRES_DB`                  | all     | shared `sentinel` DB; routing tables + pgvector + chat + feedback |
| Postgres       | `POSTGRES_USER`                | all     | |
| Postgres       | `POSTGRES_PASSWORD`            | all     | **must be changed from default** |
| Postgres       | `DATABASE_URL`                 | all     | derived in compose: `postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}` |
| Routing DB     | `ROUTING_DB_URL`               | 1, 6, 7 | default `http://routing-db:8000` |
| Routing DB     | `ROUTING_DB_ADMIN_TOKEN`       | humans, CI | sent as `X-Admin-Token` to all `/admin/*` writes |
| Routing DB     | `ROUTING_DB_READ_TOKEN`        | 1, 6, 7 | empty → `/reads/*` is public (dev only); set in prod |
| Redis          | `REDIS_URL`                    | all     | default `redis://redis:6379/0` |
| LLM            | `LLM_PROVIDER`                 | all     | `ollama` (default) \| `anthropic` \| `openai` |
| LLM            | `OLLAMA_BASE_URL`              | all     | default `http://ollama:11434` |
| LLM            | `OLLAMA_MODEL`                 | all     | default `llama3.1:8b` |
| LLM            | `EMBED_MODEL`                  | all     | `nomic-embed-text` — **always local Ollama**, never cloud, regardless of `LLM_PROVIDER` |
| LLM            | `ANTHROPIC_API_KEY`            | all     | only if `LLM_PROVIDER=anthropic`; BAA required for PHI environments |
| LLM            | `ANTHROPIC_MODEL`              | all     | default `claude-sonnet-4-20250514` |
| LLM            | `OPENAI_API_KEY`               | all     | only if `LLM_PROVIDER=openai`; BAA required for PHI environments |
| LLM            | `OPENAI_MODEL`                 | all     | default `gpt-4o-mini` |
| Dynatrace      | `DT_BASE_URL`                  | 1, 7    | tenant URL, no trailing slash |
| Dynatrace      | `DT_API_TOKEN`                 | 1, 7    | classic API token. Scopes: `entities.read`, `problems.read`, `events.read`. Logs moved to Grail/DQL (rows below). |
| Dynatrace      | `DT_WEBHOOK_SECRET`            | 1       | HMAC-SHA256 secret set in DT webhook configuration |
| Dynatrace Grail| `DT_LOGS_ENABLED`              | 7       | `false` on trial → Flow B log enrichment soft-skipped; `true` in prod once OAuth client is provisioned |
| Dynatrace Grail| `DT_PLATFORM_BASE_URL`         | 7       | e.g. `https://wkf10640.apps.dynatrace.com` — different host from `DT_URL` (Platform vs classic) |
| Dynatrace Grail| `DT_OAUTH_TOKEN_URL`           | 7       | default `https://sso.dynatrace.com/sso/oauth2/token` |
| Dynatrace Grail| `DT_OAUTH_CLIENT_ID`           | 7       | Platform OAuth client (Settings → OAuth clients). Distinct from API token. |
| Dynatrace Grail| `DT_OAUTH_CLIENT_SECRET`       | 7       | Platform OAuth client secret; rotate every 90 days per §10.6 |
| Dynatrace Grail| `DT_OAUTH_SCOPE`               | 7       | space-separated; default `storage:logs:read storage:events:read` |
| ServiceNow     | `SNOW_BASE_URL`                | 1, 3–7  | shared tenant; Ag 1 reads for pre-existence check, others write |
| ServiceNow     | `SNOW_AUTH_MODE`               | 1, 3–7  | `oauth` (preferred) \| `basic` |
| ServiceNow     | `SNOW_CLIENT_ID`               | 1, 3–7  | OAuth2 client credentials (when `oauth`) |
| ServiceNow     | `SNOW_CLIENT_SECRET`           | 1, 3–7  | OAuth2 client credentials (when `oauth`) |
| ServiceNow     | `SNOW_USERNAME`                | 1, 3–7  | basic-auth fallback (when `basic`) |
| ServiceNow     | `SNOW_PASSWORD`                | 1, 3–7  | basic-auth fallback (when `basic`) |
| ServiceNow     | `SNOW_CALLER_ID`               | 3       | `caller_id` on every INC; default `sentinel.agent` |
| ServiceNow     | `SNOW_WEBHOOK_SECRET`          | 1       | HMAC secret for SNOW outbound Business Rule (Flow B entry) |
| Splunk         | `SPLUNK_BASE_URL`              | 2, 7    | e.g. `https://splunk.internal:8089` |
| Splunk         | `SPLUNK_TOKEN`                 | 2, 7    | search permission on configured indices |
| Splunk         | `SPLUNK_INDEX`                 | 2, 7    | default index name; per-environment override via routing-db `splunk_index_map` table |
| Confluence     | `CONFLUENCE_BASE_URL`          | 6       | `https://your-org.atlassian.net/wiki` |
| Confluence     | `CONFLUENCE_TOKEN`             | 6       | Cloud API token (single token; per-team space lookup via routing-db `confluence_space_map`) |
| PagerDuty      | `PD_API_KEY`                   | 4, 5    | single API key for schedule lookup + Events v2 trigger |
| PagerDuty      | `PD_SERVICE_ID`                | 5       | default PD service; per-team routing keys are aspirational |
| PagerDuty      | `PD_FROM_EMAIL`                | 5       | `From:` email on PD events |
| Notifications  | `TEAMS_WEBHOOK_URL`            | 5       | single Teams webhook today; per-team `TEAMS_WEBHOOK_{APP,INFRA,DBA}` is **aspirational** |
| Notifications  | `SMTP_HOST`                    | 5       | direct SMTP today (no SendGrid/SES adapter wired) |
| Notifications  | `SMTP_PORT`                    | 5       | default `587` |
| Notifications  | `SMTP_USER`                    | 5       | |
| Notifications  | `SMTP_PASS`                    | 5       | |
| Notifications  | `NOTIFY_EMAIL_TO`              | 5       | default ops distribution list |
| Webapp (build) | `VITE_API_BASE`                | webapp  | public URL of Agent 1 from browser; default `http://localhost:8001` |
| Webapp (build) | `VITE_ROUTING_DB_BASE`         | webapp  | public URL of routing-db; default `http://localhost:8000` |

**Aspirational / not in current `.env.example`** (referenced by design sections elsewhere in this doc but not yet wired):
- `AGENT{N}_SHARED_SECRET` / `AGENT{N+1}_SECRET` / `AGENT{N+1}_ENDPOINT` — inter-agent auth chain (§6.1) and routing. The pattern is in the spec; per-hop secret separation isn't yet enforced in code.
- `FLOW_B_ENRICHMENT_TARGETS` — JSON fan-out list for Flow B; today hard-coded to Agents 2/6/7.
- `SNOW_AUTOMATION_USER_SYS_ID`, `SNOW_PRE_EXISTENCE_WINDOW_HOURS` — used by Agent 3 / Agent 1 specs; defaults live in code today.
- `PD_SCHEDULE_{APP,INFRA,DBA}`, `PD_ROUTING_KEY_{APP,INFRA,DBA}`, `TEAMS_WEBHOOK_{APP,INFRA,DBA}` — per-team channel split. Today a single value is used for all teams.
- `EMAIL_BACKEND`, `SENDGRID_API_KEY`, `SMS_BACKEND`, `TWILIO_ACCOUNT_SID` — pluggable email/SMS backends. Today: SMTP only, no SMS.
- `GITLAB_BASE_URL`, `GITLAB_API_TOKEN` — Agent 7 deployment correlation. Not yet in `.env.example`.
- `ROUTING_DB_FALLBACK_ENABLED` — soft-fail toggle described in §7.3; behaviour is on by default in code.
- `CONFLUENCE_EMAIL` — superseded by token-only auth in `CONFLUENCE_TOKEN`.

### 9.2 Tuning constants

| Variable                            | Default | Used by | Effect |
|-------------------------------------|---------|---------|--------|
| `REDIS_DEDUP_TTL_SECONDS`           | 28800   | 1       | DT problem dedup window (8 h) |
| `REDIS_INCIDENT_TTL_SECONDS`        | 86400   | 3       | INC binding lifetime (24 h) |
| `SPLUNK_JOB_POLL_INTERVAL_SECONDS`  | 2       | 2, 7    | poll cadence |
| `SPLUNK_JOB_MAX_POLLS`              | 10      | 2, 7    | 10 × 2s = 20s ceiling per Splunk search |
| `SPLUNK_MAX_RESULTS`                | 500     | 2, 7    | log lines fetched per search |
| `CONFLICT_RESOLUTION_THRESHOLD`     | 65.0    | 2       | Splunk min confidence to override DT hypothesis |
| `SNOW_API_TIMEOUT_SECONDS`          | 15      | 3–7     | per-call timeout |
| `RESOLUTION_POLL_INTERVAL_SECONDS`  | 300     | 7       | DT close-state poll cadence (5 min) |
| `RESOLUTION_MAX_POLLS`              | 72      | 7       | 72 × 5min = 6 h watchdog |
| `REGRESSION_CONFIDENCE_THRESHOLD`   | 75.0    | 7       | min confidence to recommend rollback |

### 9.3 Chatbot environment variables

Chatbot config lives on Agent 1. Provider-agnostic vars are always required; only the active provider's credential vars are needed.

**Common (Agent 1):**

| Variable                       | Required | Default       | Notes |
|-------------------------------|----------|---------------|-------|
| `LLM_PROVIDER`                | ✅       | —             | `anthropic` \| `openai` \| `ollama` |
| `LLM_MODEL`                   | ✅       | —             | e.g. `claude-sonnet-4-6`, `gpt-4o`, `llama3:70b` |
| `LLM_MAX_TOKENS`              |          | `4096`        | per-response ceiling |
| `LLM_MAX_TOOL_LOOPS`          |          | `8`           | **critical** — caps tool-use cycles per turn; prevents runaway LLM loops and unbounded cost |
| `CHAT_RATE_LIMIT_PER_MIN`     |          | `30`          | per dashboard-token |
| `CHAT_DAILY_TOKEN_BUDGET`     |          | `200000`      | per-user daily ceiling; over limit → "quota reached" until UTC reset |
| `CHAT_CONTEXT_WINDOW_MESSAGES`|          | `20`          | prior messages sent to LLM per turn |
| `CHAT_PERSISTENCE_BACKEND`    |          | `postgres`    | `postgres` \| `none` (session-only for lightweight envs) |
| `DASHBOARD_TOKEN`             | ✅       | —             | bearer token checked at `/api/chat/stream` (and all other dashboard endpoints) |

**Provider-specific (only the active provider's vars required):**

| Variable                       | Provider   | Notes |
|-------------------------------|------------|-------|
| `ANTHROPIC_API_KEY`           | anthropic  | from console.anthropic.com |
| `OPENAI_API_KEY`              | openai     | |
| `OPENAI_BASE_URL`             | openai     | optional; for Azure OpenAI or compatible endpoints |
| `OLLAMA_BASE_URL`             | ollama     | default `http://ollama:11434` |

**Webapp side (`VITE_` prefix — baked into bundle at build time):**

| Variable                       | Default  | Notes |
|-------------------------------|----------|-------|
| `VITE_CHAT_ENABLED`           | `true`   | feature flag; set `false` to hide the `/chat` route entirely |
| `VITE_CHAT_PLACEHOLDER`       | (see CHATBOT.md) | input field hint text |
| `VITE_CHAT_HISTORY_MAX`       | `50`     | messages kept in memory before pruning oldest |
| `VITE_CHAT_CITATION_PANE_DEFAULT` | `expanded` | `expanded` \| `collapsed` |

---

## 10. Cross-Cutting Concerns

### 10.1 Always-200 contract — Agent 1 only

Only Agent 1 has the always-200 rule, because only Agent 1 receives traffic from a system that retry-storms on non-200 (Dynatrace). Agents 2–7 use normal HTTP semantics: 200 success, 4xx contract error, 5xx transient. The chain is glued together by the upstream agent's retry+DLQ logic, not by every agent swallowing errors.

### 10.2 Pipeline-blocking vs degradable failures

| Failure                                | Blocking? | Behaviour |
|----------------------------------------|-----------|-----------|
| Inter-agent auth                       | Yes       | 401, DLQ at sender |
| Schema validation                      | Yes       | DLQ |
| SNOW unavailable (Ag 3 create)         | Yes       | retry → DLQ; alarm on `PipelineFailureError` |
| SNOW pre-existence query (Ag 1)        | No        | proceed without binding; Ag 3 catches dup via Redis |
| SNOW webhook unauthenticated (Ag 1)    | Yes       | 401; SNOW Business Rule shows error; investigate |
| SNOW unavailable (Ag 4–7 patch)        | No        | retry → log; INC exists, work note missing |
| Buffered work-note flush fails (Ag 3)  | No        | log warning; lose Ag 1/2 attribution but pipeline continues |
| DT Entities API (Ag 1 enrich)          | No        | safe defaults |
| DT Grail DQL (Ag 7 Flow B)             | No        | partial Flow B work note; deployments still posted. `DT_LOGS_ENABLED=false` forces this path on trial tenants. |
| CMDB CI lookup (Ag 3)                  | No        | INC created without `cmdb_ci`, work note posted |
| Splunk timeout / disabled (Ag 2)       | No        | falls back to DT hypothesis. `SPLUNK_ENABLED=false` forces this path on Splunk Cloud Free Trial (REST API not exposed on port 8089). |
| PagerDuty schedule (Ag 4)              | No        | static team-lead fallback |
| Single notification channel (Ag 5)     | No        | other channels deliver; failure recorded |
| Confluence search (Ag 6)               | No        | pipeline continues without KB |
| GitLab unreachable (Ag 7)              | No        | RCA verdict = `uncertain` or `non_deployment` |
| Flow B fan-out: one agent fails        | No        | the other two agents still post their work notes |
| LLM provider unreachable (chatbot)     | No        | `/api/chat/stream` returns `error` event; pipeline is unaffected; switch `LLM_PROVIDER` env var |
| LLM tool-loop hits `LLM_MAX_TOOL_LOOPS`| No        | stream emits `error` event; bot responds "stopped after N steps"; no pipeline impact |
| Chat Postgres unavailable              | No (chat only) | `CHAT_PERSISTENCE_BACKEND` auto-degrades to session memory; bot still works, history not persisted |

Principle: only **Agent 3's create** is allowed to halt the pipeline. Everything else degrades so the engineer still gets a paged incident with whatever evidence was gatherable.

### 10.3 Observability

Every agent ships structured JSON logs (structlog) with `pipeline_run_id` on every line, `/health` returning 200/503 based on critical dependencies, `/metrics` with operation counters, and updates to `pipeline_run:{runId}` Redis key at each stage entry/exit. The dashboard SSE stream reads from these.

### 10.4 Coverage targets

| Agent | Overall | Critical-path floor |
|-------|---------|---------------------|
| 1     | 90%     | severity_mapper 100%, security 100%, dedup 95% |
| 2     | 90%     | pre_classifier 100%, query_builder 95% |
| 3–7   | 90%     | per `.claude/rules/testing.md` |

### 10.5 Implementation invariants

All HTTP via `httpx.AsyncClient`. Splunk queries parameterized. ServiceNow OAuth2 tokens cached and refreshed. Confluence search uses 3-tier CQL fallback. RCA is a 4-signal weighted model (temporal, file, timeline, Davis). `pytest.ini` sets `asyncio_mode=auto`.

### 10.6 Security best practices (healthcare deployment)

This system is deployed in a healthcare environment where incident data may contain sensitive operational and patient-context information. The following practices apply regardless of specific regulatory requirements — they are simply good engineering.

**Minimum necessary data in logs and snapshots.** Log identifiers, not content. Use `problem_id`, `incident_number`, `pipeline_run_id`, `entity_id`, `error_category`, `classification_confidence` in structured logs. Never log free-text fields: alert titles, incident descriptions, work note bodies, log evidence lines, commit messages, or assignee details. Log the count of matched items, not the items themselves. Redis pipeline snapshots follow the same rule — structural metadata only (stage, status, duration, timestamp).

**Minimum necessary field forwarding between agents.** Each agent's event model must use an explicit `to_downstream()` method that selects only the fields the next agent needs, rather than forwarding the full accumulated payload. Free-text evidence fields (`matched_log_lines`, `description`, `short_description`) are forwarded only as far as Agent 3 (ServiceNow), where they are written to the SNOW incident record. They must not appear in Redis snapshots, log output, or LLM tool results.

**TLS everywhere, certificate validation always on.** All inter-service and vendor API calls use TLS 1.2+. `httpx.AsyncClient` must never be instantiated with `verify=False` — this is a pre-commit hook check. Redis uses `rediss://` connection strings in production. PostgreSQL uses `sslmode=require`.

**Named user authentication — no shared tokens.** The dashboard `X-Dashboard-Token` and the chatbot session must map to a named individual identity. Shared tokens (one token for the whole NOC team) make access logs meaningless. Use your organisation's SSO for the dashboard and enforce per-user token issuance.

**Secrets in a secrets manager, not in `.env` files.** `SNOW_CLIENT_SECRET`, `PAGERDUTY_API_KEY`, `ANTHROPIC_API_KEY`, `GITLAB_API_TOKEN`, `ROUTING_DB_ADMIN_TOKEN`, and any other credentials must be stored in AWS Secrets Manager, HashiCorp Vault, or equivalent. `.env` files are for local development only and must be gitignored. Rotate credentials every 90 days minimum.

**Access audit log.** Every dashboard page load that renders incident content, every chatbot turn, and every routing-db admin write must emit an audit event: `user_id`, `action`, `resource_id`, `timestamp`, `ip`. Write to a Postgres table (one row per event, append-only, no delete). This is operationally useful independent of any regulatory requirement — you want to know who was looking at what during a production incident.

**Encryption at rest for all persistent stores.** Redis: enable `requirepass` and TLS. PostgreSQL (all data — routing-db tables, Agent 6 feedback, chat history): use a managed cloud instance (AWS RDS, Cloud SQL) with encryption-at-rest enabled — this is a checkbox, not a code change. Default Docker local volumes are not encrypted; in production use an encrypted volume driver or managed service.

**Chatbot: prefer local LLM for sensitive environments.** When incident data flowing through the chatbot tools contains sensitive operational content, prefer `LLM_PROVIDER=ollama` with a self-hosted deployment. This keeps all data within your infrastructure boundary regardless of what the tool results contain. If using a cloud LLM provider, confirm your vendor agreement covers your data handling requirements before enabling in production.

**Chatbot: PHI scrubbing layer.** Agent 1's chat orchestrator applies a scrubbing pass on tool results before they enter the LLM message payload. Minimum patterns in `agents/dynatrace/app/services/chat_phi_scrubber.py`:

```python
PHI_PATTERNS = [
    (r'\bMRN[-:\s]?\d{4,12}\b',                      '[MRN REDACTED]'),
    (r'\b\d{3}-\d{2}-\d{4}\b',                       '[ID REDACTED]'),
    (r'\bDOB[-:\s]?\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', '[DOB REDACTED]'),
    (r'\b(patient|pt|member)\s*id[-:\s]?\w+\b',      '[PATIENT-ID REDACTED]'),
    (r'\bNPI[-:\s]?\d{10}\b',                         '[NPI REDACTED]'),
    (r'\bICD-?1[01][-:\s]?[A-Z]\d+\.?\w*\b',         '[DIAGNOSIS-CODE REDACTED]'),
]
```

The scrubber logs a count of redactions per turn (not the redacted content). It is a defence-in-depth layer — the primary control is keeping sensitive content out of the LLM message payload entirely via minimum necessary field forwarding.

**Pre-PR security checklist (lightweight):**
- [ ] No free-text incident content in any new log statement
- [ ] No new fields added to Redis pipeline snapshot beyond structural metadata
- [ ] Any new `httpx` client uses `verify=True`
- [ ] Any new external API integration uses credentials from a secrets manager
- [ ] New chatbot tools apply the scrubbing layer to their output
- [ ] Test fixtures use synthetic data, not production incident content

---

## 11. VS Code Workspace

### 11.1 Recommended extensions

Add `.vscode/extensions.json`:

```json
{
  "recommendations": [
    "ms-python.python",
    "ms-python.vscode-pylance",
    "charliermarsh.ruff",
    "ms-azuretools.vscode-docker",
    "redhat.vscode-yaml",
    "tamasfe.even-better-toml",
    "dbaeumer.vscode-eslint",
    "esbenp.prettier-vscode",
    "bradlc.vscode-tailwindcss",
    "anthropic.claude-code"
  ]
}
```

### 11.2 Workspace settings

Add `.vscode/settings.json`:

```json
{
  "python.analysis.typeCheckingMode": "basic",
  "python.testing.pytestEnabled": true,
  "python.testing.pytestArgs": ["tests"],
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": { "source.organizeImports": "explicit" }
  },
  "files.associations": { "*.env.example": "dotenv" },
  "search.exclude": { "**/node_modules": true, "**/.venv": true }
}
```

### 11.3 Multi-root workspace

For convenience, a top-level `sentinel.code-workspace` lets each agent be opened as a folder with its own Python interpreter while sharing the same VS Code window:

```json
{
  "folders": [
    { "path": "agents/dynatrace" },
    { "path": "agents/splunk" },
    { "path": "agents/servicenow" },
    { "path": "agents/pagerduty" },
    { "path": "agents/notifications" },
    { "path": "agents/confluence" },
    { "path": "agents/rca" },
    { "path": "shared" },
    { "path": "webapp" },
    { "path": "tests" },
    { "path": ".", "name": "root" }
  ]
}
```

### 11.4 Launch configurations

`.vscode/launch.json` for debugging individual agents and tests:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Agent N (uvicorn --reload)",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:app", "--reload", "--port", "${input:port}"],
      "cwd": "${workspaceFolder}",
      "envFile": "${workspaceFolder}/.env"
    },
    {
      "name": "Pytest: current file",
      "type": "debugpy",
      "request": "launch",
      "module": "pytest",
      "args": ["${file}", "-v"]
    }
  ],
  "inputs": [
    { "id": "port", "type": "promptString", "description": "Agent port (8001-8007)", "default": "8001" }
  ]
}
```

### 11.5 Tasks

`.vscode/tasks.json` for one-key stack control:

```json
{
  "version": "2.0.0",
  "tasks": [
    { "label": "stack: up",   "type": "shell", "command": "docker compose up -d" },
    { "label": "stack: logs", "type": "shell", "command": "docker compose logs -f" },
    { "label": "stack: down", "type": "shell", "command": "docker compose down" },
    { "label": "test: all",   "type": "shell", "command": "pytest tests/ -v" },
    { "label": "fire: alert", "type": "shell", "command": "python agents/Agent-1-dynatrace/scripts/send_test_webhook.py --scenario app_error" }
  ]
}
```

---

## 12. Plugins

This repo uses the following Claude Code plugins. Invoke them deliberately at the points described — do not rely on the agent to pick them up implicitly.

### `frontend-design`
Required when touching `webapp/`. Use for new React components, dashboard layout changes, SSE event renderers, the glowing report views, and any Tailwind/design-token work. Run before committing UI changes — enforces design consistency and avoids generic AI-styled output.

### `superpowers`
General multi-agent orchestration. Use for refactors that span agents (e.g., adding a field to the event contract chain and propagating it through agents 1–7) and for scaffolding new agents that follow the FastAPI + auth-header + Redis pattern.

### `context7`
Pull up-to-date docs for FastAPI, httpx, Pydantic v2, redis-py, React 18+, and the Splunk / ServiceNow / PagerDuty / Confluence / GitLab SDKs. Use **before** writing integration code — vendor APIs (SNOW Table API, PD v2/v3, Confluence v2 REST) drift fast and training-data shapes are unreliable.

### `code-review`
Run before opening any PR. Mandatory for changes to `shared/models.py` (contract chain), inter-agent auth, Redis key schemas, and the RCA scoring weights in Agent 7.

### `security-review`
Mandatory for HMAC validation in Agent 1, OAuth token handling in Agent 3, every `X-Agent{N}-Token` flow, anything touching `.env` secrets, webhook signature verification, and any new outbound integration. Also run on the webapp for XSS / SSE-injection vectors in the dashboard. **For the chatbot specifically:** run on Agent 1's `/api/chat/stream` endpoint (prompt injection from tool results, `LLM_MAX_TOOL_LOOPS` enforcement, per-user token budget, conversation authorization).

### `claude-mem`
Persistent project memory across sessions. Use to recall prior architectural decisions, known-broken integrations, vendor quirks (SNOW rate limits, PD schedule edge cases), and the rationale behind tuning constants. Update after any non-obvious decision so future sessions don't re-litigate it.

### Plugin usage matrix

| Task                                     | Plugins |
|------------------------------------------|---------|
| New React dashboard view                 | `frontend-design`, `context7`, `code-review` |
| `/chat` UI changes (citation pane, input)| `frontend-design`, `context7`, `code-review` |
| New chatbot tool (read-only)             | `context7`, `code-review`, `security-review` |
| LLM provider swap / adapter change       | `context7`, `code-review`, `security-review` |
| New agent / new event field              | `superpowers`, `context7`, `code-review`, `security-review` |
| Auth / secrets / OAuth changes           | `security-review`, `code-review` |
| Vendor SDK integration                   | `context7`, `security-review` |
| Pre-PR gate                       | `code-review`, `security-review` |
| Session start & end               | `claude-mem` |

---

## 13. Coding Conventions

Coding conventions are documented in this file and in per-agent CLAUDE.md files. Local overrides in `CLAUDE.local.md` (gitignored) take precedence over this file for the developer's own machine.

Style: ruff for Python (line length 100), prettier + eslint for the webapp. Type checking via Pylance basic mode in dev, strict in CI for `shared/models.py`.

---

## 14. Implementation Notes & Open Questions

Items that surface only when reading the architecture as a whole:

**Auth secret naming consistency.** The pattern in §6.1 is implied across the per-agent specs but never stated as a rule. Capture it in `shared/auth.py`'s docstring and add a startup check in each agent that fails fast if env vars are misnamed.

**SNOW webhook delivery mechanism.** §8 lists a SNOW Business Rule on `incident.insert` as the trigger for Flow B. Confirm whether the SNOW instance allows outbound REST messages from Business Rules (some hardened tenants disable this). Fallback: a polling worker that scans `incident` table every 30 s for new records — slower, but works without instance changes.

**P3 PagerDuty volume.** The new requirements include P3 in the PagerDuty notification set. P3 alerts are typically more frequent and lower-stakes than P1/P2 — overnight P3 pages can drive alert fatigue and erode response quality on the genuine P1/P2s. Confirm with on-call leadership: (a) is P3 PD a 24/7 page, or business-hours only? (b) should P3 use a separate PD service with a softer escalation policy than P1/P2? (c) is the SMS opt-out for P3 the right choice (currently no SMS for any P3)? Mitigation options without changing requirements: route P3 to a low-urgency PD service, mute P3 PD overnight, or aggregate P3 events with a higher dedup window.

**Flow B race against work-in-progress Flow A.** A DT alert can fire moments after a customer support agent files a manual ticket for the same underlying problem. Both flows could attach work notes to different incidents that describe the same outage. Decide whether Flow A's pre-existence check should also scan for recent manual incidents matching the entity (not just `u_source_alert_id`), and if so, what the merge story looks like. Less likely now that P3 is fully DT-originated, but P4/P5 manual tickets can still describe a problem DT later auto-creates a P1/P2/P3 for.

**Buffered work-note durability.** `pending_worknote:{problemId}` is a Redis key with a 1-hour TTL. If Redis fails between Agent 1/2 buffering and Agent 3 flushing, the Agent 1 and Agent 2 contributions are lost. Acceptable for v1; consider a Postgres journal in v2.

**DT Grail DQL cost and isolation.** Agent 7's Flow B path issues one DQL query per manual P4/P5 incident against the Platform storage API. Two concerns differ from the classic Logs API model: (a) **cost is per-scan**, not per-call — Grail bills against the GB scanned by each DQL query, so a poorly-bounded `fetch logs` with no `filter` clause is expensive even when issued infrequently. The helper at [`agents/Agent-7-Rca/dt_grail_client.py`](agents/Agent-7-Rca/dt_grail_client.py)#fetch_logs_around always pre-filters on entity + loglevel and clamps the timeframe to ±15 min, but custom DQL added later should be reviewed for unbounded scans. (b) **OAuth client is the failure mode** — Agent 7's Grail calls use `DT_OAUTH_CLIENT_ID/SECRET`, not `DT_API_TOKEN`, so a revoked or expired OAuth client only takes down Flow B logs, leaving Flow A (which uses the classic API token for problems/events/entities) unaffected. Monitor for `401` from `_ensure_token()` and alert separately from API-token alerts.

**Postgres is the single persistent store for all persistent data.** The shared `sentinel` PostgreSQL instance hosts: routing-db (routing tables), pgvector RAG tables (`sentinel_kb_articles`, `sentinel_incident_patterns`, `sentinel_incidents`, `sentinel_rca_history`), Agent 6 (`kb_recommendations` feedback), and Agent 1 (chat history). All vector tables are prefixed `sentinel_` to avoid naming collisions. Connection is via `VECTOR_DB_URL` (or `DATABASE_URL` as fallback) — a single DSN, not the five `VECTOR_DB_*` env vars that existed prior to migration 002.

**Routing-db service is critical infrastructure.** Now that routing lives in a standalone PostgreSQL-backed service, the failure surface is HTTP not file system. Two concerns: (a) routing data is stored in the shared Sentinel PostgreSQL instance — use `pg_dump` for logical backups or the managed cloud snapshot feature; (b) schema migrations run on every service boot via lifespan and refuse to start the API on failure, which is good behaviour but means a bad migration takes down all routing reads at once. Stage migrations in a non-prod environment first.

**Soft-fail vs hard-fail on routing-db unavailability.** When `routing-db` is unreachable, `RoutingClient` returns `None`. Each agent already handles `None` as a soft fail with a per-agent default: Agent 1 → `prod` index, Agent 6 → ops space, Agent 7 → `non_deployment` verdict. This is *graceful degradation*, not env-var fallback — the env vars (`ENTITY_PROJECT_MAP`, `CONFLUENCE_SPACE_*`) are no longer read by the agents at all. They remain only in `.env.example` as historical context and for any operator that wants to roll back to the embedded model temporarily. Setting `ROUTING_DB_FALLBACK_ENABLED=false` flips the soft fail to a hard fail (raise instead of return `None`) — useful in test envs where the soft path would mask real wiring bugs. In production, leave it `true`.

**Routing-db cache invalidation.** `RoutingClient` caches reads for 5 minutes per agent process. An admin endpoint write is therefore not visible to all agents instantly — there's a worst-case 5-minute window where different agents disagree about what the routing data says. Acceptable for the slowly-changing config this is, but worth knowing. If you ever need instant invalidation (e.g. for a security-related rollback removal), the cleanest path is a routing-db pub/sub channel or a forced restart of agent containers.

**SNOW custom fields.** Agent 3 requires `u_source_tool`, `u_source_alert_id`, `u_dynatrace_url`, `u_classification_confidence` on the incident table. There's no automated check; the first run fails opaquely if Studio hasn't been updated. Add a bootstrap script that verifies the schema on Agent 3 startup. The `u_source_alert_id` field is also what Agent 1's pre-existence check filters on — its index will affect query latency.

**Resolution monitor footprint.** Agent 7's 5-minute polling task per active incident scales linearly. At N concurrent incidents that's N×12 SNOW reads/hour just for close-state checks. If volume grows, switch to a shared sweeper that batches `/api/v2/problems` queries.

**P2 out-of-hours window — removed.** The simplified PD rule (§5.5) drops the BH/OOH branching. Confirm with on-call leadership that this is intentional — the previous design used OOH PD to avoid overnight Teams-only escalations going unnoticed. If retention is required, the rule belongs in Agent 4 (escalation flag) rather than Agent 5 (channel selection).

**Splunk index derivation.** Previously hardcoded in Agent 1's pre-classifier; now lives in the routing-db's `splunk_index_map` table with a wildcard fallback per environment. The seed file ships the original 9-row table verbatim — change without redeploy via the routing-db admin API. Routing-db's startup should warn if `splunk_index_map` is empty, which would otherwise cause Agent 1 to fall back to `prod` for every alert and silently swallow staging traffic.

**Chatbot: `LLM_MAX_TOOL_LOOPS` is the single most important chatbot config var.** Without it, a model stuck in a tool-use loop (retrying the same call, accumulating results, calling again) runs up unbounded LLM cost. Default is 8 — enough for complex multi-step questions. Below 4 and legitimate multi-hop queries start failing; above 15 and a confused model can rack up significant spend before the limit fires. Measure p95 tool-loop count for two weeks post-launch then tighten. The daily per-user token budget (`CHAT_DAILY_TOKEN_BUDGET`) catches abuse from a different angle — expensive single questions rather than looping.

**Chatbot: prompt injection from tool results.** The chatbot's tool results contain incident descriptions, SNOW work notes, and KB article bodies — content authored by humans and potentially crafted to manipulate the model. The orchestrator wraps every tool result in a data-vs-instructions marker that the system prompt enforces. This raises the bar but is not a complete defence. Run `security-review` on the `/api/chat/stream` endpoint before shipping, with explicit attention to cross-provider injection patterns (each provider handles system prompt boundaries differently).

**Chatbot: Postgres now used by two features.** Agent 6's `kb_recommendations` feedback table and Agent 1's `chat_conversations` + `chat_messages` tables share the same Postgres instance (configured via `FEEDBACK_DB_URL` on Agent 6 and the same connection string on Agent 1, or a separate `CHAT_DB_URL`). Confirm whether the same credentials and schema are used or whether you want database-level isolation between the two workloads. At low volume (early production) one Postgres instance is fine. At scale, the chat tables grow unbounded — add a retention policy (`DELETE FROM chat_messages WHERE created_at < NOW() - INTERVAL '90 days'`) before volume grows.

**Chatbot: provider contract tests must run in CI.** The provider adapter layer is where provider API drift hides. Anthropic, OpenAI, and Ollama all change their streaming tool-use shapes occasionally. The contract test suite (`tests/chat/test_provider_contract.py`) runs identical scenarios against all three providers using recorded fixtures. When a provider's API changes shape, regenerate its fixtures and fix the adapter — this is a maintenance task, not a bug. If the CI job for any provider is consistently red, that provider should not be configured as `LLM_PROVIDER` in production until fixed. Do not disable the test — disable the provider.

**Chatbot: the `/chat` route is bookmarkable and shareable.** Shared conversation URLs require the recipient to have independent dashboard access — the URL is a convenience, not a permission grant. If your dashboard token issuance doesn't have a concept of "user identity" (e.g. you use a single shared token for all dashboard users), conversation sharing and persistence keyed on user identity won't work as designed. Confirm the identity model before enabling `CHAT_PERSISTENCE_BACKEND=postgres`.

