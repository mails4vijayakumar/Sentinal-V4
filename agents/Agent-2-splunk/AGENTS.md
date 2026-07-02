# Agent 2 — Splunk Log Analysis & Classification

**Port:** `8002` · **Queue:** `agent:2:queue` · **Enqueues:** `agent:3:queue` (primary) or `agent:3 ∥ agent:6` (secondary)

## Role

Pulls recent logs from Splunk for the affected host/service, scores errors against three regex rule sets (app / infra / db), resolves the classification against a Dynatrace-derived hypothesis, pre-computes the SNOW routing fields, and writes a `SplunkEnrichment` to the pipeline context. Primary flow forwards to Agent 3; secondary flow posts a Splunk-evidence work note directly to SNOW and continues the enrichment fan-out.

## Layout

```
agents/Agent-2-splunk/
├── main.py           # worker loop, process_run, SSE publishing, forwarding
├── classifier.py     # six phases as pure functions + classify() entry point
├── rules.py          # DT_HYPOTHESIS_MAP, APP/INFRA/DB_RULES, CATEGORY_ROUTING, WINDOW_MIN
├── snow_notes.py     # §4.6-formatted work-note POST helper (reusable)
├── AGENTS.md         # this file
└── Dockerfile
```

## Worker Model

`worker_loop()` blocks on `BLPOP agent:2:queue`. Each dequeued `run_id` is processed in its own `asyncio` task, so multiple incidents enrich concurrently.

## The Six Phases (`classifier.classify`)

Each phase is a pure function in `classifier.py`. `main.py` calls `classifier.classify(event, severity)` and gets back a `SplunkEnrichment`.

1. **`preclassify(event) → dt_hypothesis`** — look up `(eventType, entityType)` in `DT_HYPOTHESIS_MAP` from `rules.py`. Unmapped combos default to `"app"`.
2. **`build_tiered_queries(host, service, severity, index) → [T1, T2, T3]`** — three SPL strings, tightest first. T1 uses index + host + service + error keywords. T2 drops service. T3 drops host.
3. **`run_splunk_async(client, spl) → results`** — async submit/poll/fetch against `/services/search/jobs`. Polls every `SPLUNK_JOB_POLL_INTERVAL_SECONDS` (2s) up to `SPLUNK_JOB_MAX_POLLS` (10). Uses `verify=True`.
4. **`score(results) → List[RuleMatch]`** — apply APP/INFRA/DB regex sets, count matches, capture one 200-char sample line per rule.
5. **`resolve_conflict(dt_hypothesis, rule_matches) → (category, confidence, source)`** — per-rule-set score is `Σ weight × log10(1 + match_count)`. Confidence is `100 × winner / (winner + runner_up + 1e-6)`. Splunk wins iff `confidence ≥ CONFLICT_RESOLUTION_THRESHOLD` (65).
6. **`precompute_routing(category) → routing fields`** — set `assigned_team`, `assigned_queue`, `snow_category`, `snow_subcategory` from `CATEGORY_ROUTING`.

T1 short-circuits (T2/T3 skipped) when it returns ≥ `SPLUNK_TIER1_SHORTCIRCUIT_MIN` (20) rows.

## Adaptive Time Window

SPL search window scales with severity, held in `WINDOW_MIN` in `rules.py`:

| Severity | Window |
|----------|--------|
| P1       | 15 min |
| P2 / P3  | 30 min |
| P4 / P5  | 60 min |

## Rule Weights

| Rule set | Weight | Rationale |
|----------|--------|-----------|
| `app`    | 1.0    | baseline |
| `infra`  | 1.2    | infra signals (OOM, disk full, container kills) are usually decisive |
| `db`     | 1.1    | db signals are strong but often accompanied by app errors |

Shippable v1 rules live in `rules.py` (5 app + 4 infra + 4 db patterns). Grow the sets in follow-up PRs based on the `sample_line` values that appear in SNOW work notes.

## Conflict Resolution

Splunk's argmax category wins only when the head-to-head confidence against the runner-up exceeds 65%. Below that, the DT hypothesis is preserved — but the disagreement is recorded on the enrichment (`splunk_category`, `confidence`, `hypothesis_source="dt"`) so downstream and the dashboard can see it.

## Routing Pre-compute

Small deterministic map from resolved category to SNOW fields:

| Category | assigned_team | assigned_queue | snow_category  | snow_subcategory     |
|----------|---------------|----------------|----------------|----------------------|
| `app`    | app-support   | APP_SUPPORT    | Application    | Runtime Error        |
| `infra`  | infra-ops     | INFRA_OPS      | Infrastructure | Resource             |
| `db`     | dba           | DB_ADMIN       | Database       | Query/Connection     |

Agent 3 reads these on INC create. Agent 4 uses `assigned_queue` as a soft fallback when PagerDuty schedule lookup fails.

## Outputs

- Writes `SplunkEnrichment` into `ctx.enrichments.splunk` (fields listed below)
- Records step (`status=completed`, duration) in routing-db
- Writes enrichment row in routing-db
- Publishes `agent_start` then `agent_done` SSE
- **Primary flow:** enqueue Agent 3
- **Secondary flow:** post SNOW work note via `snow_notes.post_work_note`, then enqueue Agents 3 **and** 6 in parallel

### `SplunkEnrichment` fields

Existing (unchanged, backward-compat): `log_lines_scanned`, `error_count`, `warn_count`, `top_errors`, `time_range`, `index`, `spl_query`, `llm_summary`, `classification`.

New (see `shared/models.py`):
- `tier_used` — which tier (1/2/3) produced the results
- `spl_queries` — every tier SPL actually executed
- `rule_matches: List[RuleMatch]` — scored matches with sample lines
- `dt_hypothesis` / `splunk_category` / `confidence` / `hypothesis_source` — conflict-resolution trace
- `error_category` — final resolved category (also mirrored to `classification`)
- `assigned_team` / `assigned_queue` / `snow_category` / `snow_subcategory` — routing pre-compute

## Flow B SNOW Work Note

Secondary flow ends with a §4.6-formatted work note posted to the manual SNOW incident (before Agent 2 fans out to 3 + 6). Body is rendered by `classifier.render_evidence_body(enrichment)`:

```
=== SPLUNK EVIDENCE — Agent 2 ===
Timestamp : 2026-07-01T12:00:00Z
Status    : success
Duration  : 4213
Pipeline  : <run_id>

Category (Splunk-scored): db  (confidence 78%, DT hypothesis was: app)
Tier: 1
Log lines scanned: 342
Top matches:
  • db.conn_pool         (score 3.4, 42 matches)
      sample: "HikariCP - Connection pool exhausted after 30000ms"
  • db.query_timeout     (score 1.8, 8 matches)
      sample: "org.postgresql.util.PSQLException: statement timeout"
SPL: search index=prod host="db-01" earliest=-30m@m (ERROR OR WARN OR ...) | head 500
```

Empty-evidence case renders `"No matching log evidence in last {w} min — using DT hypothesis: {cat}"`.

## Failure Behaviour

All failures are non-fatal per §10.2 of CLAUDE.md:

| Failure                                    | Behaviour |
|--------------------------------------------|-----------|
| `SPLUNK_BASE_URL` empty                    | Enrichment with `hypothesis_source="dt"`, `confidence=0`. Flow A forwards; Flow B posts a "no evidence available" work note and fans out. |
| Splunk 5xx / poll timeout                  | Same as unconfigured but `llm_summary` reflects failure. Retry §6.3 (2/4/8 s) on job submit only. |
| All three tiers return 0 rows              | `rule_matches=[]`, DT hypothesis kept. |
| SNOW PATCH fails (Flow B)                  | Log warning, continue. `agent_error` SSE fired. |
| Missing `incident_sys_id` on `ManualIncidentEvent` (Flow B) | Log error, skip SNOW write, still fan out. |
| Unmapped `(eventType, entityType)`         | Default to `"app"`, log `preclassify_unmapped`. |

## Observability

Structured log fields on every `agent2_*` event: `dt_hypothesis`, `splunk_category`, `confidence`, `tier_used`, `hypothesis_source`, `rule_match_count`, `flow`, `run_id`.

**Never logged (§10.6):** `sample_line` values, raw Splunk result rows, `spl_query` text (contains host/service). These appear only in the SNOW work note and on the dashboard.

## Key Env Vars

`SPLUNK_BASE_URL`, `SPLUNK_TOKEN`, `SPLUNK_INDEX`, `REDIS_URL`, `ROUTING_DB_URL`, `AGENT_2_PORT`, `SNOW_BASE_URL` (Flow B), plus the tuning constants in §9.2 of CLAUDE.md.

## Design Reference

Full design: [`docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md`](../../docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md).
