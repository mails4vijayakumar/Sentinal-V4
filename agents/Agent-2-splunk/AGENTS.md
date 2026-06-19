# Agent 2 — Splunk Log Analysis & Classification

**Port:** `8002` · **Queue:** `agent:2:queue` · **Enqueues:** `agent:3:queue` (primary) or `agent:3 ∥ agent:6` (secondary)

## Role

Pulls recent logs from Splunk for the affected host/service, counts errors and
warnings, classifies the dominant failure mode, and writes a `SplunkEnrichment`
to the pipeline context. This is the first enrichment stage and runs for both
primary and secondary flows.

## Worker Model

Runs a background `worker_loop()` that blocks on `BLPOP agent:2:queue`. Each
dequeued `run_id` is processed in its own asyncio task, so multiple incidents
enrich concurrently.

## Adaptive Time Window

The SPL search window scales with severity to balance detail vs. query cost:

| Severity | Window |
|----------|--------|
| P1       | 15 min |
| P2 / P3  | 30 min |
| P4 / P5  | 60 min |

## SPL Query

```
search index={SPLUNK_INDEX} host="{host}" source="*{service}*" earliest=-{window}m@m
  (ERROR OR WARN OR CRITICAL OR Exception OR Traceback)
  | stats count by log_level | sort - count | head 20
```

Submitted as a `oneshot` job against `/services/search/jobs`.

## Classification Heuristic

Maps top error strings → a `classification` label consumed downstream by
Agent 6 (KB search) and Agent 7 (RCA):

| Signal in errors                | Classification    |
|---------------------------------|-------------------|
| connection / timeout            | `db_connection`   |
| memory / oom / heap             | `memory`          |
| deploy / classnot               | `deployment`      |
| error_count > 100               | `high_error_rate` |
| (none of the above)             | `unknown`         |

## Outputs

- Writes `SplunkEnrichment` into `ctx.enrichments.splunk`
- Records step (`status=completed`, duration) in routing-db
- Writes enrichment row in routing-db
- Publishes `agent_start` then `agent_done` SSE
- **Routing:** primary → enqueue Agent 3; secondary → enqueue Agents 3 **and** 6

## Failure Behaviour

Splunk errors are non-fatal: the agent logs, emits `agent_error`, and still
forwards to Agent 3 so the pipeline completes with partial enrichment. If Splunk
is unconfigured, returns an empty enrichment with a "not configured" summary.

## Key Env Vars

`SPLUNK_BASE_URL`, `SPLUNK_TOKEN`, `SPLUNK_INDEX`, `REDIS_URL`,
`ROUTING_DB_URL`, `AGENT_2_PORT`
