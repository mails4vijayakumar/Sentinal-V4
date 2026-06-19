# Agent 4 — PagerDuty On-Call & SLA

**Port:** `8004` · **Queue:** `agent:4:queue` · **Enqueues:** `agent:5:queue` **and** `agent:6:queue` (parallel fan-out)

## Role

Triggers a PagerDuty incident for primary-flow alerts, identifies the on-call
engineer, updates the SNOW incident with that assignment, and computes the SLA
breach deadline. After completing, it fans out to **both** Agent 5 (notifications)
and Agent 6 (Confluence) concurrently.

## Behaviour by Flow / Severity

| Condition                  | Action   |
|----------------------------|----------|
| primary & severity P1–P3   | `alerted` — create PD incident, fetch on-call |
| secondary OR P4 / P5       | `skipped` |
| PD not configured          | `skipped` |

## SLA Windows

| Severity | SLA (minutes) |
|----------|---------------|
| P1       | 15 |
| P2       | 60 |
| P3       | 240 |
| P4 / P5  | 0 (no SLA) |

`sla_breach_at = now + sla_minutes` is recorded for downstream monitoring.

## PagerDuty Flow

1. `POST https://api.pagerduty.com/incidents` with urgency derived from severity
   (`high` for P1/P2, `low` for P3) and `incident_key = sentinel-{run_id}`
2. `GET /oncalls` for the incident's escalation policy → on-call name + email
3. If a SNOW sys_id exists, `PATCH` the incident's `assigned_to` and add a
   work-note naming the on-call engineer

## Outputs

- Writes `PagerDutyEnrichment` (`pd_incident_id`, `on_call_name`, `sla_minutes`, `action`)
- Records step + enrichment in routing-db
- Publishes `agent_start` / `agent_done`
- **Fan-out:** enqueues Agent 5 and Agent 6 in parallel via `asyncio.gather`

## Failure Behaviour

Non-fatal — emits `agent_error` and still fans out to Agents 5 and 6.

## Key Env Vars

`PD_API_KEY`, `PD_SERVICE_ID`, `PD_FROM_EMAIL`, `SNOW_BASE_URL`, `AGENT_4_PORT`
