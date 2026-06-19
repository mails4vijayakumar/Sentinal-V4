# Agent 1 — Dynatrace Ingestion & Flow Router

**Port:** `8001` · **Queue:** none (entry point) · **Enqueues:** `agent:2:queue`

## Role

The single entry point for the entire pipeline. Accepts inbound webhooks from
both Dynatrace and ServiceNow, validates them, classifies severity, assigns the
processing flow, deduplicates, persists the incident, and broadcasts pipeline
events over Server-Sent Events (SSE) to the dashboard.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/webhook/dynatrace`   | DT problem webhook (HMAC verified) |
| POST | `/api/webhook/servicenow`  | SNOW outbound webhook (HMAC verified) |
| GET  | `/sse/dashboard`           | Global SSE stream (all runs) |
| GET  | `/sse/run/{run_id}`        | Per-run SSE stream with replay (`?last_id=`) |
| GET  | `/health`                  | Liveness probe |
| GET  | `/ready`                   | Readiness — checks Redis + routing-db |

## Severity Mapping

**Dynatrace** (`severity` field → internal `Pn`):

| DT severity      | Internal | Flow |
|------------------|----------|------|
| AVAILABILITY     | P1 | primary |
| PERFORMANCE      | P2 | primary |
| ERROR / RESOURCE | P3 | primary |
| CUSTOM           | P4 | secondary |
| INFO             | P5 | secondary |

**ServiceNow** (`priority` number → internal `Pn`): `1→P1 … 5→P5`.
P1–P3 → **primary** flow; P4–P5 → **secondary** flow.

## Flow Assignment

- **Primary** (P1/P2/P3): full chain `1→2→3→4→[5∥6]→7`
- **Secondary** (P4/P5): enrichment-only `1→2→[3∥6]→7`

## Security

Every inbound webhook is HMAC-SHA256 verified via `shared.auth.verify_hmac_signature`.

- DT: header `X-DT-Signature`, secret `DT_WEBHOOK_SECRET`
- SNOW: header `X-SNOW-Signature`, secret `SNOW_WEBHOOK_SECRET`

If the relevant secret env var is unset, signature checks are skipped (dev only).
Missing or invalid signature → `401`.

## Deduplication

Uses a Redis distributed lock keyed on `dedup_key` (`dt:{problemId}` or
`snow:{number}`). A duplicate event arriving while a run is active returns
`{"accepted": false, "deduplicated": true}` without starting a second pipeline.

## Outputs

1. Upserts the incident to routing-db (`POST /admin/incidents`, fire-and-forget)
2. Creates a `PipelineRun`, stores its context in Redis (`run:{run_id}:ctx`, TTL 1h)
3. Creates the run in routing-db (`POST /admin/runs`)
4. Publishes `pipeline_started` SSE event
5. Enqueues `run_id` on `agent:2:queue`

## Response

```json
{ "accepted": true, "run_id": "…", "external_id": "P-123",
  "severity": "P2", "flow": "primary" }
```

`RESOLVED` DT events are ignored (`accepted: false`).

## Key Env Vars

`DT_WEBHOOK_SECRET`, `SNOW_WEBHOOK_SECRET`, `REDIS_URL`, `ROUTING_DB_URL`,
`ROUTING_DB_ADMIN_TOKEN`, `AGENT_1_PORT`

## Local Run

```bash
cd agents/Agent-1-dynatrace
uvicorn main:app --reload --port 8001
```
