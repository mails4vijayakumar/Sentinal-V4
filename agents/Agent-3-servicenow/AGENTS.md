# Agent 3 — ServiceNow INC Create / Bind

**Port:** `8003` · **Queue:** `agent:3:queue` · **Enqueues:** `agent:4:queue`

## Role

Bridges the pipeline to ServiceNow. For primary flows it **creates** a new
incident; for secondary flows it **binds** to the SNOW ticket that customer
support already opened. In both cases it flushes a structured work-note
containing the evidence gathered so far.

## Behaviour by Flow

| Flow      | Action  | Description |
|-----------|---------|-------------|
| primary   | `created` | `POST /api/now/table/incident` with impact/urgency from severity |
| secondary | `bound`   | `GET` the existing INC by number, capture sys_id + assignment group |
| (no SNOW) | `skipped` | When `SNOW_BASE_URL` is unset or number isn't an INC |

## Severity → SNOW Fields

`impact` and `urgency` both map `P1→1 … P5→5`. Category defaults to `Software`.
Caller is `SNOW_CALLER_ID`. These can be overridden per-priority via the
`routing.snow_config` table (seeded in `dev_seed.sql`).

## Work Note Flush

After create/bind, posts a work-note summarising the Splunk enrichment:

```
[Sentinel Auto-Triage]
Splunk: 42 errors / 7 warns in last 30min.
Classification: db_connection
Summary: <llm summary>
```

## Authentication

All SNOW calls obtain a bearer token from `shared.snow_auth.get_snow_token()`,
which transparently handles OAuth2 client-credentials (auto-refresh 60s before
expiry) or Basic-Auth fallback depending on `SNOW_AUTH_MODE`.

## Outputs

- Writes `ServiceNowEnrichment` (`snow_number`, `snow_sys_id`, `ci_name`, `action`)
- Records step + enrichment in routing-db
- Publishes `agent_start` / `agent_done`
- Enqueues Agent 4

## Failure Behaviour

SNOW errors are non-fatal — emits `agent_error`, still enqueues Agent 4.

## Key Env Vars

`SNOW_BASE_URL`, `SNOW_AUTH_MODE`, `SNOW_CLIENT_ID`, `SNOW_CLIENT_SECRET`,
`SNOW_USERNAME`, `SNOW_PASSWORD`, `SNOW_CALLER_ID`, `AGENT_3_PORT`
