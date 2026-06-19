# Agent 7 — Root-Cause Analysis & Resolution

**Port:** `8007` · **Queue:** `agent:7:queue` · **Enqueues:** none (terminal — finalises pipeline)

## Role

The final agent. Synthesises every enrichment (Splunk, ServiceNow, PagerDuty,
Confluence) into a structured root-cause analysis via the configured LLM,
resolves the SNOW incident, flushes a final work-note, stores the resolution to
the feedback schema, and emits the terminal `pipeline_complete` event.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET  | `/health`   | Liveness probe |
| POST | `/feedback` | Accept human feedback on resolution quality |

## LLM Providers (pluggable via `LLM_PROVIDER`)

| Provider   | Model env            | Endpoint |
|------------|----------------------|----------|
| `ollama`   | `OLLAMA_MODEL`       | `/api/generate` (format=json) |
| `anthropic`| `ANTHROPIC_MODEL`    | `/v1/messages` |
| `openai`   | `OPENAI_MODEL`       | `/v1/chat/completions` (json_object) |

Defaults to Ollama when the selected provider has no API key.

## Prompt → Structured JSON

The prompt assembles incident metadata, Splunk analysis, KB hits, and SNOW
context, then requests **JSON only**:

```json
{
  "root_cause": "one-sentence statement",
  "root_cause_category": "db_connection|memory|deployment|network|config|unknown",
  "confidence": 0.0,
  "rollback_required": false,
  "rollback_target": null,
  "resolution_steps": [
    {"step_num": 1, "action": "...", "owner": "...", "tool": "...",
     "command": null, "rationale": "..."}
  ]
}
```

Invalid JSON falls back to a low-confidence "unable to determine" result so the
pipeline always completes.

## SNOW Finalisation (primary flow)

1. `PATCH` incident → `state=6` (Resolved), `close_code`, `close_notes`
2. Flush a final work-note with root cause, category, confidence, rollback
   flag, and the numbered resolution steps

## Outputs

- Builds an `RCAResult` (root cause, confidence, steps, supporting KB, token usage)
- Records final step + marks the run **completed** in routing-db (with duration)
- Stores resolution via `POST /admin/feedback`
- Publishes terminal `pipeline_complete` SSE (or `pipeline_error` on failure)

## Feedback Loop

`POST /feedback` accepts `{run_id, rating, comment, ...}` and records it as a
`human_feedback` enrichment — feeding future model evaluation and KB tuning.

## Failure Behaviour

On unrecoverable error, marks the run `failed` in routing-db and emits
`pipeline_error` so the dashboard reflects the outcome.

## Key Env Vars

`LLM_PROVIDER`, `OLLAMA_BASE_URL`, `OLLAMA_MODEL`, `ANTHROPIC_API_KEY`,
`ANTHROPIC_MODEL`, `OPENAI_API_KEY`, `OPENAI_MODEL`, `SNOW_BASE_URL`, `AGENT_7_PORT`
