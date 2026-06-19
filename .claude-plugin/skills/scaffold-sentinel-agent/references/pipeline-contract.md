# Sentinel pipeline contract — what a new agent must respect

This reference summarises the load-bearing invariants from the repo `CLAUDE.md`
that a newly scaffolded agent must honour. Re-read the master doc for
authoritative detail.

## 1. Event chain (`shared/models.py`)

Pipeline flow today:

```
OrchestratorEvent
  → RoutedIncident       (Agent 1 assigns flow + severity)
  → EnrichedIncident     (Agents 2–6 add fields)
  → RCAResult            (Agent 7 terminal)
```

Each enrichment agent owns one Pydantic class (e.g. `SplunkEnrichment`,
`ServiceNowEnrichment`, `PagerDutyEnrichment`, `NotificationEnrichment`). When
adding a new agent, append a `{{NAME_TITLE}}Enrichment` class in the same style
and same section. Use `model_config = ConfigDict(extra="allow")` so future
additions stay backward-compatible.

The contract is additive: fields can be added, never renamed or removed without
a breaking-change migration plan.

## 2. Inter-agent secret naming (§6.1)

Every hop has **one shared secret** under **two names**:

| Sender's `.env` | Receiver's `.env` | Header sent |
|-----------------|-------------------|-------------|
| `AGENT_{N+1}_SECRET` | `AGENT_{N+1}_SHARED_SECRET` | `X-Agent{N}-Token` |

A new agent at position N must have:
- `AGENT_{N}_SHARED_SECRET` on its own side (validates inbound calls)
- `AGENT_{N+1}_SECRET` on its own side (signs outbound calls)

The upstream agent at position `N-1` must have `AGENT_{N}_SECRET` injected. All
comparisons must use `hmac.compare_digest` — never `==`.

## 3. Work-note format (§4.6)

Every agent that runs **after the SNOW INC exists** posts an attributed work
note. The format is mandatory:

```
=== <STAGE NAME> — Agent <N> ===
Timestamp : <ISO-8601>
Status    : <success | degraded | failed>
Duration  : <ms>
Pipeline  : <pipeline_run_id>

<stage-specific body>
```

Agents that run before the INC exists (Agent 1, Agent 2 in Flow A) buffer the
work note to Redis at `pending_worknote:{problemId}` (TTL 1 h) and Agent 3
flushes the buffer immediately after `POST /api/now/table/incident` returns.

## 4. Always-200 rule applies to Agent 1 only

A new mid-chain agent uses **normal HTTP semantics**:

- `200` on success
- `4xx` on contract errors (auth, schema) — sender will not retry
- `5xx` on transient errors — sender retries on `2/4/8 s` exponential backoff,
  then DLQ

Do **not** swallow errors with always-200 unless the new agent sits at a
webhook entry point that retry-storms (Dynatrace, ServiceNow). The chain is
glued together by the upstream agent's retry+DLQ logic.

## 5. PHI / log discipline (§10.6)

Healthcare deployment. Never log free-text incident fields:

- Log identifiers (`problem_id`, `incident_number`, `pipeline_run_id`,
  `entity_id`, `error_category`, `classification_confidence`).
- Log counts of matched items, never the items themselves.
- Forward only the fields the next agent needs via an explicit `to_downstream()`
  method, not the whole accumulated payload.
- Free-text fields (`matched_log_lines`, `description`, `short_description`)
  travel only as far as Agent 3, where they are written to the SNOW record.

## 6. Per-stage time budget

Total Flow A wall-clock target: **<60 s**, with the P1 PagerDuty page landing at
**~47 s**. A new mid-chain agent must fit its share of that budget. State the
target in the new agent's `AGENTS.md` and treat it as an invariant in tests.

## 7. Soft-fail vs hard-fail (§10.2)

Default to soft-fail. Only Agent 3's create-INC step is allowed to halt the
pipeline. Everything else degrades — the engineer should still get the page with
whatever evidence was gatherable. State the failure-mode policy in the new
agent's `AGENTS.md` table per the template.
