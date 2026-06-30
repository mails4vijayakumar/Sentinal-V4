# Agent 1 — Prometheus Alertmanager Intake (Design Spec)

**Date:** 2026-06-30
**Status:** Approved for planning
**Author:** Vijayakumar Shanmuganathan
**Affects:** `agents/Agent-1-dynatrace/` (primary), `shared/models.py` (1-line), Agent 3 (small branch), routing-db (new table — separate spec)

## 1. Problem

Sentinel today accepts two intake sources: Dynatrace webhooks (Flow A) and ServiceNow webhooks (Flow B). Teams running Prometheus + Alertmanager (AM) for open-source observability have no first-class path into the pipeline. Wrapping AM alerts as synthetic DT payloads (the only alternative without code changes) loses source attribution, breaks audit logging (§10.6), and pollutes DT-specific dedup keys.

We need a third intake that:

- Accepts AM v4 webhook payloads natively.
- Routes per-alert into Flow A or Flow B by severity + status, with an explicit `sentinel_flow` label escape hatch.
- Resolves AM labels to a Dynatrace entity ID (required for Agent 7 GitLab correlation) with graceful degradation.
- Handles `status=resolved` to auto-close bound SNOW incidents.
- Adds the third source without further bloating `main.py`.

## 2. Non-goals (v1)

- Grafana Alerting payload variant (separate format; later via the same endpoint).
- Inhibition / silence modeling (AM filters these before the webhook).
- Multi-tenant AM with per-tenant tokens.
- Per-alert-rule `sentinel_auto_close: false` opt-out.
- Any dashboard or chatbot UI change — AM runs flow through existing SSE/reports.

## 3. Architectural decisions

| Decision | Choice | Rationale |
|---|---|---|
| **Structure** | Refactor to `agents/Agent-1-dynatrace/intake/{dt,snow,alertmanager}.py`; `main.py` becomes routes + lifespan + SSE only. | One file currently ~320 lines; adding AM + chat orchestrator without refactor pushes past 500. Per-source unit tests become trivial. Aligns with brainstorming "smaller well-bounded units" principle. |
| **Flow routing** | severity + status: `firing` + `critical`/`warning` → Flow A; `firing` + lower → Flow B; `resolved` → close-out path. Explicit `sentinel_flow` label overrides. | Matches current `main.py` convention (severity drives flow). `warning` → HIGH is intentionally narrower than DT's MEDIUM→Flow A — warning in Prom rarely background noise. |
| **Entity resolution** | Hybrid: `dt_entity_id` label → routing-db `am_entity_map` → DT Entities name lookup → soft-fail to name-only. | Each step is cheaper and more reliable than the next falls back to. Stable entity_id when authors care; map when ops curates; name when no one's looked yet. Soft-fail keeps the pipeline running per §10.2. |
| **Dedup** | Per-alert `external_id = f"am-{fingerprint}"` using AM's stable per-alert hash. | One INC per fingerprint mirrors per-problem DT behaviour. AM groups remain visible via `groupKey` in `raw_payload` but don't collapse to a single INC. |
| **Auth** | `Authorization: Bearer <token>` via `AM_WEBHOOK_TOKEN` env. Empty env → 503. | AM supports bearer natively; no sidecar needed. Compare-digest guard. Fail-closed on empty token. |
| **Resolved handling** | Auto-close bound SNOW INC (`state=7`, close_code, close_notes) when AM reports `status=resolved`. Guarded by `u_source_tool='Alertmanager'` preflight. | User decision. Mirrors Agent 7 resolution monitor for DT. Guardrail prevents accidental close of foreign INCs on fingerprint collision. |

## 4. Module layout

```
agents/Agent-1-dynatrace/
├── main.py                     # routes + lifespan + SSE only (refactored)
├── intake/
│   ├── __init__.py
│   ├── dt.py                   # extracted from current lines 105–150
│   ├── snow.py                 # extracted from current lines 155–189
│   └── alertmanager.py         # NEW — verify + parse + to_events + dispatch + _resolve
├── adapters/
│   └── am_entity_resolver.py   # NEW — hybrid entity resolution
└── AGENTS.md                   # updated: new endpoint + AM table + labels reference
```

### 4.1 main.py contract after refactor

```python
@app.post("/api/webhook/dynatrace", status_code=202)
async def dynatrace_webhook(request, x_dt_signature):
    return await dt.handle(request, x_dt_signature, _ingest)

@app.post("/api/webhook/servicenow", status_code=202)
async def servicenow_webhook(request, x_snow_signature):
    return await snow.handle(request, x_snow_signature, _ingest)

@app.post("/api/webhook/alertmanager", status_code=202)
async def alertmanager_webhook(request, authorization):
    return await alertmanager.handle(request, authorization, _ingest, _resolve)
```

`_ingest()` stays untouched. `_resolve(external_id)` is the new sibling (§7).

### 4.2 Per-intake module contract

Each `intake/*.py` exports:

- `verify(header_value, body) -> None` — raises `HTTPException(401)` on mismatch.
- `parse(body) -> Payload` — pydantic, raises `HTTPException(422)` on bad shape.
- `to_events(payload) -> list[OrchestratorEvent]` — one per alert (always a list, length 1 for DT/SNOW).
- `handle(request, header, _ingest, _resolve=None)` — orchestrates the four steps + dispatch.

## 5. AM payload model

```python
class AMAlert(BaseModel):
    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str]
    startsAt: datetime
    endsAt: datetime | None = None
    fingerprint: str
    generatorURL: str | None = None

class AMPayload(BaseModel):
    version: Literal["4"]
    groupKey: str
    status: Literal["firing", "resolved"]
    receiver: str
    externalURL: str
    alerts: list[AMAlert]
```

**Required per-alert labels:** `alertname`, `severity`. Missing → that alert is dropped with a warning; the rest of the batch proceeds. (AM retries the whole POST otherwise — a single bad rule must not poison the batch.)

**Optional labels read:** `service`, `instance`, `job`, `dt_entity_id`, `sentinel_flow`, `environment`.

## 6. Routing logic

```python
SEV_MAP = {"critical": Severity.CRITICAL, "warning": Severity.HIGH, "info": Severity.LOW}
sev = SEV_MAP.get(labels["severity"].lower(), Severity.INFO)

if labels.get("sentinel_flow") in ("a", "b"):
    flow = IncidentFlow.PRIMARY if labels["sentinel_flow"] == "a" else IncidentFlow.SECONDARY
else:
    flow = IncidentFlow.PRIMARY if sev in (Severity.CRITICAL, Severity.HIGH) else IncidentFlow.SECONDARY

event = OrchestratorEvent(
    source       = IncidentSource.ALERTMANAGER,     # new enum value
    external_id  = f"am-{alert.fingerprint}",
    severity     = sev,
    flow         = flow,
    title        = annotations.get("summary") or labels["alertname"],
    host         = labels.get("instance"),
    service      = labels.get("service") or labels.get("job"),
    raw_payload  = alert.model_dump(),
    dedup_key    = f"am:{alert.fingerprint}",
)
```

Dispatch: `asyncio.gather(*[_ingest(e) for e in firing_events], *[_resolve(e.external_id) for e in resolved_events], return_exceptions=True)`. Per-alert exceptions logged; batch returns counts:

```json
{"accepted": 3, "deduplicated": 1, "resolved": 1, "dropped": 0, "errors": 0}
```

## 7. Resolved handler

```python
async def _resolve(external_id: str) -> dict:
    redis   = await get_redis()
    binding = await redis.get(f"snow_incident:{external_id}")
    if not binding: return {"resolved": False, "reason": "no_binding"}

    binding = json.loads(binding)
    snow    = get_snow_client()

    inc = await snow.get_incident(binding["incident_sys_id"], fields=["u_source_tool"])
    if inc.get("u_source_tool") != "Alertmanager":
        log.warning("AM resolve refused — INC source=%s", inc.get("u_source_tool"))
        return {"resolved": False, "reason": "source_mismatch"}

    await snow.patch_incident(binding["incident_sys_id"], {
        "state":       "7",
        "close_code":  "Solved (Permanently)",
        "close_notes": f"Auto-closed by Sentinel — Alertmanager reported "
                       f"status=resolved at {now_iso()}. fingerprint={external_id[3:]}",
        "work_notes":  _resolve_worknote(external_id),
    })

    await redis.delete(f"snow_incident:{external_id}")
    await redis.delete(f"problem_id:{binding['incident_number']}")
    await redis.publish_event(SSEEvent(
        event=SSEEventType.PIPELINE_RESOLVED,
        run_id=binding.get("run_id"),
        data={"external_id": external_id, "source": "alertmanager"},
    ).model_dump(mode="json"))
    return {"resolved": True, "incident_number": binding["incident_number"]}
```

Work note (§4.6 format):

```
=== RESOLUTION — Alertmanager ===
Timestamp : <ISO-8601>
Status    : auto-closed
Source    : alertmanager fingerprint=<fingerprint>
Note      : External monitoring reported condition cleared.
```

## 8. Entity resolver (`adapters/am_entity_resolver.py`)

```python
async def resolve(labels: dict) -> EntityRef:
    if eid := labels.get("dt_entity_id"):
        return EntityRef(entity_id=eid, source="label")

    key = labels.get("service") or labels.get("job")
    if key and (mapped := await routing_client.get_am_entity(key)):
        return EntityRef(entity_id=mapped.entity_id, source="routing-db")

    name = labels.get("service") or labels.get("instance") or labels.get("job")
    if name and (eid := await dt_client.resolve_entity_by_name(name)):
        return EntityRef(entity_id=eid, entity_name=name, source="dt-name")

    return EntityRef(entity_name=name, source="unresolved")
```

The `source` field is logged for observability; not forwarded downstream beyond `raw_payload`.

## 9. Configuration

| Env var | Default | Purpose |
|---|---|---|
| `AM_WEBHOOK_TOKEN` | (empty → 503) | Bearer token AM presents |
| `AM_INTAKE_ENABLED` | `true` | Feature flag — `false` returns 404 |
| `AM_DEFAULT_ENV` | `prod` | Splunk-index derivation fallback when AM has no `environment` label |

No new tuning constants. Reuses dedup TTL, SNOW timeouts, etc.

## 10. Observability

**Structured log per alert** (no free-text content per §10.6):

```
pipeline_run_id  external_id (am-<fp>)  am_fingerprint  entity_resolver_source
flow  severity  status (firing|resolved)  alertname  dropped_reason?
```

**New `/metrics` counters:**

- `am_alerts_received_total{status}`
- `am_alerts_dropped_total{reason}`  (`missing_labels` | `parse_error` | `auth_failed`)
- `am_resolve_total{outcome}`  (`closed` | `no_binding` | `source_mismatch` | `snow_error`)
- `am_entity_resolved_total{source}`  (`label` | `routing-db` | `dt-name` | `unresolved`)

## 11. Testing

| File | Type | Coverage |
|---|---|---|
| `tests/test_alertmanager_intake.py` | unit | auth (4 cases), schema (v4-only), severity+status matrix, `sentinel_flow` override, dropped-alert handling, mixed firing/resolved batch, fingerprint→external_id, `_resolve` orphan, `_resolve` source-tool guardrail |
| `tests/test_am_entity_resolver.py` | unit | hybrid order + soft-fail at each step |
| `tests/test_pipeline.py::test_alertmanager_e2e` | integration | end-to-end: firing → Agent 3 INC → resolved → state=7 |

Coverage floor: 90% overall, `intake/alertmanager.py` auth path 100% per §10.4.

## 12. Failure modes (additions to §10.2)

| Failure | Blocking? | Behaviour |
|---|---|---|
| Bearer token mismatch | Yes (401) | AM retries; investigate |
| Empty `AM_WEBHOOK_TOKEN` env | Yes (503) | Endpoint disabled; safe-by-default |
| Payload schema invalid | Yes (422) | AM retries |
| Single alert in batch missing required labels | No | That alert dropped + warning |
| Entity resolver: all four steps soft-fail | No | Event proceeds with `entity_name` only; Agent 7 degrades to `non_deployment` |
| routing-db `am_entity_map` 404 | No | Resolver falls through to DT name lookup |
| `_resolve()` no binding | No | Logged + 200 returned |
| `_resolve()` SNOW PATCH 5xx | No | Logged; AM retries; idempotent (state=7 terminal) |
| `_resolve()` source-tool mismatch | No | Refused; INC untouched |

## 13. Dependencies (cross-component)

| Component | Change | Blocks v1? |
|---|---|---|
| `shared/models.py` | Add `IncidentSource.ALERTMANAGER` | Yes — trivial |
| Agent 3 | Branch on `source=ALERTMANAGER` to set `u_source_tool='Alertmanager'` and `u_source_alert_id=fingerprint` | Yes for `_resolve` guardrail; firing-only works without |
| routing-db | New `am_entity_map` table + `GET /v1/am-entities/{key}` — **separate spec** | No — resolver soft-fails |

## 14. Rollout

1. Behind `AM_INTAKE_ENABLED=false` initially. Refactor + endpoint + tests merge dark.
2. Set `AM_WEBHOOK_TOKEN` in secrets manager; flip flag in a single non-prod cluster's AM config.
3. Watch `am_alerts_received_total`, `am_entity_resolved_total{source="unresolved"}`, and `am_resolve_total{outcome}` for one week.
4. Curate `am_entity_map` to reduce `unresolved`/`dt-name` reliance.
5. Production AM rollout once `unresolved` rate < 5%.

## 15. Open questions deferred to implementation

- Exact PATCH shape for `u_source_alert_id` when value contains characters outside SNOW's accepted set for that field — verify against tenant schema via `scripts/verify_snow_schema.py` extension.
- Whether `_resolve` should also publish to `pipeline_run:{runId}` event store for `/reports` MTTR closing-bracket calc. Likely yes; confirm during integration test.
