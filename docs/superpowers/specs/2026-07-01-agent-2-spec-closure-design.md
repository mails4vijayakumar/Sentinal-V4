# Agent 2 — Spec Closure Design

**Date:** 2026-07-01
**Scope:** Bring `agents/Agent-2-splunk` up to the behaviour specified in `CLAUDE.md` §5.2, delivered as three sequential PRs on one design.
**Non-goals:** Any change to Agents 1, 3, 4, 6, or 7. Any change to the queue-based inter-agent transport. Any LLM call from Agent 2.

---

## 1. Motivation

The running Agent 2 ([agents/Agent-2-splunk/main.py](../../../agents/Agent-2-splunk/main.py)) is a 199-line single-file worker that runs one Splunk `oneshot` search and classifies with substring matches (`connection`/`memory`/`deploy`). CLAUDE.md §5.2 describes something materially larger: six deterministic phases, an async Splunk submit/poll/fetch, weighted regex scoring, a conflict-resolution rule against a Dynatrace hypothesis at a 65% confidence threshold, routing pre-compute, and a Flow B branch that writes an evidence work note directly to ServiceNow.

Downstream agents already treat the output as if that richer classification exists — the intent of this spec is to make the reality match.

## 2. Constraints picked before design

Decisions taken during brainstorming, recorded here so the plan doesn't re-litigate them:

- **Wire protocol stays Redis-queue.** §6.2 of CLAUDE.md describes HTTP intake endpoints (`POST /intake/orchestrator-event`); the actual pipeline uses `BLPOP agent:N:queue` across every agent. We match the code, not the aspirational spec text. A follow-up doc PR should reconcile CLAUDE.md §6.2 with reality.
- **Naming stays "primary/secondary" in code.** CLAUDE.md uses "Flow A / Flow B" — the concepts are identical. This spec uses both terms interchangeably; code stays with primary/secondary.
- **Regex rule sets are hand-curated in `rules.py`.** No YAML config file, no `routing-db` table, no mining from Agent 8's synthesised KB. A small deliberate starter list (~10 patterns per rule set) that can evolve in code. Ops iteration cost is a redeploy — acceptable at current volume.
- **Query tiers fall back tight → host-only → index-wide.** T1 uses index + host + service + keywords; T2 drops the service filter; T3 drops host too. T1 short-circuits at ≥ 20 matches (new tuning constant `SPLUNK_TIER1_SHORTCIRCUIT_MIN`).
- **Layout: split into modules.** `main.py` (worker orchestration) + `classifier.py` (six phases as pure functions) + `rules.py` (data: regex + weights + maps) + `snow_notes.py` (thin reusable §4.6 SNOW work-note writer). First agent to break the flat "everything in main.py" convention — deliberate, because the phases must be unit-testable without FastAPI or Splunk.
- **Agent 2 remains deterministic.** No LLM call. `llm_summary` on `SplunkEnrichment` stays as a name for a human-readable narrative built from scored evidence, not a model call.
- **Flow B topology stays as it is in code today.** Agent 2 in secondary flow writes its SNOW evidence work note AND continues to enqueue Agents 3 + 6. This diverges from CLAUDE.md §4.2 (which places the fan-out at Agent 1). Spec-vs-code reconciliation is a follow-up documentation task; not in this scope.

## 3. Data contract changes (`shared/models.py`)

Extend `SplunkEnrichment` (which already has `model_config = ConfigDict(extra="allow")`). Every existing field is preserved so Agents 6 and 7 continue to read `.classification` without change.

New nested type:

```python
class RuleMatch(BaseModel):
    rule_id: str          # e.g. "db.connection_pool_exhausted"
    rule_set: str         # "app" | "infra" | "db"
    weight: float         # 1.0 | 1.2 | 1.1
    match_count: int      # occurrences across scanned log lines
    sample_line: str      # one representative line, truncated to 200 chars
```

Additions to `SplunkEnrichment`:

```python
tier_used: Optional[int] = None           # 1 / 2 / 3, or None if all failed
spl_queries: List[str] = []               # all tier queries actually executed
rule_matches: List[RuleMatch] = []        # scored matches across rule sets

dt_hypothesis: Optional[str] = None       # DT-derived category before conflict resolution
splunk_category: Optional[str] = None     # regex-scored category
confidence: float = 0.0                   # 0-100, Splunk's confidence in splunk_category
hypothesis_source: str = "dt"             # "dt" | "splunk" — winner of conflict resolution

error_category: Optional[str] = None      # final resolved category (mirrors .classification)
assigned_team: Optional[str] = None       # routing pre-compute — team label
assigned_queue: Optional[str] = None      # routing pre-compute — SNOW assignment_group
snow_category: Optional[str] = None       # SNOW `category` field value
snow_subcategory: Optional[str] = None    # SNOW `subcategory` field value
```

**Backward-compat rule:** `enrichment.classification` (existing string field) is always set to the same value as `enrichment.error_category`. No downstream reader needs to change.

**Confidence math.** For each rule set (app / infra / db):

```
score(rule_set) = Σ over matches in rule_set of (weight × log10(1 + match_count))
```

`splunk_category` is `argmax` over the three scores. `confidence` is the head-to-head ratio:

```
confidence = 100 × winning_score / (winning_score + runner_up_score + 1e-6)
```

Capped at 100. Zero matches → confidence 0. `1e-6` avoids division by zero when only one rule set matched.

## 4. Layout

```
agents/Agent-2-splunk/
├── main.py           # worker loop, process_run, SSE publishing, forwarding
├── classifier.py     # six phases as pure async/sync functions
├── rules.py          # DT_HYPOTHESIS_MAP, APP_RULES, INFRA_RULES, DB_RULES, CATEGORY_ROUTING
├── snow_notes.py     # §4.6-formatted work-note POST helper (reusable)
├── AGENTS.md         # updated per §9
└── Dockerfile
```

`snow_notes.py` is written to be reusable by Agents 6 and 7 when they add their own Flow B write paths — but Agent 2 is the only importer in this spec.

## 5. The six phases (`classifier.py`)

Each phase is a pure function. `main.py` calls them in sequence. All phases live in `classifier.py`; only `run_splunk_async` is I/O bound.

### 5.1 `preclassify(event: dict) → str`

Reads `event["raw_payload"]["eventType"]` and `event["raw_payload"]["entityType"]` (both strings) and looks up `DT_HYPOTHESIS_MAP` in `rules.py`:

```python
DT_HYPOTHESIS_MAP: Dict[Tuple[str, str], str] = {
    ("FAILURE_RATE_INCREASED",   "SERVICE"):      "app",
    ("RESPONSE_TIME_DEGRADATION","SERVICE"):      "app",
    ("APPLICATION_ERROR",        "APPLICATION"):  "app",
    ("PROCESS_UNAVAILABLE",      "PROCESS_GROUP"):"infra",
    ("PROCESS_MEMORY_RESOURCE_EXHAUSTED", "PROCESS_GROUP"): "infra",
    ("CPU_SATURATED",            "HOST"):         "infra",
    ("MEMORY_SATURATED",         "HOST"):         "infra",
    ("HOST_DISK_QUEUE_LENGTH_HIGH","HOST"):       "infra",
    ("HOST_NOT_AVAILABLE",       "HOST"):         "infra",
    ("DATABASE_CONNECTION_FAILURE","SERVICE"):    "db",
    ("SLOW_DB_QUERIES",          "SERVICE"):      "db",
}
```

**Shippable v1 = the eleven rows above.** Additional (eventType, entityType) combos are added in follow-up PRs as unmapped cases show up in the `preclassify_unmapped` log line. Unmapped combinations default to `"app"` — most alerts are service-level.

### 5.2 `build_tiered_queries(event, severity) → List[str]`

Returns three SPL strings, ordered tightest first, all sharing the severity-derived time window. `_WINDOW` moves from `main.py` into `rules.py`:

```python
WINDOW_MIN: Dict[str, int] = {"P1": 15, "P2": 30, "P3": 30, "P4": 60, "P5": 60}
ERROR_KEYWORDS = "(ERROR OR WARN OR CRITICAL OR Exception OR Traceback OR FATAL)"

def build_tiered_queries(host: str, service: str, severity: str, index: str) -> List[str]:
    w = WINDOW_MIN.get(severity, 30)
    earliest = f"-{w}m@m"
    max_rows = 500  # SPLUNK_MAX_RESULTS
    host_q  = f' host="{host}"' if host else ""
    svc_q   = f' source="*{service}*"' if service else ""
    t1 = f'search index={index}{host_q}{svc_q} earliest={earliest} {ERROR_KEYWORDS} | head {max_rows}'
    t2 = f'search index={index}{host_q} earliest={earliest} {ERROR_KEYWORDS} | head {max_rows}'
    t3 = f'search index={index} earliest={earliest} {ERROR_KEYWORDS} | head {max_rows}'
    return [t1, t2, t3]
```

If `host` is empty, T1 and T2 collapse to the same query — T2 is skipped in that case (deduped by `main.py`). If `service` is empty, T1 collapses to T2 — same dedup.

### 5.3 `run_splunk_async(client, spl) → List[dict]`

Replaces the current `exec_mode=oneshot` call with the async pattern from §5.2 of CLAUDE.md:

1. `POST /services/search/jobs` with `output_mode=json`, `exec_mode=normal` → returns `{"sid": "..."}`.
2. Poll `GET /services/search/jobs/{sid}` every `SPLUNK_JOB_POLL_INTERVAL_SECONDS` (default 2) up to `SPLUNK_JOB_MAX_POLLS` (default 10). Loop breaks on `dispatchState == "DONE"`. If loop exhausts without `DONE`, raise `SplunkPollTimeout` (caught by `main.py`).
3. `GET /services/search/jobs/{sid}/results?output_mode=json&count={SPLUNK_MAX_RESULTS}`.

`httpx.AsyncClient` is created with `verify=True` (fixes §10.6 violation). Timeouts split via `httpx.Timeout`: 10 s connect + submit, 20 s cumulative poll, 5 s fetch. Retry contract §6.3 (2 / 4 / 8 s) applies to job submit only — polling is not retried, a failed poll fails the tier.

### 5.4 `score(results) → List[RuleMatch]`

Iterates rows in `results`, extracts the row's dominant text field (`_raw` if present, otherwise the value of the first key that isn't a Splunk internal like `_time`, `_indextime`, `host`). Applies each rule's compiled regex; on match, increments a counter keyed on `rule_id` and stores the first matched line as `sample_line` (truncated to 200 chars).

`rules.py` defines the three rule sets:

```python
APP_RULES = [
    ("app.null_pointer",     re.compile(r"\b(NullPointer|NoneType)"),                       1.0),
    ("app.classnotfound",    re.compile(r"ClassNotFoundException|ModuleNotFoundError"),     1.0),
    ("app.deserialization",  re.compile(r"JsonProcessingException|InvalidObject|JSONDecode"),1.0),
    ("app.http_5xx",         re.compile(r"HTTP/1\.[01]\"\s+5\d\d\s"),                        1.0),
    ("app.unhandled_ex",     re.compile(r"Unhandled exception|Uncaught"),                    1.0),
]
INFRA_RULES = [
    ("infra.oom",            re.compile(r"OutOfMemory|OOMKilled|heap.*exhausted", re.I),     1.2),
    ("infra.disk_full",      re.compile(r"No space left on device|disk.*full", re.I),        1.2),
    ("infra.cpu_saturated",  re.compile(r"CPU.*(saturat|throttl)", re.I),                    1.2),
    ("infra.container_kill", re.compile(r"container.*killed|SIGKILL|Evicted"),               1.2),
]
DB_RULES = [
    ("db.conn_pool",         re.compile(r"connection pool.*(exhausted|timeout)|HikariCP.*timeout", re.I), 1.1),
    ("db.deadlock",          re.compile(r"deadlock detected|ORA-00060"),                     1.1),
    ("db.query_timeout",     re.compile(r"query timeout|statement timeout|ORA-01013"),       1.1),
    ("db.connection_refused",re.compile(r"connection refused.*(5432|3306|1521)"),            1.1),
]

**Shippable v1 = the rules listed above** (5 app, 4 infra, 4 db). Additional patterns are added in follow-up PRs based on production false-positive / false-negative feedback and monthly review of the `sample_line` values that appear in SNOW work notes.
```

Weights match §5.2 (app 1.0, infra 1.2, db 1.1).

### 5.5 `resolve_conflict(dt_hypothesis, rule_matches) → Tuple[str, float, str]`

Computes per-rule-set scores using the formula in §3 above. Returns `(final_category, confidence, source)`.

```python
def resolve_conflict(dt_hypothesis: str, matches: List[RuleMatch]) -> Tuple[str, float, str]:
    scores = defaultdict(float)
    for m in matches:
        scores[m.rule_set] += m.weight * math.log10(1 + m.match_count)
    if not scores:
        return dt_hypothesis, 0.0, "dt"
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    splunk_cat, winning = ranked[0]
    runner_up = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = 100 * winning / (winning + runner_up + 1e-6)
    if confidence >= CONFLICT_RESOLUTION_THRESHOLD:  # 65.0
        return splunk_cat, confidence, "splunk"
    return dt_hypothesis, confidence, "dt"
```

`splunk_category` (the argmax) and `confidence` are still recorded on the enrichment even when DT wins — the disagreement is visible downstream and in the work note.

### 5.6 `precompute_routing(final_category) → dict`

Static map in `rules.py`:

```python
CATEGORY_ROUTING = {
    "app":   {"team": "app-support", "queue": "APP_SUPPORT",
              "snow_category": "Application",   "snow_subcategory": "Runtime Error"},
    "infra": {"team": "infra-ops",   "queue": "INFRA_OPS",
              "snow_category": "Infrastructure","snow_subcategory": "Resource"},
    "db":    {"team": "dba",         "queue": "DB_ADMIN",
              "snow_category": "Database",     "snow_subcategory": "Query/Connection"},
}
```

Values become `assigned_team` / `assigned_queue` / `snow_category` / `snow_subcategory` on the enrichment. Agent 3 reads these on INC create; Agent 4 uses `assigned_queue` as a soft fallback when PagerDuty schedule lookup fails.

## 6. Flow orchestration (`main.py`)

`process_run` becomes shorter, not longer, because the classification logic moves out:

```python
async def process_run(run_id: str) -> None:
    redis, rc, t0 = await get_redis(), get_routing_client(), time.monotonic()
    await _emit_start(redis, run_id)

    ctx = await redis.get_context(run_id)
    if not ctx:
        return

    event    = ctx.get("event", {})
    severity = event.get("severity", "P3")
    flow     = ctx.get("flow", "primary")

    try:
        enrichment = await classifier.classify(event, severity)
        ctx.setdefault("enrichments", {})["splunk"] = enrichment.model_dump()
        await redis.store_context(run_id, ctx)
        await _record_step_and_enrichment(rc, run_id, enrichment, t0)
        await _emit_done(redis, run_id, enrichment)

        if flow == "secondary":
            sys_id = event.get("incident_sys_id")
            if sys_id:
                await snow_notes.post_work_note(
                    sys_id=sys_id, stage="SPLUNK EVIDENCE",
                    agent_num=2, status="success",
                    duration_ms=int((time.monotonic() - t0) * 1000),
                    run_id=run_id,
                    body=classifier.render_evidence_body(enrichment),
                )
            await asyncio.gather(redis.enqueue(3, run_id), redis.enqueue(6, run_id))
        else:
            await redis.enqueue(3, run_id)

    except Exception as exc:
        log.exception("agent2_error run_id=%s", run_id)
        await _emit_error(redis, run_id, exc)
        # Non-fatal: still forward to keep the pipeline moving.
        await redis.enqueue(3, run_id)
```

`classifier.classify(event, severity)` is the top-level entry point that runs the six phases in sequence and returns a `SplunkEnrichment`. `classifier.render_evidence_body(enrichment)` is a small helper that formats the body of the Flow B SNOW work note (see §7).

## 7. Flow B — SNOW work-note writer (`snow_notes.py`)

Reusable module (~50 LOC). Reuses `shared/snow_auth.get_snow_token()` and follows the same PATCH-to-`/api/now/table/incident/{sys_id}` pattern Agent 3 uses today.

```python
async def post_work_note(sys_id: str, stage: str, agent_num: int,
                         status: str, duration_ms: int, run_id: str,
                         body: str) -> None:
    """POST §4.6-formatted work note. Non-fatal on failure — logs and returns."""
    token = await get_snow_token()
    header = (f"=== {stage} — Agent {agent_num} ===\n"
              f"Timestamp : {datetime.utcnow().isoformat()}Z\n"
              f"Status    : {status}\n"
              f"Duration  : {duration_ms}\n"
              f"Pipeline  : {run_id}\n\n{body}")
    try:
        async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=15, verify=True) as c:
            r = await c.patch(f"/api/now/table/incident/{sys_id}",
                              headers={"Authorization": f"Bearer {token}"},
                              json={"work_notes": header})
            r.raise_for_status()
    except Exception as exc:
        log.warning("snow_worknote_failed sys_id=%s stage=%s error=%s", sys_id, stage, exc)
```

Rendered body (from `classifier.render_evidence_body`):

```
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

## 8. Failure modes

All non-fatal per §10.2 (Agent 3 create is the only step allowed to halt the pipeline).

| Failure                                    | Behaviour |
|--------------------------------------------|-----------|
| `SPLUNK_BASE_URL` empty                    | Enrichment with `hypothesis_source="dt"`, `confidence=0`, `llm_summary="Splunk not configured"`. Flow A forwards to Agent 3; Flow B posts a "no Splunk evidence available — using DT hypothesis: {cat}" work note and fans out to 3 + 6. |
| Splunk 5xx or `SplunkPollTimeout`          | Same as unconfigured but `llm_summary="Splunk query failed"`. Retry §6.3 applies to job submit only. |
| All 3 tiers return 0 rows                  | `rule_matches=[]`, `confidence=0`, DT hypothesis kept. Work note (Flow B) says "no matching log evidence". |
| SNOW PATCH fails (Flow B)                  | Log warning, continue. `agent_error` SSE fired; context still holds the enrichment for the dashboard. |
| Missing `incident_sys_id` on `ManualIncidentEvent` (Flow B) | Log error, skip SNOW write, still fan out to 3 + 6. Ops review — Agent 1 shouldn't dispatch Flow B without it. |
| Unmapped DT (eventType, entityType)        | Defaults to `"app"`. Log at `INFO`: `preclassify_unmapped eventType=X entityType=Y`. |
| Regex compile error at startup             | Fail fast — process exits at import. Prevents shipping a broken rules module. |

## 9. Observability

Structured log fields added on every `agent2_*` event: `dt_hypothesis`, `splunk_category`, `confidence`, `tier_used`, `hypothesis_source`, `rule_match_count`, `flow`, `run_id`.

**Never logged (per §10.6):** `sample_line` values, `spl_query` text (contains host/service), raw Splunk result rows. `sample_line` appears only in the SNOW work note and on the dashboard's problem detail view.

`llm_summary` on the enrichment gets a compact human-readable string, e.g.: `"3 db rule matches (score 5.2), 1 app match (score 0.3). Splunk overrode DT hypothesis (confidence 78%)."` — safe to log.

## 10. Tests

Target: overall 90% for the module (§10.4), 100% on `classifier.score`, `classifier.resolve_conflict`, and `snow_notes.post_work_note` formatting.

New files under `tests/`:

- `test_agent2_preclassify.py` — 15 (eventType, entityType) tuples → expected hypothesis; one "unmapped defaults to app" case.
- `test_agent2_query_builder.py` — assert T1/T2/T3 strings contain the expected filters and windows for each severity; assert host-empty and service-empty dedup.
- `test_agent2_scoring.py` — synthetic Splunk result sets → assert `RuleMatch` counts, scores, and winning rule set. Cover: pure-app, pure-infra, pure-db, mixed 60/40, empty results.
- `test_agent2_conflict.py` — matrix of (dt_hypothesis, splunk_category, confidence) → assert final category and `hypothesis_source`. Boundary cases: confidence = 64.9 (DT wins), 65.0 (Splunk wins), 100 (Splunk wins), 0 (DT wins). Zero-matches case.
- `test_agent2_routing.py` — each of the three categories → assert routing fields on the enrichment.
- `test_agent2_flow_b_worknote.py` — mocked SNOW; assert §4.6 format is byte-exact including timestamp header, and that a Splunk failure still results in a "no evidence" work note.
- `test_agent2_short_circuit.py` — T1 returns ≥ 20 rows → assert T2 and T3 not executed (verify via mock call count).

Existing `tests/test_pipeline.py` gets one addition: a Flow B end-to-end test that fires a `ManualIncidentEvent`, waits for the SNOW mock to receive both the work note and the fan-out to Agents 3 + 6 queues.

## 11. Tuning constants

Append to CLAUDE.md §9.2:

| Variable                             | Default | Purpose |
|--------------------------------------|---------|---------|
| `SPLUNK_TIER1_SHORTCIRCUIT_MIN`      | 20      | Rows in T1 before skipping T2/T3 |
| `CONFLICT_RESOLUTION_THRESHOLD`      | 65.0    | Already spec'd — now actually used |
| `SPLUNK_JOB_POLL_INTERVAL_SECONDS`   | 2       | Already spec'd — now actually used |
| `SPLUNK_JOB_MAX_POLLS`               | 10      | Already spec'd — now actually used |
| `SPLUNK_MAX_RESULTS`                 | 500     | Already spec'd — now actually used |

No new env-var contracts for other agents. `AGENT_2_PORT`, `SPLUNK_*` all unchanged.

## 12. Delivery sequence — three PRs

**PR-1: Classifier core, unwired.**
Add `rules.py`, `classifier.py`, and the widened `SplunkEnrichment` fields. Add all `test_agent2_*` tests except `test_agent2_flow_b_worknote.py`. `main.py` untouched — still calls the old `_run_splunk`. Pure-add; zero risk to running pipeline.

**PR-2: Wire classifier, async Splunk, kill `verify=False`.**
Delete `_run_splunk` and the substring `_classify` from `main.py`. Route `process_run` through `classifier.classify()`. Flow B path still fans out to Agents 3 + 6 (unchanged from today). Rollback: revert PR-2, PR-1's additions stay inert but harmless.

**PR-3: Flow B SNOW work-note write.**
Add `snow_notes.py` and `test_agent2_flow_b_worknote.py`. Modify `process_run` to call `snow_notes.post_work_note` in the secondary branch before enqueuing Agents 3 + 6.

## 13. Follow-ups (not in this spec)

- **CLAUDE.md §6.2 vs code.** The spec text describing HTTP `POST /intake/*` endpoints does not match the queue-based transport in code. A doc-only PR should either update §6.2 to describe the queue model or add an explicit "spec deviates from code" note.
- **CLAUDE.md §4.2 vs code.** The spec places Flow B fan-out at Agent 1; today the fan-out lives at Agent 2 and this design preserves that. A separate design should decide whether to move the fan-out to Agent 1 (per spec) — that touches Agents 1, 6, 7 and is out of scope here.
- **Rule set evolution.** Once a few weeks of scored classifications accumulate, review `kb_synthesis_decisions` and manual overrides to see which rule sets misfire. Consider whether YAML config or a routing-db table becomes justified at that point.
- **`snow_notes.py` reuse by Agents 6 and 7.** When those agents add Flow B write paths, they import `snow_notes` rather than re-implementing the §4.6 header.
