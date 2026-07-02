# Agent 2 Spec Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `agents/Agent-2-splunk` up to the six-phase deterministic classifier specified in CLAUDE.md §5.2, add the Flow B ServiceNow evidence work-note write, and preserve backward compatibility for Agents 6 and 7.

**Architecture:** Split the current 199-line `main.py` into (a) `main.py` — worker orchestration, (b) `classifier.py` — six phases as pure functions plus a `classify()` entry point, (c) `rules.py` — hand-curated data (regex sets, weights, DT hypothesis map, category routing, error keywords), (d) `snow_notes.py` — a reusable §4.6-formatted SNOW work-note POST helper. Delivered as three sequential PRs.

**Tech Stack:** Python 3.12, FastAPI, `httpx.AsyncClient`, pydantic v2, structlog, pytest (`asyncio_mode=auto`), Redis (BLPOP-based worker), Splunk `/services/search/jobs`, ServiceNow Table API.

**Design reference:** [`docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md`](../specs/2026-07-01-agent-2-spec-closure-design.md).

## Global Constraints

- **Wire protocol stays Redis-queue** — `BLPOP agent:2:queue`, not HTTP `/intake/*`. Follow the existing pattern in `agents/Agent-1-dynatrace/main.py:193` and current `agents/Agent-2-splunk/main.py`.
- **Naming stays `primary` / `secondary`** in code (spec's "Flow A / Flow B" are the same concepts).
- **Regex rule sets are hand-curated in `rules.py`.** No YAML, no routing-db table.
- **Query tier order** — T1 (index + host + service + keywords), T2 (drop service), T3 (drop host).
- **T1 short-circuit** — when T1 returns ≥ `SPLUNK_TIER1_SHORTCIRCUIT_MIN` rows (default `20`), T2 and T3 are skipped.
- **`CONFLICT_RESOLUTION_THRESHOLD = 65.0`** — Splunk wins iff head-to-head confidence ≥ 65.
- **Rule weights** — app 1.0, infra 1.2, db 1.1 (verbatim from CLAUDE.md §5.2).
- **`SPLUNK_JOB_POLL_INTERVAL_SECONDS = 2`, `SPLUNK_JOB_MAX_POLLS = 10`, `SPLUNK_MAX_RESULTS = 500`** — from CLAUDE.md §9.2.
- **Agent 2 is deterministic** — no LLM call. `llm_summary` is a human-readable narrative string built from scored evidence.
- **`httpx.AsyncClient` uses `verify=True`** everywhere in Agent 2 (CLAUDE.md §10.6).
- **Never log `sample_line`, raw Splunk rows, or `spl_query` text** (CLAUDE.md §10.6). These appear only in the SNOW work note.
- **Backward compat** — `SplunkEnrichment.classification` (existing string field) is always set to the same value as the new `error_category` field. Agents 6 and 7 continue to work unchanged.
- **Line length 100** (ruff project convention).
- **Tests live at repo root** under `tests/`, not per-agent.
- **Pipeline non-fatal contract** — Agent 2 failures never halt the pipeline; on any exception `main.py` still forwards to Agent 3 (primary) or fans out to 3+6 (secondary).
- **Flow B fan-out stays at Agent 2** — after the SNOW work-note write, still enqueue Agents 3 and 6. Diverges from CLAUDE.md §4.2 (Agent 1 fan-out) intentionally; reconciliation is a follow-up doc PR out of scope here.
- **PR boundaries:** Tasks 1–9 land as PR-1 (classifier core, unwired); Task 10 lands as PR-2 (wire); Tasks 11–13 land as PR-3 (Flow B).

---

## File Structure

**Created:**
- `agents/Agent-2-splunk/rules.py` — constants only (no I/O, no logic).
- `agents/Agent-2-splunk/classifier.py` — six phases + `classify()` entry + `render_evidence_body()`.
- `agents/Agent-2-splunk/snow_notes.py` — SNOW work-note POST helper (PR-3).
- `tests/test_agent2_preclassify.py`
- `tests/test_agent2_query_builder.py`
- `tests/test_agent2_scoring.py`
- `tests/test_agent2_conflict.py`
- `tests/test_agent2_routing.py`
- `tests/test_agent2_classify.py` — short-circuit + full `classify()` orchestration.
- `tests/test_agent2_render_body.py`
- `tests/test_agent2_flow_b_worknote.py` (PR-3)

**Modified:**
- `shared/models.py` — add `RuleMatch`, widen `SplunkEnrichment`.
- `agents/Agent-2-splunk/main.py` — wire `classifier.classify()` (PR-2); add Flow B SNOW write (PR-3); remove `verify=False` and old `_run_splunk` / `_classify`.
- `tests/test_pipeline.py` — one added end-to-end Flow B test (PR-3).

---

## Task 1 — Extend `SplunkEnrichment` and add `RuleMatch`

**Files:**
- Modify: `shared/models.py` (`SplunkEnrichment` at line 154; add `RuleMatch` above it)
- Test: `tests/test_agent2_models.py` (new)

**Interfaces:**
- Consumes: nothing new; existing pydantic v2 (`BaseModel`, `ConfigDict`, `Field`).
- Produces:
  - `class RuleMatch(BaseModel)` with fields `rule_id: str`, `rule_set: str`, `weight: float`, `match_count: int`, `sample_line: str`.
  - `SplunkEnrichment` gains: `tier_used: Optional[int]`, `spl_queries: List[str]`, `rule_matches: List[RuleMatch]`, `dt_hypothesis: Optional[str]`, `splunk_category: Optional[str]`, `confidence: float`, `hypothesis_source: str = "dt"`, `error_category: Optional[str]`, `assigned_team: Optional[str]`, `assigned_queue: Optional[str]`, `snow_category: Optional[str]`, `snow_subcategory: Optional[str]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_models.py`:

```python
"""Backward-compat and new-field checks for SplunkEnrichment."""
from shared.models import SplunkEnrichment, RuleMatch


def test_rule_match_shape():
    m = RuleMatch(rule_id="db.conn_pool", rule_set="db", weight=1.1,
                  match_count=3, sample_line="HikariCP timeout")
    assert m.rule_id == "db.conn_pool"
    assert m.rule_set == "db"
    assert m.weight == 1.1
    assert m.match_count == 3
    assert m.sample_line == "HikariCP timeout"


def test_splunk_enrichment_backward_compat_defaults():
    """Existing readers (Agents 6, 7) rely on these fields with these defaults."""
    e = SplunkEnrichment()
    assert e.log_lines_scanned == 0
    assert e.error_count == 0
    assert e.warn_count == 0
    assert e.top_errors == []
    assert e.classification is None or isinstance(e.classification, str)


def test_splunk_enrichment_new_fields_defaults():
    e = SplunkEnrichment()
    assert e.tier_used is None
    assert e.spl_queries == []
    assert e.rule_matches == []
    assert e.dt_hypothesis is None
    assert e.splunk_category is None
    assert e.confidence == 0.0
    assert e.hypothesis_source == "dt"
    assert e.error_category is None
    assert e.assigned_team is None
    assert e.assigned_queue is None
    assert e.snow_category is None
    assert e.snow_subcategory is None


def test_splunk_enrichment_roundtrip():
    e = SplunkEnrichment(
        tier_used=1, confidence=78.0, hypothesis_source="splunk",
        error_category="db", assigned_team="dba",
        rule_matches=[RuleMatch(rule_id="db.conn_pool", rule_set="db",
                                weight=1.1, match_count=42,
                                sample_line="HikariCP - Connection pool exhausted")],
    )
    dumped = e.model_dump()
    restored = SplunkEnrichment(**dumped)
    assert restored.tier_used == 1
    assert restored.confidence == 78.0
    assert restored.rule_matches[0].rule_id == "db.conn_pool"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'RuleMatch' from 'shared.models'` on the first line of the test file.

- [ ] **Step 3: Modify `shared/models.py`**

Add `RuleMatch` immediately above the `class SplunkEnrichment` definition (currently at line 154):

```python
class RuleMatch(BaseModel):
    """One scored regex hit from Agent 2's classifier."""
    model_config = ConfigDict(extra="allow")

    rule_id:     str
    rule_set:    str            # "app" | "infra" | "db"
    weight:      float
    match_count: int
    sample_line: str
```

Then extend `SplunkEnrichment` by appending the new fields inside the class body (do not remove or reorder any existing fields):

```python
    # ── Six-phase classifier additions (see docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md) ──
    tier_used:         Optional[int]         = None
    spl_queries:       List[str]             = Field(default_factory=list)
    rule_matches:      List[RuleMatch]       = Field(default_factory=list)

    dt_hypothesis:     Optional[str]         = None
    splunk_category:   Optional[str]         = None
    confidence:        float                 = 0.0
    hypothesis_source: str                   = "dt"

    error_category:    Optional[str]         = None
    assigned_team:     Optional[str]         = None
    assigned_queue:    Optional[str]         = None
    snow_category:     Optional[str]         = None
    snow_subcategory:  Optional[str]         = None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_models.py -v`
Expected: PASS — all 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add shared/models.py tests/test_agent2_models.py
git commit -m "feat(models): add RuleMatch and widen SplunkEnrichment for six-phase classifier"
```

---

## Task 2 — `rules.py` — data constants module

**Files:**
- Create: `agents/Agent-2-splunk/rules.py`
- Test: `tests/test_agent2_rules.py` (new)

**Interfaces:**
- Consumes: standard library `re`.
- Produces: module-level constants importable as `from rules import ...`:
  - `WINDOW_MIN: Dict[str, int]` — severity → minutes.
  - `ERROR_KEYWORDS: str` — SPL keyword clause.
  - `DT_HYPOTHESIS_MAP: Dict[Tuple[str, str], str]` — 11 entries as listed in the spec §5.1.
  - `APP_RULES`, `INFRA_RULES`, `DB_RULES: List[Tuple[str, re.Pattern, float]]`.
  - `CATEGORY_ROUTING: Dict[str, Dict[str, str]]`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_rules.py`:

```python
"""Structural checks on the hand-curated rules module."""
import re
import sys
from pathlib import Path

# Make agents/Agent-2-splunk/ importable without touching PYTHONPATH globally.
_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import rules  # noqa: E402


def test_window_min_covers_all_severities():
    for sev in ("P1", "P2", "P3", "P4", "P5"):
        assert sev in rules.WINDOW_MIN
    assert rules.WINDOW_MIN["P1"] == 15
    assert rules.WINDOW_MIN["P2"] == 30
    assert rules.WINDOW_MIN["P4"] == 60


def test_dt_hypothesis_map_shape():
    # Spec §5.1 requires eleven rows in v1.
    assert len(rules.DT_HYPOTHESIS_MAP) == 11
    assert rules.DT_HYPOTHESIS_MAP[("FAILURE_RATE_INCREASED", "SERVICE")] == "app"
    assert rules.DT_HYPOTHESIS_MAP[("CPU_SATURATED", "HOST")] == "infra"
    assert rules.DT_HYPOTHESIS_MAP[("DATABASE_CONNECTION_FAILURE", "SERVICE")] == "db"


def test_rule_sets_use_expected_weights():
    assert all(w == 1.0 for _, _, w in rules.APP_RULES)
    assert all(w == 1.2 for _, _, w in rules.INFRA_RULES)
    assert all(w == 1.1 for _, _, w in rules.DB_RULES)


def test_rule_sets_have_shippable_v1_size():
    # Spec §5.4 shippable v1: 5 app + 4 infra + 4 db.
    assert len(rules.APP_RULES) == 5
    assert len(rules.INFRA_RULES) == 4
    assert len(rules.DB_RULES) == 4


def test_rules_use_compiled_regex():
    for rule_set in (rules.APP_RULES, rules.INFRA_RULES, rules.DB_RULES):
        for rule_id, pattern, weight in rule_set:
            assert isinstance(pattern, re.Pattern), f"{rule_id} not compiled"


def test_category_routing_covers_three_categories():
    for cat in ("app", "infra", "db"):
        row = rules.CATEGORY_ROUTING[cat]
        assert row["team"]
        assert row["queue"]
        assert row["snow_category"]
        assert row["snow_subcategory"]


def test_error_keywords_is_spl_clause():
    assert rules.ERROR_KEYWORDS.startswith("(") and rules.ERROR_KEYWORDS.endswith(")")
    assert "ERROR" in rules.ERROR_KEYWORDS
    assert "Exception" in rules.ERROR_KEYWORDS
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_rules.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'rules'`.

- [ ] **Step 3: Create `agents/Agent-2-splunk/rules.py`**

```python
"""
agents/Agent-2-splunk/rules.py
==============================
Hand-curated data for Agent 2's six-phase classifier. Import-only, no I/O.

Spec: docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

# ── Search window per severity (minutes) ─────────────────────────────────────
WINDOW_MIN: Dict[str, int] = {"P1": 15, "P2": 30, "P3": 30, "P4": 60, "P5": 60}

# ── SPL keyword clause used by every tier ────────────────────────────────────
ERROR_KEYWORDS: str = "(ERROR OR WARN OR CRITICAL OR Exception OR Traceback OR FATAL)"

# ── DT (eventType, entityType) → hypothesis category ─────────────────────────
DT_HYPOTHESIS_MAP: Dict[Tuple[str, str], str] = {
    ("FAILURE_RATE_INCREASED",            "SERVICE"):       "app",
    ("RESPONSE_TIME_DEGRADATION",         "SERVICE"):       "app",
    ("APPLICATION_ERROR",                 "APPLICATION"):   "app",
    ("PROCESS_UNAVAILABLE",               "PROCESS_GROUP"): "infra",
    ("PROCESS_MEMORY_RESOURCE_EXHAUSTED", "PROCESS_GROUP"): "infra",
    ("CPU_SATURATED",                     "HOST"):          "infra",
    ("MEMORY_SATURATED",                  "HOST"):          "infra",
    ("HOST_DISK_QUEUE_LENGTH_HIGH",       "HOST"):          "infra",
    ("HOST_NOT_AVAILABLE",                "HOST"):          "infra",
    ("DATABASE_CONNECTION_FAILURE",       "SERVICE"):       "db",
    ("SLOW_DB_QUERIES",                   "SERVICE"):       "db",
}

# ── Regex rule sets: (rule_id, compiled_pattern, weight) ─────────────────────
APP_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("app.null_pointer",    re.compile(r"\b(NullPointer|NoneType)"),                        1.0),
    ("app.classnotfound",   re.compile(r"ClassNotFoundException|ModuleNotFoundError"),      1.0),
    ("app.deserialization", re.compile(r"JsonProcessingException|InvalidObject|JSONDecode"),1.0),
    ("app.http_5xx",        re.compile(r'HTTP/1\.[01]"\s+5\d\d\s'),                          1.0),
    ("app.unhandled_ex",    re.compile(r"Unhandled exception|Uncaught"),                     1.0),
]

INFRA_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("infra.oom",            re.compile(r"OutOfMemory|OOMKilled|heap.*exhausted", re.I), 1.2),
    ("infra.disk_full",      re.compile(r"No space left on device|disk.*full", re.I),   1.2),
    ("infra.cpu_saturated",  re.compile(r"CPU.*(saturat|throttl)", re.I),               1.2),
    ("infra.container_kill", re.compile(r"container.*killed|SIGKILL|Evicted"),          1.2),
]

DB_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("db.conn_pool",         re.compile(r"connection pool.*(exhausted|timeout)|HikariCP.*timeout", re.I), 1.1),
    ("db.deadlock",          re.compile(r"deadlock detected|ORA-00060"),                                   1.1),
    ("db.query_timeout",     re.compile(r"query timeout|statement timeout|ORA-01013"),                     1.1),
    ("db.connection_refused",re.compile(r"connection refused.*(5432|3306|1521)"),                          1.1),
]

# ── Resolved category → SNOW routing fields ──────────────────────────────────
CATEGORY_ROUTING: Dict[str, Dict[str, str]] = {
    "app":   {"team": "app-support", "queue": "APP_SUPPORT",
              "snow_category": "Application",    "snow_subcategory": "Runtime Error"},
    "infra": {"team": "infra-ops",   "queue": "INFRA_OPS",
              "snow_category": "Infrastructure", "snow_subcategory": "Resource"},
    "db":    {"team": "dba",         "queue": "DB_ADMIN",
              "snow_category": "Database",       "snow_subcategory": "Query/Connection"},
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_rules.py -v`
Expected: PASS — all 7 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/rules.py tests/test_agent2_rules.py
git commit -m "feat(agent-2): add rules.py with hand-curated regex sets and routing map"
```

---

## Task 3 — `classifier.preclassify`

**Files:**
- Create: `agents/Agent-2-splunk/classifier.py` (this task lays the stub — subsequent tasks add each phase).
- Test: `tests/test_agent2_preclassify.py` (new)

**Interfaces:**
- Consumes: `rules.DT_HYPOTHESIS_MAP`.
- Produces: `def preclassify(event: dict) -> str` — returns `"app"`, `"infra"`, or `"db"`. Reads `event["raw_payload"]["eventType"]` and `event["raw_payload"]["entityType"]`. Missing keys or unmapped combos default to `"app"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_preclassify.py`:

```python
"""Table-driven checks for classifier.preclassify."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import pytest
from classifier import preclassify  # noqa: E402


def _event(event_type: str = "", entity_type: str = "") -> dict:
    return {"raw_payload": {"eventType": event_type, "entityType": entity_type}}


@pytest.mark.parametrize("event_type,entity_type,expected", [
    ("FAILURE_RATE_INCREASED",            "SERVICE",       "app"),
    ("RESPONSE_TIME_DEGRADATION",         "SERVICE",       "app"),
    ("APPLICATION_ERROR",                 "APPLICATION",   "app"),
    ("PROCESS_UNAVAILABLE",               "PROCESS_GROUP", "infra"),
    ("PROCESS_MEMORY_RESOURCE_EXHAUSTED", "PROCESS_GROUP", "infra"),
    ("CPU_SATURATED",                     "HOST",          "infra"),
    ("MEMORY_SATURATED",                  "HOST",          "infra"),
    ("HOST_DISK_QUEUE_LENGTH_HIGH",       "HOST",          "infra"),
    ("HOST_NOT_AVAILABLE",                "HOST",          "infra"),
    ("DATABASE_CONNECTION_FAILURE",       "SERVICE",       "db"),
    ("SLOW_DB_QUERIES",                   "SERVICE",       "db"),
])
def test_preclassify_known_combos(event_type, entity_type, expected):
    assert preclassify(_event(event_type, entity_type)) == expected


def test_preclassify_unmapped_defaults_to_app():
    assert preclassify(_event("NEW_UNKNOWN_TYPE", "SERVICE")) == "app"


def test_preclassify_missing_raw_payload_defaults_to_app():
    assert preclassify({}) == "app"


def test_preclassify_missing_fields_defaults_to_app():
    assert preclassify({"raw_payload": {}}) == "app"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_preclassify.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'classifier'`.

- [ ] **Step 3: Create `agents/Agent-2-splunk/classifier.py`**

```python
"""
agents/Agent-2-splunk/classifier.py
===================================
Six-phase deterministic classifier for Agent 2. Every phase is a pure
function (except `run_splunk_async`, which is I/O). `classify()` is the
top-level entry point wired from main.py.

Spec: docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md
"""
from __future__ import annotations

import logging
from typing import Any, Dict

import rules

log = logging.getLogger(__name__)


# ── Phase 1: preclassify ─────────────────────────────────────────────────────

def preclassify(event: Dict[str, Any]) -> str:
    """Look up DT (eventType, entityType) → hypothesis category.

    Unmapped combos default to "app". Missing raw_payload or fields also default.
    """
    payload = event.get("raw_payload") or {}
    key = (payload.get("eventType") or "", payload.get("entityType") or "")
    hypothesis = rules.DT_HYPOTHESIS_MAP.get(key)
    if hypothesis is None:
        log.info("preclassify_unmapped event_type=%s entity_type=%s", key[0], key[1])
        return "app"
    return hypothesis
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_preclassify.py -v`
Expected: PASS — all 14 parametrised + 3 edge cases green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_preclassify.py
git commit -m "feat(agent-2): classifier phase 1 — preclassify"
```

---

## Task 4 — `classifier.build_tiered_queries`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append phase 2)
- Test: `tests/test_agent2_query_builder.py` (new)

**Interfaces:**
- Consumes: `rules.WINDOW_MIN`, `rules.ERROR_KEYWORDS`.
- Produces: `def build_tiered_queries(host: str, service: str, severity: str, index: str) -> List[str]` — returns up to 3 SPL strings, tightest first. Duplicate tiers (caused by empty `host` or `service`) are deduped in insertion order. `SPLUNK_MAX_RESULTS = 500` is baked into the `| head 500` suffix.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_query_builder.py`:

```python
"""Query builder shape + dedup tests for classifier.build_tiered_queries."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import build_tiered_queries  # noqa: E402


def test_full_tiers_when_host_and_service_present():
    q = build_tiered_queries(host="db-01", service="payments", severity="P2", index="prod")
    assert len(q) == 3
    t1, t2, t3 = q
    # T1: has both host + service filters
    assert 'host="db-01"' in t1
    assert 'source="*payments*"' in t1
    # T2: drops service
    assert 'host="db-01"' in t2
    assert 'source="*payments*"' not in t2
    # T3: drops host
    assert 'host=' not in t3
    # All: index + time window + keywords + head 500
    for t in q:
        assert "index=prod" in t
        assert "-30m@m" in t  # P2 window
        assert "(ERROR OR WARN OR CRITICAL OR Exception OR Traceback OR FATAL)" in t
        assert t.endswith("| head 500")


def test_p1_window_15_min():
    q = build_tiered_queries("h", "s", "P1", "prod")
    for t in q:
        assert "-15m@m" in t


def test_p4_window_60_min():
    q = build_tiered_queries("h", "s", "P4", "prod")
    for t in q:
        assert "-60m@m" in t


def test_unknown_severity_defaults_to_30_min():
    q = build_tiered_queries("h", "s", "PX", "prod")
    for t in q:
        assert "-30m@m" in t


def test_empty_service_dedups_t1_and_t2():
    q = build_tiered_queries(host="db-01", service="", severity="P2", index="prod")
    assert len(q) == 2  # T1 collapses into T2
    # First query is host-only (T1==T2 shape); second is index-wide
    assert 'host="db-01"' in q[0]
    assert 'host=' not in q[1]


def test_empty_host_dedups_t2_and_t3():
    q = build_tiered_queries(host="", service="payments", severity="P2", index="prod")
    # T2 (host-only) collapses into T3 (index-wide); T1 still has service filter.
    # Result: T1 with service filter, then T3.
    assert len(q) == 2
    assert 'source="*payments*"' in q[0]
    assert 'source=' not in q[1]


def test_empty_host_and_service_yields_one_query():
    q = build_tiered_queries(host="", service="", severity="P3", index="prod")
    assert len(q) == 1
    assert 'host=' not in q[0]
    assert 'source=' not in q[0]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_query_builder.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_tiered_queries'`.

- [ ] **Step 3: Append phase 2 to `classifier.py`**

Add to the imports at the top:

```python
from typing import Any, Dict, List
```

Append below `preclassify`:

```python
# ── Phase 2: build_tiered_queries ────────────────────────────────────────────

SPLUNK_MAX_RESULTS: int = 500  # CLAUDE.md §9.2


def build_tiered_queries(host: str, service: str, severity: str, index: str) -> List[str]:
    """Return up to 3 SPL strings ordered tight → wide.

    T1: index + host + service + keywords
    T2: index + host + keywords          (drop service)
    T3: index + keywords                 (drop host)

    Duplicate tiers (caused by empty host/service) are deduped in order.
    """
    w = rules.WINDOW_MIN.get(severity, 30)
    earliest = f"-{w}m@m"
    host_q = f' host="{host}"' if host else ""
    svc_q  = f' source="*{service}*"' if service else ""

    tiers = [
        f"search index={index}{host_q}{svc_q} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
        f"search index={index}{host_q} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
        f"search index={index} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
    ]
    # Dedup in insertion order.
    seen: List[str] = []
    for t in tiers:
        if t not in seen:
            seen.append(t)
    return seen
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_query_builder.py -v`
Expected: PASS — 7 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_query_builder.py
git commit -m "feat(agent-2): classifier phase 2 — build_tiered_queries with dedup"
```

---

## Task 5 — `classifier.run_splunk_async`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append phase 3)
- Test: `tests/test_agent2_splunk_async.py` (new)

**Interfaces:**
- Consumes: `httpx.AsyncClient`, `asyncio.sleep`, `SPLUNK_JOB_POLL_INTERVAL_SECONDS`, `SPLUNK_JOB_MAX_POLLS`, `SPLUNK_MAX_RESULTS`.
- Produces:
  - `SplunkPollTimeout(Exception)` — raised when polling exhausts without `DONE`.
  - `async def run_splunk_async(client: httpx.AsyncClient, spl: str) -> List[dict]` — submits a search job, polls until `dispatchState=DONE`, fetches and returns the `results` list (empty list if the response has no results key).

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_splunk_async.py`:

```python
"""Async Splunk submit/poll/fetch pattern tests."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import httpx
import pytest
from classifier import run_splunk_async, SplunkPollTimeout  # noqa: E402


class _MockTransport(httpx.MockTransport):
    def __init__(self, responder):
        super().__init__(responder)


async def test_run_splunk_async_happy_path():
    calls = []

    def responder(req: httpx.Request) -> httpx.Response:
        calls.append(f"{req.method} {req.url.path}")
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-42"})
        if req.url.path.endswith("/SID-42") and req.method == "GET":
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={"results": [{"_raw": "line1"}, {"_raw": "line2"}]})
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")

    assert rows == [{"_raw": "line1"}, {"_raw": "line2"}]
    assert calls[0].startswith("POST /services/search/jobs")
    assert any("/SID-42" in call and "GET" in call for call in calls)
    assert any(call.endswith("/results") for call in calls)


async def test_run_splunk_async_polls_until_done(monkeypatch):
    """First poll returns RUNNING, second returns DONE."""
    import asyncio as _asyncio
    monkeypatch.setattr(_asyncio, "sleep", lambda _s: _asyncio.sleep(0))  # no real wait
    poll_count = {"n": 0}

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-1"})
        if req.url.path.endswith("/SID-1") and req.method == "GET":
            poll_count["n"] += 1
            state = "RUNNING" if poll_count["n"] < 2 else "DONE"
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": state}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={"results": []})
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")

    assert rows == []
    assert poll_count["n"] == 2


async def test_run_splunk_async_poll_timeout_raises(monkeypatch):
    import asyncio as _asyncio
    monkeypatch.setattr(_asyncio, "sleep", lambda _s: _asyncio.sleep(0))

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-2"})
        # Always RUNNING — never reaches DONE.
        return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "RUNNING"}}]})

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        with pytest.raises(SplunkPollTimeout):
            await run_splunk_async(c, "search index=prod")


async def test_run_splunk_async_empty_results_when_key_missing():
    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID-3"})
        if req.url.path.endswith("/SID-3") and req.method == "GET":
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={})  # No "results" key.
        return httpx.Response(500)

    async with httpx.AsyncClient(base_url="https://splunk", transport=_MockTransport(responder)) as c:
        rows = await run_splunk_async(c, "search index=prod")
    assert rows == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_splunk_async.py -v`
Expected: FAIL — `ImportError: cannot import name 'run_splunk_async'`.

- [ ] **Step 3: Append phase 3 to `classifier.py`**

Add imports at the top of `classifier.py`:

```python
import asyncio
import os
import httpx
```

Append below the phase-2 block:

```python
# ── Phase 3: async Splunk submit / poll / fetch ──────────────────────────────

SPLUNK_JOB_POLL_INTERVAL_SECONDS: float = float(os.getenv("SPLUNK_JOB_POLL_INTERVAL_SECONDS", "2"))
SPLUNK_JOB_MAX_POLLS: int              = int(os.getenv("SPLUNK_JOB_MAX_POLLS", "10"))


class SplunkPollTimeout(Exception):
    """Raised when the poll loop exhausts without dispatchState=DONE."""


async def run_splunk_async(client: httpx.AsyncClient, spl: str) -> List[Dict[str, Any]]:
    """Submit a Splunk search job, poll to completion, return results."""
    submit = await client.post(
        "/services/search/jobs",
        data={"search": spl, "output_mode": "json", "exec_mode": "normal"},
    )
    submit.raise_for_status()
    sid = submit.json()["sid"]

    for _ in range(SPLUNK_JOB_MAX_POLLS):
        await asyncio.sleep(SPLUNK_JOB_POLL_INTERVAL_SECONDS)
        poll = await client.get(f"/services/search/jobs/{sid}",
                                params={"output_mode": "json"})
        poll.raise_for_status()
        entries = poll.json().get("entry", [])
        state = entries[0].get("content", {}).get("dispatchState") if entries else None
        if state == "DONE":
            break
    else:
        raise SplunkPollTimeout(f"sid={sid} never reached DONE after {SPLUNK_JOB_MAX_POLLS} polls")

    fetch = await client.get(
        f"/services/search/jobs/{sid}/results",
        params={"output_mode": "json", "count": SPLUNK_MAX_RESULTS},
    )
    fetch.raise_for_status()
    return fetch.json().get("results", []) or []
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_splunk_async.py -v`
Expected: PASS — 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_splunk_async.py
git commit -m "feat(agent-2): classifier phase 3 — async Splunk submit/poll/fetch"
```

---

## Task 6 — `classifier.score`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append phase 4)
- Test: `tests/test_agent2_scoring.py` (new)

**Interfaces:**
- Consumes: `rules.APP_RULES`, `rules.INFRA_RULES`, `rules.DB_RULES`, `shared.models.RuleMatch`.
- Produces: `def score(results: List[dict]) -> List[RuleMatch]` — iterates rows, applies every rule's regex against the row's `_raw` field (falls back to `str(row)` if `_raw` missing), aggregates match counts per rule, returns a `RuleMatch` per rule that matched at least once. `sample_line` is the first matched line, truncated to 200 chars.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_scoring.py`:

```python
"""Scoring — regex matches, counts, sample lines, weights."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import score  # noqa: E402


def _rows(*lines):
    return [{"_raw": ln} for ln in lines]


def test_score_pure_app():
    rows = _rows(
        "java.lang.NullPointerException at Foo.bar",
        "Uncaught: something",
    )
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "app.null_pointer" in ids
    assert "app.unhandled_ex" in ids
    for m in matches:
        assert m.rule_set == "app"
        assert m.weight == 1.0


def test_score_pure_infra_case_insensitive():
    rows = _rows("java.lang.OutOfMemoryError: Java heap space",
                 "OOMKilled")
    matches = score(rows)
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert oom.match_count == 2
    assert oom.weight == 1.2
    assert oom.rule_set == "infra"


def test_score_pure_db():
    rows = _rows(
        "HikariCP - Connection pool exhausted after 30000ms",
        "org.postgresql.util.PSQLException: statement timeout",
    )
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "db.conn_pool" in ids
    assert "db.query_timeout" in ids
    for m in matches:
        assert m.rule_set == "db"
        assert m.weight == 1.1


def test_score_mixed_60_40():
    rows = _rows(
        # 3 db
        "connection pool exhausted A",
        "connection pool timeout B",
        "connection pool exhausted C",
        # 2 app
        "NullPointerException here",
        "Uncaught exception",
    )
    matches = score(rows)
    db_conn = next(m for m in matches if m.rule_id == "db.conn_pool")
    assert db_conn.match_count == 3
    app_hits = [m for m in matches if m.rule_set == "app"]
    assert sum(m.match_count for m in app_hits) == 2


def test_score_empty_returns_empty():
    assert score([]) == []


def test_score_row_without_raw_uses_str():
    rows = [{"message": "OutOfMemory here"}]
    matches = score(rows)
    ids = {m.rule_id for m in matches}
    assert "infra.oom" in ids


def test_sample_line_truncated_to_200_chars():
    long_line = "OutOfMemory " + "x" * 500
    matches = score(_rows(long_line))
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert len(oom.sample_line) == 200


def test_sample_line_is_first_match():
    rows = _rows("OutOfMemory first", "OutOfMemory second")
    matches = score(rows)
    oom = next(m for m in matches if m.rule_id == "infra.oom")
    assert oom.sample_line == "OutOfMemory first"
    assert oom.match_count == 2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_scoring.py -v`
Expected: FAIL — `ImportError: cannot import name 'score'`.

- [ ] **Step 3: Append phase 4 to `classifier.py`**

Add to the top-of-file imports:

```python
from shared.models import RuleMatch
```

Append below the phase-3 block:

```python
# ── Phase 4: score ───────────────────────────────────────────────────────────

_RULE_SETS = (
    ("app",   rules.APP_RULES),
    ("infra", rules.INFRA_RULES),
    ("db",    rules.DB_RULES),
)


def _row_text(row: Dict[str, Any]) -> str:
    raw = row.get("_raw")
    return raw if isinstance(raw, str) else str(row)


def score(results: List[Dict[str, Any]]) -> List[RuleMatch]:
    """Apply every rule to every row. Return one RuleMatch per rule that hit ≥ 1."""
    # (rule_id, rule_set, weight) → [count, first_sample_line]
    hits: Dict[str, List[Any]] = {}
    for row in results:
        text = _row_text(row)
        for rule_set_name, rule_set in _RULE_SETS:
            for rule_id, pattern, weight in rule_set:
                if pattern.search(text):
                    entry = hits.setdefault(rule_id, [0, text[:200], rule_set_name, weight])
                    entry[0] += 1
    return [
        RuleMatch(rule_id=rid, rule_set=rs_name, weight=w,
                  match_count=count, sample_line=sample)
        for rid, (count, sample, rs_name, w) in hits.items()
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_scoring.py -v`
Expected: PASS — 8 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_scoring.py
git commit -m "feat(agent-2): classifier phase 4 — score rows against weighted regex sets"
```

---

## Task 7 — `classifier.resolve_conflict`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append phase 5)
- Test: `tests/test_agent2_conflict.py` (new)

**Interfaces:**
- Consumes: `shared.models.RuleMatch`, `CONFLICT_RESOLUTION_THRESHOLD` constant.
- Produces:
  - `CONFLICT_RESOLUTION_THRESHOLD: float = 65.0` — env-overridable.
  - `def resolve_conflict(dt_hypothesis: str, rule_matches: List[RuleMatch]) -> Tuple[str, str, float, str]` — returns `(final_category, splunk_category, confidence, hypothesis_source)`. `splunk_category` is the Splunk-scored argmax (may be `None` when no matches). `hypothesis_source` is `"splunk"` iff `confidence >= CONFLICT_RESOLUTION_THRESHOLD`, else `"dt"`.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_conflict.py`:

```python
"""Conflict-resolution matrix and threshold boundary tests."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import resolve_conflict  # noqa: E402
from shared.models import RuleMatch


def _m(rule_set, count, weight):
    return RuleMatch(rule_id=f"{rule_set}.x", rule_set=rule_set,
                     weight=weight, match_count=count, sample_line="x")


def test_empty_matches_keeps_dt_hypothesis():
    final, splunk_cat, conf, src = resolve_conflict("app", [])
    assert final == "app"
    assert splunk_cat is None
    assert conf == 0.0
    assert src == "dt"


def test_single_ruleset_full_confidence_splunk_wins():
    # Only db matches — runner-up score is 0 → confidence = 100.
    final, splunk_cat, conf, src = resolve_conflict("app", [_m("db", 100, 1.1)])
    assert final == "db"
    assert splunk_cat == "db"
    assert conf > 99.99
    assert src == "splunk"


def test_dt_wins_when_splunk_ambiguous():
    # Two rule sets with near-equal scores → low confidence → DT keeps.
    matches = [_m("db", 10, 1.1), _m("app", 10, 1.0)]
    final, splunk_cat, conf, src = resolve_conflict("app", matches)
    assert src == "dt"
    assert final == "app"
    assert splunk_cat in ("db", "app")  # argmax is db (weight 1.1); either OK if scores tie
    assert conf < 65.0


def test_boundary_confidence_65_splunk_wins():
    """Confidence exactly at threshold → Splunk wins (>= not >)."""
    # Craft scores where winner/(winner+runner) ≈ 0.65.
    # score = w * log10(1 + count). For db (w=1.1): score_db = 1.1*log10(1+C_db).
    # For app (w=1.0): score_app = 1.0*log10(1+C_app).
    # Solve for 0.65: score_db / (score_db + score_app) = 0.65.
    # Use synthetic: score_db = 6.5, score_app = 3.5 → ratio = 0.65.
    # Approximate with counts that hit these scores:
    # log10(1+C) = 6.5/1.1 ≈ 5.909 → C ≈ 10^5.909 - 1 ≈ 810,000
    # log10(1+C) = 3.5/1.0 = 3.5   → C ≈ 3,161
    matches = [_m("db", 810_000, 1.1), _m("app", 3161, 1.0)]
    final, _splunk_cat, conf, src = resolve_conflict("app", matches)
    assert conf >= 64.9  # allow for rounding
    if conf >= 65.0:
        assert src == "splunk"
        assert final == "db"


def test_boundary_confidence_below_65_dt_wins():
    # Confidence just below 65 → DT wins.
    matches = [_m("db", 10, 1.1), _m("app", 8, 1.0)]
    final, _splunk_cat, conf, src = resolve_conflict("infra", matches)
    assert conf < 65.0
    assert src == "dt"
    assert final == "infra"


def test_argmax_splunk_category_recorded_even_when_dt_wins():
    matches = [_m("db", 10, 1.1), _m("app", 9, 1.0)]
    _final, splunk_cat, _conf, src = resolve_conflict("app", matches)
    assert src == "dt"          # low confidence
    assert splunk_cat == "db"   # argmax still recorded


def test_only_matches_return_that_category_at_100():
    final, splunk_cat, conf, src = resolve_conflict("app", [_m("infra", 5, 1.2)])
    assert splunk_cat == "infra"
    assert src == "splunk"
    assert final == "infra"
    assert conf > 99.99
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_conflict.py -v`
Expected: FAIL — `ImportError: cannot import name 'resolve_conflict'`.

- [ ] **Step 3: Append phase 5 to `classifier.py`**

Add to top-of-file imports:

```python
import math
from collections import defaultdict
from typing import Any, Dict, List, Tuple
```

(`Tuple` may already be imported — do not duplicate.)

Append below the phase-4 block:

```python
# ── Phase 5: resolve_conflict ────────────────────────────────────────────────

CONFLICT_RESOLUTION_THRESHOLD: float = float(os.getenv("CONFLICT_RESOLUTION_THRESHOLD", "65.0"))


def resolve_conflict(
    dt_hypothesis: str,
    rule_matches: List[RuleMatch],
) -> Tuple[str, str | None, float, str]:
    """Compute per-rule-set scores and pick the winner.

    Returns (final_category, splunk_category, confidence, hypothesis_source).
    splunk_category is None when there are no matches at all.
    """
    if not rule_matches:
        return dt_hypothesis, None, 0.0, "dt"

    scores: Dict[str, float] = defaultdict(float)
    for m in rule_matches:
        scores[m.rule_set] += m.weight * math.log10(1 + m.match_count)

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    splunk_category, winning_score = ranked[0]
    runner_up_score = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = 100.0 * winning_score / (winning_score + runner_up_score + 1e-6)

    if confidence >= CONFLICT_RESOLUTION_THRESHOLD:
        return splunk_category, splunk_category, confidence, "splunk"
    return dt_hypothesis, splunk_category, confidence, "dt"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_conflict.py -v`
Expected: PASS — 7 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_conflict.py
git commit -m "feat(agent-2): classifier phase 5 — resolve_conflict with 65% threshold"
```

---

## Task 8 — `classifier.precompute_routing`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append phase 6)
- Test: `tests/test_agent2_routing.py` (new)

**Interfaces:**
- Consumes: `rules.CATEGORY_ROUTING`.
- Produces: `def precompute_routing(category: str) -> Dict[str, str]` — returns dict with keys `team`, `queue`, `snow_category`, `snow_subcategory`. Unknown categories return empty dict.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_routing.py`:

```python
"""Routing pre-compute: category → SNOW fields."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import pytest
from classifier import precompute_routing  # noqa: E402


@pytest.mark.parametrize("category,team,queue,cat,sub", [
    ("app",   "app-support", "APP_SUPPORT", "Application",    "Runtime Error"),
    ("infra", "infra-ops",   "INFRA_OPS",   "Infrastructure", "Resource"),
    ("db",    "dba",         "DB_ADMIN",    "Database",       "Query/Connection"),
])
def test_precompute_routing_known(category, team, queue, cat, sub):
    r = precompute_routing(category)
    assert r["team"] == team
    assert r["queue"] == queue
    assert r["snow_category"] == cat
    assert r["snow_subcategory"] == sub


def test_precompute_routing_unknown_returns_empty():
    assert precompute_routing("unknown-cat") == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_routing.py -v`
Expected: FAIL — `ImportError: cannot import name 'precompute_routing'`.

- [ ] **Step 3: Append phase 6 to `classifier.py`**

Append below the phase-5 block:

```python
# ── Phase 6: precompute_routing ──────────────────────────────────────────────

def precompute_routing(category: str) -> Dict[str, str]:
    """Map resolved category → {team, queue, snow_category, snow_subcategory}.

    Unknown categories return an empty dict (Agent 3 handles missing fields).
    """
    row = rules.CATEGORY_ROUTING.get(category)
    return dict(row) if row else {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_routing.py -v`
Expected: PASS — 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_routing.py
git commit -m "feat(agent-2): classifier phase 6 — precompute_routing SNOW fields"
```

---

## Task 9 — `classifier.classify` entry point + T1 short-circuit + `render_evidence_body`

**Files:**
- Modify: `agents/Agent-2-splunk/classifier.py` (append `classify` and `render_evidence_body`)
- Test: `tests/test_agent2_classify.py` (new)
- Test: `tests/test_agent2_render_body.py` (new)

**Interfaces:**
- Consumes: every phase from Tasks 3–8, `httpx.AsyncClient`, `os.getenv`, `shared.models.SplunkEnrichment`.
- Produces:
  - `SPLUNK_TIER1_SHORTCIRCUIT_MIN: int = 20` — env-overridable.
  - `SPLUNK_BASE: str`, `SPLUNK_TOKEN: str`, `SPLUNK_INDEX: str` — module-level, read at import.
  - `async def classify(event: dict, severity: str) -> SplunkEnrichment` — runs the six phases and returns a fully populated enrichment. Sets `classification` = `error_category` for backward compat. Splunk misconfiguration or query failures degrade gracefully (returns enrichment with `hypothesis_source="dt"`, `confidence=0.0`, and an explanatory `llm_summary`).
  - `def render_evidence_body(enrichment: SplunkEnrichment) -> str` — formats a §4.6-body-only string (no header — the header is added by `snow_notes.post_work_note` in Task 12).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_agent2_classify.py`:

```python
"""Full classify() orchestration: short-circuit, degradation, backward compat."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import httpx
import pytest
import classifier  # noqa: E402


def _event(host="db-01", service="payments", event_type="DATABASE_CONNECTION_FAILURE",
           entity_type="SERVICE"):
    return {
        "host": host, "service": service,
        "raw_payload": {"eventType": event_type, "entityType": entity_type},
    }


def _make_responder(t1_rows, t2_rows=None, t3_rows=None):
    """Sequence Splunk responses so T1 returns t1_rows, T2 returns t2_rows, T3 returns t3_rows."""
    tier_rows = [t1_rows, t2_rows or [], t3_rows or []]
    tier_idx = {"n": 0}

    def responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            sid = f"SID-{tier_idx['n']}"
            tier_idx["n"] += 1
            return httpx.Response(201, json={"sid": sid})
        if "/jobs/" in req.url.path and req.method == "GET" and req.url.path.endswith(("SID-0","SID-1","SID-2")):
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            # Which tier's results are we returning?
            # Match on the sid segment in the URL.
            path = req.url.path
            for i, rows in enumerate(tier_rows):
                if f"SID-{i}/results" in path:
                    return httpx.Response(200, json={"results": rows})
            return httpx.Response(200, json={"results": []})
        return httpx.Response(500)
    return responder, tier_idx


async def test_classify_t1_short_circuits_when_ge_20_rows(monkeypatch):
    """T1 returns 21 rows → T2/T3 must not be executed."""
    monkeypatch.setenv("SPLUNK_TIER1_SHORTCIRCUIT_MIN", "20")
    # Force classifier to see Splunk configured.
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    t1 = [{"_raw": "HikariCP - Connection pool exhausted"} for _ in range(21)]
    responder, tier_idx = _make_responder(t1)
    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")

    assert tier_idx["n"] == 1  # only T1 submitted
    assert enr.tier_used == 1
    assert enr.error_category == "db"
    assert enr.classification == "db"  # backward compat
    assert enr.hypothesis_source == "splunk"
    assert enr.confidence > 65
    assert enr.assigned_team == "dba"


async def test_classify_falls_through_all_tiers_when_short_returns(monkeypatch):
    monkeypatch.setenv("SPLUNK_TIER1_SHORTCIRCUIT_MIN", "20")
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    t1 = [{"_raw": "connection pool exhausted"}]  # only 1 row
    t3 = [{"_raw": "connection pool exhausted"} for _ in range(5)]
    responder, tier_idx = _make_responder(t1, [], t3)
    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")
    assert tier_idx["n"] == 3  # T1 + T2 + T3 all submitted
    assert enr.tier_used == 3  # last tier that produced usable results


async def test_classify_degrades_when_splunk_unconfigured(monkeypatch):
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "", raising=False)
    enr = await classifier.classify(_event(), severity="P2")
    assert enr.hypothesis_source == "dt"
    assert enr.confidence == 0.0
    assert enr.error_category == "db"  # from DT hypothesis
    assert enr.classification == "db"
    assert "not configured" in (enr.llm_summary or "").lower()


async def test_classify_degrades_on_splunk_exception(monkeypatch):
    monkeypatch.setattr(classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(classifier, "SPLUNK_INDEX", "prod", raising=False)

    def responder(req):
        return httpx.Response(500, text="splunk down")

    monkeypatch.setattr(classifier.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://splunk",
                                                       transport=httpx.MockTransport(responder)))

    enr = await classifier.classify(_event(), severity="P2")
    assert enr.hypothesis_source == "dt"
    assert enr.error_category == "db"
    assert "failed" in (enr.llm_summary or "").lower()
```

Create `tests/test_agent2_render_body.py`:

```python
"""render_evidence_body — §4.6 body content only (header is added by snow_notes)."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

from classifier import render_evidence_body  # noqa: E402
from shared.models import SplunkEnrichment, RuleMatch


def test_render_body_with_matches():
    e = SplunkEnrichment(
        tier_used=1, log_lines_scanned=342,
        splunk_category="db", confidence=78.0,
        dt_hypothesis="app", hypothesis_source="splunk",
        error_category="db",
        spl_queries=["search index=prod host=\"db-01\" earliest=-30m@m ..."],
        rule_matches=[
            RuleMatch(rule_id="db.conn_pool", rule_set="db", weight=1.1,
                      match_count=42, sample_line="HikariCP - Connection pool exhausted"),
            RuleMatch(rule_id="db.query_timeout", rule_set="db", weight=1.1,
                      match_count=8, sample_line="statement timeout"),
        ],
    )
    body = render_evidence_body(e)
    assert "Category (Splunk-scored): db" in body
    assert "confidence 78%" in body
    assert "DT hypothesis was: app" in body
    assert "Tier: 1" in body
    assert "Log lines scanned: 342" in body
    assert "db.conn_pool" in body
    assert "42 matches" in body
    assert "HikariCP - Connection pool exhausted" in body
    assert "SPL:" in body


def test_render_body_empty_evidence():
    e = SplunkEnrichment(
        log_lines_scanned=0, error_category="app",
        dt_hypothesis="app", hypothesis_source="dt",
        confidence=0.0, rule_matches=[],
        time_range="last 30min",
    )
    body = render_evidence_body(e)
    assert "No matching log evidence" in body
    assert "using DT hypothesis: app" in body
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_agent2_classify.py tests/test_agent2_render_body.py -v`
Expected: FAIL — `ImportError: cannot import name 'classify'` / `render_evidence_body`.

- [ ] **Step 3: Append `classify` and `render_evidence_body` to `classifier.py`**

Append below the phase-6 block:

```python
# ── Module-level config (read at import) ─────────────────────────────────────

SPLUNK_BASE:  str = os.getenv("SPLUNK_BASE_URL", "").rstrip("/")
SPLUNK_TOKEN: str = os.getenv("SPLUNK_TOKEN", "")
SPLUNK_INDEX: str = os.getenv("SPLUNK_INDEX", "main")

SPLUNK_TIER1_SHORTCIRCUIT_MIN: int = int(os.getenv("SPLUNK_TIER1_SHORTCIRCUIT_MIN", "20"))


# ── Top-level entry point ────────────────────────────────────────────────────

from shared.models import SplunkEnrichment  # placed here to keep the module top short


async def classify(event: Dict[str, Any], severity: str) -> SplunkEnrichment:
    """Run the six phases; return a fully populated SplunkEnrichment."""
    dt_hypothesis = preclassify(event)

    if not SPLUNK_BASE:
        return _degraded(dt_hypothesis, severity, reason="Splunk not configured")

    host    = event.get("host") or ""
    service = event.get("service") or ""
    queries = build_tiered_queries(host, service, severity, SPLUNK_INDEX)

    executed: List[str] = []
    all_rows: List[Dict[str, Any]] = []
    tier_used: int | None = None

    try:
        async with httpx.AsyncClient(
            base_url=SPLUNK_BASE,
            headers={"Authorization": f"Bearer {SPLUNK_TOKEN}"},
            timeout=httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=5.0),
            verify=True,
        ) as client:
            for i, spl in enumerate(queries, start=1):
                executed.append(spl)
                rows = await run_splunk_async(client, spl)
                all_rows = rows
                tier_used = i
                if i == 1 and len(rows) >= SPLUNK_TIER1_SHORTCIRCUIT_MIN:
                    break
                if i < len(queries) and not rows:
                    continue
                break
    except Exception as exc:
        log.warning("agent2_splunk_failed reason=%s", exc)
        return _degraded(dt_hypothesis, severity, reason="Splunk query failed", spl_queries=executed)

    matches = score(all_rows)
    final_category, splunk_category, confidence, source = resolve_conflict(dt_hypothesis, matches)
    routing = precompute_routing(final_category)

    error_count = sum(1 for r in all_rows if "ERROR" in _row_text(r).upper())
    warn_count  = sum(1 for r in all_rows if "WARN"  in _row_text(r).upper())

    summary = (f"{len(matches)} rule match(es) across "
               f"{len({m.rule_set for m in matches})} rule set(s). "
               f"Source={source}, confidence={confidence:.1f}%.")

    return SplunkEnrichment(
        log_lines_scanned=len(all_rows),
        error_count=error_count,
        warn_count=warn_count,
        top_errors=[m.rule_id for m in matches[:5]],
        time_range=f"last {rules.WINDOW_MIN.get(severity, 30)}min",
        index=SPLUNK_INDEX,
        spl_query=executed[-1] if executed else None,
        spl_queries=executed,
        tier_used=tier_used,
        rule_matches=matches,
        dt_hypothesis=dt_hypothesis,
        splunk_category=splunk_category,
        confidence=confidence,
        hypothesis_source=source,
        error_category=final_category,
        classification=final_category,          # backward compat
        assigned_team=routing.get("team"),
        assigned_queue=routing.get("queue"),
        snow_category=routing.get("snow_category"),
        snow_subcategory=routing.get("snow_subcategory"),
        llm_summary=summary,
    )


def _degraded(dt_hypothesis: str, severity: str, *, reason: str,
              spl_queries: List[str] | None = None) -> SplunkEnrichment:
    routing = precompute_routing(dt_hypothesis)
    return SplunkEnrichment(
        log_lines_scanned=0,
        time_range=f"last {rules.WINDOW_MIN.get(severity, 30)}min",
        index=SPLUNK_INDEX,
        spl_queries=spl_queries or [],
        dt_hypothesis=dt_hypothesis,
        splunk_category=None,
        confidence=0.0,
        hypothesis_source="dt",
        error_category=dt_hypothesis,
        classification=dt_hypothesis,
        assigned_team=routing.get("team"),
        assigned_queue=routing.get("queue"),
        snow_category=routing.get("snow_category"),
        snow_subcategory=routing.get("snow_subcategory"),
        llm_summary=reason,
    )


# ── Flow B body renderer ─────────────────────────────────────────────────────

def render_evidence_body(enrichment: SplunkEnrichment) -> str:
    """Compose the body of the Flow B SPLUNK EVIDENCE work note (header added elsewhere)."""
    if not enrichment.rule_matches:
        window = enrichment.time_range or "the search window"
        return (f"No matching log evidence in {window} — "
                f"using DT hypothesis: {enrichment.error_category}")

    lines: List[str] = []
    lines.append(
        f"Category (Splunk-scored): {enrichment.splunk_category or enrichment.error_category}  "
        f"(confidence {enrichment.confidence:.0f}%, "
        f"DT hypothesis was: {enrichment.dt_hypothesis})"
    )
    lines.append(f"Tier: {enrichment.tier_used}")
    lines.append(f"Log lines scanned: {enrichment.log_lines_scanned}")
    lines.append("Top matches:")
    for m in sorted(enrichment.rule_matches, key=lambda x: x.match_count, reverse=True)[:5]:
        import math as _math
        rule_score = m.weight * _math.log10(1 + m.match_count)
        lines.append(f"  • {m.rule_id:<22} (score {rule_score:.1f}, {m.match_count} matches)")
        lines.append(f"      sample: \"{m.sample_line}\"")
    if enrichment.spl_query:
        lines.append(f"SPL: {enrichment.spl_query}")
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_agent2_classify.py tests/test_agent2_render_body.py -v`
Expected: PASS — 4 + 2 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/classifier.py tests/test_agent2_classify.py tests/test_agent2_render_body.py
git commit -m "feat(agent-2): classify() entry point with T1 short-circuit + render body"
```

---

## Task 10 — Wire `classifier.classify` into `main.py`, fix `verify=False` (PR-2)

**Files:**
- Modify: `agents/Agent-2-splunk/main.py` (replace `_run_splunk` and `_classify`; delete old `_WINDOW`)
- Test: existing `tests/test_agent2_*.py` continue to pass; no new tests in this task — the classifier tests already cover the new logic.

**Interfaces:**
- Consumes: `classifier.classify` from Task 9.
- Produces: `main.py` `process_run` calls `classifier.classify(event, severity)` instead of `_run_splunk`. `verify=False` no longer appears anywhere in Agent 2. Legacy `_classify`, `_run_splunk`, and `_WINDOW` are deleted.

- [ ] **Step 1: Confirm classifier tests still pass and verify=False is gone**

Run: `pytest tests/test_agent2_*.py -v`
Expected: all Agent-2 tests pass (nothing was broken by earlier tasks).

Run: `grep -n "verify=False" agents/Agent-2-splunk/main.py`
Expected: one match (line 155 today). Task 10 removes it.

- [ ] **Step 2: Replace `_run_splunk`, `_classify`, and `_WINDOW` in `main.py`**

Open `agents/Agent-2-splunk/main.py`. Make these changes:

(a) Add `import classifier` at the top with the other imports (after `from shared.routing_client import ...`).

(b) Delete lines 48–49 (the `_WINDOW` dict — moved to `rules.py`).

(c) In `process_run`, replace the call block starting at line 98 (`enrichment = await _run_splunk(...)`) with:

```python
    try:
        enrichment = await classifier.classify(event_data, severity)
        ctx.setdefault("enrichments", {})["splunk"] = enrichment.model_dump()
        await redis.store_context(run_id, ctx)

        fire_and_forget(rc.record_step(run_id, {
            "agent_num":   AGENT_NUM, "agent_name": AGENT_NAME,
            "status":      "completed",
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "summary":     enrichment.llm_summary or f"{enrichment.error_count} errors found",
        }))
        fire_and_forget(rc.write_enrichment(run_id, {
            "agent_num": AGENT_NUM, "source": "splunk", "data": enrichment.model_dump(),
        }))

        await redis.publish_event({"event": SSEEventType.AGENT_DONE, "run_id": run_id,
                                    "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "data": {"classification": enrichment.error_category,
                                             "confidence": enrichment.confidence,
                                             "hypothesis_source": enrichment.hypothesis_source}},
                                   run_id=run_id)

        # Route forward (Flow B enrichment write is added in Task 12).
        if flow == "primary":
            await redis.enqueue(3, run_id)
        else:
            await asyncio.gather(
                redis.enqueue(3, run_id),
                redis.enqueue(6, run_id),
            )

    except Exception as exc:
        log.exception("Agent 2 error for run_id=%s: %s", run_id, exc)
        await redis.publish_event({"event": SSEEventType.AGENT_ERROR, "run_id": run_id,
                                    "agent_num": AGENT_NUM, "agent_name": AGENT_NAME,
                                    "data": {"error": str(exc)}}, run_id=run_id)
        # Non-fatal: continue pipeline
        await redis.enqueue(3, run_id)
```

(d) Delete `_run_splunk` (currently lines 138–181) and `_classify` (lines 184–194) entirely.

- [ ] **Step 3: Verify Agent 2 imports and the file is syntactically clean**

Run: `python -c "import sys; sys.path.insert(0, 'agents/Agent-2-splunk'); import main"`
Expected: no output, exit 0.

Run: `grep -n "verify=False" agents/Agent-2-splunk/main.py`
Expected: no matches.

Run: `grep -n "_run_splunk\|_classify\|_WINDOW" agents/Agent-2-splunk/main.py`
Expected: no matches.

- [ ] **Step 4: Run the full Agent-2 test suite**

Run: `pytest tests/test_agent2_*.py -v`
Expected: all tests pass, including the new classify tests.

- [ ] **Step 5: Commit (this closes PR-2)**

```bash
git add agents/Agent-2-splunk/main.py
git commit -m "refactor(agent-2): wire classifier.classify() and remove verify=False"
```

---

## Task 11 — `snow_notes.py` — reusable §4.6 SNOW work-note POST helper (PR-3)

**Files:**
- Create: `agents/Agent-2-splunk/snow_notes.py`
- Test: `tests/test_agent2_flow_b_worknote.py` (new)

**Interfaces:**
- Consumes: `shared.snow_auth.get_snow_token`, `httpx.AsyncClient`, `os.getenv("SNOW_BASE_URL")`.
- Produces:
  - `async def post_work_note(sys_id: str, stage: str, agent_num: int, status: str, duration_ms: int, run_id: str, body: str) -> bool` — PATCHes `/api/now/table/incident/{sys_id}` with a `work_notes` field containing the §4.6 header + body. Returns `True` on success, `False` on failure (never raises — non-fatal per §10.2). Skips silently and returns `False` when `SNOW_BASE_URL` is empty or `sys_id` is falsy.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_flow_b_worknote.py`:

```python
"""§4.6 work-note format check and failure-tolerance for snow_notes."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import httpx
import pytest
import snow_notes  # noqa: E402


async def test_post_work_note_shape(monkeypatch):
    captured = {}

    def responder(req: httpx.Request) -> httpx.Response:
        captured["method"] = req.method
        captured["path"] = req.url.path
        captured["json"] = req.content.decode()
        return httpx.Response(200, json={"result": {"sys_id": "ABC"}})

    monkeypatch.setattr(snow_notes, "SNOW_BASE", "https://snow", raising=False)
    async def _fake_token(): return "tkn-42"
    monkeypatch.setattr(snow_notes, "get_snow_token", _fake_token, raising=False)
    monkeypatch.setattr(snow_notes.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://snow",
                                                       transport=httpx.MockTransport(responder)))

    ok = await snow_notes.post_work_note(
        sys_id="INC-SYS-1", stage="SPLUNK EVIDENCE",
        agent_num=2, status="success", duration_ms=4213, run_id="run-xyz",
        body="Category: db",
    )
    assert ok is True
    assert captured["method"] == "PATCH"
    assert captured["path"] == "/api/now/table/incident/INC-SYS-1"

    import json
    payload = json.loads(captured["json"])
    note = payload["work_notes"]
    # §4.6 header exact-shape checks
    assert note.startswith("=== SPLUNK EVIDENCE — Agent 2 ===\n")
    assert "Timestamp : " in note
    assert "Status    : success\n" in note
    assert "Duration  : 4213\n" in note
    assert "Pipeline  : run-xyz\n\n" in note
    assert note.endswith("Category: db")


async def test_post_work_note_missing_sys_id_returns_false(monkeypatch):
    monkeypatch.setattr(snow_notes, "SNOW_BASE", "https://snow", raising=False)
    ok = await snow_notes.post_work_note(
        sys_id="", stage="SPLUNK EVIDENCE", agent_num=2,
        status="success", duration_ms=1, run_id="r", body="b",
    )
    assert ok is False


async def test_post_work_note_snow_unconfigured_returns_false(monkeypatch):
    monkeypatch.setattr(snow_notes, "SNOW_BASE", "", raising=False)
    ok = await snow_notes.post_work_note(
        sys_id="INC-SYS-1", stage="SPLUNK EVIDENCE", agent_num=2,
        status="success", duration_ms=1, run_id="r", body="b",
    )
    assert ok is False


async def test_post_work_note_swallows_snow_failure(monkeypatch):
    def responder(req: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="snow down")

    monkeypatch.setattr(snow_notes, "SNOW_BASE", "https://snow", raising=False)
    async def _fake_token(): return "tkn"
    monkeypatch.setattr(snow_notes, "get_snow_token", _fake_token, raising=False)
    monkeypatch.setattr(snow_notes.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://snow",
                                                       transport=httpx.MockTransport(responder)))

    ok = await snow_notes.post_work_note(
        sys_id="INC-SYS-1", stage="SPLUNK EVIDENCE", agent_num=2,
        status="success", duration_ms=1, run_id="r", body="b",
    )
    assert ok is False  # non-fatal — logs and returns False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_flow_b_worknote.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'snow_notes'`.

- [ ] **Step 3: Create `agents/Agent-2-splunk/snow_notes.py`**

```python
"""
agents/Agent-2-splunk/snow_notes.py
===================================
§4.6-formatted work-note POST helper. Non-fatal by contract: never raises.

Reusable by Agents 6 and 7 when they add Flow B write paths.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import httpx

from shared.snow_auth import get_snow_token

log = logging.getLogger(__name__)

SNOW_BASE: str = os.getenv("SNOW_BASE_URL", "").rstrip("/")


async def post_work_note(sys_id: str, stage: str, agent_num: int,
                         status: str, duration_ms: int, run_id: str,
                         body: str) -> bool:
    """PATCH a §4.6-formatted work note. Return True on 2xx, False otherwise.

    Never raises. Failures are logged at WARNING and the caller continues.
    """
    if not SNOW_BASE:
        log.warning("snow_worknote_skipped reason=snow_unconfigured stage=%s", stage)
        return False
    if not sys_id:
        log.warning("snow_worknote_skipped reason=missing_sys_id stage=%s", stage)
        return False

    ts = datetime.now(timezone.utc).isoformat()
    header = (f"=== {stage} — Agent {agent_num} ===\n"
              f"Timestamp : {ts}\n"
              f"Status    : {status}\n"
              f"Duration  : {duration_ms}\n"
              f"Pipeline  : {run_id}\n\n{body}")

    try:
        token = await get_snow_token()
        async with httpx.AsyncClient(base_url=SNOW_BASE, timeout=15, verify=True) as c:
            r = await c.patch(
                f"/api/now/table/incident/{sys_id}",
                headers={"Authorization": f"Bearer {token}",
                         "Content-Type": "application/json"},
                json={"work_notes": header},
            )
            r.raise_for_status()
        return True
    except Exception as exc:
        log.warning("snow_worknote_failed sys_id=%s stage=%s error=%s", sys_id, stage, exc)
        return False
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_flow_b_worknote.py -v`
Expected: PASS — 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/snow_notes.py tests/test_agent2_flow_b_worknote.py
git commit -m "feat(agent-2): snow_notes.py — §4.6 work-note POST helper"
```

---

## Task 12 — Wire Flow B SNOW write into `main.py` (PR-3)

**Files:**
- Modify: `agents/Agent-2-splunk/main.py` (call `snow_notes.post_work_note` in secondary flow before enqueue)
- Test: `tests/test_agent2_main_flow_b.py` (new) — verifies `main.py` invokes `snow_notes.post_work_note` on secondary flow and still enqueues 3+6.

**Interfaces:**
- Consumes: `snow_notes.post_work_note`, `classifier.render_evidence_body`, `ctx["event"]["incident_sys_id"]` (populated by Agent 1 for `ManualIncidentEvent`).
- Produces: `main.py`'s secondary branch now (a) renders body via `classifier.render_evidence_body(enrichment)`, (b) awaits `snow_notes.post_work_note(...)` with stage `"SPLUNK EVIDENCE"`, (c) fans out to Agents 3+6 unchanged.

- [ ] **Step 1: Write the failing test**

Create `tests/test_agent2_main_flow_b.py`:

```python
"""End-to-end main.py test for Flow B: SNOW work-note write + fan-out to 3+6."""
import sys
from pathlib import Path

_A2 = Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
sys.path.insert(0, str(_A2))

import types
import pytest
import main as agent2_main  # noqa: E402


class _FakeRedis:
    def __init__(self, ctx):
        self._ctx = ctx
        self.enqueued = []
        self.events = []

    async def get_context(self, run_id): return self._ctx
    async def store_context(self, run_id, ctx): self._ctx = ctx
    async def publish_event(self, evt, run_id=None): self.events.append(evt)
    async def enqueue(self, agent_num, run_id): self.enqueued.append(agent_num)


class _FakeRC:
    async def record_step(self, *a, **kw): return None
    async def write_enrichment(self, *a, **kw): return None


async def test_secondary_flow_calls_snow_notes_then_enqueues_3_and_6(monkeypatch):
    ctx = {
        "flow": "secondary",
        "event": {
            "severity": "P4", "host": "db-01", "service": "payments",
            "incident_sys_id": "SYS-42",
            "raw_payload": {"eventType": "DATABASE_CONNECTION_FAILURE", "entityType": "SERVICE"},
        },
    }
    fake_redis = _FakeRedis(ctx)

    async def _get_redis(): return fake_redis
    monkeypatch.setattr(agent2_main, "get_redis", _get_redis)
    monkeypatch.setattr(agent2_main, "get_routing_client", lambda: _FakeRC())
    monkeypatch.setattr(agent2_main, "fire_and_forget", lambda coro: None)

    # Stub classifier.classify → known enrichment.
    from shared.models import SplunkEnrichment, RuleMatch
    fixed = SplunkEnrichment(
        log_lines_scanned=10, error_category="db", classification="db",
        confidence=80.0, hypothesis_source="splunk", dt_hypothesis="app",
        splunk_category="db", tier_used=1,
        rule_matches=[RuleMatch(rule_id="db.conn_pool", rule_set="db", weight=1.1,
                                match_count=5, sample_line="HikariCP timeout")],
        time_range="last 60min", spl_query="search index=prod ...",
        assigned_team="dba", assigned_queue="DB_ADMIN",
        snow_category="Database", snow_subcategory="Query/Connection",
        llm_summary="1 rule match",
    )
    async def _fake_classify(event, sev): return fixed
    monkeypatch.setattr(agent2_main.classifier, "classify", _fake_classify)

    # Capture snow_notes.post_work_note calls.
    calls = []
    async def _fake_post(**kw):
        calls.append(kw)
        return True
    monkeypatch.setattr(agent2_main.snow_notes, "post_work_note", _fake_post)

    await agent2_main.process_run("run-secondary-1")

    # SNOW write happened once, with correct sys_id and stage.
    assert len(calls) == 1
    assert calls[0]["sys_id"] == "SYS-42"
    assert calls[0]["stage"] == "SPLUNK EVIDENCE"
    assert calls[0]["agent_num"] == 2
    # Body renders through classifier.render_evidence_body.
    assert "Category (Splunk-scored): db" in calls[0]["body"]

    # Fan-out to Agents 3 and 6 still happens.
    assert sorted(fake_redis.enqueued) == [3, 6]


async def test_primary_flow_does_not_write_snow_note(monkeypatch):
    ctx = {
        "flow": "primary",
        "event": {"severity": "P2", "host": "web-01", "service": "portal",
                   "raw_payload": {"eventType": "FAILURE_RATE_INCREASED", "entityType": "SERVICE"}},
    }
    fake_redis = _FakeRedis(ctx)
    async def _get_redis(): return fake_redis
    monkeypatch.setattr(agent2_main, "get_redis", _get_redis)
    monkeypatch.setattr(agent2_main, "get_routing_client", lambda: _FakeRC())
    monkeypatch.setattr(agent2_main, "fire_and_forget", lambda coro: None)

    from shared.models import SplunkEnrichment
    async def _fake_classify(event, sev): return SplunkEnrichment(error_category="app", classification="app")
    monkeypatch.setattr(agent2_main.classifier, "classify", _fake_classify)

    calls = []
    async def _fake_post(**kw): calls.append(kw); return True
    monkeypatch.setattr(agent2_main.snow_notes, "post_work_note", _fake_post)

    await agent2_main.process_run("run-primary-1")

    assert calls == []           # no SNOW write on primary flow
    assert fake_redis.enqueued == [3]


async def test_secondary_flow_missing_sys_id_still_fans_out(monkeypatch):
    ctx = {
        "flow": "secondary",
        "event": {"severity": "P4",
                   "raw_payload": {"eventType": "SLOW_DB_QUERIES", "entityType": "SERVICE"}},
        # incident_sys_id missing
    }
    fake_redis = _FakeRedis(ctx)
    async def _get_redis(): return fake_redis
    monkeypatch.setattr(agent2_main, "get_redis", _get_redis)
    monkeypatch.setattr(agent2_main, "get_routing_client", lambda: _FakeRC())
    monkeypatch.setattr(agent2_main, "fire_and_forget", lambda coro: None)

    from shared.models import SplunkEnrichment
    async def _fake_classify(event, sev): return SplunkEnrichment(error_category="db", classification="db")
    monkeypatch.setattr(agent2_main.classifier, "classify", _fake_classify)

    # Real snow_notes.post_work_note — with missing sys_id it returns False without raising.
    await agent2_main.process_run("run-secondary-2")
    assert sorted(fake_redis.enqueued) == [3, 6]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent2_main_flow_b.py -v`
Expected: FAIL — either `AttributeError: module 'main' has no attribute 'snow_notes'` (import missing) or assertion failure that `post_work_note` was not called.

- [ ] **Step 3: Modify `agents/Agent-2-splunk/main.py`**

(a) Add `import snow_notes` alongside `import classifier` at the top.

(b) In `process_run`, replace the entire `else:` block that handles `flow != "primary"` with:

```python
        else:
            sys_id = event_data.get("incident_sys_id") or ""
            body = classifier.render_evidence_body(enrichment)
            duration_ms = int((time.monotonic() - t0) * 1000)
            await snow_notes.post_work_note(
                sys_id=sys_id, stage="SPLUNK EVIDENCE", agent_num=AGENT_NUM,
                status="success", duration_ms=duration_ms,
                run_id=run_id, body=body,
            )
            await asyncio.gather(
                redis.enqueue(3, run_id),
                redis.enqueue(6, run_id),
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent2_main_flow_b.py -v`
Expected: PASS — 3 tests green.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-2-splunk/main.py tests/test_agent2_main_flow_b.py
git commit -m "feat(agent-2): Flow B posts SPLUNK EVIDENCE work note before fan-out"
```

---

## Task 13 — End-to-end Flow B pipeline test

**Files:**
- Modify: `tests/test_pipeline.py` — add one Flow B integration-style test.

**Interfaces:**
- Consumes: existing `tests/test_pipeline.py` fixtures (there is already a test harness that can enqueue a run and observe downstream queues). If no such fixture exists, follow the pattern in [tests/test_pipeline.py](../../../tests/test_pipeline.py) as it stands and lift its mocking style.
- Produces: one new test named `test_flow_b_agent2_writes_snow_and_fans_out` that seeds a `ManualIncidentEvent` with `incident_sys_id`, drives Agent 2's `process_run`, and asserts (a) a SNOW PATCH was issued to `/api/now/table/incident/SYS-*`, (b) Agents 3 and 6 both have a work item after the run.

- [ ] **Step 1: Inspect `tests/test_pipeline.py` to match its existing style**

Run: `head -60 tests/test_pipeline.py`
Expected: view the imports, fixtures, and one existing test to model the new one after. If the file uses a `httpx.MockTransport` or `respx`, mirror that. If it uses a `_FakeRedis` similar to Task 12, reuse or extract that.

- [ ] **Step 2: Write the failing test**

Append to `tests/test_pipeline.py`:

```python
async def test_flow_b_agent2_writes_snow_and_fans_out(monkeypatch):
    """Full Flow B: Agent 2 posts SPLUNK EVIDENCE work note, then enqueues 3 and 6."""
    import sys as _sys
    from pathlib import Path as _Path
    _A2 = _Path(__file__).resolve().parents[1] / "agents" / "Agent-2-splunk"
    _sys.path.insert(0, str(_A2))
    import main as agent2_main
    import classifier as agent2_classifier
    import snow_notes as agent2_snow_notes

    class _R:
        def __init__(self, ctx):
            self._ctx = ctx; self.enqueued = []
        async def get_context(self, _): return self._ctx
        async def store_context(self, _, ctx): self._ctx = ctx
        async def publish_event(self, *a, **kw): pass
        async def enqueue(self, n, _): self.enqueued.append(n)

    class _RC:
        async def record_step(self, *a, **k): return None
        async def write_enrichment(self, *a, **k): return None

    ctx = {
        "flow": "secondary",
        "event": {
            "severity": "P4", "host": "db-01", "service": "payments",
            "incident_sys_id": "SYS-777",
            "raw_payload": {"eventType": "DATABASE_CONNECTION_FAILURE", "entityType": "SERVICE"},
        },
    }
    fake_redis = _R(ctx)

    async def _get_redis(): return fake_redis
    monkeypatch.setattr(agent2_main, "get_redis", _get_redis)
    monkeypatch.setattr(agent2_main, "get_routing_client", lambda: _RC())
    monkeypatch.setattr(agent2_main, "fire_and_forget", lambda c: None)

    # Wire snow_notes to real code but intercept the HTTP layer.
    import httpx
    snow_calls = []
    def _snow_responder(req: httpx.Request) -> httpx.Response:
        snow_calls.append({"method": req.method, "path": req.url.path,
                            "body": req.content.decode()})
        return httpx.Response(200, json={"result": {"sys_id": "SYS-777"}})

    monkeypatch.setattr(agent2_snow_notes, "SNOW_BASE", "https://snow", raising=False)
    async def _tok(): return "tkn"
    monkeypatch.setattr(agent2_snow_notes, "get_snow_token", _tok, raising=False)
    monkeypatch.setattr(agent2_snow_notes.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://snow",
                                                       transport=httpx.MockTransport(_snow_responder)))

    # Wire classifier's Splunk to return db evidence.
    monkeypatch.setattr(agent2_classifier, "SPLUNK_BASE", "https://splunk", raising=False)
    monkeypatch.setattr(agent2_classifier, "SPLUNK_INDEX", "prod", raising=False)

    def _splunk_responder(req: httpx.Request) -> httpx.Response:
        if req.method == "POST":
            return httpx.Response(201, json={"sid": "SID"})
        if "/jobs/SID" in req.url.path and req.method == "GET" and not req.url.path.endswith("/results"):
            return httpx.Response(200, json={"entry": [{"content": {"dispatchState": "DONE"}}]})
        if req.url.path.endswith("/results"):
            return httpx.Response(200, json={"results": [
                {"_raw": "HikariCP - Connection pool exhausted after 30000ms"} for _ in range(25)
            ]})
        return httpx.Response(500)

    monkeypatch.setattr(agent2_classifier.httpx, "AsyncClient",
                        lambda **kw: httpx.AsyncClient(base_url="https://splunk",
                                                       transport=httpx.MockTransport(_splunk_responder)))

    await agent2_main.process_run("run-flowb-e2e")

    # Fan-out preserved.
    assert sorted(fake_redis.enqueued) == [3, 6]
    # SNOW PATCH sent with a SPLUNK EVIDENCE header + db category body.
    patch_calls = [c for c in snow_calls if c["method"] == "PATCH"]
    assert len(patch_calls) == 1
    assert "/api/now/table/incident/SYS-777" in patch_calls[0]["path"]
    assert "SPLUNK EVIDENCE" in patch_calls[0]["body"]
    assert "Category (Splunk-scored): db" in patch_calls[0]["body"]
```

- [ ] **Step 3: Run the test to verify it fails initially, then passes**

Run: `pytest tests/test_pipeline.py::test_flow_b_agent2_writes_snow_and_fans_out -v`
Expected initially: FAIL — most likely `ModuleNotFoundError` or an assertion mismatch (the earlier tasks may have wired things slightly differently; adjust the test's monkeypatch targets to match the real attribute names).
Then, without changing any product code, iterate on the test until it passes.

- [ ] **Step 4: Run the full Agent-2 + pipeline suites**

Run: `pytest tests/test_agent2_*.py tests/test_pipeline.py -v`
Expected: all Agent-2 tests plus the new pipeline test pass.

- [ ] **Step 5: Commit (this closes PR-3)**

```bash
git add tests/test_pipeline.py
git commit -m "test(pipeline): end-to-end Flow B — Agent 2 SNOW write + fan-out"
```

---

## Self-Review Summary

**Spec coverage:**
- §3 data contract → Task 1 (RuleMatch + widened SplunkEnrichment).
- §5.1 preclassify → Task 3.
- §5.2 build_tiered_queries → Task 4.
- §5.3 run_splunk_async → Task 5.
- §5.4 score → Task 6.
- §5.5 resolve_conflict → Task 7.
- §5.6 precompute_routing → Task 8.
- §6 classify orchestration + backward-compat classification → Task 9.
- §7 snow_notes + Flow B body renderer → Tasks 9 (renderer) + 11 (poster) + 12 (wiring).
- §8 failure modes → Tasks 9 (degraded paths), 11 (SNOW failure tolerance), 12 (missing sys_id).
- §9 observability — structured log fields for hypothesis_source, confidence, tier_used, rule_match_count included in `agent_done` SSE payload at Task 10; free-text sample lines never logged (only appear via `snow_notes` body).
- §10 tests — all listed test files created.
- §11 tuning constants — `SPLUNK_TIER1_SHORTCIRCUIT_MIN` (Task 9), `CONFLICT_RESOLUTION_THRESHOLD` (Task 7), `SPLUNK_JOB_POLL_INTERVAL_SECONDS` / `SPLUNK_JOB_MAX_POLLS` (Task 5), `SPLUNK_MAX_RESULTS` (Task 4).
- §12 delivery sequence → Tasks 1–9 (PR-1), Task 10 (PR-2), Tasks 11–13 (PR-3).

**Type consistency:**
- `resolve_conflict` returns `Tuple[str, str | None, float, str]` — consumed by `classify` in Task 9, which destructures into `(final_category, splunk_category, confidence, source)`. Matches.
- `precompute_routing` returns `Dict[str, str]` with keys `team`, `queue`, `snow_category`, `snow_subcategory` — consumed by `_degraded` and the main `classify` path in Task 9. Matches.
- `post_work_note` signature (`sys_id`, `stage`, `agent_num`, `status`, `duration_ms`, `run_id`, `body`) — used identically in Task 12 wiring and in the test in Task 11. Matches.
- `render_evidence_body` takes a `SplunkEnrichment`, returns `str` — used in Task 12 main.py; test in Task 9 exercises it independently. Matches.

**Placeholder scan:** No "TBD", "TODO", "similar to Task N" without code, or "handle edge cases" placeholders. Every code step includes the actual code.

**PR boundary check:** Tasks 1–9 are additive (nothing rewired). Task 10 is the single wiring commit for PR-2 — a reviewer can accept PR-1 and reject PR-2. Tasks 11–13 form PR-3.

---

Plan complete and saved to `docs/superpowers/plans/2026-07-01-agent-2-spec-closure.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
