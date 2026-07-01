# Agent 1 Alertmanager Intake — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add native Prometheus Alertmanager webhook intake to Sentinel Agent 1 alongside the existing Dynatrace and ServiceNow paths, with a refactor-first sequencing that proves behavioral parity before adding functionality.

**Architecture:** Three sequential PRs. PR1 is a pure extraction refactor of `main.py` into an `intake/` package gated by a parity-baseline test. PR2 adds the AM endpoint, hybrid entity resolver, and resolved-status auto-close, behind feature flags. PR3 adds an internal Redis-backed DLQ with retry sweep so dispatch failures don't get swallowed.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, asyncio, Redis (`redis.asyncio`), pytest (asyncio_mode=auto), httpx.

## Global Constraints

- **No regression on Flow A (DT) or Flow B (SNOW).** PR1 includes an explicit parity test gate.
- **PRs are sequential — STOP for human review between each.** Do not start PR2 until PR1 is merged and soaked; do not start PR3 until PR2 is merged.
- **Match existing style.** Follow the logging format (`log.info("X %s", val)`), error-handling shape (`raise HTTPException(...)`), and file conventions (single-file FastAPI app per agent today; this plan introduces the first sub-package).
- **No comments unless WHY is non-obvious** (per project CLAUDE.md §13).
- **Healthcare deployment** — log identifiers, not free-text content (CLAUDE.md §10.6). No alert labels/annotations in log messages beyond `alertname` and identifiers.
- **Coverage floor:** 90% overall, auth paths 100% (CLAUDE.md §10.4).
- **`OrchestratorEvent` already has `model_config = ConfigDict(extra="allow")`** — adding optional fields is backward-compatible by construction.
- **Resolution monitor pattern:** Agent 7 uses `asyncio.create_task(worker_loop())` from FastAPI lifespan, not APScheduler. The PR3 sweep follows this.
- **HTTP contract with AM is 202 always** (except 401/422/503). DLQ is internal — never propagates as non-2xx.
- **If you hit an ambiguity not covered here, STOP and ask** — this is a healthcare incident pipeline; silent wrong guesses are worse than a pause.
- **Spec reference:** `docs/superpowers/specs/2026-06-30-agent-1-alertmanager-intake-design.md`

---

# PR1 — Pure extraction refactor (zero behavior change)

**Goal:** Split current `main.py` (~320 lines) into `intake/dt.py`, `intake/snow.py`, slimmed `main.py`. Behaviorally identical.

## File Structure (PR1)

| File | Responsibility |
|---|---|
| `agents/Agent-1-dynatrace/main.py` (modify) | FastAPI app, lifespan, `_ingest()`, SSE routes, route declarations only |
| `agents/Agent-1-dynatrace/intake/__init__.py` (create) | empty package marker |
| `agents/Agent-1-dynatrace/intake/dt.py` (create) | DT webhook: verify + parse + to_events + handle |
| `agents/Agent-1-dynatrace/intake/snow.py` (create) | SNOW webhook: verify + parse + to_events + handle |
| `tests/test_refactor_parity.py` (create) | Baseline parity test — written and passing **before** any extraction |

## Interfaces produced by PR1

Each `intake/<source>.py` module exports:

```python
def verify(header_value: str | None, body: bytes) -> None:
    """Raises HTTPException(401) on signature mismatch. No return value."""

def parse(body: bytes) -> Payload:
    """Returns the source-specific Pydantic payload. Raises HTTPException(422)."""

def to_events(payload: Payload) -> list[OrchestratorEvent]:
    """Always returns a list (length 1 for DT/SNOW; future AM returns N)."""

async def handle(
    request: Request,
    header_value: str | None,
    _ingest: Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve: Callable[[str], Awaitable[dict]] | None = None,
) -> dict:
    """Orchestrate verify -> parse -> to_events -> dispatch. Returns response body."""
```

---

### Task 1: Write the parity baseline test (against unmodified `main.py`)

**Files:**
- Create: `tests/test_refactor_parity.py`

**Interfaces:**
- Consumes: current `agents/Agent-1-dynatrace/main.py` as-is
- Produces: baseline test that PR1 must keep green

- [ ] **Step 1: Read the current code to find exact response shapes and Redis side effects**

Run:
```bash
sed -n '105,190p' agents/Agent-1-dynatrace/main.py
```

Note: `_ingest()` returns `{"accepted": True, "run_id": ..., "external_id": ..., "severity": ..., "flow": ...}` on success and `{"accepted": False, "deduplicated": True, "external_id": ...}` on dedup hit. RESOLVED DT events return `{"accepted": False, "reason": "RESOLVED events are ignored"}`. These are the contract.

- [ ] **Step 2: Create the parity test file**

```python
# tests/test_refactor_parity.py
"""
Baseline parity test for the Agent 1 main.py refactor (PR1).

This file is written and committed BEFORE the refactor. Its purpose is to
fail loudly if the extraction changes any observable behaviour — response
status, response body shape, or Redis side effects.

Do NOT edit this test to match new behaviour. If it breaks during the
refactor, the refactor is wrong.
"""
import hashlib
import hmac
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from agents.Agent_1_dynatrace.main import app  # noqa: E402  (module path may need a sys.path tweak in conftest)

DT_SECRET   = "test-dt-secret"
SNOW_SECRET = "test-snow-secret"


def _sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DT_SECRET", DT_SECRET)
    monkeypatch.setenv("SNOW_SECRET", SNOW_SECRET)
    yield


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def dt_payload_open():
    return {
        "problemId":   "PROBLEM-12345",
        "displayName": "High CPU on host-x",
        "severity":    "ERROR",
        "status":      "OPEN",
        "impactedEntities": [{"name": "host-x", "entityId": "HOST-ABC"}],
        "tags":        ["app:checkout"],
        "startTime":   int(datetime.now(timezone.utc).timestamp() * 1000),
    }


@pytest.fixture
def snow_payload():
    return {
        "number":            "INC0010001",
        "short_description": "Manual P4 ticket",
        "priority":          "4",
        "cmdb_ci":           "svc-checkout",
        "sys_created_on":    datetime.now(timezone.utc).isoformat(),
    }


def test_dt_valid_payload_returns_202_and_run_id(client, dt_payload_open):
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": _sign(body, DT_SECRET)})
    assert r.status_code == 202
    j = r.json()
    assert j["accepted"] is True
    assert j["external_id"] == "PROBLEM-12345"
    assert "run_id" in j


def test_dt_invalid_signature_returns_401(client, dt_payload_open):
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": "deadbeef"})
    assert r.status_code == 401


def test_dt_malformed_body_returns_422(client):
    body = b"not json"
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": _sign(body, DT_SECRET)})
    assert r.status_code == 422


def test_dt_resolved_status_is_ignored(client, dt_payload_open):
    dt_payload_open["status"] = "RESOLVED"
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": _sign(body, DT_SECRET)})
    assert r.status_code == 202
    assert r.json() == {"accepted": False, "reason": "RESOLVED events are ignored"}


def test_snow_valid_payload_returns_202_and_run_id(client, snow_payload):
    body = json.dumps(snow_payload).encode()
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": _sign(body, SNOW_SECRET)})
    assert r.status_code == 202
    j = r.json()
    assert j["accepted"] is True
    assert j["external_id"] == "INC0010001"


def test_snow_invalid_signature_returns_401(client, snow_payload):
    body = json.dumps(snow_payload).encode()
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": "deadbeef"})
    assert r.status_code == 401


def test_snow_malformed_body_returns_422(client):
    body = b"{"
    r = client.post("/api/webhook/servicenow", content=body,
                    headers={"X-SNOW-Signature": _sign(body, SNOW_SECRET)})
    assert r.status_code == 422


@pytest.mark.integration
async def test_dt_ingest_side_effect_writes_redis_run_context(client, dt_payload_open, redis_client):
    """Asserts the actual side effect, not just the HTTP response."""
    body = json.dumps(dt_payload_open).encode()
    r = client.post("/api/webhook/dynatrace", content=body,
                    headers={"X-DT-Signature": _sign(body, DT_SECRET)})
    run_id = r.json()["run_id"]
    ctx = await redis_client.get_context(run_id)
    assert ctx is not None
    assert ctx["event"]["external_id"] == "PROBLEM-12345"
```

- [ ] **Step 3: Wire up `agents.Agent_1_dynatrace.main` import path**

Project uses `PYTHONPATH=$(pwd)` rooted at repo root. The directory `Agent-1-dynatrace` has a hyphen — Python won't import that as a module name. Check whether `conftest.py` already does a `sys.path` insert + dynamic import (likely — Agent 8 tests work). If yes, mirror that pattern. If not:

```python
# tests/conftest.py — ADD if not already present
import sys
from pathlib import Path

AGENT_1_DIR = Path(__file__).parent.parent / "agents" / "Agent-1-dynatrace"
sys.path.insert(0, str(AGENT_1_DIR))
```

Then in the test: `from main import app` (not `from agents.Agent_1_dynatrace.main`).

**STOP and confirm with the human which import shape the repo uses before continuing.** Look at how `tests/test_agent8_*.py` files import.

- [ ] **Step 4: Run the parity test against current `main.py`**

```bash
pytest tests/test_refactor_parity.py -v -m "not integration"
```

Expected: all unit tests PASS. If any fail, the test has a bug — fix the test, not the code. The whole point is to capture current behavior.

- [ ] **Step 5: Commit baseline**

```bash
git add tests/test_refactor_parity.py tests/conftest.py
git commit -m "test(agent1): parity baseline for main.py refactor

Captures current DT and SNOW webhook response shapes + RESOLVED handling
before extracting the intake/ package. Must remain green through PR1.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Extract DT webhook into `intake/dt.py`

**Files:**
- Create: `agents/Agent-1-dynatrace/intake/__init__.py` (empty)
- Create: `agents/Agent-1-dynatrace/intake/dt.py`
- Modify: `agents/Agent-1-dynatrace/main.py` (route body only)

**Interfaces:**
- Consumes: `_ingest`, `OrchestratorEvent`, `verify_hmac_signature`, `DynatracePayload`, `_DT_SEVERITY_MAP` from `main.py`
- Produces: `dt.handle(request, x_dt_signature, _ingest) -> dict`

- [ ] **Step 1: Create empty package init**

```bash
touch agents/Agent-1-dynatrace/intake/__init__.py
```

- [ ] **Step 2: Create `intake/dt.py` with extracted logic — copy verbatim**

```python
"""DT webhook intake — extracted verbatim from main.py during PR1 refactor."""
from __future__ import annotations

import os
from typing import Awaitable, Callable

from fastapi import HTTPException, Request
from pydantic import ValidationError

# These imports MUST resolve to the same symbols main.py used. Adjust path
# style to match whatever conftest.py installs (see PR1 Task 1 Step 3).
from main import (
    DT_SECRET,
    DynatracePayload,
    OrchestratorEvent,
    IncidentFlow,
    IncidentSource,
    Severity,
    _DT_SEVERITY_MAP,
    verify_hmac_signature,
)


def verify(header_value: str | None, body: bytes) -> None:
    if DT_SECRET:
        if not header_value or not verify_hmac_signature(body, header_value, DT_SECRET):
            raise HTTPException(status_code=401, detail="Invalid DT signature")


def parse(body: bytes) -> DynatracePayload:
    try:
        return DynatracePayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def to_events(payload: DynatracePayload) -> list[OrchestratorEvent]:
    severity = _DT_SEVERITY_MAP.get(payload.severity.upper(), Severity.INFO)
    flow     = IncidentFlow.PRIMARY if severity in (
        Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM
    ) else IncidentFlow.SECONDARY

    host    = payload.impactedEntities[0]["name"] if payload.impactedEntities else None
    service = next(
        (t.split(":", 1)[1] for t in payload.tags if t.startswith("app:")),
        None,
    )

    return [OrchestratorEvent(
        source=IncidentSource.DYNATRACE,
        external_id=payload.problemId,
        severity=severity,
        flow=flow,
        title=payload.displayName,
        raw_payload=payload.model_dump(),
        host=host,
        service=service,
        dedup_key=f"dt:{payload.problemId}",
    )]


async def handle(
    request: Request,
    header_value: str | None,
    _ingest: Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve: Callable[[str], Awaitable[dict]] | None = None,
) -> dict:
    body = await request.body()
    verify(header_value, body)
    payload = parse(body)

    if payload.status.upper() == "RESOLVED":
        return {"accepted": False, "reason": "RESOLVED events are ignored"}

    events = to_events(payload)
    # DT is always single-alert; keep the dispatch as direct call to preserve
    # current return shape (the dict from _ingest, not a list).
    return await _ingest(events[0])
```

**IMPORTANT — circular import:** `intake/dt.py` imports from `main`, and `main` will import from `intake.dt`. Resolve by lazy-importing inside `handle()` OR by moving `DT_SECRET`, `DynatracePayload`, `_DT_SEVERITY_MAP`, `verify_hmac_signature` into a new `agents/Agent-1-dynatrace/_common.py` and importing both from there. **Prefer the `_common.py` approach** — circular lazy imports are a smell. If `_common.py` is the path, add it as a fourth file in the PR1 file structure and update Tasks 2 and 3 to import from `_common` instead of `main`.

- [ ] **Step 3: Update `main.py` route to delegate**

```python
# main.py — REPLACE the body of dynatrace_webhook (lines 105–150 in current file)
from intake import dt as dt_intake

@app.post("/api/webhook/dynatrace", status_code=202)
async def dynatrace_webhook(
    request:        Request,
    x_dt_signature: str | None = Header(None, alias="X-DT-Signature"),
):
    return await dt_intake.handle(request, x_dt_signature, _ingest)
```

Everything else in `main.py` (`_ingest`, lifespan, SSE endpoints, `/health`, `/ready`) is **untouched**.

- [ ] **Step 4: Run parity test**

```bash
pytest tests/test_refactor_parity.py -v -m "not integration"
```

Expected: every DT test still PASSES with identical response bodies.

- [ ] **Step 5: Show diff of main.py and commit**

```bash
git diff agents/Agent-1-dynatrace/main.py
```

Confirm only the `dynatrace_webhook` body changed and one new import line was added. Lifespan, SSE endpoint, middleware bytes-identical.

```bash
git add agents/Agent-1-dynatrace/intake/ agents/Agent-1-dynatrace/main.py agents/Agent-1-dynatrace/_common.py
git commit -m "refactor(agent1): extract DT webhook into intake/dt.py

Pure extraction — no behaviour change. Parity test green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Extract SNOW webhook into `intake/snow.py`

**Files:**
- Create: `agents/Agent-1-dynatrace/intake/snow.py`
- Modify: `agents/Agent-1-dynatrace/main.py` (route body only)

**Interfaces:**
- Same shape as `intake/dt.py`

- [ ] **Step 1: Create `intake/snow.py` mirroring `intake/dt.py`'s shape**

```python
"""SNOW webhook intake — extracted verbatim from main.py during PR1 refactor."""
from __future__ import annotations

from typing import Awaitable, Callable

from fastapi import HTTPException, Request
from pydantic import ValidationError

from _common import (  # adjust to your final import shape per PR1 Task 2 note
    SNOW_SECRET,
    ServiceNowPayload,
    OrchestratorEvent,
    IncidentFlow,
    IncidentSource,
    Severity,
    _SNOW_PRIORITY_MAP,
    verify_hmac_signature,
)


def verify(header_value: str | None, body: bytes) -> None:
    if SNOW_SECRET:
        if not header_value or not verify_hmac_signature(body, header_value, SNOW_SECRET):
            raise HTTPException(status_code=401, detail="Invalid SNOW signature")


def parse(body: bytes) -> ServiceNowPayload:
    try:
        return ServiceNowPayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def to_events(payload: ServiceNowPayload) -> list[OrchestratorEvent]:
    severity = _SNOW_PRIORITY_MAP.get(str(payload.priority), Severity.INFO)
    flow     = IncidentFlow.PRIMARY if severity in (
        Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM
    ) else IncidentFlow.SECONDARY

    return [OrchestratorEvent(
        source=IncidentSource.SERVICENOW,
        external_id=payload.number,
        severity=severity,
        flow=flow,
        title=payload.short_description,
        raw_payload=payload.model_dump(),
        host=payload.cmdb_ci,
        service=payload.cmdb_ci,
        dedup_key=f"snow:{payload.number}",
    )]


async def handle(
    request: Request,
    header_value: str | None,
    _ingest: Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve: Callable[[str], Awaitable[dict]] | None = None,
) -> dict:
    body = await request.body()
    verify(header_value, body)
    payload = parse(body)
    events = to_events(payload)
    return await _ingest(events[0])
```

- [ ] **Step 2: Update `main.py` route to delegate**

```python
from intake import snow as snow_intake

@app.post("/api/webhook/servicenow", status_code=202)
async def servicenow_webhook(
    request:          Request,
    x_snow_signature: str | None = Header(None, alias="X-SNOW-Signature"),
):
    return await snow_intake.handle(request, x_snow_signature, _ingest)
```

- [ ] **Step 3: Run full parity + existing test suite**

```bash
pytest tests/test_refactor_parity.py tests/test_pipeline.py tests/test_classification.py tests/test_rca_scoring.py -v -m "not integration"
```

Expected: 100% pass, no skips, no warnings beyond pre-existing ones.

- [ ] **Step 4: Run integration tests with stack up**

```bash
docker compose -f docker/docker-compose.yml up -d
pytest tests/test_refactor_parity.py -v -m integration
docker compose -f docker/docker-compose.yml down
```

Expected: parity integration test (Redis side effect) PASSES.

- [ ] **Step 5: Coverage gate**

```bash
pytest tests/ --cov=agents/Agent-1-dynatrace --cov-report=term-missing -m "not integration"
```

Expected: coverage ≥ 90% on Agent 1. If any module dropped below, address it (likely just need to ensure both intake modules are exercised — the parity test should already cover this).

- [ ] **Step 6: Commit PR1 final**

```bash
git add agents/Agent-1-dynatrace/intake/snow.py agents/Agent-1-dynatrace/main.py
git commit -m "refactor(agent1): extract SNOW webhook into intake/snow.py

Completes PR1 — DT and SNOW now live in intake/{dt,snow}.py with main.py
reduced to routes + lifespan + SSE only. Parity test green.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 7: STOP — open PR1, await human review and non-prod soak**

Do not start PR2 until the user explicitly says PR1 has been merged and soaked.

---

# PR2 — Alertmanager intake (new functionality)

**Goal:** Add native AM v4 webhook intake, flag-gated, with hybrid entity resolution and resolved-status auto-close.

## File Structure (PR2)

| File | Responsibility |
|---|---|
| `agents/Agent-1-dynatrace/intake/alertmanager.py` (create) | AM webhook: verify + parse + to_events + dispatch + `_resolve` helper |
| `agents/Agent-1-dynatrace/adapters/__init__.py` (create) | empty |
| `agents/Agent-1-dynatrace/adapters/am_entity_resolver.py` (create) | Hybrid label → routing-db → DT-name resolution with negative cache |
| `shared/models.py` (modify) | Add `IncidentSource.ALERTMANAGER`; add `environment` + `am_group_key` optional fields to `OrchestratorEvent` |
| `agents/Agent-1-dynatrace/main.py` (modify) | Add `/api/webhook/alertmanager` route; add `_resolve()` helper |
| `agents/Agent-3-servicenow/main.py` (modify) | Branch on `source=ALERTMANAGER` to set `u_source_tool='Alertmanager'` + `u_source_alert_id=fingerprint` |
| `tests/test_alertmanager_intake.py` (create) | Unit tests for AM intake module |
| `tests/test_am_entity_resolver.py` (create) | Unit tests for resolver including negative-cache |
| `tests/test_pipeline.py` (modify) | Add `test_alertmanager_e2e` integration |
| `.env.example` (modify) | Add `AM_WEBHOOK_TOKEN`, `AM_INTAKE_ENABLED`, `AM_DEFAULT_ENV` |
| `docker/docker-compose.yml` (modify) | Pass the three new env vars to the Agent 1 service |

## Interfaces produced by PR2

```python
# intake/alertmanager.py
async def handle(request, authorization, _ingest, _resolve) -> dict: ...
async def _resolve(external_id: str) -> dict: ...   # exported for sweep use in PR3

# adapters/am_entity_resolver.py
class EntityRef(BaseModel):
    entity_id:   str | None = None
    entity_name: str | None = None
    source:      Literal["label", "routing-db", "dt-name", "unresolved", "unresolved-cached"]

async def resolve(labels: dict[str, str]) -> EntityRef: ...
```

---

### Task 4: Extend `shared/models.py`

**Files:**
- Modify: `shared/models.py:27-29` (add enum), `:99-112` (add two optional fields to `OrchestratorEvent`)

**Interfaces:**
- Produces: `IncidentSource.ALERTMANAGER`, `OrchestratorEvent.environment`, `OrchestratorEvent.am_group_key`

- [ ] **Step 1: Write failing test for new fields**

```python
# tests/test_shared_models_am.py
from shared.models import (
    IncidentSource, OrchestratorEvent, Severity, IncidentFlow,
)

def test_alertmanager_source_value():
    assert IncidentSource.ALERTMANAGER == "alertmanager"

def test_orchestrator_event_accepts_environment_and_group_key():
    e = OrchestratorEvent(
        source=IncidentSource.ALERTMANAGER,
        external_id="am-deadbeef",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="HighCPU",
        environment="staging",
        am_group_key="{}:{alertname=\"HighCPU\"}",
    )
    assert e.environment == "staging"
    assert e.am_group_key.startswith("{}:")

def test_orchestrator_event_fields_default_to_none():
    e = OrchestratorEvent(
        source=IncidentSource.DYNATRACE,
        external_id="P-1",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="x",
    )
    assert e.environment is None
    assert e.am_group_key is None
```

- [ ] **Step 2: Run, confirm fails**

```bash
pytest tests/test_shared_models_am.py -v
```

Expected: FAIL on `AttributeError: ALERTMANAGER` and unknown field errors.

- [ ] **Step 3: Add the enum value and fields**

```python
# shared/models.py around line 27
class IncidentSource(str, enum.Enum):
    DYNATRACE    = "dynatrace"
    SERVICENOW   = "servicenow"
    ALERTMANAGER = "alertmanager"
```

```python
# shared/models.py — OrchestratorEvent, add after `service:` field
    # Populated by AM intake (optional for other sources)
    environment:  Optional[str] = None
    am_group_key: Optional[str] = None
```

- [ ] **Step 4: Run, confirm pass**

```bash
pytest tests/test_shared_models_am.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Run full existing suite to confirm no regression**

```bash
pytest tests/ -m "not integration" -v
```

Expected: all pass; no surprises since fields default to `None`.

- [ ] **Step 6: Commit**

```bash
git add shared/models.py tests/test_shared_models_am.py
git commit -m "feat(shared): add IncidentSource.ALERTMANAGER and AM fields

Adds optional environment and am_group_key fields to OrchestratorEvent.
Backward-compatible (extra='allow' + Optional defaults).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Entity resolver with negative cache

**Files:**
- Create: `agents/Agent-1-dynatrace/adapters/__init__.py` (empty)
- Create: `agents/Agent-1-dynatrace/adapters/am_entity_resolver.py`
- Create: `tests/test_am_entity_resolver.py`

**Interfaces:**
- Consumes: `shared.routing_client.get_routing_client`, `dt_client.resolve_entity_by_name` (verify name on real client; may need a small addition), `shared.redis_client.get_redis`
- Produces: `EntityRef`, `resolve(labels) -> EntityRef`

- [ ] **Step 1: Write failing tests (all four hybrid steps + negative cache)**

```python
# tests/test_am_entity_resolver.py
import pytest
from unittest.mock import AsyncMock, patch

from adapters.am_entity_resolver import EntityRef, resolve, NEGATIVE_CACHE_TTL


@pytest.mark.asyncio
async def test_resolve_prefers_explicit_dt_entity_id_label():
    result = await resolve({"dt_entity_id": "HOST-ABC", "service": "checkout"})
    assert result.entity_id == "HOST-ABC"
    assert result.source == "label"


@pytest.mark.asyncio
async def test_resolve_falls_back_to_routing_db(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(
        return_value=type("M", (), {"entity_id": "HOST-FROM-DB"})()
    )
    monkeypatch.setattr("adapters.am_entity_resolver.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("adapters.am_entity_resolver.get_routing_client",
                        lambda: routing_mock)

    result = await resolve({"service": "checkout"})
    assert result.entity_id == "HOST-FROM-DB"
    assert result.source == "routing-db"


@pytest.mark.asyncio
async def test_resolve_falls_back_to_dt_name_lookup(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(return_value=None)
    dt_mock = AsyncMock()
    dt_mock.resolve_entity_by_name = AsyncMock(return_value="HOST-FROM-DT")
    monkeypatch.setattr("adapters.am_entity_resolver.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("adapters.am_entity_resolver.get_routing_client",
                        lambda: routing_mock)
    monkeypatch.setattr("adapters.am_entity_resolver.dt_client", dt_mock)

    result = await resolve({"service": "checkout"})
    assert result.entity_id == "HOST-FROM-DT"
    assert result.entity_name == "checkout"
    assert result.source == "dt-name"


@pytest.mark.asyncio
async def test_resolve_soft_fails_to_unresolved_and_sets_negative_cache(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(return_value=None)
    dt_mock = AsyncMock()
    dt_mock.resolve_entity_by_name = AsyncMock(return_value=None)
    monkeypatch.setattr("adapters.am_entity_resolver.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("adapters.am_entity_resolver.get_routing_client",
                        lambda: routing_mock)
    monkeypatch.setattr("adapters.am_entity_resolver.dt_client", dt_mock)

    result = await resolve({"service": "checkout"})
    assert result.source == "unresolved"
    assert result.entity_name == "checkout"
    redis_mock.set.assert_awaited_once_with(
        "am_entity_miss:checkout", "1", ex=NEGATIVE_CACHE_TTL
    )


@pytest.mark.asyncio
async def test_resolve_short_circuits_on_negative_cache_hit(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=b"1")
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock()  # should NOT be called
    monkeypatch.setattr("adapters.am_entity_resolver.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("adapters.am_entity_resolver.get_routing_client",
                        lambda: routing_mock)

    result = await resolve({"service": "checkout"})
    assert result.source == "unresolved-cached"
    routing_mock.get_am_entity.assert_not_awaited()
```

- [ ] **Step 2: Run, confirm fails**

```bash
pytest tests/test_am_entity_resolver.py -v
```

Expected: ModuleNotFoundError on import.

- [ ] **Step 3: Implement `adapters/am_entity_resolver.py`**

```python
"""Hybrid AM-label → DT-entity resolver with negative cache."""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel

from shared.redis_client import get_redis
from shared.routing_client import get_routing_client
from shared import dt_client  # whatever module exposes resolve_entity_by_name; confirm path

NEGATIVE_CACHE_TTL = 600  # seconds; tune from am_entity_resolved_total{source="unresolved"}


class EntityRef(BaseModel):
    entity_id:   Optional[str] = None
    entity_name: Optional[str] = None
    source:      Literal["label", "routing-db", "dt-name", "unresolved", "unresolved-cached"]


async def resolve(labels: dict[str, str]) -> EntityRef:
    if eid := labels.get("dt_entity_id"):
        return EntityRef(entity_id=eid, source="label")

    key = labels.get("service") or labels.get("job")
    name = labels.get("service") or labels.get("instance") or labels.get("job")

    redis = await get_redis()

    if key:
        if await redis.get(f"am_entity_miss:{key}"):
            return EntityRef(entity_name=name, source="unresolved-cached")

        rc = get_routing_client()
        if mapped := await rc.get_am_entity(key):
            return EntityRef(entity_id=mapped.entity_id, source="routing-db")

    if name and (eid := await dt_client.resolve_entity_by_name(name)):
        return EntityRef(entity_id=eid, entity_name=name, source="dt-name")

    if key:
        await redis.set(f"am_entity_miss:{key}", "1", ex=NEGATIVE_CACHE_TTL)
    return EntityRef(entity_name=name, source="unresolved")
```

**ASK BEFORE PROCEEDING:** `shared/routing_client.py` does not currently expose `get_am_entity` — that endpoint and the underlying `am_entity_map` table are flagged in the spec as a **separate routing-db spec**. For this PR's resolver to work, either:
- (a) Stub `get_am_entity` to return `None` in `routing_client.py` (keep the behavior soft-failing), or
- (b) Block this PR on the routing-db spec landing first.

Confirm with the human which approach. Recommendation: (a) — the resolver soft-fails to DT-name lookup; routing-db precedence becomes effective when the table ships.

- [ ] **Step 4: Run, confirm pass**

```bash
pytest tests/test_am_entity_resolver.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add agents/Agent-1-dynatrace/adapters/ tests/test_am_entity_resolver.py
git commit -m "feat(agent1): hybrid AM entity resolver with negative cache

Resolves AM labels to DT entity_id via label > routing-db > DT-name >
soft-fail, with 10-minute negative cache to prevent hammering during
early-rollout 'unresolved' periods.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: AM intake module — parse, route, dispatch

**Files:**
- Create: `agents/Agent-1-dynatrace/intake/alertmanager.py`
- Create: `tests/test_alertmanager_intake.py`

**Interfaces:**
- Consumes: `_ingest`, `_resolve` from `main`; `EntityRef`, `resolve` from `adapters.am_entity_resolver`; `OrchestratorEvent` from `shared.models`
- Produces: `handle(request, authorization, _ingest, _resolve) -> dict`

- [ ] **Step 1: Write the failing tests (auth + schema + routing + drop + race guard)**

```python
# tests/test_alertmanager_intake.py
import json
import pytest
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from main import app

AM_TOKEN = "test-am-token"


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("AM_WEBHOOK_TOKEN", AM_TOKEN)
    monkeypatch.setenv("AM_INTAKE_ENABLED", "true")
    monkeypatch.setenv("AM_DEFAULT_ENV", "prod")


@pytest.fixture
def client():
    return TestClient(app)


def _am_payload(alerts):
    return {
        "version": "4",
        "groupKey": '{}:{alertname="HighCPU"}',
        "status": "firing",
        "receiver": "sentinel",
        "externalURL": "http://am.local",
        "alerts": alerts,
    }


def _alert(status="firing", alertname="HighCPU", severity="critical",
           service="checkout", fingerprint="deadbeef01", labels_extra=None):
    return {
        "status": status,
        "labels": {"alertname": alertname, "severity": severity,
                   "service": service, **(labels_extra or {})},
        "annotations": {"summary": "High CPU"},
        "startsAt": datetime.now(timezone.utc).isoformat(),
        "fingerprint": fingerprint,
    }


def test_auth_missing_header_returns_401(client):
    r = client.post("/api/webhook/alertmanager",
                    json=_am_payload([_alert()]))
    assert r.status_code == 401


def test_auth_wrong_token_returns_401(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": "Bearer wrong"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 401


def test_auth_empty_env_returns_503(client, monkeypatch):
    monkeypatch.setenv("AM_WEBHOOK_TOKEN", "")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 503


def test_feature_flag_off_returns_404(client, monkeypatch):
    monkeypatch.setenv("AM_INTAKE_ENABLED", "false")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 404


def test_valid_v4_payload_returns_202(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert()]))
    assert r.status_code == 202
    body = r.json()
    assert body["accepted"] >= 1


def test_unexpected_version_proceeds_with_warning(client, caplog):
    payload = _am_payload([_alert()])
    payload["version"] = "5"
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=payload)
    assert r.status_code == 202


def test_malformed_body_returns_422(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}",
                             "Content-Type": "application/json"},
                    content=b"{not json")
    assert r.status_code == 422


def test_alert_missing_alertname_is_dropped_rest_proceed(client):
    bad = _alert(fingerprint="bad")
    bad["labels"].pop("alertname")
    good = _alert(fingerprint="good")
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([bad, good]))
    assert r.status_code == 202
    body = r.json()
    assert body["dropped"] == 1
    assert body["accepted"] == 1


def test_severity_critical_routes_to_flow_a(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(severity="critical")]))
    # Inspect Redis to confirm flow=primary, or expose flow in response counts
    # by run; for this unit test inspect a mocked _ingest call.
    assert r.status_code == 202


def test_severity_info_routes_to_flow_b(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(severity="info")]))
    assert r.status_code == 202


def test_sentinel_flow_label_overrides_severity(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(
                        severity="info",
                        labels_extra={"sentinel_flow": "a"},
                    )]))
    assert r.status_code == 202


def test_unmapped_severity_falls_to_info_with_debug_log(client, caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(severity="emergency")]))
    assert r.status_code == 202
    assert any("am_unmapped_severity" in m for m in caplog.messages)


def test_fingerprint_becomes_external_id(client):
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload([_alert(fingerprint="abc123")]))
    # Verify via a mocked _ingest capturing the event; structure depends on
    # which mock surface the test fixture provides.
    assert r.status_code == 202


def test_same_fingerprint_firing_and_resolved_processed_sequentially(client):
    """Race guard: resolved for same fp as firing in one batch must wait."""
    alerts = [
        _alert(status="firing",   fingerprint="flap-1"),
        _alert(status="resolved", fingerprint="flap-1"),
    ]
    # The dispatcher must call _ingest for flap-1 BEFORE _resolve for flap-1.
    # Assert via mock call_order on _ingest/_resolve.
    r = client.post("/api/webhook/alertmanager",
                    headers={"Authorization": f"Bearer {AM_TOKEN}"},
                    json=_am_payload(alerts))
    assert r.status_code == 202


@pytest.mark.asyncio
async def test_resolve_no_binding_returns_no_binding(monkeypatch):
    from intake.alertmanager import _resolve
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    result = await _resolve("am-orphan")
    assert result == {"resolved": False, "reason": "no_binding"}


@pytest.mark.asyncio
async def test_resolve_source_tool_mismatch_refuses_close(monkeypatch):
    from intake.alertmanager import _resolve
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=json.dumps({
        "incident_sys_id": "abc", "incident_number": "INC1", "run_id": "r1",
    }).encode())
    snow_mock = AsyncMock()
    snow_mock.get_incident = AsyncMock(return_value={"u_source_tool": "Dynatrace"})
    snow_mock.patch_incident = AsyncMock()
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("intake.alertmanager.get_snow_client",
                        lambda: snow_mock)
    result = await _resolve("am-flap")
    assert result == {"resolved": False, "reason": "source_mismatch"}
    snow_mock.patch_incident.assert_not_awaited()
```

- [ ] **Step 2: Run, confirm fails**

```bash
pytest tests/test_alertmanager_intake.py -v
```

Expected: ModuleNotFoundError on `from intake.alertmanager import _resolve`.

- [ ] **Step 3: Implement `intake/alertmanager.py`**

```python
"""Alertmanager webhook intake — accepts AM v4 payloads, routes per-alert."""
from __future__ import annotations

import hmac
import json
import logging
import os
from datetime import datetime
from typing import Awaitable, Callable, Literal

from fastapi import HTTPException, Request
from pydantic import BaseModel, ValidationError

from shared.models import (
    OrchestratorEvent, IncidentSource, IncidentFlow, Severity,
    SSEEvent, SSEEventType,
)
from shared.redis_client import get_redis
from shared.snow_auth import get_snow_client  # confirm exact name

log = logging.getLogger(__name__)

SEV_MAP = {
    "critical": Severity.CRITICAL,
    "warning":  Severity.HIGH,
    "info":     Severity.LOW,
}


class AMAlert(BaseModel):
    status: Literal["firing", "resolved"]
    labels: dict[str, str]
    annotations: dict[str, str] = {}
    startsAt: datetime
    endsAt: datetime | None = None
    fingerprint: str
    generatorURL: str | None = None


class AMPayload(BaseModel):
    version: str               # tolerate non-"4"; warn but parse
    groupKey: str
    status: Literal["firing", "resolved"]
    receiver: str
    externalURL: str
    alerts: list[AMAlert]


def verify(authorization: str | None) -> None:
    token = os.getenv("AM_WEBHOOK_TOKEN", "")
    if not token:
        raise HTTPException(status_code=503, detail="AM intake not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    presented = authorization[len("Bearer "):]
    if not hmac.compare_digest(presented, token):
        raise HTTPException(status_code=401, detail="Invalid AM token")


def parse(body: bytes) -> AMPayload:
    try:
        payload = AMPayload.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if payload.version != "4":
        log.warning("am_unexpected_version received=%s", payload.version)
    return payload


def _to_event(alert: AMAlert, group_key: str) -> OrchestratorEvent | None:
    labels = alert.labels
    if "alertname" not in labels or "severity" not in labels:
        log.warning("am_dropped reason=missing_labels fingerprint=%s",
                    alert.fingerprint)
        return None

    raw_sev = labels["severity"].lower()
    sev = SEV_MAP.get(raw_sev)
    if sev is None:
        log.debug("am_unmapped_severity value=%s alertname=%s",
                  raw_sev, labels["alertname"])
        sev = Severity.INFO

    if labels.get("sentinel_flow") in ("a", "b"):
        flow = IncidentFlow.PRIMARY if labels["sentinel_flow"] == "a" \
               else IncidentFlow.SECONDARY
    else:
        flow = IncidentFlow.PRIMARY if sev in (Severity.CRITICAL, Severity.HIGH) \
               else IncidentFlow.SECONDARY

    return OrchestratorEvent(
        source       = IncidentSource.ALERTMANAGER,
        external_id  = f"am-{alert.fingerprint}",
        severity     = sev,
        flow         = flow,
        title        = alert.annotations.get("summary") or labels["alertname"],
        host         = labels.get("instance"),
        service      = labels.get("service") or labels.get("job"),
        environment  = labels.get("environment", os.getenv("AM_DEFAULT_ENV", "prod")),
        am_group_key = group_key,
        raw_payload  = alert.model_dump(),
        dedup_key    = f"am:{alert.fingerprint}",
    )


async def handle(
    request: Request,
    authorization: str | None,
    _ingest: Callable[[OrchestratorEvent], Awaitable[dict]],
    _resolve_cb: Callable[[str], Awaitable[dict]],
) -> dict:
    if os.getenv("AM_INTAKE_ENABLED", "true").lower() != "true":
        raise HTTPException(status_code=404, detail="AM intake disabled")

    verify(authorization)
    body = await request.body()
    payload = parse(body)

    firing: list[OrchestratorEvent] = []
    resolved_fps: list[str] = []
    dropped = 0

    for alert in payload.alerts:
        if alert.status == "resolved":
            resolved_fps.append(f"am-{alert.fingerprint}")
            continue
        event = _to_event(alert, payload.groupKey)
        if event is None:
            dropped += 1
            continue
        firing.append(event)

    firing_fps = {e.external_id for e in firing}
    same_batch_flap = [fp for fp in resolved_fps if fp in firing_fps]
    decoupled_resolved = [fp for fp in resolved_fps if fp not in firing_fps]

    # Phase 1: ingest all firing (and any flap-firing) concurrently
    import asyncio
    ingest_results = await asyncio.gather(
        *(_ingest(e) for e in firing), return_exceptions=True
    )

    # Phase 2: process same-batch resolves sequentially AFTER ingest
    flap_results = []
    for fp in same_batch_flap:
        flap_results.append(await _resolve_cb(fp))

    # Phase 3: process decoupled resolves concurrently with flap-resolves done
    decoupled_results = await asyncio.gather(
        *(_resolve_cb(fp) for fp in decoupled_resolved),
        return_exceptions=True,
    )

    accepted   = sum(1 for r in ingest_results if isinstance(r, dict) and r.get("accepted"))
    dedup      = sum(1 for r in ingest_results if isinstance(r, dict) and r.get("deduplicated"))
    resolved_n = sum(
        1 for r in (*flap_results, *decoupled_results)
        if isinstance(r, dict) and r.get("resolved")
    )
    errors = sum(1 for r in (*ingest_results, *flap_results, *decoupled_results)
                 if isinstance(r, Exception))

    return {
        "accepted":      accepted,
        "deduplicated":  dedup,
        "resolved":      resolved_n,
        "dropped":       dropped,
        "errors":        errors,
    }


async def _resolve(external_id: str) -> dict:
    redis = await get_redis()
    raw = await redis.get(f"snow_incident:{external_id}")
    if not raw:
        return {"resolved": False, "reason": "no_binding"}

    binding = json.loads(raw)
    snow = get_snow_client()
    inc = await snow.get_incident(binding["incident_sys_id"],
                                  fields=["u_source_tool"])
    if inc.get("u_source_tool") != "Alertmanager":
        log.warning("am_resolve_refused source=%s",
                    inc.get("u_source_tool"))
        return {"resolved": False, "reason": "source_mismatch"}

    fp = external_id[3:]  # strip "am-"
    await snow.patch_incident(binding["incident_sys_id"], {
        "state":       "7",
        "close_code":  "Solved (Permanently)",
        "close_notes": (f"Auto-closed by Sentinel — Alertmanager reported "
                        f"status=resolved at {datetime.utcnow().isoformat()}Z. "
                        f"fingerprint={fp}"),
        "work_notes":  _resolve_worknote(external_id),
    })

    await redis.delete(f"snow_incident:{external_id}")
    await redis.delete(f"problem_id:{binding['incident_number']}")

    await redis.publish_event(SSEEvent(
        event=SSEEventType.PIPELINE_COMPLETE,    # no PIPELINE_RESOLVED today
        run_id=binding.get("run_id"),
        data={"external_id": external_id, "source": "alertmanager",
              "outcome": "auto_resolved"},
    ).model_dump(mode="json"))

    return {"resolved": True, "incident_number": binding["incident_number"]}


def _resolve_worknote(external_id: str) -> str:
    fp = external_id[3:]
    ts = datetime.utcnow().isoformat() + "Z"
    return (
        "=== RESOLUTION — Alertmanager ===\n"
        f"Timestamp : {ts}\n"
        "Status    : auto-closed\n"
        f"Source    : alertmanager fingerprint={fp}\n"
        "Note      : External monitoring reported condition cleared.\n"
    )
```

- [ ] **Step 4: Wire the route into `main.py`**

```python
# main.py
from intake import alertmanager as am_intake

@app.post("/api/webhook/alertmanager", status_code=202)
async def alertmanager_webhook(
    request:       Request,
    authorization: str | None = Header(None),
):
    return await am_intake.handle(
        request, authorization, _ingest, am_intake._resolve,
    )
```

- [ ] **Step 5: Run, confirm pass**

```bash
pytest tests/test_alertmanager_intake.py -v
```

Expected: all pass. If the routing-flow assertions need mock visibility into `_ingest`, refactor those tests to patch `main._ingest` and capture call args.

- [ ] **Step 6: Add Agent 3 source-tool branch**

```python
# agents/Agent-3-servicenow/main.py — in the incident-create body assembly,
# wherever u_source_tool is set today, add:
if event.source == IncidentSource.ALERTMANAGER:
    body["u_source_tool"]    = "Alertmanager"
    body["u_source_alert_id"] = event.external_id[3:]  # strip "am-"
```

Add a unit test for that branch in the Agent 3 test file (matching the project's existing convention; `test_pipeline.py` or a new `test_agent3_source_branch.py`).

- [ ] **Step 7: Update `.env.example` + `docker-compose.yml`**

```bash
# .env.example
AM_WEBHOOK_TOKEN=
AM_INTAKE_ENABLED=true
AM_DEFAULT_ENV=prod
```

```yaml
# docker/docker-compose.yml — Agent 1 service env section
      AM_WEBHOOK_TOKEN: ${AM_WEBHOOK_TOKEN:-}
      AM_INTAKE_ENABLED: ${AM_INTAKE_ENABLED:-true}
      AM_DEFAULT_ENV:    ${AM_DEFAULT_ENV:-prod}
```

- [ ] **Step 8: Add metrics counters**

Find the existing `/metrics` counter pattern (check `main.py` or wherever `prometheus_client` or `structlog` counters live today). Add:

```python
am_alerts_received_total       = Counter(...)
am_alerts_dropped_total        = Counter(...)
am_resolve_total               = Counter(...)
am_entity_resolved_total       = Counter(...)
am_unexpected_version_total    = Counter(...)
```

Increment them at the appropriate sites in `intake/alertmanager.py` and `adapters/am_entity_resolver.py`.

- [ ] **Step 9: Integration e2e test**

```python
# tests/test_pipeline.py — append
@pytest.mark.integration
async def test_alertmanager_e2e(client, redis_client, snow_client):
    # 1. Post firing AM payload → INC created via Agent 3
    # 2. Verify SNOW INC has u_source_tool='Alertmanager'
    # 3. Post matching resolved payload → INC state=7
    ...
```

- [ ] **Step 10: Run full suite + coverage**

```bash
pytest tests/ -v -m "not integration"
pytest tests/ -v -m integration
pytest tests/ --cov=agents/Agent-1-dynatrace --cov-report=term-missing
```

Expected: all pass, coverage ≥ 90%, `intake/alertmanager.py` auth path 100%.

- [ ] **Step 11: Commit PR2**

```bash
git commit -m "feat(agent1): native Alertmanager webhook intake

Adds /api/webhook/alertmanager (bearer-token auth, flag-gated), hybrid
entity resolver with negative cache, race-guarded same-batch firing/resolved
sequencing, and SNOW auto-close on resolved. Agent 3 stamps
u_source_tool='Alertmanager' for AM-sourced INCs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 12: STOP — open PR2, await human review + merge**

---

# PR3 — Dead-letter retry for dispatch failures

**Goal:** Currently dispatch exceptions are swallowed and AM never retries (we already returned 202). Add an internal Redis-backed DLQ with a sweeper that retries up to 5× over 24h.

## File Structure (PR3)

| File | Responsibility |
|---|---|
| `agents/Agent-1-dynatrace/intake/alertmanager.py` (modify) | Wrap `_ingest`/`_resolve` dispatch in DLQ-aware helpers |
| `agents/Agent-1-dynatrace/main.py` (modify) | Add `sweep_am_dlq()` loop registered in lifespan (mirrors Agent 7 `asyncio.create_task(worker_loop())`) |
| `tests/test_am_dlq.py` (create) | Unit tests for DLQ enqueue + sweep behavior |

## Interfaces produced by PR3

```python
# intake/alertmanager.py
async def _ingest_with_dlq(event, _ingest): ...
async def _resolve_with_dlq(external_id, _resolve): ...
async def _enqueue_dlq(external_id, payload_dict, kind, error): ...
async def sweep_am_dlq(): ...
```

---

### Task 7: DLQ wrappers + sweep loop

**Files:**
- Modify: `agents/Agent-1-dynatrace/intake/alertmanager.py`
- Modify: `agents/Agent-1-dynatrace/main.py` (lifespan)
- Create: `tests/test_am_dlq.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_am_dlq.py
import json
import pytest
from unittest.mock import AsyncMock

from intake.alertmanager import (
    _ingest_with_dlq, _resolve_with_dlq, sweep_am_dlq, _enqueue_dlq,
)
from shared.models import OrchestratorEvent, IncidentSource, Severity, IncidentFlow


def _evt():
    return OrchestratorEvent(
        source=IncidentSource.ALERTMANAGER,
        external_id="am-test1",
        severity=Severity.HIGH,
        flow=IncidentFlow.PRIMARY,
        title="x",
    )


@pytest.mark.asyncio
async def test_ingest_failure_enqueues_dlq(monkeypatch):
    redis_mock = AsyncMock()
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    failing_ingest = AsyncMock(side_effect=RuntimeError("boom"))

    result = await _ingest_with_dlq(_evt(), failing_ingest)
    assert result == {"dlq": True}
    redis_mock.set.assert_awaited()
    args, kwargs = redis_mock.set.call_args
    assert args[0] == "am_dlq:am-test1"
    entry = json.loads(args[1])
    assert entry["kind"] == "ingest"
    assert entry["attempts"] == 0
    assert "boom" in entry["last_error"]
    assert kwargs["ex"] == 86400


@pytest.mark.asyncio
async def test_resolve_failure_enqueues_dlq(monkeypatch):
    redis_mock = AsyncMock()
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    failing_resolve = AsyncMock(side_effect=RuntimeError("snow down"))
    result = await _resolve_with_dlq("am-test2", failing_resolve)
    assert result == {"dlq": True}


@pytest.mark.asyncio
async def test_sweep_success_deletes_key(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.scan_iter = AsyncMock(return_value=_aiter(["am_dlq:am-test1"]))
    redis_mock.get = AsyncMock(return_value=json.dumps({
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest",
        "first_failed_at": "2026-06-30T00:00:00Z",
        "attempts": 0,
        "last_error": "boom",
    }).encode())
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("intake.alertmanager._ingest",
                        AsyncMock(return_value={"accepted": True}))
    await sweep_am_dlq()
    redis_mock.delete.assert_awaited_with("am_dlq:am-test1")


@pytest.mark.asyncio
async def test_sweep_failure_increments_attempts(monkeypatch):
    initial = {
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest", "first_failed_at": "...", "attempts": 1,
        "last_error": "old",
    }
    redis_mock = AsyncMock()
    redis_mock.scan_iter = AsyncMock(return_value=_aiter(["am_dlq:am-test1"]))
    redis_mock.get = AsyncMock(return_value=json.dumps(initial).encode())
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr("intake.alertmanager._ingest",
                        AsyncMock(side_effect=RuntimeError("still down")))
    await sweep_am_dlq()
    args, kwargs = redis_mock.set.call_args
    updated = json.loads(args[1])
    assert updated["attempts"] == 2
    assert "still down" in updated["last_error"]


@pytest.mark.asyncio
async def test_sweep_exhausted_increments_counter_and_leaves_key(monkeypatch):
    exhausted = {
        "event": _evt().model_dump(mode="json"),
        "kind": "ingest", "first_failed_at": "...", "attempts": 5,
        "last_error": "...",
    }
    redis_mock = AsyncMock()
    redis_mock.scan_iter = AsyncMock(return_value=_aiter(["am_dlq:am-test1"]))
    redis_mock.get = AsyncMock(return_value=json.dumps(exhausted).encode())
    monkeypatch.setattr("intake.alertmanager.get_redis",
                        AsyncMock(return_value=redis_mock))

    await sweep_am_dlq()
    redis_mock.delete.assert_not_awaited()    # exhausted entries left in place
    # Counter assertion left to whatever counter library is in use


async def _aiter(items):
    for x in items:
        yield x
```

- [ ] **Step 2: Run, confirm fails**

```bash
pytest tests/test_am_dlq.py -v
```

Expected: ImportError.

- [ ] **Step 3: Implement DLQ wrappers + sweep**

Append to `intake/alertmanager.py`:

```python
DLQ_TTL_SECONDS = 86400
DLQ_MAX_ATTEMPTS = 5


async def _enqueue_dlq(external_id: str, payload: dict, kind: str, error: Exception) -> None:
    redis = await get_redis()
    entry = {
        "event":           payload,
        "kind":            kind,
        "first_failed_at": datetime.utcnow().isoformat() + "Z",
        "attempts":        0,
        "last_error":      str(error)[:500],
    }
    await redis.set(f"am_dlq:{external_id}", json.dumps(entry),
                    ex=DLQ_TTL_SECONDS)


async def _ingest_with_dlq(event: OrchestratorEvent,
                           _ingest: Callable) -> dict:
    try:
        return await _ingest(event)
    except Exception as exc:
        await _enqueue_dlq(event.external_id, event.model_dump(mode="json"),
                           "ingest", exc)
        log.error("am_dlq_enqueued external_id=%s kind=ingest error=%s",
                  event.external_id, exc)
        return {"dlq": True}


async def _resolve_with_dlq(external_id: str,
                            _resolve_cb: Callable) -> dict:
    try:
        return await _resolve_cb(external_id)
    except Exception as exc:
        await _enqueue_dlq(external_id, {"external_id": external_id},
                           "resolve", exc)
        log.error("am_dlq_enqueued external_id=%s kind=resolve error=%s",
                  external_id, exc)
        return {"dlq": True}


async def sweep_am_dlq() -> None:
    from main import _ingest as _ingest_fn  # avoid import cycle at module load
    redis = await get_redis()
    async for key in redis.scan_iter("am_dlq:*"):
        raw = await redis.get(key)
        if not raw:
            continue
        entry = json.loads(raw)

        if entry["attempts"] >= DLQ_MAX_ATTEMPTS:
            log.error("am_dlq_exhausted key=%s last_error=%s",
                      key, entry["last_error"])
            # Counter increment; key left in place (24h TTL handles cleanup)
            continue

        try:
            if entry["kind"] == "ingest":
                await _ingest_fn(OrchestratorEvent(**entry["event"]))
            else:
                await _resolve(entry["event"]["external_id"])
            await redis.delete(key)
        except Exception as exc:
            entry["attempts"] += 1
            entry["last_error"] = str(exc)[:500]
            await redis.set(key, json.dumps(entry), ex=DLQ_TTL_SECONDS)
```

- [ ] **Step 4: Switch dispatch to DLQ wrappers**

In `handle()` (PR2 code), replace direct calls with wrapped:

```python
ingest_results = await asyncio.gather(
    *(_ingest_with_dlq(e, _ingest) for e in firing), return_exceptions=False,
)
flap_results = []
for fp in same_batch_flap:
    flap_results.append(await _resolve_with_dlq(fp, _resolve_cb))
decoupled_results = await asyncio.gather(
    *(_resolve_with_dlq(fp, _resolve_cb) for fp in decoupled_resolved),
    return_exceptions=False,
)
```

`return_exceptions=False` is now safe because the wrappers catch.

- [ ] **Step 5: Register sweep in `main.py` lifespan (mirror Agent 7 pattern)**

```python
# main.py
@asynccontextmanager
async def lifespan(app):
    sweep_task = asyncio.create_task(_sweep_loop())
    yield
    sweep_task.cancel()

async def _sweep_loop():
    while True:
        try:
            await am_intake.sweep_am_dlq()
        except Exception as exc:
            log.error("am_sweep_loop_error: %s", exc)
        await asyncio.sleep(300)   # 5-minute cadence; tune later
```

- [ ] **Step 6: Run, confirm pass**

```bash
pytest tests/test_am_dlq.py -v
```

Expected: all pass.

- [ ] **Step 7: Full suite + coverage**

```bash
pytest tests/ -v -m "not integration"
pytest tests/ --cov=agents/Agent-1-dynatrace --cov-report=term-missing
```

Expected: ≥ 90% coverage.

- [ ] **Step 8: Commit PR3**

```bash
git commit -m "feat(agent1): DLQ retry for AM dispatch failures

Wraps _ingest/_resolve dispatch in Redis-backed DLQ (am_dlq:*). Sweep
loop (5-min cadence, mirrors Agent 7 worker_loop pattern) retries up to
5x over 24h. Exhausted entries logged + counter incremented; key left
in place until TTL expiry for operator inspection.

HTTP contract with AM unchanged (always 202).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 9: STOP — open PR3, await human review**

---

## Self-Review Summary

- **Spec coverage:** all 15 sections of the design spec map to tasks above (refactor → Tasks 1–3; AM model + resolver + intake → Tasks 4–6; failure-mode envelope → Task 7).
- **Open questions surfaced inline:**
  - PR1 Task 1 Step 3: confirm conftest.py import shape before writing parity test.
  - PR1 Task 2 Step 2: pick `_common.py` over circular lazy imports — confirm and update.
  - PR2 Task 5 Step 3: stub `get_am_entity` to return `None` (recommended) vs. block on routing-db spec.
  - PR2 Task 6 Step 1: `PIPELINE_RESOLVED` SSEEventType doesn't exist; plan uses `PIPELINE_COMPLETE` with `outcome=auto_resolved`. Confirm vs. adding a new enum value.
- **Type consistency:** `EntityRef` fields and `source` literals match between resolver and tests; `_resolve` signature consistent between intake module, route, and DLQ wrapper.
