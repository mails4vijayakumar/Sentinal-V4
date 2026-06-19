# Integration points — exact files and edits

When the scope is "files + infra + event-contract integration", these are the
five touch-points outside the new agent's own directory. Each is a small,
mechanical edit — keep them surgical.

## 1. `shared/models.py` — append enrichment class

Read `assets/event_model.py.tmpl`. The class belongs in the
`# ── Enrichment Payloads (agent outputs) ──` section, **after** the existing
Agent {{N_PREV}} class (e.g. inserted between `SplunkEnrichment` and
`ServiceNowEnrichment` for a new agent at N=2.5 — choose the appropriate
neighbour for your N).

Verify imports `BaseModel`, `ConfigDict`, and `Optional` are already at the top
of the file. They will be — they're used by every existing enrichment class.

## 2. Upstream agent main.py — forward to new agent

Open `agents/Agent-{{N_PREV}}-<upstream_name>/main.py`. Find the call to the
**previous** downstream endpoint — search for `AGENT_{{N}}_ENDPOINT` (today this
will be the path to Agent {{N_NEXT}}, which the scaffolded agent now sits in
front of).

Two cases:

### Case A — new agent inserted into the chain
Re-point the existing forward from Agent {{N_PREV}} → Agent {{N_NEXT}} so it
goes Agent {{N_PREV}} → Agent {{N}}. Then the scaffolded `main.py` already
contains the next-hop forward to Agent {{N_NEXT}}.

Concrete edit on the upstream agent:

```python
# before
NEXT_AGENT_URL = os.environ.get("AGENT_{{N_NEXT}}_ENDPOINT", "http://agent-{{N_NEXT}}:{{PORT_NEXT}}/...")

# after
NEXT_AGENT_URL = os.environ.get("AGENT_{{N}}_ENDPOINT",    "http://agent-{{N}}:{{PORT}}/intake/{{INTAKE_PATH}}")
```

Also update the outbound header name on the upstream agent — it now signs with
`X-Agent{{N_PREV}}-Token` (which was already the case) but the **secret**
variable name changes from `AGENT_{{N_NEXT}}_SECRET` to `AGENT_{{N}}_SECRET`.

### Case B — new agent as a Flow B fan-out target
Edit Agent 1's Flow B dispatcher (in `agents/Agent-1-dynatrace/main.py`) to add
the new endpoint to its fan-out list. Today this is hard-coded; tomorrow it
will come from `FLOW_B_ENRICHMENT_TARGETS`. Pick whichever wiring is current.

## 3. `docker/docker-compose.yml` — append service block

Read `assets/compose_service.yml.tmpl`. Append the rendered block **at the end
of the `# ── Agents ──` section**, after the last existing `agent-<N>:` entry
and before the `# ── Webapp ──` block (or wherever the agents section ends).

The block uses the `<<: *common-env` anchor defined at the top of the file —
that anchor is already in scope, no changes needed there.

## 4. `.env.example` — append secret placeholders

Append two lines under the existing agent secrets section (search for
`AGENT_` prefixes — if none exist yet, append at end of file):

```
# Agent {{N}} ({{NAME}}) — inter-agent secrets
AGENT_{{N}}_SHARED_SECRET=CHANGE_ME
AGENT_{{N_NEXT}}_SECRET=CHANGE_ME
```

If a corresponding `AGENT_{{N}}_SECRET=` line exists upstream (because the
upstream agent now signs to this new agent), make sure it's present too —
upstream's outbound secret = this agent's inbound secret. They are the same
value under two different env-var names.

## 5. `tests/test_agent_{{N}}_{{NAME}}.py` — stub

Drop a single health-check test so CI has something to run:

```python
import pytest
import httpx

pytestmark = pytest.mark.integration

@pytest.mark.asyncio
async def test_agent_{{N}}_health() -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get("http://localhost:{{PORT}}/health")
    assert resp.status_code == 200
    assert resp.json()["agent"] == {{N}}
```

This requires `docker compose ... up -d` to be running — that's why it carries
the `integration` marker. Add real unit tests once the intake handler does
real work.

## 6. After everything is staged

Print this checklist to the user:

- [ ] `cat agents/Agent-{{N}}-{{NAME}}/main.py` — verify the TODOs
- [ ] `cat shared/models.py | tail -40` — verify the new enrichment class
- [ ] `git diff docker/docker-compose.yml` — verify the appended service block
- [ ] `git diff .env.example` — verify secrets were added
- [ ] `git diff agents/Agent-{{N_PREV}}-*/main.py` — verify upstream forward
- [ ] `docker compose -f docker/docker-compose.yml --profile ollama up -d --build agent-{{N}}` — first boot
- [ ] `curl localhost:{{PORT}}/health` — confirm 200
- [ ] Replace stubs with real logic
