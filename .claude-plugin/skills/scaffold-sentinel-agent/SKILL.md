---
name: scaffold-sentinel-agent
description: This skill should be used when the user asks to "scaffold a new Sentinel agent", "add an agent to the pipeline", "create Agent N", or "wire a new agent between X and Y". Creates a fully wired Sentinel agent — directory with main.py + Dockerfile + AGENTS.md, the matching docker-compose service block, .env.example secrets, a new event-contract class in shared/models.py, an upstream-forward edit on the prior agent, and a stub integration test.
version: 0.1.0
---

# Scaffold a new Sentinel agent

This skill drops a new agent into the 7-agent Sentinel pipeline so it compiles,
boots, and round-trips a health check on first `docker compose up` — leaving
the *business logic* of the new agent as a clearly-marked TODO for the
developer.

## When to invoke

Trigger this skill when the user asks to add a new agent to the pipeline.
Typical asks: "scaffold Agent 8 for X", "add a new enrichment agent between
Agent 4 and Agent 5", "wire a Flow B summarizer agent".

Do **not** trigger this skill for: changes to an existing agent, dashboard
work, routing-db changes, or anything that doesn't create a new
`agents/Agent-N-<name>/` directory.

## Required inputs

Collect these via `AskUserQuestion` (one at a time). Each is needed before any
file is touched.

1. **Agent number N** — integer. Must be free (no existing `agents/Agent-N-*`
   directory). Convention: port is `8000 + N`.
2. **Name** — short snake/kebab identifier matching `[a-z][a-z0-9-]+`. Becomes
   the directory suffix and the Dockerfile path.
3. **Position in chain** — between which two existing agents does this one sit?
   Provide `N_PREV` and `N_NEXT`. For a terminal agent, `N_NEXT` is omitted
   (the scaffolded `main.py` has its forward block commented out). For a Flow B
   fan-out target, `N_PREV = 1` and there is no `N_NEXT`.
4. **One-line purpose** — used in docstrings and AGENTS.md.
5. **Intake path slug** — e.g. `enriched-event`, `kb-attached`. Becomes
   `/intake/<slug>`.

## Procedure

Follow these steps in order. Do not skip the preflight.

### Step 1 — Preflight

Run the validator. Bail on any failure:

```bash
python ${CLAUDE_PLUGIN_ROOT}/skills/scaffold-sentinel-agent/scripts/validate_scaffold.py \
    --n <N> --name <NAME> --repo-root <REPO_ROOT>
```

The script verifies N is free, name format is valid, the target directory does
not exist, and port `8000+N` is not bound in `docker-compose.yml`.

### Step 2 — Render templates

Read each `assets/*.tmpl` and substitute the placeholders below. Use the Write
tool for new files; use the Edit tool to append to existing files.

| Placeholder | Source | Example |
|-------------|--------|---------|
| `{{N}}` | input | `8` |
| `{{NAME}}` | input | `fidelis` |
| `{{NAME_TITLE}}` | name with first letter upcased per word | `Fidelis` |
| `{{NAME_UPPER}}` | name in upper-snake | `FIDELIS` |
| `{{N_PREV}}` | input | `7` |
| `{{N_NEXT}}` | input (omit forward block if none) | `9` |
| `{{PORT}}` | `8000 + N` | `8008` |
| `{{PORT_NEXT}}` | `8000 + N_NEXT` | `8009` |
| `{{PURPOSE}}` | input | `aggregate Flow B evidence into a single SNOW note` |
| `{{INTAKE_PATH}}` | input | `enriched-event` |
| `{{NEXT_INTAKE_PATH}}` | look up downstream agent's path or `intake` | `kb-attached` |
| `{{UPSTREAM_HEADER}}` | `X-Agent{N_PREV}-Token` | `X-Agent7-Token` |
| `{{UPSTREAM_HEADER_PYNAME}}` | `x_agent{N_PREV}_token` | `x_agent7_token` |

### Step 3 — Write the agent directory

Render and write **three** files:

- `agents/Agent-<N>-<NAME>/main.py` — from `assets/agent_main.py.tmpl`
- `agents/Agent-<N>-<NAME>/Dockerfile` — from `assets/agent_dockerfile.tmpl`
- `agents/Agent-<N>-<NAME>/AGENTS.md` — from `assets/agent_agents_md.tmpl`

### Step 4 — Wire infra

- **`docker/docker-compose.yml`** — append the rendered
  `assets/compose_service.yml.tmpl` block at the end of the `# ── Agents ──`
  section (just before the next section header, or at end-of-file if the
  agents section is last). Use the Edit tool with the last existing
  `agent-<N>:` block as the unique anchor.
- **`.env.example`** — append two lines under the existing agent secrets:
  ```
  # Agent <N> (<NAME>) — inter-agent secrets
  AGENT_<N>_SHARED_SECRET=CHANGE_ME
  AGENT_<N_NEXT>_SECRET=CHANGE_ME
  ```

### Step 5 — Wire the event contract

Read `references/integration-points.md` and follow the two-touch edit:

1. Append the rendered `assets/event_model.py.tmpl` class to
   `shared/models.py`, in the `# ── Enrichment Payloads ──` section, after the
   Agent `N_PREV` class.
2. Edit `agents/Agent-<N_PREV>-*/main.py` to forward to the new agent instead
   of to the old `N_NEXT`. Search for the existing `AGENT_<N_NEXT>_ENDPOINT`
   reference and re-point it to `AGENT_<N>_ENDPOINT`.

### Step 6 — Drop a stub test

Write `tests/test_agent_<N>_<NAME>.py` with the integration-marker health check
in `references/integration-points.md` §5.

### Step 7 — Report

Print the post-scaffold checklist from
`references/integration-points.md` §6. Do **not** auto-commit, do **not** run
`docker compose build`. Leave the files staged for the developer.

## Critical invariants (do not violate)

These come from the repo `CLAUDE.md` master spec. The full guidance lives in
`references/pipeline-contract.md`. The three most load-bearing rules:

1. **Inter-agent auth** — `hmac.compare_digest`, never `==`. Inbound and
   outbound secrets must use the §6.1 dual-name convention.
2. **Work-note format** — the §4.6 header block is mandatory. Don't invent a
   new format.
3. **PHI logging** — never log free-text fields (`description`,
   `short_description`, `matched_log_lines`). Log identifiers and counts only.
   Use an explicit `to_downstream()` to select fields for the next hop.

## Additional resources

### Reference files

- **`references/pipeline-contract.md`** — event chain, secret naming convention,
  work-note format, soft-fail policy, time budget, PHI discipline.
- **`references/integration-points.md`** — line-level edits for
  `shared/models.py`, the upstream agent's `main.py`, `docker-compose.yml`,
  `.env.example`, and the stub test.

### Templates (`assets/`)

- `agent_main.py.tmpl` — FastAPI app with health/metrics/intake, HMAC token
  check, lifespan, optional outbound forward
- `agent_dockerfile.tmpl` — slim Python image, non-root user, HEALTHCHECK
- `agent_agents_md.tmpl` — per-agent deep-dive doc skeleton
- `compose_service.yml.tmpl` — compose service block using existing anchors
- `event_model.py.tmpl` — `<NAME>Enrichment` Pydantic class

### Scripts (`scripts/`)

- `validate_scaffold.py` — preflight checks (N free, name format, port free,
  target dir absent). Run before any file write.
