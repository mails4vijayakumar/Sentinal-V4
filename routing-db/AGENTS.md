# routing-db — AGENTS.md

Dedicated FastAPI service (:8000) that owns all PostgreSQL routing state.

## API

### Reads (GET /reads/*)
- `GET /reads/metrics` — dashboard KPIs
- `GET /reads/runs?status=running` — active pipeline runs
- `GET /reads/runs/{run_id}` — full run with steps
- `GET /reads/incidents/{external_id}` — incident by external ID

### Admin (POST/PATCH /admin/*, requires X-Admin-Token)
- `POST /admin/incidents` — upsert incident
- `POST /admin/runs` — create pipeline run
- `PATCH /admin/runs/{id}` — update run status
- `POST /admin/runs/{id}/steps` — upsert agent step
- `POST /admin/runs/{id}/enrichments` — write enrichment data
- `POST /admin/feedback` — store RCA resolution
- `POST /admin/ratings` — store human feedback

## Auth
Set `ROUTING_DB_ADMIN_TOKEN` env var. Pass as `X-Admin-Token` header on all `/admin/*` calls.
