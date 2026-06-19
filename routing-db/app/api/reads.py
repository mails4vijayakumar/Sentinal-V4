"""routing-db/app/api/reads.py — GET (read) endpoints"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from routing_db.app.core.security import require_read
from routing_db.app.db.connection import db_session
from routing_db.app.db import queries

router = APIRouter(prefix="/reads", tags=["reads"])


# ── Dependency shim ───────────────────────────────────────────────────────────
async def get_session():
    async with db_session() as s:
        yield s


# ── Incidents ─────────────────────────────────────────────────────────────────

@router.get("/incidents/{external_id}", dependencies=[Depends(require_read)])
async def get_incident(
    external_id: str,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    inc = await queries.get_incident_by_external_id(session, external_id)
    if not inc:
        raise HTTPException(status_code=404, detail="Incident not found")
    return {
        "id":          str(inc.id),
        "external_id": inc.external_id,
        "source":      inc.source,
        "severity":    inc.severity,
        "flow":        inc.flow,
        "title":       inc.title,
        "description": inc.description,
        "host":        inc.host,
        "service":     inc.service,
        "created_at":  inc.created_at.isoformat() if inc.created_at else None,
    }


# ── Pipeline runs ─────────────────────────────────────────────────────────────

@router.get("/runs", dependencies=[Depends(require_read)])
async def list_runs(
    status:  Optional[str] = Query(None, description="Filter by status"),
    limit:   int           = Query(50,   le=200),
    hours:   int           = Query(24,   description="Lookback window in hours"),
    session: AsyncSession  = Depends(get_session),
) -> List[Dict[str, Any]]:
    if status == "running":
        runs = await queries.get_active_runs(session, limit=limit)
    else:
        runs = await queries.get_recent_runs(session, limit=limit, hours=hours)

    return [_run_dict(r) for r in runs]


@router.get("/runs/{run_id}", dependencies=[Depends(require_read)])
async def get_run(
    run_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    run = await queries.get_run(session, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return _run_dict(run, include_steps=True)


# ── Dashboard metrics ─────────────────────────────────────────────────────────

@router.get("/metrics", dependencies=[Depends(require_read)])
async def get_metrics(
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    return await queries.get_dashboard_metrics(session)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_dict(run, include_steps: bool = False) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "run_id":       str(run.id),
        "incident_id":  str(run.incident_id),
        "status":       run.status,
        "flow":         run.flow,
        "started_at":   run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "duration_ms":  run.duration_ms,
    }
    if include_steps and run.steps:
        d["steps"] = [
            {
                "agent_num":    s.agent_num,
                "agent_name":   s.agent_name,
                "status":       s.status,
                "started_at":   s.started_at.isoformat() if s.started_at else None,
                "completed_at": s.completed_at.isoformat() if s.completed_at else None,
                "duration_ms":  s.duration_ms,
                "summary":      s.summary,
                "error":        s.error,
            }
            for s in sorted(run.steps, key=lambda s: s.agent_num)
        ]
    return d
