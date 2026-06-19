"""routing-db/app/db/queries.py — common query helpers"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import desc, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from routing_db.app.models import Enrichment, Incident, PipelineRun, PipelineStep, Resolution


# ── Incidents ─────────────────────────────────────────────────────────────────

async def get_incident_by_external_id(
    session: AsyncSession,
    external_id: str,
) -> Optional[Incident]:
    result = await session.execute(
        select(Incident).where(Incident.external_id == external_id)
    )
    return result.scalar_one_or_none()


async def upsert_incident(
    session: AsyncSession,
    data: Dict[str, Any],
) -> Incident:
    existing = await get_incident_by_external_id(session, data["external_id"])
    if existing:
        for k, v in data.items():
            if hasattr(existing, k):
                setattr(existing, k, v)
        return existing
    inc = Incident(**{k: v for k, v in data.items() if hasattr(Incident, k)})
    session.add(inc)
    await session.flush()
    return inc


# ── Pipeline runs ─────────────────────────────────────────────────────────────

async def create_run(
    session: AsyncSession,
    data: Dict[str, Any],
) -> PipelineRun:
    run = PipelineRun(**{k: v for k, v in data.items() if hasattr(PipelineRun, k)})
    session.add(run)
    await session.flush()
    return run


async def get_run(
    session: AsyncSession,
    run_id: UUID,
) -> Optional[PipelineRun]:
    result = await session.execute(
        select(PipelineRun).where(PipelineRun.id == run_id)
    )
    return result.scalar_one_or_none()


async def get_active_runs(
    session: AsyncSession,
    limit: int = 50,
) -> List[PipelineRun]:
    result = await session.execute(
        select(PipelineRun)
        .where(PipelineRun.status == "running")
        .order_by(desc(PipelineRun.started_at))
        .limit(limit)
    )
    return list(result.scalars().all())


async def get_recent_runs(
    session: AsyncSession,
    limit: int = 50,
    hours: int = 24,
) -> List[PipelineRun]:
    since = datetime.utcnow() - timedelta(hours=hours)
    result = await session.execute(
        select(PipelineRun)
        .where(PipelineRun.started_at >= since)
        .order_by(desc(PipelineRun.started_at))
        .limit(limit)
    )
    return list(result.scalars().all())


async def update_run_status(
    session: AsyncSession,
    run_id: UUID,
    status: str,
    duration_ms: Optional[int] = None,
    completed_at: Optional[datetime] = None,
) -> None:
    patch: Dict[str, Any] = {"status": status}
    if duration_ms is not None:
        patch["duration_ms"] = duration_ms
    if completed_at is not None:
        patch["completed_at"] = completed_at
    await session.execute(
        update(PipelineRun).where(PipelineRun.id == run_id).values(**patch)
    )


# ── Steps ─────────────────────────────────────────────────────────────────────

async def upsert_step(
    session: AsyncSession,
    run_id: UUID,
    data: Dict[str, Any],
) -> PipelineStep:
    result = await session.execute(
        select(PipelineStep)
        .where(PipelineStep.run_id == run_id, PipelineStep.agent_num == data["agent_num"])
    )
    step = result.scalar_one_or_none()
    if step:
        for k, v in data.items():
            if hasattr(step, k):
                setattr(step, k, v)
    else:
        step = PipelineStep(run_id=run_id, **{k: v for k, v in data.items() if hasattr(PipelineStep, k)})
        session.add(step)
    await session.flush()
    return step


# ── Enrichments ───────────────────────────────────────────────────────────────

async def write_enrichment(
    session: AsyncSession,
    run_id: UUID,
    data: Dict[str, Any],
) -> Enrichment:
    enr = Enrichment(run_id=run_id, **{k: v for k, v in data.items() if hasattr(Enrichment, k)})
    session.add(enr)
    await session.flush()
    return enr


# ── Dashboard stats ───────────────────────────────────────────────────────────

async def get_dashboard_metrics(
    session: AsyncSession,
) -> Dict[str, Any]:
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    active_count = (await session.execute(
        select(func.count()).where(PipelineRun.status == "running")
    )).scalar() or 0

    completed_today = (await session.execute(
        select(func.count()).where(
            PipelineRun.status == "completed",
            PipelineRun.completed_at >= today,
        )
    )).scalar() or 0

    avg_ms = (await session.execute(
        select(func.avg(PipelineRun.duration_ms)).where(
            PipelineRun.status == "completed",
            PipelineRun.completed_at >= today,
        )
    )).scalar() or 0

    p1_open = (await session.execute(
        select(func.count())
        .select_from(PipelineRun)
        .join(Incident, PipelineRun.incident_id == Incident.id)
        .where(PipelineRun.status == "running", Incident.severity == "P1")
    )).scalar() or 0

    total = (await session.execute(select(func.count()).where(
        PipelineRun.started_at >= today
    ))).scalar() or 0

    success_rate = (
        round(100 * completed_today / total) if total > 0 else 100
    )

    return {
        "active":          active_count,
        "completed_today": completed_today,
        "avg_duration_ms": round(avg_ms),
        "p1_open":         p1_open,
        "success_rate":    success_rate,
    }
