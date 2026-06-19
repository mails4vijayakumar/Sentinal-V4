"""routing-db/app/api/admin.py — write (admin) endpoints"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from routing_db.app.core.security import require_admin
from routing_db.app.db.connection import db_session
from routing_db.app.db import queries

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_admin)])


async def get_session():
    async with db_session() as s:
        yield s


# ── Request bodies ────────────────────────────────────────────────────────────

class IncidentUpsert(BaseModel):
    external_id: str
    source:      str
    severity:    str
    flow:        str
    title:       str
    description: Optional[str] = None
    host:        Optional[str] = None
    service:     Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None


class RunCreate(BaseModel):
    incident_id: UUID
    flow:        str
    meta:        Optional[Dict[str, Any]] = None


class RunPatch(BaseModel):
    status:       Optional[str]      = None
    duration_ms:  Optional[int]      = None
    completed_at: Optional[datetime] = None
    meta:         Optional[Dict[str, Any]] = None


class StepUpsert(BaseModel):
    agent_num:   int
    agent_name:  str
    status:      str = "pending"
    started_at:  Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int]      = None
    summary:     Optional[str]      = None
    error:       Optional[str]      = None
    retry_count: int                = 0


class EnrichmentWrite(BaseModel):
    agent_num: int
    source:    str
    data:      Dict[str, Any] = Field(default_factory=dict)


class FeedbackWrite(BaseModel):
    run_id:           UUID
    incident_id:      UUID
    root_cause:       str
    root_cause_cat:   Optional[str] = None
    resolution_steps: List[Dict[str, Any]] = Field(default_factory=list)
    confidence:       int = Field(ge=0, le=100)
    llm_provider:     Optional[str] = None
    llm_model:        Optional[str] = None
    tokens_used:      Optional[int] = None


class RatingWrite(BaseModel):
    resolution_id: UUID
    rated_by:      Optional[str] = None
    rating:        int = Field(ge=1, le=5)
    comment:       Optional[str] = None


# ── Incidents ─────────────────────────────────────────────────────────────────

@router.post("/incidents", status_code=201)
async def upsert_incident(
    body:    IncidentUpsert,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    inc = await queries.upsert_incident(session, body.model_dump())
    return {"id": str(inc.id), "external_id": inc.external_id}


# ── Pipeline runs ─────────────────────────────────────────────────────────────

@router.post("/runs", status_code=201)
async def create_run(
    body:    RunCreate,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    run = await queries.create_run(session, body.model_dump())
    return {"run_id": str(run.id)}


@router.patch("/runs/{run_id}")
async def patch_run(
    run_id:  UUID,
    body:    RunPatch,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    if not patch:
        return {"ok": True}
    await queries.update_run_status(
        session,
        run_id,
        status=patch.get("status", "running"),
        duration_ms=patch.get("duration_ms"),
        completed_at=patch.get("completed_at"),
    )
    return {"ok": True}


# ── Steps ─────────────────────────────────────────────────────────────────────

@router.post("/runs/{run_id}/steps", status_code=201)
async def upsert_step(
    run_id:  UUID,
    body:    StepUpsert,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    step = await queries.upsert_step(session, run_id, body.model_dump())
    return {"id": str(step.id), "agent_num": step.agent_num}


@router.patch("/runs/{run_id}/steps/{agent_num}")
async def patch_step(
    run_id:    UUID,
    agent_num: int,
    body:      StepUpsert,
    session:   AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    data = body.model_dump()
    data["agent_num"] = agent_num
    await queries.upsert_step(session, run_id, data)
    return {"ok": True}


# ── Enrichments ───────────────────────────────────────────────────────────────

@router.post("/runs/{run_id}/enrichments", status_code=201)
async def write_enrichment(
    run_id:  UUID,
    body:    EnrichmentWrite,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    enr = await queries.write_enrichment(session, run_id, body.model_dump())
    return {"id": str(enr.id)}


# ── Feedback ──────────────────────────────────────────────────────────────────

@router.post("/feedback", status_code=201)
async def write_resolution(
    body:    FeedbackWrite,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    from routing_db.app.models import Resolution
    res = Resolution(**body.model_dump())
    session.add(res)
    await session.flush()
    return {"id": str(res.id)}


@router.post("/ratings", status_code=201)
async def write_rating(
    body:    RatingWrite,
    session: AsyncSession = Depends(get_session),
) -> Dict[str, Any]:
    from routing_db.app.models import Rating
    rating = Rating(**body.model_dump())
    session.add(rating)
    await session.flush()
    return {"id": str(rating.id)}
