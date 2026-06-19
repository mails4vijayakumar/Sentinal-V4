"""routing-db/app/models.py — SQLAlchemy ORM (routing schema)"""
from __future__ import annotations

import enum
from datetime import datetime
from uuid import uuid4

from sqlalchemy import (
    Boolean, CheckConstraint, Column, DateTime, ForeignKey,
    Integer, SmallInteger, String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# ── routing.incidents ────────────────────────────────────────────────────────

class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (
        UniqueConstraint("external_id", name="uq_incidents_external_id"),
        {"schema": "routing"},
    )

    id          = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    external_id = Column(Text, nullable=False)
    source      = Column(String(20), nullable=False)
    severity    = Column(String(2),  nullable=False)
    flow        = Column(String(12), nullable=False)
    title       = Column(Text, nullable=False)
    description = Column(Text)
    host        = Column(Text)
    service     = Column(Text)
    raw_payload = Column(JSONB)
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at  = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    runs        = relationship("PipelineRun", back_populates="incident", lazy="selectin")


# ── routing.pipeline_runs ────────────────────────────────────────────────────

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "routing"}

    id           = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    incident_id  = Column(PG_UUID(as_uuid=True), ForeignKey("routing.incidents.id"), nullable=False)
    status       = Column(String(12), nullable=False, default="running",
                          info={"choices": ("running","completed","failed","cancelled")})
    flow         = Column(String(12), nullable=False)
    started_at   = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    duration_ms  = Column(Integer)
    meta         = Column(JSONB, default=dict)

    incident     = relationship("Incident", back_populates="runs")
    steps        = relationship("PipelineStep", back_populates="run", lazy="selectin")
    enrichments  = relationship("Enrichment",   back_populates="run", lazy="selectin")


# ── routing.pipeline_steps ───────────────────────────────────────────────────

class PipelineStep(Base):
    __tablename__ = "pipeline_steps"
    __table_args__ = (
        UniqueConstraint("run_id", "agent_num", name="uq_steps_run_agent"),
        {"schema": "routing"},
    )

    id           = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    run_id       = Column(PG_UUID(as_uuid=True), ForeignKey("routing.pipeline_runs.id"), nullable=False)
    agent_num    = Column(SmallInteger, nullable=False)
    agent_name   = Column(Text, nullable=False)
    status       = Column(String(12), nullable=False, default="pending")
    started_at   = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_ms  = Column(Integer)
    summary      = Column(Text)
    error        = Column(Text)
    retry_count  = Column(SmallInteger, default=0)

    run          = relationship("PipelineRun", back_populates="steps")


# ── routing.enrichments ──────────────────────────────────────────────────────

class Enrichment(Base):
    __tablename__ = "enrichments"
    __table_args__ = {"schema": "routing"}

    id         = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    run_id     = Column(PG_UUID(as_uuid=True), ForeignKey("routing.pipeline_runs.id"), nullable=False)
    agent_num  = Column(SmallInteger, nullable=False)
    source     = Column(Text, nullable=False)
    data       = Column(JSONB, nullable=False, default=dict)
    written_at = Column(DateTime(timezone=True), server_default=func.now())

    run        = relationship("PipelineRun", back_populates="enrichments")


# ── routing.snow_config ──────────────────────────────────────────────────────

class SnowConfig(Base):
    __tablename__ = "snow_config"
    __table_args__ = {"schema": "routing"}

    config_key   = Column(Text, primary_key=True)
    config_value = Column(Text, nullable=False)
    description  = Column(Text)
    updated_at   = Column(DateTime(timezone=True), server_default=func.now())


# ── feedback.resolutions ──────────────────────────────────────────────────────

class Resolution(Base):
    __tablename__ = "resolutions"
    __table_args__ = {"schema": "feedback"}

    id               = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    run_id           = Column(PG_UUID(as_uuid=True), nullable=False)
    incident_id      = Column(PG_UUID(as_uuid=True), nullable=False)
    root_cause       = Column(Text)
    root_cause_cat   = Column(Text)
    resolution_steps = Column(JSONB, default=list)
    confidence       = Column(Integer)   # 0-100
    llm_provider     = Column(Text)
    llm_model        = Column(Text)
    tokens_used      = Column(Integer)
    generated_at     = Column(DateTime(timezone=True), server_default=func.now())

    ratings          = relationship("Rating", back_populates="resolution", lazy="selectin")


class Rating(Base):
    __tablename__ = "ratings"
    __table_args__ = {"schema": "feedback"}

    id            = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    resolution_id = Column(PG_UUID(as_uuid=True), ForeignKey("feedback.resolutions.id"), nullable=False)
    rated_by      = Column(Text)
    rating        = Column(SmallInteger)
    comment       = Column(Text)
    rated_at      = Column(DateTime(timezone=True), server_default=func.now())

    resolution    = relationship("Resolution", back_populates="ratings")
