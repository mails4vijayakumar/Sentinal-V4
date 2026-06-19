from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field, ConfigDict


class ResolutionStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    step: int = Field(ge=1)
    action: str = Field(min_length=1)
    command: Optional[str] = None


class SynthesizedArticle(BaseModel):
    """Output schema for the LLM synthesis call. Validated post-parse."""
    model_config = ConfigDict(extra="forbid")
    title: str = Field(min_length=5, max_length=200)
    problem_summary: str = Field(min_length=20)
    root_cause: Optional[str] = None
    resolution_steps: list[ResolutionStep] = Field(min_length=1)
    keywords: list[str] = Field(default_factory=list, max_length=20)
    assignment_group: str = Field(min_length=1)
    category: Optional[str] = None
    subcategory: Optional[str] = None
    confidence_self_rating: float = Field(ge=0.0, le=1.0)


class ClusterMember(BaseModel):
    """A single incident inside a cluster."""
    model_config = ConfigDict(extra="forbid")
    incident_id: str
    short_description: str
    description: str
    resolution_notes: str
    close_code: Optional[str] = None
    assignment_group: str
    category: Optional[str] = None
    subcategory: Optional[str] = None
    closed_at: datetime
    quality_score: float = Field(ge=0.0, le=1.0)


class ClusterResult(BaseModel):
    """A surviving cluster after quality gates."""
    model_config = ConfigDict(extra="forbid")
    signature: str
    assignment_group: str
    members: list[ClusterMember]
    cohesion: float = Field(ge=0.0, le=1.0)
    medoid_index: int


class RunCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")
    extracted: int = 0
    filtered: int = 0
    clustered: int = 0
    created: int = 0
    updated: int = 0
    flagged_for_review: int = 0
    retired: int = 0
    skipped: int = 0


class DecisionType:
    CREATE = "create"
    UPDATE = "update"
    REVIEW = "review"
    SKIP = "skip"


class SynthesisDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cluster_signature: str
    decision: Literal["create", "update", "review", "skip"]
    article_id: Optional[UUID] = None
    similarity_score: Optional[float] = None
    notes: Optional[str] = None
