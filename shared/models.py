"""
shared/models.py
================
Canonical Pydantic v2 model chain for the Sentinel orchestrator.

Event flow:
  OrchestratorEvent                 (raw inbound — DT or SNOW webhook)
  → RoutedIncident                  (after Agent 1 assigns flow + severity)
  → EnrichedIncident                (after Agents 2–6 add data)
  → RCAResult                       (Agent 7 final output)

All models use model_config = ConfigDict(extra='allow') so downstream
agents can tack on fields without breaking strict validation.
"""
from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


# ── Enumerations ──────────────────────────────────────────────────────────────

class IncidentSource(str, enum.Enum):
    DYNATRACE   = "dynatrace"
    SERVICENOW  = "servicenow"

class IncidentFlow(str, enum.Enum):
    PRIMARY   = "primary"    # DT P1/P2/P3 → full 7-agent chain
    SECONDARY = "secondary"  # SNOW P4/P5 → enrichment-only, no INC creation

class Severity(str, enum.Enum):
    CRITICAL = "P1"
    HIGH     = "P2"
    MEDIUM   = "P3"
    LOW      = "P4"
    INFO     = "P5"

class PipelineStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    CANCELLED = "cancelled"

class AgentStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    COMPLETED = "completed"
    FAILED    = "failed"
    SKIPPED   = "skipped"


# ── Raw Inbound Events ────────────────────────────────────────────────────────

class DynatracePayload(BaseModel):
    """Raw Dynatrace webhook payload (subset of the full DT schema)."""
    model_config = ConfigDict(extra="allow")

    problemId:         str
    displayName:       str
    severity:          str                    # "AVAILABILITY" | "PERFORMANCE" | "ERROR" | "INFO"
    status:            str                    # "OPEN" | "RESOLVED"
    eventType:         str
    tags:              List[str] = Field(default_factory=list)
    impactedEntities:  List[Dict[str, Any]] = Field(default_factory=list)
    deploymentEvent:   bool = False
    startTime:         Optional[datetime] = None
    endTime:           Optional[datetime] = None


class ServiceNowPayload(BaseModel):
    """Raw ServiceNow outbound webhook payload."""
    model_config = ConfigDict(extra="allow")

    number:            str                    # INC0001234
    priority:          str                    # "1" … "5"
    short_description: str
    caller_id:         Optional[str] = None
    cmdb_ci:           Optional[str] = None   # configuration item name
    state:             Optional[str] = None
    category:          Optional[str] = None
    subcategory:       Optional[str] = None
    assignment_group:  Optional[str] = None
    u_service_tier:    Optional[str] = None   # custom SNOW field
    u_hipaa_flag:      Optional[bool] = None  # custom SNOW field


class OrchestratorEvent(BaseModel):
    """
    Canonical inbound event after Agent 1 validates + normalises the webhook.
    This is stored to routing-db and placed on the Redis work-queue.
    """
    model_config = ConfigDict(extra="allow")

    event_id:    UUID             = Field(default_factory=uuid4)
    source:      IncidentSource
    external_id: str              # DT problemId or SNOW INC number
    severity:    Severity
    flow:        IncidentFlow
    title:       str
    description: Optional[str]    = None
    raw_payload: Dict[str, Any]   = Field(default_factory=dict)
    received_at: datetime         = Field(default_factory=datetime.utcnow)

    # Populated by Agent 1
    dedup_key:   Optional[str]    = None   # used for idempotency lock
    host:        Optional[str]    = None   # primary affected host
    service:     Optional[str]    = None   # primary affected service/app


# ── Pipeline State ────────────────────────────────────────────────────────────

class AgentStep(BaseModel):
    """One agent's execution record within a pipeline run."""
    model_config = ConfigDict(extra="allow")

    agent_num:    int
    agent_name:   str
    status:       AgentStatus  = AgentStatus.PENDING
    started_at:   Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms:  Optional[int]      = None
    summary:      Optional[str]      = None   # human-readable outcome
    error:        Optional[str]      = None
    retry_count:  int                = 0


class PipelineRun(BaseModel):
    """Full pipeline state, stored in Redis and synced to routing-db."""
    model_config = ConfigDict(extra="allow")

    run_id:       UUID              = Field(default_factory=uuid4)
    incident_id:  UUID              = Field(default_factory=uuid4)
    event:        OrchestratorEvent
    status:       PipelineStatus    = PipelineStatus.RUNNING
    flow:         IncidentFlow
    steps:        List[AgentStep]   = Field(default_factory=list)
    started_at:   datetime          = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_ms:  Optional[int]      = None


# ── Enrichment Payloads (agent outputs) ───────────────────────────────────────

class SplunkEnrichment(BaseModel):
    """Agent 2 output — Splunk log analysis."""
    model_config = ConfigDict(extra="allow")

    log_lines_scanned: int                   = 0
    error_count:       int                   = 0
    warn_count:        int                   = 0
    top_errors:        List[str]             = Field(default_factory=list)
    time_range:        Optional[str]         = None
    index:             Optional[str]         = None
    spl_query:         Optional[str]         = None
    llm_summary:       Optional[str]         = None   # LLM-generated narrative
    classification:    Optional[str]         = None   # "db_timeout" | "oom" | "network" | etc.


class ServiceNowEnrichment(BaseModel):
    """Agent 3 output — SNOW INC creation + CMDB data."""
    model_config = ConfigDict(extra="allow")

    snow_number:      Optional[str]  = None
    snow_sys_id:      Optional[str]  = None
    ci_name:          Optional[str]  = None
    ci_class:         Optional[str]  = None
    owner_group:      Optional[str]  = None
    sla_breach_at:    Optional[datetime] = None
    u_service_tier:   Optional[str]  = None
    action:           str            = "created"   # "created" | "bound" | "skipped"


class PagerDutyEnrichment(BaseModel):
    """Agent 4 output — PD on-call resolution + SLA data."""
    model_config = ConfigDict(extra="allow")

    pd_incident_id:    Optional[str]     = None
    pd_incident_key:   Optional[str]     = None
    on_call_name:      Optional[str]     = None
    on_call_email:     Optional[str]     = None
    escalation_policy: Optional[str]     = None
    sla_minutes:       Optional[int]     = None
    action:            str               = "alerted"  # "alerted" | "skipped"


class NotificationEnrichment(BaseModel):
    """Agent 5 output — notification delivery receipts."""
    model_config = ConfigDict(extra="allow")

    channels_notified: List[str]         = Field(default_factory=list)
    teams_ok:          bool              = False
    email_ok:          bool              = False
    sms_ok:            bool              = False


class ConfluenceKBHit(BaseModel):
    page_id:    str
    title:      str
    url:        str
    score:      float              # cosine similarity
    excerpt:    Optional[str] = None

class ConfluenceEnrichment(BaseModel):
    """Agent 6 output — KB search + scoring."""
    model_config = ConfigDict(extra="allow")

    query:       str                          = ""
    hits:        List[ConfluenceKBHit]        = Field(default_factory=list)
    top_score:   float                        = 0.0
    kb_attached: bool                         = False  # True if a hit was attached to SNOW


class DeploymentInfo(BaseModel):
    service:      str
    version:      str
    deployed_at:  datetime
    deployed_by:  Optional[str] = None
    change_id:    Optional[str] = None

class EnrichedIncident(BaseModel):
    """
    Accumulated enrichments from Agents 2-6.
    Passed to Agent 7 (RCA) as a single coherent context object.
    """
    model_config = ConfigDict(extra="allow")

    run_id:         UUID
    event:          OrchestratorEvent
    splunk:         Optional[SplunkEnrichment]     = None
    servicenow:     Optional[ServiceNowEnrichment] = None
    pagerduty:      Optional[PagerDutyEnrichment]  = None
    notifications:  Optional[NotificationEnrichment] = None
    confluence:     Optional[ConfluenceEnrichment] = None
    deployments:    List[DeploymentInfo]           = Field(default_factory=list)
    dt_entities:    List[Dict[str, Any]]           = Field(default_factory=list)
    dt_metrics:     Dict[str, Any]                 = Field(default_factory=dict)


# ── RCA Result ────────────────────────────────────────────────────────────────

class ResolutionStep(BaseModel):
    step_num:    int
    action:      str
    owner:       Optional[str] = None
    tool:        Optional[str] = None   # "kubectl" | "sql" | "api" | etc.
    command:     Optional[str] = None   # optional CLI command or query
    rationale:   Optional[str] = None

class RCAResult(BaseModel):
    """Final output of Agent 7 — the root-cause analysis and resolution plan."""
    model_config = ConfigDict(extra="allow")

    run_id:           UUID
    incident_id:      UUID
    external_id:      str
    severity:         Severity
    flow:             IncidentFlow

    root_cause:       str
    root_cause_category: Optional[str] = None   # "db_connection" | "memory" | "deployment" | etc.
    confidence:       float = Field(ge=0.0, le=1.0)
    resolution_steps: List[ResolutionStep]       = Field(default_factory=list)
    rollback_required: bool                      = False
    rollback_target:  Optional[str]              = None

    # Monitoring
    monitor_until:    Optional[datetime]         = None
    resolved_at:      Optional[datetime]         = None

    # Evidence
    supporting_kb:    List[ConfluenceKBHit]      = Field(default_factory=list)
    log_evidence:     Optional[str]              = None

    # Metadata
    llm_provider:     Optional[str]              = None
    llm_model:        Optional[str]              = None
    tokens_used:      Optional[int]              = None
    generated_at:     datetime                   = Field(default_factory=datetime.utcnow)


# ── SSE Events ────────────────────────────────────────────────────────────────

class SSEEventType(str, enum.Enum):
    PIPELINE_STARTED   = "pipeline_started"
    AGENT_START        = "agent_start"
    AGENT_DONE         = "agent_done"
    AGENT_ERROR        = "agent_error"
    PIPELINE_COMPLETE  = "pipeline_complete"
    PIPELINE_ERROR     = "pipeline_error"
    HEARTBEAT          = "heartbeat"

class SSEEvent(BaseModel):
    """Published to Redis Streams and forwarded to SSE clients."""
    event:      SSEEventType
    run_id:     Optional[str]   = None
    agent_num:  Optional[int]   = None
    agent_name: Optional[str]   = None
    timestamp:  datetime        = Field(default_factory=datetime.utcnow)
    data:       Dict[str, Any]  = Field(default_factory=dict)
