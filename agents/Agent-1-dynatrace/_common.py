"""
agents/Agent-1-dynatrace/_common.py
====================================
Shared symbols for intake modules (dt.py, snow.py) to avoid circular imports.

This module holds configuration constants, severity maps, and model/function
re-exports that would otherwise be imported by intake modules from main.py,
creating a circular dependency (main.py imports intake.dt, intake.dt imports main).
"""
from __future__ import annotations

import os

from shared.auth import verify_hmac_signature
from shared.models import (
    DynatracePayload,
    IncidentFlow,
    IncidentSource,
    OrchestratorEvent,
    ServiceNowPayload,
    Severity,
)

# DT webhook secret (HMAC-SHA256)
DT_SECRET = os.getenv("DT_WEBHOOK_SECRET", "")

# SNOW webhook secret (HMAC-SHA256)
SNOW_SECRET = os.getenv("SNOW_WEBHOOK_SECRET", "")

# DT severity → internal Severity
_DT_SEVERITY_MAP = {
    "AVAILABILITY": Severity.CRITICAL,   # P1
    "PERFORMANCE":  Severity.HIGH,       # P2
    "ERROR":        Severity.MEDIUM,     # P3
    "RESOURCE":     Severity.MEDIUM,     # P3
    "CUSTOM":       Severity.LOW,        # P4
    "INFO":         Severity.INFO,       # P5
}

# SNOW priority number → internal Severity
_SNOW_PRIORITY_MAP = {
    "1": Severity.CRITICAL,
    "2": Severity.HIGH,
    "3": Severity.MEDIUM,
    "4": Severity.LOW,
    "5": Severity.INFO,
}

__all__ = [
    "DT_SECRET",
    "SNOW_SECRET",
    "_DT_SEVERITY_MAP",
    "_SNOW_PRIORITY_MAP",
    "DynatracePayload",
    "ServiceNowPayload",
    "IncidentFlow",
    "IncidentSource",
    "Severity",
    "OrchestratorEvent",
    "verify_hmac_signature",
]
