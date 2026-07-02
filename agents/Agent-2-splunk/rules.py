"""
agents/Agent-2-splunk/rules.py
==============================
Hand-curated data for Agent 2's six-phase classifier. Import-only, no I/O.

Spec: docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md
"""
from __future__ import annotations

import re
from typing import Dict, List, Tuple

# ── Search window per severity (minutes) ─────────────────────────────────────
WINDOW_MIN: Dict[str, int] = {"P1": 15, "P2": 30, "P3": 30, "P4": 60, "P5": 60}

# ── SPL keyword clause used by every tier ────────────────────────────────────
ERROR_KEYWORDS: str = "(ERROR OR WARN OR CRITICAL OR Exception OR Traceback OR FATAL)"

# ── DT (eventType, entityType) → hypothesis category ─────────────────────────
DT_HYPOTHESIS_MAP: Dict[Tuple[str, str], str] = {
    ("FAILURE_RATE_INCREASED",            "SERVICE"):       "app",
    ("RESPONSE_TIME_DEGRADATION",         "SERVICE"):       "app",
    ("APPLICATION_ERROR",                 "APPLICATION"):   "app",
    ("PROCESS_UNAVAILABLE",               "PROCESS_GROUP"): "infra",
    ("PROCESS_MEMORY_RESOURCE_EXHAUSTED", "PROCESS_GROUP"): "infra",
    ("CPU_SATURATED",                     "HOST"):          "infra",
    ("MEMORY_SATURATED",                  "HOST"):          "infra",
    ("HOST_DISK_QUEUE_LENGTH_HIGH",       "HOST"):          "infra",
    ("HOST_NOT_AVAILABLE",                "HOST"):          "infra",
    ("DATABASE_CONNECTION_FAILURE",       "SERVICE"):       "db",
    ("SLOW_DB_QUERIES",                   "SERVICE"):       "db",
}

# ── Regex rule sets: (rule_id, compiled_pattern, weight) ─────────────────────
APP_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("app.null_pointer",    re.compile(r"\b(NullPointer|NoneType)"),                        1.0),
    ("app.classnotfound",   re.compile(r"ClassNotFoundException|ModuleNotFoundError"),      1.0),
    ("app.deserialization", re.compile(r"JsonProcessingException|InvalidObject|JSONDecode"),1.0),
    ("app.http_5xx",        re.compile(r'HTTP/1\.[01]"\s+5\d\d\s'),                          1.0),
    ("app.unhandled_ex",    re.compile(r"Unhandled exception|Uncaught"),                     1.0),
]

INFRA_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("infra.oom",            re.compile(r"OutOfMemory|OOMKilled|heap.*exhausted", re.I), 1.2),
    ("infra.disk_full",      re.compile(r"No space left on device|disk.*full", re.I),   1.2),
    ("infra.cpu_saturated",  re.compile(r"CPU.*(saturat|throttl)", re.I),               1.2),
    ("infra.container_kill", re.compile(r"container.*killed|SIGKILL|Evicted"),          1.2),
]

DB_RULES: List[Tuple[str, re.Pattern, float]] = [
    ("db.conn_pool",         re.compile(r"connection pool.*(exhausted|timeout)|HikariCP.*timeout", re.I), 1.1),
    ("db.deadlock",          re.compile(r"deadlock detected|ORA-00060"),                                   1.1),
    ("db.query_timeout",     re.compile(r"query timeout|statement timeout|ORA-01013"),                     1.1),
    ("db.connection_refused",re.compile(r"connection refused.*(5432|3306|1521)"),                          1.1),
]

# ── Resolved category → SNOW routing fields ──────────────────────────────────
CATEGORY_ROUTING: Dict[str, Dict[str, str]] = {
    "app":   {"team": "app-support", "queue": "APP_SUPPORT",
              "snow_category": "Application",    "snow_subcategory": "Runtime Error"},
    "infra": {"team": "infra-ops",   "queue": "INFRA_OPS",
              "snow_category": "Infrastructure", "snow_subcategory": "Resource"},
    "db":    {"team": "dba",         "queue": "DB_ADMIN",
              "snow_category": "Database",       "snow_subcategory": "Query/Connection"},
}
