"""
agents/Agent-2-splunk/classifier.py
===================================
Six-phase deterministic classifier for Agent 2. Every phase is a pure
function (except `run_splunk_async`, which is I/O). `classify()` is the
top-level entry point wired from main.py.

Spec: docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

import rules

log = logging.getLogger(__name__)


# ── Phase 1: preclassify ─────────────────────────────────────────────────────

def preclassify(event: Dict[str, Any]) -> str:
    """Look up DT (eventType, entityType) → hypothesis category.

    Unmapped combos default to "app". Missing raw_payload or fields also default.
    """
    payload = event.get("raw_payload") or {}
    key = (payload.get("eventType") or "", payload.get("entityType") or "")
    hypothesis = rules.DT_HYPOTHESIS_MAP.get(key)
    if hypothesis is None:
        log.info("preclassify_unmapped event_type=%s entity_type=%s", key[0], key[1])
        return "app"
    return hypothesis


# ── Phase 2: build_tiered_queries ────────────────────────────────────────────

SPLUNK_MAX_RESULTS: int = 500  # CLAUDE.md §9.2


def build_tiered_queries(host: str, service: str, severity: str, index: str) -> List[str]:
    """Return up to 3 SPL strings ordered tight → wide.

    T1: index + host + service + keywords
    T2: index + host + keywords          (drop service)
    T3: index + keywords                 (drop host)

    Duplicate tiers (caused by empty host/service) are deduped in order.
    """
    w = rules.WINDOW_MIN.get(severity, 30)
    earliest = f"-{w}m@m"
    host_q = f' host="{host}"' if host else ""
    svc_q  = f' source="*{service}*"' if service else ""

    tiers = [
        f"search index={index}{host_q}{svc_q} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
        f"search index={index}{host_q} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
        f"search index={index} earliest={earliest} "
        f"{rules.ERROR_KEYWORDS} | head {SPLUNK_MAX_RESULTS}",
    ]
    # Dedup in insertion order.
    seen: List[str] = []
    for t in tiers:
        if t not in seen:
            seen.append(t)
    return seen
