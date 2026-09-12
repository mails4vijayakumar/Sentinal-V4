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
from typing import Any, Dict

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
