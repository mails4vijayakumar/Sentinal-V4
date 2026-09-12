"""
agents/Agent-2-splunk/classifier.py
===================================
Six-phase deterministic classifier for Agent 2. Every phase is a pure
function (except `run_splunk_async`, which is I/O). `classify()` is the
top-level entry point wired from main.py.

Spec: docs/superpowers/specs/2026-07-01-agent-2-spec-closure-design.md
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import httpx

import rules
from shared.models import RuleMatch

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


# ── Phase 3: async Splunk submit / poll / fetch ──────────────────────────────

SPLUNK_JOB_POLL_INTERVAL_SECONDS: float = float(os.getenv("SPLUNK_JOB_POLL_INTERVAL_SECONDS", "2"))
SPLUNK_JOB_MAX_POLLS: int              = int(os.getenv("SPLUNK_JOB_MAX_POLLS", "10"))


class SplunkPollTimeout(Exception):
    """Raised when the poll loop exhausts without dispatchState=DONE."""


async def run_splunk_async(client: httpx.AsyncClient, spl: str) -> List[Dict[str, Any]]:
    """Submit a Splunk search job, poll to completion, return results."""
    submit = await client.post(
        "/services/search/jobs",
        data={"search": spl, "output_mode": "json", "exec_mode": "normal"},
    )
    submit.raise_for_status()
    sid = submit.json()["sid"]

    for _ in range(SPLUNK_JOB_MAX_POLLS):
        await asyncio.sleep(SPLUNK_JOB_POLL_INTERVAL_SECONDS)
        poll = await client.get(f"/services/search/jobs/{sid}",
                                params={"output_mode": "json"})
        poll.raise_for_status()
        entries = poll.json().get("entry", [])
        state = entries[0].get("content", {}).get("dispatchState") if entries else None
        if state == "DONE":
            break
    else:
        raise SplunkPollTimeout(f"sid={sid} never reached DONE after {SPLUNK_JOB_MAX_POLLS} polls")

    fetch = await client.get(
        f"/services/search/jobs/{sid}/results",
        params={"output_mode": "json", "count": SPLUNK_MAX_RESULTS},
    )
    fetch.raise_for_status()
    return fetch.json().get("results", []) or []


# ── Phase 4: score ───────────────────────────────────────────────────────────

_RULE_SETS = (
    ("app",   rules.APP_RULES),
    ("infra", rules.INFRA_RULES),
    ("db",    rules.DB_RULES),
)


def _row_text(row: Dict[str, Any]) -> str:
    raw = row.get("_raw")
    return raw if isinstance(raw, str) else str(row)


def score(results: List[Dict[str, Any]]) -> List[RuleMatch]:
    """Apply every rule to every row. Return one RuleMatch per rule that hit ≥ 1."""
    # (rule_id, rule_set, weight) → [count, first_sample_line]
    hits: Dict[str, List[Any]] = {}
    for row in results:
        text = _row_text(row)
        for rule_set_name, rule_set in _RULE_SETS:
            for rule_id, pattern, weight in rule_set:
                if pattern.search(text):
                    entry = hits.setdefault(rule_id, [0, text[:200], rule_set_name, weight])
                    entry[0] += 1
    return [
        RuleMatch(rule_id=rid, rule_set=rs_name, weight=w,
                  match_count=count, sample_line=sample)
        for rid, (count, sample, rs_name, w) in hits.items()
    ]


# ── Phase 5: resolve_conflict ────────────────────────────────────────────────

CONFLICT_RESOLUTION_THRESHOLD: float = float(os.getenv("CONFLICT_RESOLUTION_THRESHOLD", "65.0"))


def resolve_conflict(
    dt_hypothesis: str,
    rule_matches: List[RuleMatch],
) -> Tuple[str, str | None, float, str]:
    """Compute per-rule-set scores and pick the winner.

    Returns (final_category, splunk_category, confidence, hypothesis_source).
    splunk_category is None when there are no matches at all.
    """
    if not rule_matches:
        return dt_hypothesis, None, 0.0, "dt"

    scores: Dict[str, float] = defaultdict(float)
    for m in rule_matches:
        scores[m.rule_set] += m.weight * math.log10(1 + m.match_count)

    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    splunk_category, winning_score = ranked[0]
    runner_up_score = ranked[1][1] if len(ranked) > 1 else 0.0
    confidence = 100.0 * winning_score / (winning_score + runner_up_score + 1e-6)

    if confidence >= CONFLICT_RESOLUTION_THRESHOLD:
        return splunk_category, splunk_category, confidence, "splunk"
    return dt_hypothesis, splunk_category, confidence, "dt"


# ── Phase 6: precompute_routing ──────────────────────────────────────────────

def precompute_routing(category: str) -> Dict[str, str]:
    """Map resolved category → {team, queue, snow_category, snow_subcategory}.

    Unknown categories return an empty dict (Agent 3 handles missing fields).
    """
    row = rules.CATEGORY_ROUTING.get(category)
    return dict(row) if row else {}
