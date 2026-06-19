from __future__ import annotations

import re
from datetime import datetime
from html.parser import HTMLParser
from typing import Any, Optional

EXCLUDED_CLOSE_CODES = {
    "Cannot Reproduce",
    "Duplicate",
    "User Error - No Action",
}


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _strip_html(text: str) -> str:
    if not text:
        return ""
    parser = _HTMLStripper()
    parser.feed(text)
    return "".join(parser.parts)


_WS_COLLAPSE = re.compile(r"[ \t\f\v]+")
_NL_COLLAPSE = re.compile(r"\n{2,}")


def _collapse_whitespace(text: str) -> str:
    text = _WS_COLLAPSE.sub(" ", text)
    text = _NL_COLLAPSE.sub("\n", text)
    return text.strip()


def normalize_incident(raw: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return a normalised incident dict, or None to drop the incident."""
    close_code = (raw.get("close_code") or "").strip()
    if close_code in EXCLUDED_CLOSE_CODES:
        return None

    resolution = _collapse_whitespace(_strip_html(raw.get("close_notes") or ""))
    if not resolution:
        return None

    closed_at_raw = raw.get("closed_at")
    try:
        closed_at = datetime.strptime(closed_at_raw, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None

    return {
        "number": raw["number"],
        "short_description": _collapse_whitespace(_strip_html(raw.get("short_description") or "")),
        "description": _collapse_whitespace(_strip_html(raw.get("description") or "")),
        "close_notes": resolution,
        "close_code": close_code,
        "assignment_group": (raw.get("assignment_group") or "").strip(),
        "category": (raw.get("category") or None) or None,
        "subcategory": (raw.get("subcategory") or None) or None,
        "closed_at_iso": closed_at.isoformat(),
        "closed_at": closed_at,
    }


_STEP_MARKER = re.compile(r"(?im)(^\s*(?:step\s*\d+|first|then|finally|\d+[\.\)])\b)")


def quality_score(inc: dict) -> float:
    """0.0 - 1.0 score used as the gate before clustering."""
    score = 0.0

    notes = inc.get("close_notes") or ""
    word_count = len(notes.split())
    char_count = len(notes)
    # Either many words or substantial character length signals detailed notes.
    if word_count >= 50 or char_count >= 200:
        score += 0.30
    elif word_count >= 20 or char_count >= 80:
        score += 0.15

    if _STEP_MARKER.search(notes):
        score += 0.20

    if inc.get("close_code") == "Solved (Permanently)":
        score += 0.20
    elif (inc.get("close_code") or "").startswith("Solved"):
        score += 0.10

    if inc.get("has_sentinel_attribution"):
        score += 0.20

    # Unique-reporter bonus — fires only when caller_id is plumbed and present
    if inc.get("caller_id"):
        score += 0.10

    return min(round(score, 10), 1.0)
