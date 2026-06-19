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
    }
