from __future__ import annotations

import re

# Patterns mirror CLAUDE.md §10.6 (chat_phi_scrubber.py) with extensions for incident text.
# Order matters: more specific patterns must come before broader ones.
_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bMRN[-:\s]?\d{4,12}\b", re.IGNORECASE),                          "[MRN REDACTED]"),
    (re.compile(r"\bDOB[-:\s]?\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b", re.IGNORECASE),     "[DOB REDACTED]"),
    (re.compile(r"\bNPI[-:\s]?\d{10}\b", re.IGNORECASE),                            "[NPI REDACTED]"),
    (re.compile(r"\bICD-?1[01][-:\s]?[A-Z]\d+\.?\w*\b", re.IGNORECASE),             "[DIAGNOSIS-CODE REDACTED]"),
    (re.compile(r"\b(patient|pt|member)\s*id[-:\s]?\w+\b", re.IGNORECASE),          "[PATIENT-ID REDACTED]"),
    (re.compile(r"\b(member|patient)\s+id[-:\s]+\d{6,}\b", re.IGNORECASE),          "[PATIENT-ID REDACTED]"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),                                          "[ID REDACTED]"),
    (re.compile(r"\b(member|patient)\s+\w+\s+\d{6,}\b", re.IGNORECASE),             "[PATIENT-ID REDACTED]"),
]


def scrub_phi(text: str) -> tuple[str, int]:
    """Return (scrubbed_text, redaction_count). Never logs the redacted content."""
    if not text:
        return text, 0
    count = 0
    out = text
    for pattern, marker in _PATTERNS:
        new_out, n = pattern.subn(marker, out)
        out = new_out
        count += n
    return out, count


def scrub_incident_fields(incident: dict) -> tuple[dict, int]:
    """Scrub all text fields of an incident dict in place. Returns (incident, total_count)."""
    total = 0
    scrubbed = dict(incident)
    for field in ("short_description", "description", "close_notes"):
        if field in scrubbed and scrubbed[field]:
            scrubbed[field], n = scrub_phi(scrubbed[field])
            total += n
    return scrubbed, total
