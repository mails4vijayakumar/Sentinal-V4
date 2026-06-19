from __future__ import annotations

from typing import Optional


def classify_dedup_decision(
    similarity: Optional[float],
    *,
    update_threshold: float,
    review_threshold: float,
) -> str:
    """Map a cosine similarity to a decision: 'update', 'review', or 'create'."""
    if similarity is None:
        return "create"
    if similarity >= update_threshold:
        return "update"
    if similarity >= review_threshold:
        return "review"
    return "create"
