from __future__ import annotations

import math
from typing import Optional
from uuid import UUID

import asyncpg

from agents.Agent_8_knowledge_synth import queries as q
from agents.Agent_8_knowledge_synth.schemas import SynthesizedArticle


async def upsert_article_versioned(
    conn: asyncpg.Connection,
    *,
    cluster_signature: str,
    article: SynthesizedArticle,
    embedding_title: list[float],
    embedding_full: list[float],
    cluster_cohesion: float,
    source_incident_ids: list[str],
    embedding_model_version: str,
    confidence_score: float,
) -> UUID:
    """Insert a new version row, then deactivate prior versions for the same signature."""
    async with conn.transaction():
        prior = await q.latest_version_for_signature(conn, cluster_signature)
        new_version = (prior or 0) + 1
        article_id = await q.insert_article(
            conn,
            cluster_signature=cluster_signature,
            version=new_version,
            title=article.title,
            problem_summary=article.problem_summary,
            root_cause=article.root_cause,
            resolution_steps=[step.model_dump() for step in article.resolution_steps],
            keywords=article.keywords,
            assignment_group=article.assignment_group,
            category=article.category,
            subcategory=article.subcategory,
            source_incident_ids=source_incident_ids,
            confidence_score=confidence_score,
            llm_self_rating=article.confidence_self_rating,
            cluster_cohesion=cluster_cohesion,
            embedding_title=embedding_title,
            embedding_full=embedding_full,
            embedding_model_version=embedding_model_version,
        )
        await q.deactivate_prior_versions(
            conn, cluster_signature, keep_version=new_version
        )
    return article_id


def compute_confidence_score(
    cluster_cohesion: float,
    source_incident_count: int,
    llm_self_rating: float,
    rolling_feedback_score: Optional[float],
) -> float:
    """Spec §5.4 — blend four signals into a final 0..1 confidence."""
    count_score = (
        min(1.0, math.log10(source_incident_count + 1) / 1.5)
        if source_incident_count > 0
        else 0.0
    )
    feedback = rolling_feedback_score if rolling_feedback_score is not None else 0.5
    return min(
        1.0,
        max(
            0.0,
            0.30 * cluster_cohesion
            + 0.20 * count_score
            + 0.20 * llm_self_rating
            + 0.30 * feedback,
        ),
    )
