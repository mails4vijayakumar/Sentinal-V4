from __future__ import annotations

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
    prior = await q.latest_version_for_signature(conn, cluster_signature)
    new_version = (prior or 0) + 1

    async with conn.transaction():
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
