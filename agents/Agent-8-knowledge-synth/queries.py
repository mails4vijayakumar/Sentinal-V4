from __future__ import annotations

import json
from datetime import date
from typing import Any, Optional
from uuid import UUID

import asyncpg


async def create_run(
    conn: asyncpg.Connection, *, window_start: date, window_end: date
) -> UUID:
    row = await conn.fetchrow(
        """
        INSERT INTO kb_synthesis_runs (status, window_start, window_end, counts)
        VALUES ('running', $1, $2, '{}'::jsonb)
        RETURNING run_id
        """,
        window_start, window_end,
    )
    return row["run_id"]


async def finalize_run(
    conn: asyncpg.Connection,
    run_id: UUID,
    *,
    status: str,
    counts: dict[str, Any],
    error: Optional[str] = None,
    stage_durations: Optional[dict[str, float]] = None,
) -> None:
    await conn.execute(
        """
        UPDATE kb_synthesis_runs
        SET status=$2, counts=$3, error_message=$4, stage_durations=$5, finished_at=NOW()
        WHERE run_id=$1
        """,
        run_id, status, json.dumps(counts), error,
        json.dumps(stage_durations) if stage_durations is not None else None,
    )


async def insert_decision(
    conn: asyncpg.Connection,
    *,
    run_id: UUID,
    cluster_signature: str,
    decision: str,
    article_id: Optional[UUID],
    similarity_score: Optional[float],
    notes: Optional[str] = None,
) -> UUID:
    row = await conn.fetchrow(
        """
        INSERT INTO kb_synthesis_decisions
            (run_id, cluster_signature, decision, article_id, similarity_score, notes)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING decision_id
        """,
        run_id, cluster_signature, decision, article_id, similarity_score, notes,
    )
    return row["decision_id"]


async def insert_article(
    conn: asyncpg.Connection, *,
    cluster_signature: str,
    version: int,
    title: str,
    problem_summary: str,
    root_cause: Optional[str],
    resolution_steps: list[dict[str, Any]],
    keywords: list[str],
    assignment_group: str,
    category: Optional[str],
    subcategory: Optional[str],
    source_incident_ids: list[str],
    confidence_score: float,
    llm_self_rating: float,
    cluster_cohesion: float,
    embedding_title: list[float],
    embedding_full: list[float],
    embedding_model_version: str,
) -> UUID:
    row = await conn.fetchrow(
        """
        INSERT INTO sentinel_synthesized_kb (
            cluster_signature, version, title, problem_summary, root_cause,
            resolution_steps, keywords, assignment_group, category, subcategory,
            source_incident_ids, confidence_score, llm_self_rating, cluster_cohesion,
            embedding_title, embedding_full, embedding_model_version
        )
        VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        RETURNING id
        """,
        cluster_signature, version, title, problem_summary, root_cause,
        json.dumps(resolution_steps), keywords, assignment_group, category, subcategory,
        source_incident_ids, confidence_score, llm_self_rating, cluster_cohesion,
        embedding_title, embedding_full, embedding_model_version,
    )
    return row["id"]


async def latest_version_for_signature(
    conn: asyncpg.Connection, cluster_signature: str
) -> Optional[int]:
    row = await conn.fetchrow(
        "SELECT MAX(version) AS v FROM sentinel_synthesized_kb WHERE cluster_signature=$1",
        cluster_signature,
    )
    return row["v"]


async def deactivate_prior_versions(
    conn: asyncpg.Connection, cluster_signature: str, keep_version: int
) -> None:
    await conn.execute(
        "UPDATE sentinel_synthesized_kb SET is_active=FALSE "
        "WHERE cluster_signature=$1 AND version<>$2",
        cluster_signature, keep_version,
    )


async def find_similar_article(
    conn: asyncpg.Connection,
    embedding_title: list[float],
    *,
    min_similarity: float,
) -> Optional[dict[str, Any]]:
    """Return {'id': UUID, 'cluster_signature': str, 'similarity': float} or None."""
    row = await conn.fetchrow(
        """
        SELECT id, cluster_signature, 1 - (embedding_title <=> $1::vector) AS similarity
        FROM sentinel_synthesized_kb
        WHERE is_active=TRUE
        ORDER BY embedding_title <=> $1::vector
        LIMIT 1
        """,
        embedding_title,
    )
    if row is None:
        return None
    if row["similarity"] < min_similarity:
        return None
    return {
        "id": row["id"],
        "cluster_signature": row["cluster_signature"],
        "similarity": float(row["similarity"]),
    }
