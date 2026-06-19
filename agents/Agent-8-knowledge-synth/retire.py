from __future__ import annotations

import logging
from uuid import UUID

import asyncpg

logger = logging.getLogger("agent8.retire")


async def retire_low_feedback_articles(
    conn: asyncpg.Connection,
    *,
    months_window: int,
    min_recommendations: int,
    min_score: float,
) -> list[UUID]:
    """Mark articles inactive when their feedback score from kb_recommendations is below threshold.

    Returns the list of retired article IDs.
    Soft-fails if `kb_recommendations` table is not present in the database.
    """
    has_table = await conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name='kb_recommendations')"
    )
    if not has_table:
        logger.info("retire_skipped_no_table")
        return []

    rows = await conn.fetch(
        f"""
        WITH stats AS (
          SELECT skb.id AS article_id,
                 COUNT(kr.*) AS reco_count,
                 AVG(kr.feedback_score) AS avg_score
          FROM sentinel_synthesized_kb skb
          LEFT JOIN kb_recommendations kr
            ON kr.kb_article_id = skb.id::text
           AND kr.created_at >= NOW() - INTERVAL '{int(months_window)} months'
          WHERE skb.is_active = TRUE
          GROUP BY skb.id
        )
        UPDATE sentinel_synthesized_kb
        SET is_active = FALSE, retired_at = NOW()
        FROM stats
        WHERE sentinel_synthesized_kb.id = stats.article_id
          AND stats.reco_count >= $1
          AND stats.avg_score < $2
        RETURNING sentinel_synthesized_kb.id
        """,
        min_recommendations, min_score,
    )
    return [r["id"] for r in rows]
