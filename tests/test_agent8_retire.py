import os
from uuid import uuid4

import asyncpg
import pytest

from agents.Agent_8_knowledge_synth.retire import retire_low_feedback_articles

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DSN = os.environ.get("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")


@pytest.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    await c.execute("DELETE FROM sentinel_synthesized_kb WHERE cluster_signature LIKE 'RET_%'")
    yield c
    await c.execute("DELETE FROM sentinel_synthesized_kb WHERE cluster_signature LIKE 'RET_%'")
    await c.close()


async def test_retire_marks_inactive_no_kb_recos_table(conn):
    """When kb_recommendations table is absent, retirement is a no-op (graceful)."""
    sig = f"RET_{uuid4().hex[:8]}"
    await conn.execute(
        """
        INSERT INTO sentinel_synthesized_kb
            (cluster_signature, version, title, problem_summary, resolution_steps,
             assignment_group, source_incident_ids, confidence_score, embedding_model_version)
        VALUES ($1, 1, 't', 'p', '[]'::jsonb, 'g', ARRAY['INC1'], 0.7, 'v1')
        """,
        sig,
    )
    retired_ids = await retire_low_feedback_articles(
        conn, months_window=6, min_recommendations=10, min_score=0.30,
    )
    assert retired_ids == []  # nothing matches the threshold
