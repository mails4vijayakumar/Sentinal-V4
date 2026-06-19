import os
from uuid import uuid4

import asyncpg
import pytest

from agents.Agent_8_knowledge_synth.upsert import upsert_article_versioned
from agents.Agent_8_knowledge_synth.schemas import (
    ResolutionStep,
    SynthesizedArticle,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel"
)


@pytest.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    # Clean slate for this test
    await c.execute(
        "DELETE FROM sentinel_synthesized_kb WHERE cluster_signature LIKE 'TEST_%'"
    )
    yield c
    await c.execute(
        "DELETE FROM sentinel_synthesized_kb WHERE cluster_signature LIKE 'TEST_%'"
    )
    await c.close()


def _article(title="DB pool exhausted"):
    return SynthesizedArticle(
        title=title,
        problem_summary="Pods crash with HikariCP timeouts during peak.",
        root_cause="Pool size too small for concurrent load.",
        resolution_steps=[
            ResolutionStep(step=1, action="Restart"),
            ResolutionStep(step=2, action="Raise pool"),
        ],
        keywords=["HikariCP"],
        assignment_group="App-Backend",
        category="Application",
        subcategory="DB",
        confidence_self_rating=0.8,
    )


async def test_upsert_creates_version_1_when_new(conn):
    sig = f"TEST_{uuid4().hex[:8]}"
    article_id = await upsert_article_versioned(
        conn,
        cluster_signature=sig,
        article=_article(),
        embedding_title=[0.1] * 768,
        embedding_full=[0.2] * 768,
        cluster_cohesion=0.8,
        source_incident_ids=["INC1", "INC2", "INC3", "INC4", "INC5"],
        embedding_model_version="nomic-embed-text:v1.5",
        confidence_score=0.78,
    )
    row = await conn.fetchrow(
        "SELECT version, is_active FROM sentinel_synthesized_kb WHERE id=$1",
        article_id,
    )
    assert row["version"] == 1
    assert row["is_active"] is True


async def test_upsert_increments_version_and_deactivates_prior(conn):
    sig = f"TEST_{uuid4().hex[:8]}"
    # v1
    await upsert_article_versioned(
        conn,
        cluster_signature=sig,
        article=_article(),
        embedding_title=[0.1] * 768,
        embedding_full=[0.2] * 768,
        cluster_cohesion=0.8,
        source_incident_ids=["INC1"],
        embedding_model_version="v1",
        confidence_score=0.7,
    )
    # v2
    await upsert_article_versioned(
        conn,
        cluster_signature=sig,
        article=_article(title="DB pool exhausted (refined)"),
        embedding_title=[0.1] * 768,
        embedding_full=[0.2] * 768,
        cluster_cohesion=0.85,
        source_incident_ids=["INC1", "INC2"],
        embedding_model_version="v1",
        confidence_score=0.8,
    )
    rows = await conn.fetch(
        "SELECT version, is_active FROM sentinel_synthesized_kb "
        "WHERE cluster_signature=$1 ORDER BY version",
        sig,
    )
    assert [r["version"] for r in rows] == [1, 2]
    assert [r["is_active"] for r in rows] == [False, True]
