import os
from datetime import date
from uuid import UUID

import asyncpg
import pytest

from agents.Agent_8_knowledge_synth.queries import (
    create_run,
    finalize_run,
    insert_decision,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel"
)


@pytest.fixture
async def conn():
    c = await asyncpg.connect(DSN)
    yield c
    await c.close()


async def test_create_and_finalize_run(conn):
    run_id = await create_run(
        conn, window_start=date(2026, 5, 1), window_end=date(2026, 5, 31)
    )
    assert isinstance(run_id, UUID)
    row = await conn.fetchrow(
        "SELECT status FROM kb_synthesis_runs WHERE run_id=$1", run_id
    )
    assert row["status"] == "running"

    await finalize_run(conn, run_id, status="succeeded", counts={"created": 3}, error=None)
    row = await conn.fetchrow(
        "SELECT status, counts FROM kb_synthesis_runs WHERE run_id=$1", run_id
    )
    assert row["status"] == "succeeded"


async def test_insert_decision(conn):
    run_id = await create_run(
        conn, window_start=date(2026, 5, 1), window_end=date(2026, 5, 31)
    )
    decision_id = await insert_decision(
        conn,
        run_id=run_id,
        cluster_signature="T_abc",
        decision="skip",
        article_id=None,
        similarity_score=None,
        notes="no LLM output",
    )
    assert isinstance(decision_id, UUID)
