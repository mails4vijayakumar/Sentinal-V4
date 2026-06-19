import os
import pytest
import asyncpg

pytestmark = pytest.mark.integration

DSN = os.environ.get("DATABASE_URL", "postgresql://sentinel:sentinel@localhost:5432/sentinel")


@pytest.mark.asyncio
async def test_synthesized_kb_tables_exist():
    conn = await asyncpg.connect(DSN)
    try:
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' "
            "AND table_name IN ('sentinel_synthesized_kb','kb_synthesis_runs','kb_synthesis_decisions')"
        )
        names = {r["table_name"] for r in rows}
        assert names == {"sentinel_synthesized_kb", "kb_synthesis_runs", "kb_synthesis_decisions"}

        col = await conn.fetchrow(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name='sentinel_synthesized_kb' AND column_name='embedding_title'"
        )
        assert col["data_type"] == "USER-DEFINED"  # pgvector type
    finally:
        await conn.close()
