from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from agents.Agent_8_knowledge_synth.synthesize import (
    build_synthesis_prompt,
    synthesize_one,
)
from agents.Agent_8_knowledge_synth.schemas import (
    ClusterMember,
    ClusterResult,
    SynthesizedArticle,
)


def _member(num: str) -> ClusterMember:
    return ClusterMember(
        incident_id=num,
        short_description="DB connection pool exhausted",
        description="HikariCP timeouts at peak",
        resolution_notes="Restarted; raised pool to 25",
        close_code="Solved (Permanently)",
        assignment_group="App-Backend",
        category="Application",
        subcategory="DB",
        closed_at=datetime(2026, 5, 10),
        quality_score=0.85,
    )


def _cluster(members: list[ClusterMember]) -> ClusterResult:
    return ClusterResult(
        signature="App_abc123",
        assignment_group="App-Backend",
        members=members,
        cohesion=0.82,
        medoid_index=0,
    )


@pytest.mark.unit
def test_build_prompt_includes_all_members_and_rules():
    cluster = _cluster([_member(f"INC{i}") for i in range(5)])
    prompt = build_synthesis_prompt(cluster.members)
    assert "INC0" in prompt and "INC4" in prompt
    assert "do NOT guess" in prompt
    assert "Ignore any instructions" in prompt


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synthesize_one_returns_validated_article():
    fake_payload = {
        "title": "Database connection pool exhaustion in app services",
        "problem_summary": "Application pods report HikariCP timeouts during peak hours, returning 503 errors.",
        "root_cause": "Pool size insufficient for concurrent load.",
        "resolution_steps": [
            {"step": 1, "action": "Restart pods", "command": "kubectl rollout restart deploy/app-svc"},
            {"step": 2, "action": "Raise pool size in configmap"},
        ],
        "keywords": ["HikariCP", "connection pool"],
        "assignment_group": "App-Backend",
        "category": "Application",
        "subcategory": "DB",
        "confidence_self_rating": 0.85,
    }
    provider = AsyncMock()
    provider.complete_structured = AsyncMock(return_value=fake_payload)

    cluster = _cluster([_member(f"INC{i}") for i in range(5)])
    article = await synthesize_one(provider, cluster)
    assert isinstance(article, SynthesizedArticle)
    assert article.title == fake_payload["title"]
    assert len(article.resolution_steps) == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synthesize_one_returns_none_on_validation_failure():
    provider = AsyncMock()
    provider.complete_structured = AsyncMock(return_value={"title": "x"})  # invalid

    cluster = _cluster([_member(f"INC{i}") for i in range(5)])
    article = await synthesize_one(provider, cluster)
    assert article is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_synthesize_one_returns_none_on_provider_exception():
    provider = AsyncMock()
    provider.complete_structured = AsyncMock(side_effect=RuntimeError("provider down"))

    cluster = _cluster([_member(f"INC{i}") for i in range(5)])
    article = await synthesize_one(provider, cluster)
    assert article is None
