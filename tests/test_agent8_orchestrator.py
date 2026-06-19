from datetime import date
from unittest.mock import AsyncMock

import pytest

from agents.Agent_8_knowledge_synth.orchestrator import run_synthesis_with_deps
from agents.Agent_8_knowledge_synth.schemas import RunCounts


@pytest.mark.unit
@pytest.mark.asyncio
async def test_orchestrator_empty_extract_finalizes_succeeded():
    deps = AsyncMock()
    deps.extract.return_value = []
    deps.create_run.return_value = "run-uuid"
    # Scalars on AsyncMock are also AsyncMocks unless we set them — set them as plain values.
    deps.quality_score_floor = 0.40
    deps.min_cluster_size = 5
    deps.min_samples = 3
    deps.min_cohesion = 0.65
    deps.dedup_update_threshold = 0.92
    deps.dedup_review_threshold = 0.80
    deps.max_concurrent_synthesize = 10
    deps.embedding_model_version = "nomic-embed-text:v1.5"
    deps.publish_confluence = True

    counts = await run_synthesis_with_deps(
        deps=deps, window_start=date(2026, 5, 1), window_end=date(2026, 5, 31),
    )
    assert counts.extracted == 0
    deps.finalize_run.assert_awaited_once()
    finalize_kwargs = deps.finalize_run.await_args.kwargs
    assert finalize_kwargs["status"] == "succeeded"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_orchestrator_filters_and_passes_to_embed():
    deps = AsyncMock()
    # INC1 must clear the 0.40 quality_score floor.
    # "good fix " * 50 → ~450 chars, 100 words → 0.30 (length) + 0.20 (Solved Permanently) = 0.50.
    deps.extract.return_value = [
        {"number": "INC1", "short_description": "x", "description": "y",
         "close_notes": "good fix " * 50, "close_code": "Solved (Permanently)",
         "assignment_group": "T", "category": None, "subcategory": None,
         "closed_at": "2026-05-12 14:23:00"},
        {"number": "INC2", "short_description": "x", "description": "y",
         "close_notes": "x", "close_code": "Duplicate",
         "assignment_group": "T", "category": None, "subcategory": None,
         "closed_at": "2026-05-12 14:23:00"},
    ]
    deps.create_run.return_value = "run-uuid"
    deps.embed_batch.return_value = [[0.1] * 768]
    deps.cluster_per_team.return_value = []
    deps.quality_score_floor = 0.40
    deps.min_cluster_size = 5
    deps.min_samples = 3
    deps.min_cohesion = 0.65
    deps.dedup_update_threshold = 0.92
    deps.dedup_review_threshold = 0.80
    deps.max_concurrent_synthesize = 10
    deps.embedding_model_version = "nomic-embed-text:v1.5"
    deps.publish_confluence = True

    counts = await run_synthesis_with_deps(
        deps=deps, window_start=date(2026, 5, 1), window_end=date(2026, 5, 31),
    )
    assert counts.extracted == 2
    assert counts.filtered == 1  # INC2 dropped (Duplicate)
    deps.embed_batch.assert_awaited_once()
