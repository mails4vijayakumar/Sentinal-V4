from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import asyncpg
import httpx

from agents.Agent_8_knowledge_synth.config import Settings, get_settings
from agents.Agent_8_knowledge_synth import (
    cluster as cl,
    embed as em,
    extract as ex,
    publish as pub,
    queries as q,
    retire as rt,
    synthesize as syn,
    upsert as up,
)
from agents.Agent_8_knowledge_synth.orchestrator import OrchestratorDeps


@dataclass
class RuntimeContext:
    settings: Settings
    pool: asyncpg.Pool
    http: httpx.AsyncClient
    llm_provider: Any  # LLMProvider; concrete class injected at startup
    embedding_client: Any  # exposes async embed_batch(texts)->list[list[float]]
    snow_token_provider: Any  # exposes async get_token()->str
    confluence_token: str
    confluence_space: str


def build_deps(ctx: RuntimeContext | None = None) -> OrchestratorDeps:
    """Pure dependency wiring.

    With ``ctx=None`` returns no-op async/sync callables that satisfy the
    interface — used by unit tests that exercise the orchestrator without I/O.
    With a real ``RuntimeContext`` returns closures over the pool, http client,
    LLM, embedding, SNOW, and Confluence dependencies.
    """
    if ctx is None:
        # Lightweight default for unit tests — settings only, no I/O.
        settings = get_settings()

        async def _noop(*args, **kwargs):
            return None

        async def _noop_list(*args, **kwargs):
            return []

        def _sync_noop(*args, **kwargs):
            return []

        return OrchestratorDeps(
            extract=_noop_list,
            embed_batch=_noop_list,
            cluster_per_team=_sync_noop,
            apply_quality_gate=cl.apply_quality_gate,
            pick_representatives=cl.pick_representatives,
            synthesize_one=_noop,
            find_similar_article=_noop,
            upsert_article_versioned=_noop,
            insert_decision=_noop,
            publish_to_confluence=_noop,
            retire_low_feedback_articles=_noop_list,
            create_run=_noop,
            finalize_run=_noop,
            quality_score_floor=settings.synth_quality_score_floor,
            min_cluster_size=settings.synth_min_cluster_size,
            min_cohesion=settings.synth_min_cluster_cohesion,
            dedup_update_threshold=settings.synth_dedup_update_threshold,
            dedup_review_threshold=settings.synth_dedup_review_threshold,
            max_concurrent_synthesize=settings.synth_max_concurrent_synthesize,
            publish_confluence=settings.synth_publish_confluence,
        )

    s = ctx.settings

    async def _extract(*, window_start, window_end):
        token = await ctx.snow_token_provider.get_token()
        return await ex.snow_extract_closed(
            client=ctx.http, window_start=window_start, window_end=window_end,
            page_size=1000, access_token=token,
        )

    async def _embed_batch(texts):
        return await em.embed_batch(ctx.embedding_client, texts, batch_size=32)

    async def _synth(cluster_result):
        return await syn.synthesize_one(ctx.llm_provider, cluster_result)

    async def _find_similar(*, embedding_title, min_similarity):
        async with ctx.pool.acquire() as conn:
            return await q.find_similar_article(
                conn, embedding_title, min_similarity=min_similarity,
            )

    async def _upsert(**kwargs):
        async with ctx.pool.acquire() as conn:
            return await up.upsert_article_versioned(conn, **kwargs)

    async def _insert_decision(**kwargs):
        async with ctx.pool.acquire() as conn:
            return await q.insert_decision(conn, **kwargs)

    async def _publish(*, article, source_incident_ids):
        return await pub.publish_to_confluence(
            client=ctx.http, space_key=ctx.confluence_space,
            article=article, source_incident_ids=source_incident_ids,
            auth_token=ctx.confluence_token,
        )

    async def _retire(*, months_window, min_recommendations, min_score):
        async with ctx.pool.acquire() as conn:
            return await rt.retire_low_feedback_articles(
                conn, months_window=months_window,
                min_recommendations=min_recommendations, min_score=min_score,
            )

    async def _create_run(*, window_start, window_end):
        async with ctx.pool.acquire() as conn:
            return await q.create_run(
                conn, window_start=window_start, window_end=window_end,
            )

    async def _finalize(*, run_id, status, counts, stage_durations=None, error=None):
        async with ctx.pool.acquire() as conn:
            return await q.finalize_run(
                conn, run_id, status=status, counts=counts,
                stage_durations=stage_durations, error=error,
            )

    return OrchestratorDeps(
        extract=_extract,
        embed_batch=_embed_batch,
        cluster_per_team=cl.cluster_per_team,
        apply_quality_gate=cl.apply_quality_gate,
        pick_representatives=cl.pick_representatives,
        synthesize_one=_synth,
        find_similar_article=_find_similar,
        upsert_article_versioned=_upsert,
        insert_decision=_insert_decision,
        publish_to_confluence=_publish,
        retire_low_feedback_articles=_retire,
        create_run=_create_run,
        finalize_run=_finalize,
        quality_score_floor=s.synth_quality_score_floor,
        min_cluster_size=s.synth_min_cluster_size,
        min_cohesion=s.synth_min_cluster_cohesion,
        dedup_update_threshold=s.synth_dedup_update_threshold,
        dedup_review_threshold=s.synth_dedup_review_threshold,
        max_concurrent_synthesize=s.synth_max_concurrent_synthesize,
        publish_confluence=s.synth_publish_confluence,
    )
