from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable

from agents.Agent_8_knowledge_synth.dedup import classify_dedup_decision
from agents.Agent_8_knowledge_synth.embed import build_embedding_text
from agents.Agent_8_knowledge_synth.normalize import normalize_incident, quality_score
from agents.Agent_8_knowledge_synth.phi_scrub import scrub_incident_fields
from agents.Agent_8_knowledge_synth.upsert import compute_confidence_score
from agents.Agent_8_knowledge_synth.schemas import (
    ClusterMember, ClusterResult, RunCounts,
)

logger = logging.getLogger("agent8.orchestrator")


async def _maybe_await(result: Any) -> Any:
    """Allow callers to pass either sync or async callables.

    The unit-test seam uses ``AsyncMock`` for the entire dep container, so even
    nominally synchronous deps (``cluster_per_team``, ``apply_quality_gate``,
    ``pick_representatives``) come back as coroutines. The real wiring passes
    the underlying sync functions directly. Coercing either to a value here
    keeps the orchestrator transparent to both worlds.
    """
    if inspect.isawaitable(result):
        return await result
    return result


@dataclass
class OrchestratorDeps:
    """Injectable side-effect dependencies — makes orchestrator testable with mocks."""
    extract: Callable
    embed_batch: Callable
    cluster_per_team: Callable
    apply_quality_gate: Callable
    pick_representatives: Callable
    synthesize_one: Callable
    find_similar_article: Callable
    upsert_article_versioned: Callable
    insert_decision: Callable
    publish_to_confluence: Callable
    retire_low_feedback_articles: Callable
    create_run: Callable
    finalize_run: Callable

    # Config
    quality_score_floor: float = 0.40
    min_cluster_size: int = 5
    min_samples: int = 3
    min_cohesion: float = 0.65
    dedup_update_threshold: float = 0.92
    dedup_review_threshold: float = 0.80
    max_concurrent_synthesize: int = 10
    embedding_model_version: str = "nomic-embed-text:v1.5"
    publish_confluence: bool = True


async def run_synthesis_with_deps(
    *,
    deps,
    window_start: date,
    window_end: date,
) -> RunCounts:
    counts = RunCounts()
    durations: dict[str, float] = {}
    run_id = await deps.create_run(window_start=window_start, window_end=window_end)

    try:
        # 1. Extract
        t0 = time.perf_counter()
        raw_incidents = await deps.extract(window_start=window_start, window_end=window_end)
        durations["extract"] = time.perf_counter() - t0
        counts.extracted = len(raw_incidents)

        # 2-4. Normalize, scrub, quality-filter
        t0 = time.perf_counter()
        clean: list[dict[str, Any]] = []
        for raw in raw_incidents:
            norm = normalize_incident(raw)
            if norm is None:
                continue
            scrubbed, _ = scrub_incident_fields(norm)
            if quality_score(scrubbed) < deps.quality_score_floor:
                continue
            clean.append(scrubbed)
        counts.filtered = len(raw_incidents) - len(clean)
        durations["filter"] = time.perf_counter() - t0

        if not clean:
            await deps.finalize_run(
                run_id=run_id, status="succeeded",
                counts=counts.model_dump(), stage_durations=durations,
            )
            return counts

        # 5. Embed
        t0 = time.perf_counter()
        texts = [build_embedding_text(i) for i in clean]
        embeddings = await deps.embed_batch(texts)
        durations["embed"] = time.perf_counter() - t0

        # 6. Cluster per team
        t0 = time.perf_counter()
        raw_clusters = await _maybe_await(deps.cluster_per_team(
            clean, embeddings,
            min_cluster_size=deps.min_cluster_size, min_samples=deps.min_samples,
        ))
        # 7. Quality gate
        good_clusters = await _maybe_await(
            deps.apply_quality_gate(raw_clusters, min_cohesion=deps.min_cohesion)
        )
        counts.clustered = len(good_clusters)
        durations["cluster"] = time.perf_counter() - t0

        # 8-9. Pick reps + synthesize in parallel
        t0 = time.perf_counter()
        sem = asyncio.Semaphore(deps.max_concurrent_synthesize)

        async def _synth_one(cluster_raw):
            async with sem:
                cluster_vecs = [embeddings[idx] for idx in cluster_raw.member_indices]
                # find local medoid index
                local_medoid = cluster_raw.member_indices.index(cluster_raw.medoid_index)
                rep_indices_local = await _maybe_await(deps.pick_representatives(
                    cluster_vecs, medoid_local_index=local_medoid, k=4,
                ))
                members = [
                    ClusterMember(
                        incident_id=clean[cluster_raw.member_indices[i]]["number"],
                        short_description=clean[cluster_raw.member_indices[i]]["short_description"],
                        description=clean[cluster_raw.member_indices[i]]["description"],
                        resolution_notes=clean[cluster_raw.member_indices[i]]["close_notes"],
                        close_code=clean[cluster_raw.member_indices[i]]["close_code"],
                        assignment_group=clean[cluster_raw.member_indices[i]]["assignment_group"],
                        category=clean[cluster_raw.member_indices[i]].get("category"),
                        subcategory=clean[cluster_raw.member_indices[i]].get("subcategory"),
                        closed_at=clean[cluster_raw.member_indices[i]]["closed_at_iso"],
                        quality_score=quality_score(clean[cluster_raw.member_indices[i]]),
                    )
                    for i in rep_indices_local
                ]
                cluster_result = ClusterResult(
                    signature=cluster_raw.signature,
                    assignment_group=cluster_raw.assignment_group,
                    members=members,
                    cohesion=cluster_raw.cohesion,
                    medoid_index=0,  # medoid is index 0 of representatives by construction
                )
                article = await deps.synthesize_one(cluster_result)
                return cluster_raw, cluster_result, article

        synth_results = await asyncio.gather(*[_synth_one(c) for c in good_clusters])
        durations["synthesize"] = time.perf_counter() - t0

        # 10-12. Dedup, upsert, publish
        t0 = time.perf_counter()
        for cluster_raw, cluster_result, article in synth_results:
            if article is None:
                counts.skipped += 1
                await deps.insert_decision(
                    run_id=run_id, cluster_signature=cluster_raw.signature,
                    decision="skip", article_id=None, similarity_score=None,
                    notes="synthesis returned None",
                )
                continue

            # Embed the new article's title for dedup
            title_text = f"{article.title}\n{article.problem_summary}"
            title_emb_list = await deps.embed_batch([title_text])
            title_emb = title_emb_list[0]
            full_emb_list = await deps.embed_batch([build_embedding_text({
                "short_description": article.title,
                "description": article.problem_summary,
                "close_notes": " ".join(s.action for s in article.resolution_steps),
            })])
            full_emb = full_emb_list[0]

            existing = await deps.find_similar_article(
                embedding_title=title_emb,
                min_similarity=deps.dedup_review_threshold,
            )
            sim = existing["similarity"] if existing else None
            decision = classify_dedup_decision(
                sim,
                update_threshold=deps.dedup_update_threshold,
                review_threshold=deps.dedup_review_threshold,
            )

            confidence = compute_confidence_score(
                cluster_cohesion=cluster_raw.cohesion,
                source_incident_count=len(cluster_raw.member_indices),
                llm_self_rating=article.confidence_self_rating,
                rolling_feedback_score=None,
            )

            if decision == "review":
                counts.flagged_for_review += 1
                await deps.insert_decision(
                    run_id=run_id, cluster_signature=cluster_raw.signature,
                    decision="review", article_id=existing["id"], similarity_score=sim,
                    notes="gray-zone similarity; human review required",
                )
                continue

            # Use existing signature when updating, new one when creating
            target_signature = (
                existing["cluster_signature"] if decision == "update" else cluster_raw.signature
            )
            article_id = await deps.upsert_article_versioned(
                cluster_signature=target_signature, article=article,
                embedding_title=title_emb, embedding_full=full_emb,
                cluster_cohesion=cluster_raw.cohesion,
                source_incident_ids=[clean[i]["number"] for i in cluster_raw.member_indices],
                embedding_model_version=deps.embedding_model_version,
                confidence_score=confidence,
            )
            if decision == "update":
                counts.updated += 1
            else:
                counts.created += 1
            await deps.insert_decision(
                run_id=run_id, cluster_signature=target_signature,
                decision=decision, article_id=article_id, similarity_score=sim,
            )

            if deps.publish_confluence:
                try:
                    await deps.publish_to_confluence(
                        article=article,
                        source_incident_ids=[clean[i]["number"] for i in cluster_raw.member_indices],
                    )
                except Exception as e:
                    logger.warning(
                        "publish_failed",
                        extra={"sig": target_signature, "error": str(e)},
                    )

        durations["upsert_publish"] = time.perf_counter() - t0

        # 13. Retire low-utility
        t0 = time.perf_counter()
        retired = await deps.retire_low_feedback_articles(
            months_window=6, min_recommendations=10, min_score=0.30,
        )
        counts.retired = len(retired) if retired is not None else 0
        durations["retire"] = time.perf_counter() - t0

        # 14. Summary
        await deps.finalize_run(
            run_id=run_id, status="succeeded",
            counts=counts.model_dump(), stage_durations=durations,
        )
        logger.info(
            "synthesis_complete",
            extra={"run_id": str(run_id), "counts": counts.model_dump()},
        )
        return counts

    except Exception as e:
        logger.exception("synthesis_failed", extra={"run_id": str(run_id)})
        await deps.finalize_run(
            run_id=run_id, status="failed",
            counts=counts.model_dump(), stage_durations=durations, error=str(e),
        )
        raise
