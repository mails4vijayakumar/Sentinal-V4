from __future__ import annotations

import hmac
import logging
import os
from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from agents.Agent_8_knowledge_synth.config import get_settings
from agents.Agent_8_knowledge_synth.orchestrator import run_synthesis_with_deps
from agents.Agent_8_knowledge_synth.schemas import RunCounts
from agents.Agent_8_knowledge_synth.wiring import RuntimeContext, build_deps

if TYPE_CHECKING:  # pragma: no cover
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

logger = logging.getLogger("agent8")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Agent 8 — Knowledge Synthesizer", version="0.1.0")

# Populated by the FastAPI startup hook. Held at module scope so the manual
# endpoint and the cron-driven job can both reach it.
_scheduler: Optional["AsyncIOScheduler"] = None
_ctx: Optional[RuntimeContext] = None


# ── LLM provider stub ────────────────────────────────────────────────────────
# TODO(agent8): replace with the real LLMProvider factory. The shared
# `LLMProvider` abstraction lives under Agent 1 in this repo, but its concrete
# factory (build_provider_from_env / similar) is not yet exposed as a stable
# symbol. Phase 7 + production deployment will wire the real provider; for now
# the synthesis pipeline raises NotImplementedError if it actually reaches the
# LLM call, which keeps test envs and the FastAPI app loadable while making the
# missing wire-up loud at runtime.
class _NullProvider:
    """Placeholder LLM provider — fails loudly if invoked."""

    async def complete_structured(self, **kwargs):  # noqa: D401
        raise NotImplementedError(
            "LLM provider wiring deferred — set LLM_PROVIDER and wire the real "
            "factory before invoking synthesis in production"
        )


class _SnowTokenAdapter:
    """Wraps shared.snow_auth.get_snow_token in the small object protocol the
    orchestrator deps expect (``async get_token() -> str``)."""

    async def get_token(self) -> str:
        from shared.snow_auth import get_snow_token  # lazy import
        return await get_snow_token()


class SynthesizeRequest(BaseModel):
    window_start: date
    window_end: date


def _previous_month_window() -> tuple[date, date]:
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_of_prev = first_of_this_month - timedelta(days=1)
    first_of_prev = last_of_prev.replace(day=1)
    return first_of_prev, last_of_prev


async def _scheduled_run() -> None:
    start, end = _previous_month_window()
    logger.info("scheduled_run_start", extra={"start": str(start), "end": str(end)})
    await _run_synthesis_now(start, end)


async def _run_synthesis_now(window_start: date, window_end: date) -> RunCounts:
    if _ctx is None:
        raise RuntimeError(
            "RuntimeContext not initialised — service still starting up"
        )
    deps = build_deps(_ctx)
    return await run_synthesis_with_deps(
        deps=deps, window_start=window_start, window_end=window_end,
    )


def _require_admin(token_header: str | None) -> None:
    expected = os.environ.get("SYNTH_ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=503, detail="admin endpoint disabled (no token configured)"
        )
    if not token_header or not hmac.compare_digest(token_header, expected):
        raise HTTPException(status_code=401, detail="unauthorised")


@app.on_event("startup")
async def _startup() -> None:
    """Initialise pool, http client, embedding client, scheduler.

    Each sub-step is best-effort: a failure in (say) pool construction must
    not stop the FastAPI app from coming up — /health and unit-test invocations
    that mock ``_run_synthesis_now`` need to remain serviceable. Real synthesis
    calls will surface the original error at runtime via ``_run_synthesis_now``
    (which checks ``_ctx is None``).
    """
    global _ctx, _scheduler
    try:
        import asyncpg  # local import to keep module loadable in minimal envs
        import httpx
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
        from shared.embedding_client import EmbeddingClient

        s = get_settings()
        pool = await asyncpg.create_pool(s.database_url, min_size=1, max_size=5)
        http = httpx.AsyncClient(timeout=httpx.Timeout(s.snow_api_timeout_seconds))

        _ctx = RuntimeContext(
            settings=s,
            pool=pool,
            http=http,
            # See _NullProvider docstring — real wire-up is a Phase-7 follow-up.
            llm_provider=_NullProvider(),
            embedding_client=EmbeddingClient(),
            snow_token_provider=_SnowTokenAdapter(),
            confluence_token=s.confluence_token,
            confluence_space=s.synth_confluence_space,
        )

        _scheduler = AsyncIOScheduler()
        cron = CronTrigger.from_crontab(s.synth_schedule_cron)
        _scheduler.add_job(
            _scheduled_run, cron, id="monthly_synthesis", replace_existing=True,
        )
        _scheduler.start()
        logger.info("agent8_started", extra={"cron": s.synth_schedule_cron})
    except Exception as exc:  # pragma: no cover — covered by integration tests
        logger.warning(
            "agent8_startup_degraded",
            extra={"error": str(exc), "error_type": type(exc).__name__},
        )


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
    if _ctx is not None:
        await _ctx.http.aclose()
        await _ctx.pool.close()


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "knowledge-synth", "version": "0.1.0"}


@app.post("/jobs/synthesize")
async def manual_run(
    req: SynthesizeRequest,
    x_synth_admin_token: str | None = Header(default=None),
):
    _require_admin(x_synth_admin_token)
    counts = await _run_synthesis_now(req.window_start, req.window_end)
    return {"status": "succeeded", "counts": counts.model_dump()}
