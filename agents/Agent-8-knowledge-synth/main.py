from __future__ import annotations

import hmac
import logging
import os
from datetime import date

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from agents.Agent_8_knowledge_synth.config import get_settings  # noqa: F401 — eager import for env validation
from agents.Agent_8_knowledge_synth.schemas import RunCounts

logger = logging.getLogger("agent8")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Agent 8 — Knowledge Synthesizer", version="0.1.0")


class SynthesizeRequest(BaseModel):
    window_start: date
    window_end: date


async def _run_synthesis_now(window_start: date, window_end: date) -> RunCounts:
    """Wire-up of orchestrator + real dependencies. Patched in unit tests.

    Real wiring lands in Task 21; for now this is the seam tests mock.
    """
    raise NotImplementedError("wire-up added in Task 21")


def _require_admin(token_header: str | None) -> None:
    expected = os.environ.get("SYNTH_ADMIN_TOKEN", "")
    if not expected:
        raise HTTPException(
            status_code=503, detail="admin endpoint disabled (no token configured)"
        )
    if not token_header or not hmac.compare_digest(token_header, expected):
        raise HTTPException(status_code=401, detail="unauthorised")


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
