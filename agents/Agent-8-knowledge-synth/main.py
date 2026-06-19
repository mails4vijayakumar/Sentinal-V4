from __future__ import annotations

import logging
from fastapi import FastAPI

from agents.Agent_8_knowledge_synth.config import get_settings

logger = logging.getLogger(__name__)

app = FastAPI(title="Agent 8 - Knowledge Synthesizer", version="0.1.0")


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "knowledge-synth", "version": "0.1.0"}
