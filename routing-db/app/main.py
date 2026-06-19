"""routing-db/app/main.py — Routing DB service (:8000)"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routing_db.app.core.config import get_settings
from routing_db.app.core.logging import configure_logging
from routing_db.app.db.connection import dispose_engine
from routing_db.app.api.reads import router as reads_router
from routing_db.app.api.admin import router as admin_router

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_settings()
    configure_logging(cfg.log_level, cfg.service_name)
    log.info("routing-db starting on :%d", cfg.port)
    yield
    await dispose_engine()
    log.info("routing-db shutdown complete")


def create_app() -> FastAPI:
    cfg = get_settings()
    app = FastAPI(
        title="Sentinel Routing DB",
        description="Pipeline state management service for Sentinel Agents",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )
    app.include_router(reads_router)
    app.include_router(admin_router)

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "routing-db"}

    @app.get("/ready")
    async def ready():
        # Could add a real DB ping here
        return {"status": "ready"}

    return app


app = create_app()

if __name__ == "__main__":
    cfg = get_settings()
    uvicorn.run("routing_db.app.main:app", host="0.0.0.0", port=cfg.port, reload=False)
