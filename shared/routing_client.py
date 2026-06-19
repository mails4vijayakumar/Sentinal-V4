"""
shared/routing_client.py
========================
Typed HTTP client for the routing-db service (:8000).

Agents call routing-db to:
  - Register new incidents
  - Create and update pipeline runs
  - Record per-agent step results
  - Publish heartbeat / health data

All writes are fire-and-forget when called with `background=True`; agents
do not block the pipeline on routing-db availability.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Dict, Optional
from uuid import UUID

import httpx

from shared.http_client import AgentHTTPClient

log = logging.getLogger(__name__)

ROUTING_DB_URL   = os.getenv("ROUTING_DB_URL", "http://routing-db:8000")
ROUTING_DB_TOKEN = os.getenv("ROUTING_DB_ADMIN_TOKEN", "")


class RoutingClient:
    """Client for the routing-db REST API."""

    def __init__(self) -> None:
        self._http = AgentHTTPClient(
            base_url=ROUTING_DB_URL,
            extra_headers={"X-Admin-Token": ROUTING_DB_TOKEN} if ROUTING_DB_TOKEN else {},
        )

    async def close(self) -> None:
        await self._http.close()

    # ── Incidents ─────────────────────────────────────────────────────────────
    async def upsert_incident(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = await self._http.post("/admin/incidents", json=payload)
        return resp.json()

    async def get_incident(self, external_id: str) -> Optional[Dict[str, Any]]:
        try:
            resp = await self._http.get(f"/reads/incidents/{external_id}")
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    # ── Pipeline runs ─────────────────────────────────────────────────────────
    async def create_run(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = await self._http.post("/admin/runs", json=payload)
        return resp.json()

    async def update_run(self, run_id: str | UUID, patch: Dict[str, Any]) -> None:
        await self._http.patch(f"/admin/runs/{run_id}", json=patch)

    async def complete_run(
        self,
        run_id: str | UUID,
        status: str,
        duration_ms: Optional[int] = None,
    ) -> None:
        await self.update_run(run_id, {
            "status":      status,
            "duration_ms": duration_ms,
        })

    # ── Pipeline steps ────────────────────────────────────────────────────────
    async def record_step(self, run_id: str | UUID, payload: Dict[str, Any]) -> None:
        await self._http.post(f"/admin/runs/{run_id}/steps", json=payload)

    async def update_step(
        self,
        run_id: str | UUID,
        agent_num: int,
        patch: Dict[str, Any],
    ) -> None:
        await self._http.patch(f"/admin/runs/{run_id}/steps/{agent_num}", json=patch)

    # ── Enrichments ───────────────────────────────────────────────────────────
    async def write_enrichment(self, run_id: str | UUID, payload: Dict[str, Any]) -> None:
        await self._http.post(f"/admin/runs/{run_id}/enrichments", json=payload)

    # ── Dashboard reads ───────────────────────────────────────────────────────
    async def get_active_runs(self) -> list[Dict[str, Any]]:
        resp = await self._http.get("/reads/runs?status=running&limit=50")
        return resp.json()

    async def get_run(self, run_id: str | UUID) -> Optional[Dict[str, Any]]:
        try:
            resp = await self._http.get(f"/reads/runs/{run_id}")
            return resp.json()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                return None
            raise

    # ── Health ────────────────────────────────────────────────────────────────
    async def ping(self) -> bool:
        try:
            resp = await self._http.get("/health")
            return resp.status_code == 200
        except Exception:
            return False


# ── Background write helper ───────────────────────────────────────────────────

async def _bg_write(coro: Any) -> None:
    """Run a routing-db write in the background, logging but not raising errors."""
    try:
        await coro
    except Exception as exc:
        log.warning("routing-db background write failed (non-fatal): %s", exc)


def fire_and_forget(coro: Any) -> None:
    """Schedule a coroutine on the running event loop without awaiting it."""
    asyncio.ensure_future(_bg_write(coro))


# ── Module-level singleton ────────────────────────────────────────────────────
_client: Optional[RoutingClient] = None


def get_routing_client() -> RoutingClient:
    global _client
    if _client is None:
        _client = RoutingClient()
    return _client
