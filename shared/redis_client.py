"""
shared/redis_client.py
======================
Singleton async Redis client used by all agents and routing-db.

Provides:
  - Context store/retrieve (pipeline state as JSON)
  - Agent work queues (LPUSH / BLPOP)
  - SSE event publishing via Redis Streams (XADD)
  - Distributed locks (SET NX EX)
  - Pub/sub helper for internal signals
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis

log = logging.getLogger(__name__)

REDIS_URL       = os.getenv("REDIS_URL", "redis://redis:6379/0")
STREAM_DASHBOARD = "stream:dashboard"     # global fan-out
STREAM_RUN_PREFIX = "stream:run:"         # per-run: stream:run:{run_id}
LOCK_TTL_S      = 300                    # distributed lock TTL (5 min)
CTX_TTL_S       = 3600                   # pipeline context TTL (1 h)
QUEUE_TIMEOUT_S = 5                      # BLPOP block timeout


class RedisClient:
    """Singleton wrapper around aioredis providing high-level helpers."""

    _instance: Optional[RedisClient] = None

    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool
        self._redis: Redis = aioredis.Redis(connection_pool=pool)

    # ── Singleton factory ─────────────────────────────────────────────────────
    @classmethod
    async def get_instance(cls) -> RedisClient:
        if cls._instance is None:
            pool = aioredis.ConnectionPool.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                max_connections=20,
            )
            cls._instance = cls(pool)
            # Smoke-test the connection
            await cls._instance._redis.ping()
            log.info("Redis connected: %s", REDIS_URL)
        return cls._instance

    async def close(self) -> None:
        await self._pool.aclose()
        RedisClient._instance = None

    # ── Pipeline context (run state) ──────────────────────────────────────────
    def _ctx_key(self, run_id: str) -> str:
        return f"run:{run_id}:ctx"

    async def store_context(self, run_id: str, data: Dict[str, Any]) -> None:
        key = self._ctx_key(run_id)
        await self._redis.set(key, json.dumps(data, default=str), ex=CTX_TTL_S)

    async def get_context(self, run_id: str) -> Optional[Dict[str, Any]]:
        raw = await self._redis.get(self._ctx_key(run_id))
        return json.loads(raw) if raw else None

    async def update_context(self, run_id: str, patch: Dict[str, Any]) -> None:
        """Merge patch into the existing context (not atomic — safe for non-critical updates)."""
        ctx = await self.get_context(run_id) or {}
        ctx.update(patch)
        await self.store_context(run_id, ctx)

    async def delete_context(self, run_id: str) -> None:
        await self._redis.delete(self._ctx_key(run_id))

    # ── Agent work queues ─────────────────────────────────────────────────────
    @staticmethod
    def _queue_key(agent_num: int) -> str:
        return f"agent:{agent_num}:queue"

    async def enqueue(self, agent_num: int, run_id: str) -> None:
        await self._redis.lpush(self._queue_key(agent_num), run_id)

    async def dequeue(self, agent_num: int) -> Optional[str]:
        """Block-pop from the queue. Returns run_id or None on timeout."""
        result = await self._redis.brpop(
            self._queue_key(agent_num), timeout=QUEUE_TIMEOUT_S
        )
        if result:
            _, run_id = result
            return run_id
        return None

    # ── SSE Streams ───────────────────────────────────────────────────────────
    async def publish_event(self, event: Dict[str, Any], run_id: Optional[str] = None) -> None:
        """Publish to the global dashboard stream + optionally the per-run stream."""
        payload = {k: json.dumps(v, default=str) if not isinstance(v, str) else v
                   for k, v in event.items()}
        await self._redis.xadd(STREAM_DASHBOARD, payload, maxlen=5000, approximate=True)
        if run_id:
            stream = f"{STREAM_RUN_PREFIX}{run_id}"
            await self._redis.xadd(stream, payload, maxlen=500, approximate=True)
            await self._redis.expire(stream, CTX_TTL_S)

    async def read_stream(
        self,
        stream: str,
        last_id: str = "0",
        count: int = 100,
        block_ms: int = 5000,
    ) -> list[tuple[str, Dict[str, str]]]:
        """Read new entries from a Redis Stream (for SSE fan-out)."""
        results = await self._redis.xread(
            {stream: last_id}, count=count, block=block_ms
        )
        if not results:
            return []
        _, entries = results[0]
        return [(entry_id, fields) for entry_id, fields in entries]

    # ── Distributed locks ─────────────────────────────────────────────────────
    async def acquire_lock(self, key: str, ttl: int = LOCK_TTL_S) -> bool:
        """Try to acquire a distributed lock. Returns True if acquired."""
        result = await self._redis.set(
            f"lock:{key}", "1", nx=True, ex=ttl
        )
        return result is True

    async def release_lock(self, key: str) -> None:
        await self._redis.delete(f"lock:{key}")

    @asynccontextmanager
    async def lock(self, key: str, ttl: int = LOCK_TTL_S) -> AsyncIterator[bool]:
        """Context manager that acquires and releases a distributed lock."""
        acquired = await self.acquire_lock(key, ttl)
        try:
            yield acquired
        finally:
            if acquired:
                await self.release_lock(key)

    # ── Simple pub/sub for agent signals ──────────────────────────────────────
    async def publish_signal(self, channel: str, message: Dict[str, Any]) -> None:
        await self._redis.publish(channel, json.dumps(message, default=str))

    # ── Health check ──────────────────────────────────────────────────────────
    async def ping(self) -> bool:
        try:
            return await self._redis.ping()
        except Exception:
            return False


# ── Convenience singleton getter ──────────────────────────────────────────────
async def get_redis() -> RedisClient:
    return await RedisClient.get_instance()
