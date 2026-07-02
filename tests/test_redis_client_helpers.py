"""
Public-helper contract for RedisClient.

RedisClient exposed high-level methods (store_context, enqueue, publish_event, ...)
but reached into the underlying redis-py client via `._redis.<op>` for arbitrary
GET/SET/DELETE/SCAN_ITER operations. That leaked implementation detail into
callers (see PR2 _resolve() and PR3 DLQ code). These tests define the public
helper API that retires the private-access pattern.

The helpers are thin delegates — these tests verify the delegation shape and
argument forwarding, nothing more. Behavior fidelity is the responsibility of
redis.asyncio itself and its own test suite.
"""
from __future__ import annotations

from typing import List
from unittest.mock import AsyncMock, MagicMock

import pytest

from shared.redis_client import RedisClient


@pytest.fixture
def client() -> RedisClient:
    """RedisClient instance with a fully-mocked underlying redis.asyncio.Redis."""
    pool = MagicMock()
    rc = RedisClient(pool)
    rc._redis = MagicMock()  # replace the real Redis object with a mock
    return rc


@pytest.mark.asyncio
async def test_get_delegates_to_underlying_redis(client: RedisClient) -> None:
    client._redis.get = AsyncMock(return_value="stored-value")
    result = await client.get("some-key")
    assert result == "stored-value"
    client._redis.get.assert_awaited_once_with("some-key")


@pytest.mark.asyncio
async def test_get_returns_none_when_key_absent(client: RedisClient) -> None:
    client._redis.get = AsyncMock(return_value=None)
    result = await client.get("missing")
    assert result is None


@pytest.mark.asyncio
async def test_set_delegates_with_ex_kwarg(client: RedisClient) -> None:
    client._redis.set = AsyncMock(return_value=True)
    result = await client.set("k", "v", ex=60)
    assert result is True
    client._redis.set.assert_awaited_once_with("k", "v", ex=60, nx=False)


@pytest.mark.asyncio
async def test_set_with_nx_returns_none_when_key_exists(client: RedisClient) -> None:
    # redis-py returns None from SET NX when the key already exists.
    client._redis.set = AsyncMock(return_value=None)
    result = await client.set("existing", "v", ex=60, nx=True)
    assert result is None
    client._redis.set.assert_awaited_once_with("existing", "v", ex=60, nx=True)


@pytest.mark.asyncio
async def test_set_without_ex_forwards_no_ex(client: RedisClient) -> None:
    """A caller that omits ex should not accidentally set ex=None on the underlying call."""
    client._redis.set = AsyncMock(return_value=True)
    await client.set("k", "v")
    client._redis.set.assert_awaited_once_with("k", "v", ex=None, nx=False)


@pytest.mark.asyncio
async def test_delete_single_key(client: RedisClient) -> None:
    client._redis.delete = AsyncMock(return_value=1)
    n = await client.delete("k1")
    assert n == 1
    client._redis.delete.assert_awaited_once_with("k1")


@pytest.mark.asyncio
async def test_delete_variadic_keys(client: RedisClient) -> None:
    client._redis.delete = AsyncMock(return_value=2)
    n = await client.delete("k1", "k2", "k3")
    assert n == 2
    client._redis.delete.assert_awaited_once_with("k1", "k2", "k3")


@pytest.mark.asyncio
async def test_scan_iter_yields_matching_keys(client: RedisClient) -> None:
    async def _aiter(pattern: str):
        assert pattern == "am_dlq:*"
        for k in ("am_dlq:am-x", "am_dlq:am-y"):
            yield k

    client._redis.scan_iter = _aiter

    seen: List[str] = []
    async for key in client.scan_iter("am_dlq:*"):
        seen.append(key)
    assert seen == ["am_dlq:am-x", "am_dlq:am-y"]
