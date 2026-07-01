import pytest
from unittest.mock import AsyncMock, patch
import sys

from agents.Agent_1_dynatrace.adapters.am_entity_resolver import EntityRef, resolve, NEGATIVE_CACHE_TTL
import agents.Agent_1_dynatrace.adapters.am_entity_resolver as resolver_module


@pytest.mark.asyncio
async def test_resolve_prefers_explicit_dt_entity_id_label():
    result = await resolve({"dt_entity_id": "HOST-ABC", "service": "checkout"})
    assert result.entity_id == "HOST-ABC"
    assert result.source == "label"


@pytest.mark.asyncio
async def test_resolve_falls_back_to_routing_db(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(
        return_value=type("M", (), {"entity_id": "HOST-FROM-DB"})()
    )
    monkeypatch.setattr(resolver_module, "get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(resolver_module, "get_routing_client",
                        lambda: routing_mock)

    result = await resolve({"service": "checkout"})
    assert result.entity_id == "HOST-FROM-DB"
    assert result.source == "routing-db"


@pytest.mark.asyncio
async def test_resolve_falls_back_to_dt_name_lookup(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(return_value=None)
    dt_mock = AsyncMock()
    dt_mock.resolve_entity_by_name = AsyncMock(return_value="HOST-FROM-DT")
    monkeypatch.setattr(resolver_module, "get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(resolver_module, "get_routing_client",
                        lambda: routing_mock)
    monkeypatch.setattr(resolver_module, "dt_client", dt_mock)

    result = await resolve({"service": "checkout"})
    assert result.entity_id == "HOST-FROM-DT"
    assert result.entity_name == "checkout"
    assert result.source == "dt-name"


@pytest.mark.asyncio
async def test_resolve_soft_fails_to_unresolved_and_sets_negative_cache(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=None)
    redis_mock.set = AsyncMock()
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock(return_value=None)
    dt_mock = AsyncMock()
    dt_mock.resolve_entity_by_name = AsyncMock(return_value=None)
    monkeypatch.setattr(resolver_module, "get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(resolver_module, "get_routing_client",
                        lambda: routing_mock)
    monkeypatch.setattr(resolver_module, "dt_client", dt_mock)

    result = await resolve({"service": "checkout"})
    assert result.source == "unresolved"
    assert result.entity_name == "checkout"
    redis_mock.set.assert_awaited_once_with(
        "am_entity_miss:checkout", "1", ex=NEGATIVE_CACHE_TTL
    )


@pytest.mark.asyncio
async def test_resolve_short_circuits_on_negative_cache_hit(monkeypatch):
    redis_mock = AsyncMock()
    redis_mock.get = AsyncMock(return_value=b"1")
    routing_mock = AsyncMock()
    routing_mock.get_am_entity = AsyncMock()  # should NOT be called
    monkeypatch.setattr(resolver_module, "get_redis",
                        AsyncMock(return_value=redis_mock))
    monkeypatch.setattr(resolver_module, "get_routing_client",
                        lambda: routing_mock)

    result = await resolve({"service": "checkout"})
    assert result.source == "unresolved-cached"
    routing_mock.get_am_entity.assert_not_awaited()
