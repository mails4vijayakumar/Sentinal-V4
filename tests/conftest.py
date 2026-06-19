"""tests/conftest.py — shared fixtures"""
import asyncio, base64, hashlib, hmac, json, os
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("REDIS_URL",           "redis://localhost:6379/0")
os.environ.setdefault("DATABASE_URL",        "postgresql+asyncpg://sentinel:test@localhost:5432/sentinel")
os.environ.setdefault("LLM_PROVIDER",        "stub")
os.environ.setdefault("DT_WEBHOOK_SECRET",   "test-dt-secret-32-chars-long!!!")
os.environ.setdefault("SNOW_WEBHOOK_SECRET", "test-snow-secret-32-chars-long!!")
os.environ.setdefault("SNOW_BASE_URL",       "https://snow.example.com")
os.environ.setdefault("SNOW_AUTH_MODE",      "basic")
os.environ.setdefault("SNOW_USERNAME",       "test")
os.environ.setdefault("SNOW_PASSWORD",       "test")
os.environ.setdefault("ROUTING_DB_URL",      "http://localhost:8000")
os.environ.setdefault("ROUTING_DB_ADMIN_TOKEN", "test-admin-token")


def sign(body: bytes, secret: str) -> str:
    sig = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return base64.b64encode(sig).decode()

def dt_sig(body: bytes)   -> str: return sign(body, os.environ["DT_WEBHOOK_SECRET"])
def snow_sig(body: bytes) -> str: return sign(body, os.environ["SNOW_WEBHOOK_SECRET"])

def dt_payload(problem_id="P-TEST-001", severity="AVAILABILITY") -> dict:
    return {"eventType": "PERFORMANCE_EVENT", "severity": severity, "status": "OPEN",
            "problemId": problem_id, "displayName": f"[Test] {severity} issue",
            "tags": ["app:ehr","env:prod"], "impactedEntities": [], "deploymentEvent": False}

def snow_payload(number="INC9990001", priority="4") -> dict:
    return {"number": number, "priority": priority,
            "short_description": "Test incident", "caller_id": "test@example.com"}

@pytest.fixture
def mock_redis():
    with patch("shared.redis_client.RedisClient.get_instance") as m:
        inst = AsyncMock()
        inst.ping = AsyncMock(return_value=True)
        inst.store_context = AsyncMock()
        inst.get_context   = AsyncMock(return_value=None)
        inst.enqueue       = AsyncMock()
        inst.publish_event = AsyncMock()
        inst.acquire_lock  = AsyncMock(return_value=True)
        inst.release_lock  = AsyncMock()
        inst.__aenter__    = AsyncMock(return_value=True)
        inst.__aexit__     = AsyncMock(return_value=False)
        m.return_value = inst
        yield inst

@pytest.fixture
def mock_routing():
    with patch("shared.routing_client.get_routing_client") as m:
        inst = AsyncMock()
        inst.ping           = AsyncMock(return_value=True)
        inst.upsert_incident = AsyncMock(return_value={"id": "mock-id"})
        inst.create_run     = AsyncMock(return_value={"run_id": "mock-run-id"})
        inst.record_step    = AsyncMock()
        inst.write_enrichment = AsyncMock()
        m.return_value = inst
        yield inst
