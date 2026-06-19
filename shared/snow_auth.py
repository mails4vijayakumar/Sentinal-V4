"""
shared/snow_auth.py
===================
ServiceNow OAuth2 client-credentials token cache.

Agents 1, 3, 4, 5, 6, 7 all call SNOW APIs.  Rather than each agent
managing its own token lifecycle, this module provides a shared async
cache that auto-refreshes 60 s before expiry.

Usage:
    from shared.snow_auth import get_snow_token
    token = await get_snow_token()
    headers = {"Authorization": f"Bearer {token}"}

Configuration env vars (all required when LLM_PROVIDER != stub):
    SNOW_BASE_URL          https://your-instance.service-now.com
    SNOW_CLIENT_ID         OAuth2 client ID
    SNOW_CLIENT_SECRET     OAuth2 client secret
    SNOW_USERNAME          fallback Basic-Auth user (if OAuth disabled)
    SNOW_PASSWORD          fallback Basic-Auth password
    SNOW_AUTH_MODE         "oauth" (default) | "basic"
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from base64 import b64encode
from typing import Optional

import httpx

log = logging.getLogger(__name__)

SNOW_BASE      = os.getenv("SNOW_BASE_URL", "").rstrip("/")
CLIENT_ID      = os.getenv("SNOW_CLIENT_ID", "")
CLIENT_SECRET  = os.getenv("SNOW_CLIENT_SECRET", "")
SNOW_USER      = os.getenv("SNOW_USERNAME", "")
SNOW_PASS      = os.getenv("SNOW_PASSWORD", "")
AUTH_MODE      = os.getenv("SNOW_AUTH_MODE", "oauth").lower()   # "oauth" | "basic"
REFRESH_BUFFER = 60   # refresh token this many seconds before it expires


class _TokenCache:
    """Thread-safe (asyncio) OAuth2 token cache with auto-refresh."""

    def __init__(self) -> None:
        self._token:   Optional[str] = None
        self._expires: float = 0.0
        self._lock     = asyncio.Lock()

    async def get(self) -> str:
        if AUTH_MODE == "basic":
            return self._basic_creds()

        async with self._lock:
            if self._token and time.time() < self._expires - REFRESH_BUFFER:
                return self._token
            await self._refresh()
        return self._token  # type: ignore[return-value]

    async def _refresh(self) -> None:
        if not (SNOW_BASE and CLIENT_ID and CLIENT_SECRET):
            raise RuntimeError(
                "SNOW_BASE_URL, SNOW_CLIENT_ID and SNOW_CLIENT_SECRET must be set "
                "when SNOW_AUTH_MODE=oauth"
            )
        url = f"{SNOW_BASE}/oauth_token.do"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            })
            resp.raise_for_status()
            data = resp.json()

        self._token   = data["access_token"]
        self._expires = time.time() + int(data.get("expires_in", 1800))
        log.info("ServiceNow OAuth2 token refreshed (expires in %ds)", data.get("expires_in", 1800))

    @staticmethod
    def _basic_creds() -> str:
        """Return a Basic-Auth value (used as a fallback when OAuth is disabled in SNOW)."""
        if not (SNOW_USER and SNOW_PASS):
            raise RuntimeError("SNOW_USERNAME and SNOW_PASSWORD must be set when SNOW_AUTH_MODE=basic")
        creds = b64encode(f"{SNOW_USER}:{SNOW_PASS}".encode()).decode()
        return f"Basic {creds}"

    async def invalidate(self) -> None:
        async with self._lock:
            self._token   = None
            self._expires = 0.0


_cache = _TokenCache()


async def get_snow_token() -> str:
    """Return a valid SNOW bearer token (refreshing as needed)."""
    return await _cache.get()


async def invalidate_snow_token() -> None:
    """Force a token refresh on the next call (e.g., after a 401 response)."""
    await _cache.invalidate()


def snow_auth_headers() -> dict:
    """
    Return sync Basic-Auth headers — useful for startup health-checks
    that run before the async event loop is available.
    Only works when SNOW_AUTH_MODE=basic.
    """
    if AUTH_MODE != "basic":
        raise RuntimeError("snow_auth_headers() only works with SNOW_AUTH_MODE=basic")
    return {"Authorization": _TokenCache._basic_creds()}
