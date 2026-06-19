"""
shared/http_client.py
=====================
Shared httpx.AsyncClient factory with sane defaults for all agents.

Features:
  - Connection pooling (httpx built-in)
  - Configurable timeout per call site
  - Automatic exponential-backoff retry on 429 / 5xx
  - Request ID propagation header
  - Structured error logging
  - System trust store integration via `truststore` so corporate-CA-signed
    TLS interception (Zscaler, Netskope, etc.) works without `verify=False`
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import uuid
from typing import Any, Dict, Optional

import httpx

log = logging.getLogger(__name__)


def _inject_system_trust_store() -> None:
    """
    Inject the OS trust store into Python's ssl module so httpx picks up
    corporate root CAs (Cognizant/Zscaler/Netskope) without disabling TLS
    verification. Idempotent. No-op if disabled or unavailable.

    Disable with `SENTINEL_DISABLE_TRUSTSTORE=1` for environments where
    certifi's bundle is sufficient (e.g. Linux containers in production).
    """
    if os.environ.get("SENTINEL_DISABLE_TRUSTSTORE") == "1":
        return
    if sys.version_info < (3, 10):
        return  # truststore requires Python 3.10+
    try:
        import truststore
        truststore.inject_into_ssl()
        log.debug("System trust store injected via truststore")
    except ImportError:
        log.debug("truststore not installed; using certifi CA bundle")
    except Exception as exc:
        log.warning("Failed to inject system trust store: %s", exc)


_inject_system_trust_store()

# Default timeouts (seconds)
DEFAULT_CONNECT_TIMEOUT  = 5.0
DEFAULT_READ_TIMEOUT     = 30.0
DEFAULT_WRITE_TIMEOUT    = 10.0
DEFAULT_POOL_TIMEOUT     = 5.0

# Retry config
MAX_RETRIES  = 3
RETRY_STATUSES = {429, 500, 502, 503, 504}
RETRY_BACKOFF  = [0.5, 1.5, 4.0]   # seconds between retries


def _make_client(
    base_url: str = "",
    headers: Optional[Dict[str, str]] = None,
    connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
    read_timeout:    float = DEFAULT_READ_TIMEOUT,
) -> httpx.AsyncClient:
    """
    Create a shared httpx.AsyncClient instance.
    Intended to be created at agent startup and reused for the lifetime of the process.
    """
    timeout = httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=DEFAULT_WRITE_TIMEOUT,
        pool=DEFAULT_POOL_TIMEOUT,
    )
    return httpx.AsyncClient(
        base_url=base_url,
        headers=headers or {},
        timeout=timeout,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
    )


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    max_retries: int = MAX_RETRIES,
    request_id: Optional[str] = None,
    **kwargs: Any,
) -> httpx.Response:
    """
    Execute an HTTP request with exponential-backoff retry on transient errors.

    Raises:
        httpx.HTTPStatusError  if the final attempt returns a non-2xx status
        httpx.RequestError     if a network error occurs after all retries
    """
    req_id = request_id or uuid.uuid4().hex[:8]
    if "headers" not in kwargs:
        kwargs["headers"] = {}
    kwargs["headers"]["X-Request-ID"] = req_id

    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = await client.request(method, url, **kwargs)
            if resp.status_code in RETRY_STATUSES and attempt < max_retries - 1:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(
                    "Retry %d/%d for %s %s → HTTP %d (req_id=%s, wait=%.1fs)",
                    attempt + 1, max_retries, method, url, resp.status_code, req_id, wait,
                )
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except httpx.RequestError as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(
                    "Network error on attempt %d/%d for %s %s (req_id=%s, wait=%.1fs): %s",
                    attempt + 1, max_retries, method, url, req_id, wait, exc,
                )
                await asyncio.sleep(wait)
            else:
                raise

    # Shouldn't reach here, but satisfy type checker
    raise last_exc or RuntimeError("Unexpected retry loop exit")


class AgentHTTPClient:
    """
    Per-agent HTTP client wrapper.
    Manage as a context manager to ensure cleanup.
    """
    def __init__(
        self,
        base_url: str = "",
        auth_token: Optional[str] = None,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        headers: Dict[str, str] = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        if extra_headers:
            headers.update(extra_headers)
        self._client = _make_client(base_url=base_url, headers=headers)

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await request_with_retry(self._client, "GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await request_with_retry(self._client, "POST", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any) -> httpx.Response:
        return await request_with_retry(self._client, "PATCH", url, **kwargs)

    async def put(self, url: str, **kwargs: Any) -> httpx.Response:
        return await request_with_retry(self._client, "PUT", url, **kwargs)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AgentHTTPClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()
