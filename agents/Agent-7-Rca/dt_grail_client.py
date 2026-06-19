"""Dynatrace Grail / DQL client for Agent 7 Flow B log enrichment.

OAuth2 client-credentials grant -> bearer token -> DQL query against the
Platform storage API. The bearer token is cached in-memory with a 60 s
safety buffer before expiry; refresh is serialised across coroutines via
an asyncio.Lock so concurrent Flow B requests don't stampede the token URL.

Used by Agent 7's Flow B handler when a SNOW-originated P4/P5 incident
fans out to enrichment. The result is the "matching DT log events" block
of the work note (see CLAUDE.md sections 4.2 and 5.7).

Failure model: every public method raises DTGrailError on auth or transport
failure. Agent 7's caller wraps the call in a try/except and degrades to a
"logs unavailable" line in the work note rather than failing the pipeline.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx


class DTGrailError(Exception):
    """Any failure path in OAuth, DQL execute, or DQL poll."""


@dataclass
class DTGrailConfig:
    platform_base_url: str
    oauth_token_url: str
    client_id: str
    client_secret: str
    scope: str = "storage:logs:read storage:events:read"

    @classmethod
    def from_env(cls) -> "DTGrailConfig":
        required = {
            "DT_PLATFORM_BASE_URL": os.environ.get("DT_PLATFORM_BASE_URL", ""),
            "DT_OAUTH_TOKEN_URL": os.environ.get(
                "DT_OAUTH_TOKEN_URL", "https://sso.dynatrace.com/sso/oauth2/token"
            ),
            "DT_OAUTH_CLIENT_ID": os.environ.get("DT_OAUTH_CLIENT_ID", ""),
            "DT_OAUTH_CLIENT_SECRET": os.environ.get("DT_OAUTH_CLIENT_SECRET", ""),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise DTGrailError(f"Missing required env vars: {', '.join(missing)}")
        return cls(
            platform_base_url=required["DT_PLATFORM_BASE_URL"].rstrip("/"),
            oauth_token_url=required["DT_OAUTH_TOKEN_URL"],
            client_id=required["DT_OAUTH_CLIENT_ID"],
            client_secret=required["DT_OAUTH_CLIENT_SECRET"],
            scope=os.environ.get(
                "DT_OAUTH_SCOPE", "storage:logs:read storage:events:read"
            ),
        )


class DTGrailClient:
    """Async DQL client backed by Dynatrace Platform OAuth."""

    def __init__(
        self,
        config: DTGrailConfig,
        *,
        http: httpx.AsyncClient | None = None,
        owns_http: bool = False,
    ) -> None:
        self._cfg = config
        self._http = http or httpx.AsyncClient(timeout=20.0)
        self._owns_http = http is None or owns_http
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._token_lock = asyncio.Lock()

    @classmethod
    def from_env(cls, **kwargs: Any) -> "DTGrailClient":
        return cls(DTGrailConfig.from_env(), **kwargs)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> "DTGrailClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    # ----------------------------------------------------------------- OAuth

    async def _ensure_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token

        async with self._token_lock:
            if self._token and time.time() < self._token_expiry - 60:
                return self._token

            r = await self._http.post(
                self._cfg.oauth_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._cfg.client_id,
                    "client_secret": self._cfg.client_secret,
                    "scope": self._cfg.scope,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            if r.status_code != 200:
                raise DTGrailError(
                    f"OAuth token request failed: HTTP {r.status_code} - {r.text[:200]}"
                )
            body = r.json()
            self._token = body["access_token"]
            self._token_expiry = time.time() + int(body.get("expires_in", 300))
            return self._token

    # ------------------------------------------------------------------- DQL

    async def query(
        self,
        dql: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        poll_interval: float = 1.0,
        max_polls: int = 10,
    ) -> list[dict[str, Any]]:
        """Execute a DQL query and return the list of result records.

        Small/fast queries return inline (state=SUCCEEDED on the first response).
        Slower queries return a requestToken; we poll up to max_polls times.
        """
        token = await self._ensure_token()
        body: dict[str, Any] = {"query": dql}
        if start:
            body["defaultTimeframeStart"] = _iso(start)
        if end:
            body["defaultTimeframeEnd"] = _iso(end)

        r = await self._http.post(
            f"{self._cfg.platform_base_url}/platform/storage/query/v1/query:execute",
            json=body,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        if r.status_code not in (200, 202):
            raise DTGrailError(
                f"DQL execute failed: HTTP {r.status_code} - {r.text[:300]}"
            )
        payload = r.json()

        if payload.get("state") == "SUCCEEDED" and "result" in payload:
            return payload["result"].get("records", [])

        request_token = payload.get("requestToken")
        if not request_token:
            raise DTGrailError(
                f"DQL response missing both inline result and requestToken: {payload}"
            )

        for _ in range(max_polls):
            await asyncio.sleep(poll_interval)
            pr = await self._http.get(
                f"{self._cfg.platform_base_url}/platform/storage/query/v1/query:poll",
                params={"request-token": request_token},
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                },
            )
            if pr.status_code != 200:
                raise DTGrailError(
                    f"DQL poll failed: HTTP {pr.status_code} - {pr.text[:200]}"
                )
            pp = pr.json()
            state = pp.get("state")
            if state == "SUCCEEDED":
                return pp.get("result", {}).get("records", [])
            if state == "FAILED":
                raise DTGrailError(f"DQL state=FAILED: {pp}")

        raise DTGrailError(f"DQL polling exceeded {max_polls} attempts")

    # ----------------------------------------------- Agent 7 Flow B helper

    async def fetch_logs_around(
        self,
        *,
        entity_id: str,
        ts: datetime,
        window_minutes: int = 15,
        keywords: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return ERROR/WARN log lines for an entity within +/- window_minutes of ts.

        entity_id is matched against host, service, and process_group entity
        types so the caller doesn't need to pre-classify the CMDB CI.
        keywords are joined with OR and each tested with matchesPhrase().
        """
        start = ts - timedelta(minutes=window_minutes)
        end = ts + timedelta(minutes=window_minutes)

        filters = [
            f'dt.entity.host == "{entity_id}" '
            f'or dt.entity.service == "{entity_id}" '
            f'or dt.entity.process_group == "{entity_id}"',
            'loglevel in {"ERROR", "WARN", "SEVERE"}',
        ]
        if keywords:
            kw = " or ".join(f'matchesPhrase(content, "{k}")' for k in keywords)
            filters.append(f"({kw})")

        dql = (
            "fetch logs"
            + "".join(f" | filter {f}" for f in filters)
            + " | sort timestamp desc"
            + f" | limit {limit}"
            + " | fields timestamp, loglevel, content, "
            "dt.entity.service, dt.entity.host"
        )
        return await self.query(dql, start=start, end=end)


def _iso(dt: datetime) -> str:
    """RFC3339 with Z suffix (Dynatrace Platform timestamp format)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
