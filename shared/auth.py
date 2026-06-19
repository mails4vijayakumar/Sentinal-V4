"""
shared/auth.py
==============
Inter-agent token validation.

Each agent is assigned a static bearer token: X-Agent{N}-Token.
Agents calling other agents must present the correct token.
Tokens are loaded from environment variables at startup.

Usage in FastAPI:
    from shared.auth import require_agent_token
    router = APIRouter(dependencies=[Depends(require_agent_token(1))])
"""
from __future__ import annotations

import hmac
import os
import secrets
from functools import lru_cache
from typing import Callable

from fastapi import Depends, HTTPException, Request, status


@lru_cache(maxsize=8)
def _get_token(agent_num: int) -> str:
    env_key = f"AGENT_{agent_num}_TOKEN"
    token = os.getenv(env_key, "")
    if not token:
        raise RuntimeError(
            f"{env_key} is not set. Every agent must have a unique token."
        )
    return token


def _verify_token(expected: str, provided: str) -> bool:
    """Constant-time token comparison to prevent timing attacks."""
    return hmac.compare_digest(
        expected.encode("utf-8"),
        provided.encode("utf-8"),
    )


def require_agent_token(agent_num: int) -> Callable:
    """
    FastAPI dependency factory.

    Example:
        @router.get("/internal/ping", dependencies=[Depends(require_agent_token(3))])
        async def ping(): ...
    """
    async def _dependency(request: Request) -> None:
        header = request.headers.get("X-Agent-Token") or request.headers.get(
            f"X-Agent{agent_num}-Token"
        )
        if not header:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Missing X-Agent{agent_num}-Token header",
            )
        try:
            expected = _get_token(agent_num)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=str(exc),
            ) from exc

        if not _verify_token(expected, header):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid agent token",
            )

    return _dependency


def generate_token(length: int = 48) -> str:
    """Generate a cryptographically random token for .env setup."""
    return secrets.token_urlsafe(length)


# ── HMAC webhook signature validation ────────────────────────────────────────

import base64
import hashlib


def verify_hmac_signature(
    body: bytes,
    provided_sig: str,
    secret: str,
    algorithm: str = "sha256",
) -> bool:
    """
    Validate an HMAC-SHA256 (base64-encoded) webhook signature.
    Used by Agent 1 to validate DT and SNOW inbound webhooks.
    """
    try:
        digest = hmac.new(
            secret.encode("utf-8"),
            body,
            getattr(hashlib, algorithm),
        ).digest()
        expected = base64.b64encode(digest).decode()
        return hmac.compare_digest(expected, provided_sig)
    except Exception:
        return False
