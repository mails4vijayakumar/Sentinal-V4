"""routing-db/app/core/security.py"""
from __future__ import annotations

import hmac

from fastapi import Depends, HTTPException, Request, status

from routing_db.app.core.config import get_settings


def _check(provided: str | None, expected: str, label: str) -> None:
    if not provided:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Missing {label}")
    if not expected:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{label} not configured")
    if not hmac.compare_digest(expected, provided):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Invalid {label}")


async def require_admin(request: Request) -> None:
    """Dependency: validates X-Admin-Token header for mutating endpoints."""
    token = request.headers.get("X-Admin-Token")
    _check(token, get_settings().admin_token, "X-Admin-Token")


async def require_read(request: Request) -> None:
    """Dependency: validates X-Read-Token header (skip if read_token is empty → public)."""
    cfg = get_settings()
    if not cfg.read_token:
        return   # public reads enabled
    token = request.headers.get("X-Read-Token")
    _check(token, cfg.read_token, "X-Read-Token")
