"""routing-db/app/db/connection.py"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from routing_db.app.core.config import get_settings

_engine: create_async_engine | None = None  # type: ignore[valid-type]
_SessionLocal: async_sessionmaker | None = None


def _init_engine() -> None:
    global _engine, _SessionLocal
    cfg = get_settings()
    _engine = create_async_engine(
        cfg.database_url,
        pool_size=cfg.db_pool_min,
        max_overflow=cfg.db_pool_max - cfg.db_pool_min,
        pool_pre_ping=True,
        echo=cfg.sql_echo,
    )
    _SessionLocal = async_sessionmaker(
        bind=_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


def get_engine():
    if _engine is None:
        _init_engine()
    return _engine


@asynccontextmanager
async def db_session() -> AsyncIterator[AsyncSession]:
    if _SessionLocal is None:
        _init_engine()
    async with _SessionLocal() as session:  # type: ignore[misc]
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def dispose_engine() -> None:
    global _engine, _SessionLocal
    if _engine:
        await _engine.dispose()
        _engine = None
        _SessionLocal = None
