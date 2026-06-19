"""
routing-db/app/db/migrations.py
================================
Lightweight forward-only SQL migration runner.

Applies every `migrations/NNN_*.sql` file in lexical order, tracking which have
run in a `routing.schema_migrations` table. Idempotent — already-applied files
are skipped. Optionally applies `seed/dev_seed.sql` when SEED_DEV=true.

The Postgres container also auto-applies `001_initial.sql` via
docker-entrypoint-initdb.d on first boot; this runner is for environments where
the DB already exists (managed Postgres, restores, CI) and for adding future
migrations without recreating the volume.

Usage:
    python -m routing_db.app.db.migrations            # apply pending migrations
    SEED_DEV=true python -m routing_db.app.db.migrations
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)

# routing-db/migrations and routing-db/seed relative to this file
_THIS_DIR       = Path(__file__).resolve()
_ROUTING_DB_DIR = _THIS_DIR.parents[2]            # …/routing-db
MIGRATIONS_DIR  = _ROUTING_DB_DIR / "migrations"
SEED_DIR        = _ROUTING_DB_DIR / "seed"

# asyncpg needs a plain DSN (no +asyncpg driver suffix)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://sentinel:changeme@postgres:5432/sentinel",
).replace("+asyncpg", "")

_TRACKING_DDL = """
CREATE SCHEMA IF NOT EXISTS routing;
CREATE TABLE IF NOT EXISTS routing.schema_migrations (
    filename    TEXT PRIMARY KEY,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _checksum(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


async def _applied_set(conn: asyncpg.Connection) -> set[str]:
    rows = await conn.fetch("SELECT filename FROM routing.schema_migrations")
    return {r["filename"] for r in rows}


async def run_migrations(seed_dev: bool | None = None) -> int:
    """
    Apply all pending migrations. Returns the number of files applied.
    """
    if seed_dev is None:
        seed_dev = os.getenv("SEED_DEV", "false").lower() == "true"

    conn = await asyncpg.connect(DATABASE_URL)
    applied_count = 0
    try:
        await conn.execute(_TRACKING_DDL)
        already = await _applied_set(conn)

        if not MIGRATIONS_DIR.exists():
            log.warning("No migrations directory at %s", MIGRATIONS_DIR)
            return 0

        files = sorted(MIGRATIONS_DIR.glob("*.sql"))
        for path in files:
            if path.name in already:
                log.info("skip   %s (already applied)", path.name)
                continue
            sql = path.read_text()
            log.info("apply  %s", path.name)
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO routing.schema_migrations (filename, checksum) VALUES ($1, $2)",
                    path.name, _checksum(sql),
                )
            applied_count += 1

        if seed_dev:
            seed_file = SEED_DIR / "dev_seed.sql"
            if seed_file.exists():
                log.info("seed   %s", seed_file.name)
                await conn.execute(seed_file.read_text())
            else:
                log.warning("SEED_DEV set but %s not found", seed_file)

        log.info("Migrations complete: %d applied, %d already present",
                 applied_count, len(already))
        return applied_count
    finally:
        await conn.close()


async def current_version(conn: asyncpg.Connection | None = None) -> str | None:
    """Return the filename of the most recently applied migration (or None)."""
    own = conn is None
    if own:
        conn = await asyncpg.connect(DATABASE_URL)
    try:
        row = await conn.fetchrow(
            "SELECT filename FROM routing.schema_migrations ORDER BY applied_at DESC LIMIT 1"
        )
        return row["filename"] if row else None
    except asyncpg.UndefinedTableError:
        return None
    finally:
        if own:
            await conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run_migrations())
