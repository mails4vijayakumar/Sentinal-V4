#!/usr/bin/env python3
"""
scripts/migrate_sqlite_to_pg.py
================================
One-off migration: copies routing data from a legacy SQLite file to PostgreSQL.
Usage:
    SQLITE_PATH=./sentinel_old.db DATABASE_URL=... python scripts/migrate_sqlite_to_pg.py
"""
import asyncio, json, logging, os, sqlite3, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger(__name__)
import asyncpg

SQLITE_PATH  = os.getenv("SQLITE_PATH", "sentinel_old.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("+asyncpg","")

async def main():
    if not os.path.exists(SQLITE_PATH):
        log.error("SQLite file not found: %s", SQLITE_PATH); return
    src = sqlite3.connect(SQLITE_PATH)
    src.row_factory = sqlite3.Row
    pg  = await asyncpg.connect(DATABASE_URL)

    # Migrate incidents
    rows = src.execute("SELECT * FROM incidents").fetchall()
    log.info("Migrating %d incidents…", len(rows))
    for row in rows:
        d = dict(row)
        await pg.execute("""
            INSERT INTO routing.incidents (id, external_id, source, severity, flow, title, description, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (external_id) DO NOTHING
        """, d.get("id"), d.get("external_id"), d.get("source","dynatrace"),
             d.get("severity","P3"), d.get("flow","primary"),
             d.get("title",""), d.get("description"), d.get("created_at"))

    log.info("Migration complete.")
    await pg.close(); src.close()

if __name__ == "__main__":
    asyncio.run(main())
