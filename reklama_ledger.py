"""
Per-post view ledger for the incremental #reklama scheduler.

Tracks, per (run_date, channel, msg_id), how many of a post's views have
already been converted into visits ('accounted_views'), so each 2-hour cycle
acts only on the NEW views since the last check.

Rule: 2 visits per full 100 new views; the remainder (<100) carries to the
next cycle. Stored in the same SQLite file as the task queue (tasks.db).
"""
import os
import aiosqlite

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tasks.db")


async def init_ledger():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reklama_ledger (
                run_date        TEXT NOT NULL,
                channel         TEXT NOT NULL,
                msg_id          INTEGER NOT NULL,
                url             TEXT NOT NULL,
                accounted_views INTEGER DEFAULT 0,
                total_queued    INTEGER DEFAULT 0,
                last_views      INTEGER DEFAULT 0,
                updated_at      TEXT,
                PRIMARY KEY (run_date, channel, msg_id)
            )
        """)
        await db.commit()


async def ledger_count_for_date(run_date: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM reklama_ledger WHERE run_date = ?", (run_date,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0


async def get_ledger_map(run_date: str) -> dict:
    """Return {(channel, msg_id): {accounted, total_queued, url, last_views}}."""
    out = {}
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT channel, msg_id, url, accounted_views, total_queued, last_views "
            "FROM reklama_ledger WHERE run_date = ?", (run_date,)
        ) as cur:
            for r in await cur.fetchall():
                out[(r["channel"], r["msg_id"])] = {
                    "accounted": r["accounted_views"],
                    "total_queued": r["total_queued"],
                    "url": r["url"],
                    "last_views": r["last_views"],
                }
    return out


async def upsert_ledger(run_date, channel, msg_id, url,
                        accounted_views, total_queued, last_views, updated_at):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("""
            INSERT INTO reklama_ledger
              (run_date, channel, msg_id, url, accounted_views, total_queued, last_views, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_date, channel, msg_id) DO UPDATE SET
              url             = excluded.url,
              accounted_views = excluded.accounted_views,
              total_queued    = excluded.total_queued,
              last_views      = excluded.last_views,
              updated_at      = excluded.updated_at
        """, (run_date, channel, msg_id, url,
              accounted_views, total_queued, last_views, updated_at))
        await db.commit()


async def purge_ledger_before(run_date: str):
    """Delete ledger rows from previous days."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM reklama_ledger WHERE run_date < ?", (run_date,))
        await db.commit()
