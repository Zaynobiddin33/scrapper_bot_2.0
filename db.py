"""
SQLite async database layer for the task queue.
Schema: id, url, target_clicks, current_clicks, status (pending/active/done)
Thread-safe atomic increments via BEGIN IMMEDIATE transactions.
"""
import aiosqlite
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tasks.db")


async def init_db():
    """Create tasks table if it doesn't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                target_clicks INTEGER NOT NULL,
                current_clicks INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending'
            )
        """)
        await db.commit()


async def add_tasks_bulk(items: list[tuple[str, int]]):
    """Insert multiple (url, target_clicks) pairs at once."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            "INSERT INTO tasks (url, target_clicks) VALUES (?, ?)", items
        )
        await db.commit()
    return len(items)


async def get_pending_tasks() -> list[dict]:
    """Fetch all tasks that still need clicks."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, url, target_clicks, current_clicks, status "
            "FROM tasks WHERE current_clicks < target_clicks AND status != 'done'"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def increment_click(task_id: int) -> bool:
    """
    Atomically increment current_clicks and mark done if target reached.
    Returns True if this click was accepted (not over-counted).
    Explicit BEGIN IMMEDIATE wraps the entire read-modify-write as one transaction.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        # Explicit transaction — locks DB from first statement to COMMIT
        await db.execute("BEGIN IMMEDIATE")
        try:
            async with db.execute(
                "SELECT current_clicks, target_clicks FROM tasks WHERE id = ?",
                (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if not row or row[0] >= row[1]:
                    await db.execute("ROLLBACK")
                    return False

            await db.execute(
                "UPDATE tasks SET current_clicks = current_clicks + 1 WHERE id = ?",
                (task_id,)
            )
            await db.execute(
                "UPDATE tasks SET status = 'done' "
                "WHERE id = ? AND current_clicks >= target_clicks",
                (task_id,)
            )
            await db.execute("COMMIT")
            return True
        except Exception:
            try:
                await db.execute("ROLLBACK")
            except Exception:
                pass
            raise


async def get_dashboard() -> list[dict]:
    """Get all tasks with their progress for the dashboard display."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT id, url, target_clicks, current_clicks, status "
            "FROM tasks ORDER BY id"
        ) as cursor:
            return [dict(r) for r in await cursor.fetchall()]


async def get_totals() -> tuple[int, int, int]:
    """Returns (total_target, total_current, total_tasks)."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COALESCE(SUM(target_clicks),0), "
            "COALESCE(SUM(current_clicks),0), COUNT(*) FROM tasks"
        ) as cursor:
            row = await cursor.fetchone()
            return row[0], row[1], row[2]


async def clear_all_tasks():
    """Delete all tasks from the database."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM tasks")
        await db.commit()


async def mark_all_active():
    """Mark all pending tasks as active (when run starts)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tasks SET status = 'active' WHERE status = 'pending'"
        )
        await db.commit()


async def reset_active_to_pending():
    """Reset active tasks back to pending (when run is stopped)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE tasks SET status = 'pending' WHERE status = 'active'"
        )
        await db.commit()
