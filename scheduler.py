"""
Incremental #reklama scheduler — runs every 2 hours, 08:00 → 24:00 Tashkent.

Each cycle:
  1. Connect Telethon (through the proxy) and scan ALL subscribed channels for
     today's #reklama posts.
  2. For each post, look at how many NEW views accumulated since the last check
     (ledger). Convert every full 100 new views into 2 visits; carry the
     remainder (<100) to the next cycle.
        new = current_views - accounted_views
        blocks  = new // 100
        visits  = blocks * 2
        accounted_views += blocks * 100   (remainder waits)
  3. Enqueue those visits and run the workers to drain them (small batch).
  4. Notify admins with a per-cycle summary.

A fresh day (first cycle after midnight) clears the task queue and old ledger
rows so counting restarts from zero.
"""
import asyncio
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession

TASHKENT_TZ = timezone(timedelta(hours=5))

# Cycle times (Tashkent). 23:59 is the "24:00" close-out sweep — it must run
# just before midnight so the day's final view deltas are still counted as
# "today" (a sweep at 00:00 would belong to the next day).
CYCLE_TIMES = [(8, 0), (10, 0), (12, 0), (14, 0), (16, 0),
               (18, 0), (20, 0), (22, 0), (23, 59)]

VIEWS_PER_BLOCK = 100      # every full 100 new views ...
VISITS_PER_BLOCK = 2       # ... yields 2 visits (= 2%)
NUM_WORKERS = 8

# Shared auto-run state (read by bot.py for the 📡 Auto status display)
auto_run_info: dict = {
    "status": "idle",        # idle | running | done | failed
    "run_date": None,
    "last_cycle": None,      # "HH:MM" of the most recent cycle
    "tasks": [],             # [{channel, url, views, target_clicks}] cumulative today
    "total_target": 0,       # cumulative visits queued today
    "total_done": 0,         # cumulative visits completed today
    "started_at": None,
    "finished_at": None,
    "error": None,
}

# Reference to the active dispatcher so bot.py can stop it
auto_dispatcher = None

# Set True by bot.py while a manual (▶️) run is active, so a cycle won't run
# workers concurrently with a manual run.
manual_run_active = False


def any_run_active() -> bool:
    """True if either a manual run or an auto #reklama cycle is in progress."""
    return manual_run_active or auto_run_info.get("status") == "running"


def compute_cycle_visits(current_views: int, accounted_views: int) -> tuple[int, int]:
    """
    Given a post's current total views and how many views were already
    converted to visits (accounted_views), return (visits_to_add, new_accounted).

    2 visits per full 100 NEW views; the remainder (<100) stays unaccounted and
    carries to the next cycle. Examples:
      (200,   0) -> (4, 200)      first check, 200 views
      (400, 200) -> (4, 400)      +200 new
      (230,   0) -> (4, 200)      4 visits = 2% of 200; 30 carried
      ( 80,   0) -> (0,   0)      <100 new -> wait
      (280, 200) -> (0, 200) ... wait, 80 new -> wait
    """
    new = max(0, current_views - accounted_views)
    blocks = new // VIEWS_PER_BLOCK
    return blocks * VISITS_PER_BLOCK, accounted_views + blocks * VIEWS_PER_BLOCK


def _next_cycle_dt(now: datetime) -> datetime:
    todays = [now.replace(hour=h, minute=m, second=0, microsecond=0)
              for h, m in CYCLE_TIMES]
    future = [t for t in todays if t > now]
    if future:
        return min(future)
    h, m = CYCLE_TIMES[0]
    return (now + timedelta(days=1)).replace(hour=h, minute=m, second=0, microsecond=0)


def _secs_until_trigger() -> float:
    """Seconds until the next cycle (name kept for bot.py compatibility)."""
    now = datetime.now(TASHKENT_TZ)
    return (_next_cycle_dt(now) - now).total_seconds()


async def run_scheduler_loop(bot, admin_ids, api_id, api_hash, session_string, channels):
    """Runs forever in background; fires every 2 hours 08:00–24:00 Tashkent."""
    from reklama_ledger import init_ledger
    await init_ledger()
    print("[scheduler] Incremental #reklama scheduler started (08:00–24:00, every 2h).")
    while True:
        now = datetime.now(TASHKENT_TZ)
        target = _next_cycle_dt(now)
        secs = (target - now).total_seconds()
        print(f"[scheduler] Next cycle at {target.strftime('%H:%M')} "
              f"(in {int(secs // 3600)}h {int(secs % 3600 // 60)}m)")
        await asyncio.sleep(secs)
        try:
            await _do_reklama_cycle(bot, admin_ids, api_id, api_hash, session_string, channels)
        except Exception as exc:
            print(f"[scheduler] Cycle crashed: {exc}")
            auto_run_info["status"] = "failed"
            auto_run_info["error"] = str(exc)


async def _do_reklama_cycle(bot, admin_ids, api_id, api_hash, session_string, channels):
    global auto_run_info, auto_dispatcher
    from telethon_scraper import fetch_reklama_tasks, build_telethon_proxy
    from db import add_tasks_bulk, clear_all_tasks
    from reklama_ledger import (
        ledger_count_for_date, get_ledger_map, upsert_ledger, purge_ledger_before,
    )

    now = datetime.now(TASHKENT_TZ)
    run_date = now.strftime("%Y-%m-%d")
    cycle_label = now.strftime("%H:%M")

    async def notify(text: str):
        for uid in admin_ids:
            try:
                await bot.send_message(uid, text, parse_mode="HTML")
            except Exception:
                pass

    # First cycle of a new day → reset queue + old ledger so counting restarts.
    if await ledger_count_for_date(run_date) == 0:
        await clear_all_tasks()
        await purge_ledger_before(run_date)
        auto_run_info.update({
            "run_date": run_date, "tasks": [],
            "total_target": 0, "total_done": 0,
            "started_at": now.isoformat(), "error": None,
        })

    auto_run_info["status"] = "running"
    auto_run_info["last_cycle"] = cycle_label
    await notify(f"🕐 <b>{cycle_label} tekshiruv</b> — #reklama postlar ko'rilmoqda...")

    # ---- Scrape (through proxy) ----
    client = TelegramClient(
        StringSession(session_string), api_id, api_hash,
        proxy=build_telethon_proxy(),
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telethon session invalid/expired — re-run auth_telethon.py "
                "and update TELETHON_SESSION_STRING."
            )
        posts = await fetch_reklama_tasks(client, channels)
    except Exception as exc:
        auto_run_info["status"] = "failed"
        auto_run_info["error"] = str(exc)
        await notify(f"❌ <b>{cycle_label} scrape xatosi:</b>\n<code>{exc}</code>")
        return
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    # ---- Apply the incremental rule against the ledger ----
    ledger = await get_ledger_map(run_date)
    new_batches = []          # [(url, visits)] to enqueue this cycle
    added_visits = 0
    posts_with_visits = 0
    now_iso = datetime.now(TASHKENT_TZ).isoformat()

    for p in posts:
        key = (p["channel"], p["post_id"])
        prev = ledger.get(key)
        prev_acc = prev["accounted"] if prev else 0
        prev_total = prev["total_queued"] if prev else 0

        visits, new_acc = compute_cycle_visits(p["views"], prev_acc)
        new_total = prev_total + visits

        await upsert_ledger(run_date, p["channel"], p["post_id"], p["url"],
                            new_acc, new_total, p["views"], now_iso)

        if visits > 0:
            new_batches.append((p["url"], visits))
            added_visits += visits
            posts_with_visits += 1

    # Refresh the cumulative view for the 📡 dashboard
    ledger2 = await get_ledger_map(run_date)
    auto_run_info["tasks"] = [
        {"channel": k[0], "url": v["url"],
         "views": v["last_views"], "target_clicks": v["total_queued"]}
        for k, v in ledger2.items() if v["total_queued"] > 0
    ]
    auto_run_info["total_target"] = sum(v["total_queued"] for v in ledger2.values())

    # ---- Enqueue + drain ----
    if new_batches:
        await add_tasks_bulk(new_batches)
        # Don't run workers concurrently with a manual run — wait it out.
        if manual_run_active:
            await notify("⏳ Qo'lda boshlangan session tugashi kutilmoqda...")
            while manual_run_active:
                await asyncio.sleep(10)
        await _drain_queue(notify)
        await notify(
            f"✅ <b>{cycle_label}:</b> {posts_with_visits} ta postdan "
            f"<b>+{added_visits}</b> visit qo'shildi.\n"
            f"📊 Bugun jami: {auto_run_info['total_done']}/{auto_run_info['total_target']}"
        )
    else:
        if not posts:
            await notify(f"ℹ️ <b>{cycle_label}:</b> bugun #reklama post topilmadi.")
        else:
            await notify(
                f"ℹ️ <b>{cycle_label}:</b> {len(posts)} ta post ko'rildi, "
                f"yangi visit yo'q (100 dan kam yangi ko'rish to'plandi)."
            )

    auto_run_info["status"] = "idle"
    auto_run_info["finished_at"] = datetime.now(TASHKENT_TZ).isoformat()


async def _drain_queue(notify):
    """Run the worker pool to complete all currently-pending tasks."""
    global auto_dispatcher
    from dispatcher import Dispatcher
    from db import mark_all_active, reset_active_to_pending
    from runner_v2 import run_workers_v2
    from pacing import DeadlinePacer

    dispatcher = Dispatcher()
    total = await dispatcher.build_queue()
    if total == 0:
        return

    await mark_all_active()
    auto_dispatcher = dispatcher
    base_done = auto_run_info["total_done"]
    visit_durations: list[float] = []
    pacer = DeadlinePacer(
        deadline_ts=None, total_tasks=total, dispatcher=dispatcher,
        visit_durations=visit_durations, default_delay=8.0,
    )

    async def on_progress(done, total_t):
        auto_run_info["total_done"] = base_done + done

    try:
        done, _ = await run_workers_v2(
            dispatcher, num_workers=NUM_WORKERS, get_delay=pacer.get_delay,
            on_progress=on_progress, visit_durations=visit_durations,
            pace_first_task=False,
        )
        auto_run_info["total_done"] = base_done + done
    except Exception as exc:
        auto_run_info["error"] = str(exc)
        try:
            await reset_active_to_pending()
        except Exception:
            pass
        await notify(f"❌ <b>Visit xatosi:</b>\n<code>{exc}</code>")
    finally:
        auto_dispatcher = None
