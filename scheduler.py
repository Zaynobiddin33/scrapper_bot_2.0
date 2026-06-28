"""
Daily 19:00 Tashkent #reklama scheduler.

Flow:
  1. Sleep until 19:00 Tashkent time
  2. Connect Telethon, fetch today's #reklama posts from configured channels
  3. Calculate visit target = int(views * 0.02) per post URL
  4. Insert tasks into the DB queue
  5. Start workers automatically (same pipeline as ▶️ button)
  6. Notify admins with summary and progress milestones
"""
import asyncio
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.sessions import StringSession

TASHKENT_TZ = timezone(timedelta(hours=5))
TRIGGER_HOUR = 13
TRIGGER_MINUTE = 0

# Shared auto-run state (read by bot.py for the 📡 Auto status display)
auto_run_info: dict = {
    "status": "idle",       # idle | running | done | failed
    "run_date": None,
    "tasks": [],            # [{channel, url, views, target_clicks}]
    "total_target": 0,
    "total_done": 0,
    "started_at": None,
    "finished_at": None,
    "error": None,
}

# Reference to the active dispatcher so bot.py can stop it
auto_dispatcher = None

# Set True by bot.py while a manual (▶️) run is active. Lets the manual-start
# handler and the scheduler avoid running two visiting sessions at once.
manual_run_active = False


def any_run_active() -> bool:
    """True if either a manual run or the auto #reklama run is in progress."""
    return manual_run_active or auto_run_info.get("status") == "running"


def _secs_until_trigger() -> float:
    now = datetime.now(TASHKENT_TZ)
    target = now.replace(hour=TRIGGER_HOUR, minute=TRIGGER_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def run_scheduler_loop(
    bot,
    admin_ids: list[int],
    api_id: int,
    api_hash: str,
    session_string: str,
    channels: list,
):
    """Runs forever in background; fires at 19:00 Tashkent every day."""
    print("[scheduler] Started — waiting for 19:00 Tashkent.")
    while True:
        secs = _secs_until_trigger()
        h, m = int(secs // 3600), int((secs % 3600) // 60)
        print(f"[scheduler] Next #reklama run in {h}h {m}m")
        await asyncio.sleep(secs)
        await _do_reklama_run(bot, admin_ids, api_id, api_hash, session_string, channels)


async def _do_reklama_run(bot, admin_ids, api_id, api_hash, session_string, channels):
    global auto_run_info, auto_dispatcher
    from telethon_scraper import fetch_reklama_tasks
    from db import add_tasks_bulk, mark_all_active, reset_active_to_pending
    from dispatcher import Dispatcher
    from runner_v2 import run_workers_v2
    from pacing import DeadlinePacer

    now = datetime.now(TASHKENT_TZ)
    auto_run_info.update({
        "status": "running",
        "run_date": now.strftime("%Y-%m-%d"),
        "tasks": [],
        "total_target": 0,
        "total_done": 0,
        "started_at": now.isoformat(),
        "finished_at": None,
        "error": None,
    })

    async def notify(text: str):
        for uid in admin_ids:
            try:
                await bot.send_message(uid, text, parse_mode="HTML")
            except Exception:
                pass

    await notify("🕕 <b>19:00 — #reklama scrape boshlandi...</b>")

    # Connect Telethon and scrape.
    # Use connect()+is_user_authorized() rather than start(): this runs
    # unattended at 19:00, and start() would block on input() for a phone/code
    # if the session were ever invalid. Fail cleanly instead.
    client = TelegramClient(StringSession(session_string), api_id, api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise RuntimeError(
                "Telethon session invalid/expired — re-run auth_telethon.py "
                "and update TELETHON_SESSION_STRING."
            )
        tasks = await fetch_reklama_tasks(client, channels)
    except Exception as exc:
        auto_run_info["status"] = "failed"
        auto_run_info["error"] = str(exc)
        await notify(f"❌ <b>Scrape xatosi:</b>\n<code>{exc}</code>")
        return
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    if not tasks:
        auto_run_info["status"] = "done"
        auto_run_info["finished_at"] = datetime.now(TASHKENT_TZ).isoformat()
        await notify("📭 Bugun #reklama postlar topilmadi.")
        return

    auto_run_info["tasks"] = tasks
    total_target = sum(t["target_clicks"] for t in tasks)
    auto_run_info["total_target"] = total_target

    # Insert into task queue
    items = [(t["url"], t["target_clicks"]) for t in tasks]
    await add_tasks_bulk(items)

    # Build summary for admins
    lines = [
        f"✅ <b>{len(tasks)} ta URL topildi</b> — jami <b>{total_target}</b> visit\n"
    ]
    for t in tasks:
        ch = t["channel"].lstrip("@")
        lines.append(
            f"• @{ch} — {t['views']:,} views → <b>{t['target_clicks']}</b> visit\n"
            f"  <code>{t['url'][:70]}</code>"
        )
    lines.append("\n🚀 <b>Avtomatik run boshlanmoqda...</b>")
    await notify("\n".join(lines))

    # Never run two visiting sessions at once. If an admin started a manual run,
    # wait for it to finish before launching the auto-run workers. Tasks are
    # already queued in the DB, so nothing is lost by waiting.
    if manual_run_active:
        await notify("⏳ Qo'lda boshlangan session tugashi kutilmoqda...")
        while manual_run_active:
            await asyncio.sleep(15)

    # Build dispatcher and start workers
    dispatcher = Dispatcher()
    total = await dispatcher.build_queue()
    if total == 0:
        auto_run_info["status"] = "done"
        auto_run_info["finished_at"] = datetime.now(TASHKENT_TZ).isoformat()
        await notify("📭 Queue bo'sh — hamma task allaqachon done.")
        return

    await mark_all_active()
    auto_dispatcher = dispatcher

    visit_durations: list[float] = []
    pacer = DeadlinePacer(
        deadline_ts=None,
        total_tasks=total,
        dispatcher=dispatcher,
        visit_durations=visit_durations,
        default_delay=8.0,
    )

    last_milestone = {"v": 0}

    async def on_progress(done: int, total_t: int):
        auto_run_info["total_done"] = done
        if total_t == 0:
            return
        pct = done * 100 // total_t
        milestone = (pct // 25) * 25
        if milestone > 0 and milestone != last_milestone["v"]:
            last_milestone["v"] = milestone
            bar = "█" * (milestone // 10) + "░" * (10 - milestone // 10)
            await notify(
                f"📊 <b>Auto run:</b> {done}/{total_t}\n"
                f"<code>{bar}</code> {pct}%"
            )

    try:
        done, total_t = await run_workers_v2(
            dispatcher,
            num_workers=5,
            get_delay=pacer.get_delay,
            on_progress=on_progress,
            visit_durations=visit_durations,
            pace_first_task=False,
        )
        auto_run_info["total_done"] = done
        auto_run_info["status"] = "done"
        auto_run_info["finished_at"] = datetime.now(TASHKENT_TZ).isoformat()
        pct = round(done / total_t * 100) if total_t else 0
        await notify(
            f"✅ <b>Auto run tugadi!</b>\n"
            f"Bajarildi: <b>{done}/{total_t}</b> ({pct}%)"
        )
    except Exception as exc:
        auto_run_info["status"] = "failed"
        auto_run_info["error"] = str(exc)
        await notify(f"❌ <b>Run xatosi:</b>\n<code>{exc}</code>")
        try:
            await reset_active_to_pending()
        except Exception:
            pass
    finally:
        auto_dispatcher = None
