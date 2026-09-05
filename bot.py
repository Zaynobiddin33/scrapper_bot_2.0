"""
Telegram bot — Worker Pool orchestration UI.
Mass upload (url:count), dynamic scheduling, global dashboard.
"""
import asyncio
import html
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

# Server may be in any timezone — always use Tashkent (UTC+5) explicitly
TASHKENT_TZ = timezone(timedelta(hours=5))

from aiogram import Bot, Router, types, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.dispatcher.dispatcher import Dispatcher as AiogramDispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    TelegramObject,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from tokens import BOT_TOKEN, AUTHORIZED_USER_IDS
from db import (
    init_db, add_tasks_bulk, get_dashboard, get_totals,
    clear_all_tasks, mark_all_active, reset_active_to_pending,
)
from dispatcher import Dispatcher as TaskDispatcher
from pacing import DeadlinePacer
from runner_v2 import run_workers_v2 as run_workers
import scheduler as _scheduler

# Try importing SERVICE_NAME (optional, not all setups have it)
try:
    from tokens import SERVICE_NAME
except ImportError:
    SERVICE_NAME = None

# Telethon / auto-scheduler config (optional — scheduler won't start if missing)
try:
    from tokens import (
        TELETHON_API_ID, TELETHON_API_HASH,
        TELETHON_SESSION_STRING, REKLAMA_CHANNELS,
    )
    # REKLAMA_CHANNELS is optional: empty = scan ALL channels the account joined.
    _TELETHON_READY = bool(TELETHON_API_ID and TELETHON_API_HASH and TELETHON_SESSION_STRING)
except ImportError:
    TELETHON_API_ID = TELETHON_API_HASH = TELETHON_SESSION_STRING = None
    REKLAMA_CHANNELS = []
    _TELETHON_READY = False


# ==================== AUTH MIDDLEWARE ====================
# This is the idiomatic aiogram 3 approach — runs before handler dispatch,
# doesn't interfere with handler signature introspection.

class AuthMiddleware(BaseMiddleware):
    """Reject messages/callbacks from unauthorized users."""

    async def __call__(self, handler, event: TelegramObject, data: dict):
        user = getattr(event, 'from_user', None)
        if user is None or user.id not in AUTHORIZED_USER_IDS:
            # Silently ignore unauthorized users
            if isinstance(event, types.CallbackQuery):
                await event.answer("Ruxsat yo'q", show_alert=True)
            return
        return await handler(event, data)


# ==================== BOT SETUP ====================
# 15s instead of aiogram's 60s default: if the route to api.telegram.org
# breaks again, a report fails fast and loudly, not 60s per admin in silence.
bot = Bot(token=BOT_TOKEN, session=AiohttpSession(timeout=15))
storage = MemoryStorage()
dp = AiogramDispatcher(storage=storage)
router = Router()

# Register auth middleware on all event types
router.message.middleware(AuthMiddleware())
router.callback_query.middleware(AuthMiddleware())

dp.include_router(router)

# Active run state (module-level)
_active_dispatcher: TaskDispatcher | None = None
_active_run_task: asyncio.Task | None = None


# ==================== FSM STATES ====================
class Form(StatesGroup):
    waiting_urls = State()
    waiting_deadline = State()


# ==================== KEYBOARDS ====================
main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📝 Vazifalar qo'shish")],
        [KeyboardButton(text="▶️ Boshlash"), KeyboardButton(text="📊 Dashboard")],
        [KeyboardButton(text="🗑️ Tozalash"), KeyboardButton(text="🛑 To'xtatish")],
        [KeyboardButton(text="📡 Auto (#reklama)")],
    ],
    resize_keyboard=True,
)

stop_kb = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🛑 TO'XTATISH", callback_data="stop_run")]
])

confirm_clear_kb = InlineKeyboardMarkup(inline_keyboard=[
    [
        InlineKeyboardButton(text="✅ Ha, o'chirish", callback_data="clear_yes"),
        InlineKeyboardButton(text="❌ Bekor qilish", callback_data="clear_no"),
    ]
])


# ==================== HELPERS ====================
def progress_bar(current: int, total: int) -> str:
    """Enhanced progress bar with animation characters."""
    if total == 0:
        return "────── 0%"
    
    pct = min(100, round(current / total * 100))
    filled = 0 if pct == 0 else min(10, (pct + 5) // 10)
    empty = 10 - filled
    
    # Different progress bar styles based on percentage
    if pct < 20:
        bar = "●" * filled + "○" * empty
    elif pct < 50:
        bar = "●" * filled + "○" * empty
    elif pct < 80:
        bar = "■" * filled + "□" * empty
    else:
        bar = "█" * filled + "░" * empty
    
    return f"{bar} {pct}%"


def emoji_for_status(status: str) -> str:
    """Get emoji for task status."""
    emojis = {
        "pending": "⏳",
        "active": "🔄",
        "done": "✅",
        "failed": "❌",
        "stopped": "🛑",
    }
    return emojis.get(status, "⏳")


def parse_urls(text: str) -> list[tuple[str, int]]:
    """
    Parse mass upload format. Flexible parser supports:
      https://example.com : 50
      https://site2.uz:30
      site3.com 20
    """
    items = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        url = None
        count = None

        # Strategy: find the last number in the line, everything before it is the URL
        # This handles all formats: "url : 50", "url:50", "url 50"
        parts = line.rsplit(None, 1)  # split on last whitespace
        if len(parts) == 2 and parts[1].strip().isdigit():
            url = parts[0].strip().rstrip(":").strip()
            count = int(parts[1].strip())
        else:
            # Try colon separator: find last colon followed by digits
            for i in range(len(line) - 1, 0, -1):
                if line[i] == ':' and line[i+1:].strip().isdigit():
                    url = line[:i].strip()
                    count = int(line[i+1:].strip())
                    break

        if url and count and count > 0:
            if not url.startswith("http"):
                url = "https://" + url
            items.append((url, count))

    return items


def format_dashboard(tasks: list[dict], is_running: bool = False) -> str:
    """Enhanced dashboard with better visual feedback and status indicators."""
    if not tasks:
        return (
            "📭 <b>Vazifalar mavjud emas</b>\n\n"
            "📋 <b>Yangi vazifa qo'shish:</b>\n"
            "📝 <b>Vazifalar qo'shish</b> tugmasini bosing"
        )

    lines = ["📊 <b>Dashboard</b> (scrp_v2)\n"]
    lines.append(f"⚡ <b>Parallel browser:</b> 5 ta worker\n")
    total_target = 0
    total_done = 0
    
    # Group by domain for better organization
    domains = {}
    for t in tasks:
        domain = urlparse(t['url']).netloc.replace("www.", "")
        if domain not in domains:
            domains[domain] = []
        domains[domain].append(t)

    # Show ALL tasks - remove the limit of 5 domains and 2 per domain
    domain_count = 0
    for domain, domain_tasks in sorted(domains.items(), key=lambda x: -sum(t['target_clicks'] for t in x[1])):
        domain_count += 1
        lines.append(f"\n🌐 <b>{domain} ({len(domain_tasks)} ta vazifa)</b>")
        
        # Show ALL tasks for this domain (not just 2)
        for t in domain_tasks:
            done = t['current_clicks']
            target = t['target_clicks']
            total_target += target
            total_done += done
            
            status_emoji = {
                'pending': '⏳',
                'active': '🔄',
                'done': '✅',
                'failed': '❌',
            }.get(t['status'], '⏳')
            
            bar = progress_bar(done, target)
            lines.append(
                f"{status_emoji} {done}/{target} {bar}"
            )

    # Show totals if not already counted above
    if total_target == 0:
        for t in tasks:
            total_target += t['target_clicks']
            total_done += t['current_clicks']

    lines.append(f"\n{'🔄 ' if is_running else ''}<b>Jami:</b> {total_done}/{total_target} "
                 f"{progress_bar(total_done, total_target)}")

    return "\n".join(lines)


def get_latest_block_summary(run_started_ts: float | None = None) -> str:
    block_dir = Path(__file__).resolve().parent / "latest_logs" / "blocks"
    if not block_dir.exists():
        return ""

    candidates = sorted(block_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        return ""

    latest = candidates[0]
    if run_started_ts is not None and latest.stat().st_mtime < run_started_ts:
        return ""

    try:
        data = json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return ""

    title = html.escape((data.get("title") or "")[:120])
    snippet = html.escape((data.get("snippet") or "")[:220])
    page_url = html.escape((data.get("url") or "")[:140])
    is_cf = "ha" if data.get("is_cloudflare") else "yo'q"
    artifact = html.escape(str(latest))

    lines = ["", "🚫 <b>Oxirgi block</b>"]
    if title:
        lines.append(f"Sarlavha: <code>{title}</code>")
    if page_url:
        lines.append(f"URL: <code>{page_url}</code>")
    lines.append(f"Cloudflare: <b>{is_cf}</b>")
    if snippet:
        lines.append(f"Snippet: <code>{snippet}</code>")
    lines.append(f"Fayl: <code>{artifact}</code>")
    return "\n".join(lines)


def find_shortlink_domains(items: list[tuple[str, int]]) -> list[str]:
    shorteners = {
        "ya.cc", "bit.ly", "t.co", "tinyurl.com", "goo.gl",
        "cutt.ly", "is.gd", "ow.ly", "rb.gy", "shorturl.at",
    }
    found = []
    for url, _ in items:
        domain = (urlparse(url).netloc or "").replace("www.", "").lower()
        if domain in shorteners and domain not in found:
            found.append(domain)
    return found


# ==================== HANDLERS ====================

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    total_target, total_done, task_count = await get_totals()
    
    # Enhanced start message with status
    if task_count > 0:
        remaining = total_target - total_done
        status = (
            f"\n\n📋 <b>Status:</b> {emoji_for_status('active')} {task_count} vazifa bor "
            f"({remaining} click qoldi)\n"
            f"🚀 <b>Workerlar:</b> 5 ta paralleldan foydalanamiz"
        )
    else:
        status = "\n\n👋 <b>Yangi foydalanuvchi? Qo'llanma:</b>\n"
        status += "1️⃣ <b>Vazifalar qo'shish</b> — linklar va ma'lumotlar\n"
        status += "2️⃣ <b>Boshlash</b> — ishga tushirish\n"
        status += "3️⃣ <b>Dashboard</b> — kuzatuv\n"
        status += "4️⃣ <b>To'xtatish</b> — xohlagan paytda to'xtating"
    
    await message.answer(
        f"👋 <b>Salom! Yandex Metrica Bot</b>\n"
        f"🚀 <b>Parallel Browser System</b>\n\n"
        f"5 ta browser, har biri alohida proxy bilan ishlaydi.\n"
        f"scrp_v2, human simulation, Metrika hit check.\n\n"
        f"{status}",
        parse_mode="HTML",
        reply_markup=main_kb,
    )


# ---- Mass URL Upload ----
@router.message(lambda m: m.text and m.text.startswith("📝"))
async def add_urls_prompt(message: types.Message, state: FSMContext):
    await message.answer(
        "📝 <b>Linklarni yuboring</b>\n\n"
        "Har bir qatorda: <code>url : count</code>\n\n"
        "Misol:\n"
        "<code>https://example.com : 50\n"
        "https://site2.uz : 30\n"
        "site3.com : 20</code>\n\n"
        "Yoki <code>bekor</code> yozing bekor qilish uchun.",
        parse_mode="HTML",
    )
    await state.set_state(Form.waiting_urls)


@router.message(Form.waiting_urls)
async def add_urls_receive(message: types.Message, state: FSMContext):
    if message.text and message.text.lower().strip() in ("bekor", "cancel", "/cancel"):
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_kb)
        return

    items = parse_urls(message.text or "")
    if not items:
        await message.answer(
            "❌ Format xato.\n\nHar qatorda: <code>url : son</code>\nMasalan: <code>site.com : 50</code>",
            parse_mode="HTML",
        )
        return

    count = await add_tasks_bulk(items)
    total_clicks = sum(c for _, c in items)

    lines = [f"✅ <b>{count} ta vazifa qo'shildi</b> (jami {total_clicks} click)\n"]
    for url, clicks in items:
        domain = urlparse(url).netloc or url[:30]
        lines.append(f"  • <code>{domain}</code> — {clicks}")

    short_domains = find_shortlink_domains(items)
    if short_domains:
        joined = ", ".join(short_domains)
        lines.append(
            f"\n⚠️ <b>Ogohlantirish:</b> <code>{joined}</code> shortlink."
            f"\nRedirector bloklansa, worker final sahifaga umuman yetmaydi."
        )

    await state.clear()
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=main_kb)


# ---- Start Run ----
@router.message(lambda m: m.text and m.text.startswith("▶️"))
async def start_run_prompt(message: types.Message, state: FSMContext):
    global _active_run_task
    manual_running = _active_run_task is not None and not _active_run_task.done()
    if manual_running or _scheduler.any_run_active():
        is_auto = _scheduler.auto_run_info.get("status") == "running" and not manual_running
        which = "📡 Avtomatik #reklama session" if is_auto else "▶️ Qo'lda boshlangan session"
        await message.answer(
            f"⚠️ <b>{which} hozir ishlayapti!</b>\n"
            "Yangi session boshlash uchun avval shu session tugashi kerak "
            "(yoki <b>🛑 To'xtatish</b> tugmasini bosing).",
            parse_mode="HTML",
        )
        return

    total_target, total_done, task_count = await get_totals()
    remaining = total_target - total_done

    if remaining <= 0:
        await message.answer(
            "📭 Bajarilishi kerak bo'lgan vazifa yo'q.\n"
            "Avval <b>📝 Vazifalar qo'shish</b> orqali link qo'shing.",
            parse_mode="HTML",
        )
        return

    await message.answer(
        f"📋 <b>{task_count} vazifa, {remaining} click qoldi.</b>\n\n"
        "⏰ Qachongacha bajarilsin?\n\n"
        "Vaqtni yuboring: <code>22:00</code>\n"
        "Yoki: <code>auto</code> (standart tezlik)\n\n"
        "<code>bekor</code> — bekor qilish",
        parse_mode="HTML",
    )
    await state.set_state(Form.waiting_deadline)


@router.message(Form.waiting_deadline)
async def start_run_execute(message: types.Message, state: FSMContext):
    global _active_dispatcher, _active_run_task

    text = (message.text or "").strip().lower()

    if text in ("bekor", "cancel", "/cancel"):
        await state.clear()
        await message.answer("Bekor qilindi.", reply_markup=main_kb)
        return

    deadline = None
    num_workers = 5

    if text != "auto":
        try:
            # Support "22:00", "22.00", "22 00"
            cleaned = text.replace(".", ":").replace(" ", ":")
            parts = [p for p in cleaned.split(":") if p]
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
            now = datetime.now(TASHKENT_TZ)
            deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if deadline <= now:
                deadline += timedelta(days=1)
        except (ValueError, IndexError):
            await message.answer(
                "❌ Format xato.\n<code>HH:MM</code> yoki <code>auto</code> yuboring.",
                parse_mode="HTML",
            )
            return

    # Only now (input validated) claim the run slot. The guard check and the
    # claim happen with no await between them, so the 19:00 auto-run cannot
    # interleave; once claimed, the auto-run defers to us. Auto-run may have
    # fired while the admin was typing the deadline — reject in that case.
    manual_running = _active_run_task is not None and not _active_run_task.done()
    if manual_running or _scheduler.any_run_active():
        await state.clear()
        await message.answer(
            "⚠️ Boshqa session hozir ishlayapti — yangi session boshlab bo'lmaydi.",
            parse_mode="HTML",
            reply_markup=main_kb,
        )
        return
    _scheduler.manual_run_active = True

    await state.clear()
    run_started_ts = datetime.now().timestamp()

    # Build the shuffled task queue. If anything here fails, release the run
    # slot so a DB hiccup doesn't permanently block manual and auto runs.
    try:
        _active_dispatcher = TaskDispatcher()
        total = await _active_dispatcher.build_queue()

        if total == 0:
            _scheduler.manual_run_active = False
            await message.answer("📭 Bajarilishi kerak bo'lgan click yo'q.")
            return

        await mark_all_active()
    except Exception as e:
        _scheduler.manual_run_active = False
        _active_dispatcher = None
        await message.answer(
            f"❌ Boshlashda xato: <code>{html.escape(str(e)[:200])}</code>",
            parse_mode="HTML",
        )
        return

    # Shared list — workers append actual visit durations here
    visit_durations: list[float] = []

    pacer = DeadlinePacer(
        deadline_ts=deadline.timestamp() if deadline else None,
        total_tasks=total,
        dispatcher=_active_dispatcher,
        visit_durations=visit_durations,
        default_delay=8.0,
    )

    def calc_delay() -> float:
        return pacer.get_delay()

    deadline_str = deadline.strftime("%H:%M") if deadline else "auto"
    eta = ""
    if deadline:
        mins = int((deadline - datetime.now(TASHKENT_TZ)).total_seconds() / 60)
        hours = mins // 60
        mins = mins % 60
        eta = f"\n⏱ Taxminiy: {hours}h {mins}m"

    status_msg = await message.answer(
        f"🚀 <b>Ishga tushdi!</b>\n\n"
        f"📋 Vazifalar: {total}\n"
        f"👷 Workerlar: {num_workers} (parallel browsers)\n"
        f"🌐 Proxy: {num_workers} ta alohida session\n"
        f"⏰ Deadline: {deadline_str}{eta}\n\n"
        f"0/{total} {progress_bar(0, total)}",
        parse_mode="HTML",
        reply_markup=stop_kb,
    )

    # Progress callback with enhanced UI
    _last_progress_text = {"v": ""}
    _last_progress_time = {"v": 0}

    async def do_progress_update(completed: int, total_t: int):
        # Rate limit edits to avoid Telegram API errors
        current_time = datetime.now().timestamp()
        if current_time - _last_progress_time["v"] < 3:
            return  # Skip rapid updates
        
        text = (
            f"🔄 <b>Ishlayapti...</b>\n\n"
            f"{completed}/{total_t} {progress_bar(completed, total_t)}\n"
            f"⏰ Deadline: {deadline_str}"
        )
        # Avoid editing with same text (Telegram error)
        if text == _last_progress_text["v"]:
            return
        _last_progress_text["v"] = text
        _last_progress_time["v"] = current_time
        try:
            await status_msg.edit_text(text, parse_mode="HTML", reply_markup=stop_kb)
        except Exception:
            pass

    # Run workers in background with enhanced error handling
    async def run():
        global _active_dispatcher, _active_run_task
        try:
            done, total_t = await run_workers(
                _active_dispatcher,
                num_workers=num_workers,
                get_delay=calc_delay,
                on_progress=do_progress_update,
                visit_durations=visit_durations,
                pace_first_task=pacer.enabled,
            )
            await reset_active_to_pending()
            was_stopped = _active_dispatcher.is_stopped if _active_dispatcher else False
            block_summary = get_latest_block_summary(run_started_ts)
            
            # Enhanced result messages
            try:
                if was_stopped:
                    await status_msg.edit_text(
                        f"🛑 <b>To'xtatildi</b>\n\n"
                        f"✅ Bajarildi: {done}/{total_t} {progress_bar(done, total_t)}\n"
                        f"⏰ Deadline: {deadline_str}"
                        f"{block_summary}",
                        parse_mode="HTML",
                    )
                else:
                    success_rate = (done / total_t * 100) if total_t > 0 else 0
                    await status_msg.edit_text(
                        f"✅ <b>Tugadi! {emoji_for_status('done')}</b>\n\n"
                        f"✅ Bajarildi: {done}/{total_t} {progress_bar(done, total_t)}\n"
                        f"📈 Samaradorlik: {success_rate:.1f}%\n"
                        f"⏰ Deadline: {deadline_str}"
                        f"{block_summary}",
                        parse_mode="HTML",
                    )
            except Exception:
                pass
        except Exception as e:
            await reset_active_to_pending()
            block_summary = get_latest_block_summary(run_started_ts)
            error_msg = str(e)[:200] if str(e) else "Noma'lum xato"
            
            try:
                await status_msg.edit_text(
                    f"❌ <b>Xato yuz berdi</b>\n\n"
                    f"<code>{html.escape(error_msg)}</code>\n"
                    f"⚠️ <b>Yechim:</b>\n"
                    f"• Proxy tekshiring\n"
                    f"• Internet ulanishini tekshiring\n"
                    f"• Boshqatdan boshlang\n\n"
                    f"{block_summary}",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        finally:
            _active_dispatcher = None
            _active_run_task = None
            _scheduler.manual_run_active = False

    # (manual_run_active was already set True after the start guard above)
    _active_run_task = asyncio.create_task(run())


# ---- Stop (button in reply keyboard) ----
@router.message(lambda m: m.text and m.text.startswith("🛑"))
async def stop_from_keyboard(message: types.Message):
    global _active_dispatcher
    stopped = False
    if _active_dispatcher and not _active_dispatcher.is_stopped:
        _active_dispatcher.stop()
        stopped = True
    # Also stop the auto (#reklama) run if one is in progress
    auto = _scheduler.auto_dispatcher
    if auto and not auto.is_stopped:
        auto.stop()
        stopped = True
    if stopped:
        await reset_active_to_pending()
        await message.answer("🛑 <b>To'xtatildi.</b> Workerlar tugatilmoqda...", parse_mode="HTML")
    else:
        await message.answer("Hech narsa ishlamayapti. (Sessionlar yopilayotgan bo'lishi mumkin, biroz kuting!)")


# ---- Stop (inline button on progress message) ----
@router.callback_query(lambda c: c.data == "stop_run")
async def stop_run_handler(callback: types.CallbackQuery):
    global _active_dispatcher
    if _active_dispatcher and not _active_dispatcher.is_stopped:
        _active_dispatcher.stop()
        await reset_active_to_pending()
        await callback.answer("To'xtatilmoqda...", show_alert=True)
        try:
            await callback.message.edit_text(
                (callback.message.text or "") + "\n\n🛑 <b>TO'XTATILDI</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass
    else:
        await callback.answer("Hech narsa ishlamayapti")


# ---- Dashboard ----
@router.message(lambda m: m.text and m.text.startswith("📊"))
async def dashboard_handler(message: types.Message):
    tasks = await get_dashboard()
    is_running = _active_run_task is not None and not _active_run_task.done()
    text = format_dashboard(tasks, is_running=is_running)
    await message.answer(text, parse_mode="HTML", reply_markup=main_kb)


# ---- Clear ----
@router.message(lambda m: m.text and m.text.startswith("🗑️"))
async def clear_prompt(message: types.Message):
    total_target, total_done, task_count = await get_totals()
    if task_count == 0:
        await message.answer("📭 O'chiradigan vazifa yo'q.")
        return

    await message.answer(
        f"⚠️ <b>{task_count} ta vazifani o'chirishni xohlaysizmi?</b>\n"
        f"({total_done}/{total_target} bajarilgan)",
        parse_mode="HTML",
        reply_markup=confirm_clear_kb,
    )


@router.callback_query(lambda c: c.data == "clear_yes")
async def clear_yes(callback: types.CallbackQuery):
    global _active_dispatcher
    if _active_dispatcher and not _active_dispatcher.is_stopped:
        _active_dispatcher.stop()
        await reset_active_to_pending()

    await clear_all_tasks()
    await callback.message.edit_text("🗑️ Barcha vazifalar o'chirildi ✅")
    await callback.answer()


@router.callback_query(lambda c: c.data == "clear_no")
async def clear_no(callback: types.CallbackQuery):
    await callback.message.edit_text("Bekor qilindi.")
    await callback.answer()


# ---- Auto (#reklama) status ----
@router.message(lambda m: m.text and m.text.startswith("📡"))
async def auto_status_handler(message: types.Message):
    info = _scheduler.auto_run_info
    status = info.get("status", "idle")

    if not _TELETHON_READY:
        await message.answer(
            "⚠️ <b>Telethon sozlanmagan</b>\n\n"
            "tokens.py ichiga quyidagilarni kiriting:\n"
            "• <code>TELETHON_API_ID</code>\n"
            "• <code>TELETHON_API_HASH</code>\n"
            "• <code>TELETHON_SESSION_STRING</code> (auth_telethon.py orqali)\n"
            "• <code>REKLAMA_CHANNELS</code>",
            parse_mode="HTML",
        )
        return

    now = datetime.now(TASHKENT_TZ)
    from scheduler import _secs_until_trigger
    secs = _secs_until_trigger()
    next_h, next_m = int(secs // 3600), int((secs % 3600) // 60)

    status_labels = {
        "idle": "😴 Kutilmoqda",
        "running": "🔄 Ishlayapti",
        "done": "✅ Tugadi",
        "failed": "❌ Xato",
    }
    status_str = status_labels.get(status, status)

    lines = [
        f"📡 <b>Auto #reklama</b>\n",
        f"Status: {status_str}",
        f"Keyingi tekshiruv: <b>{next_h}h {next_m}m</b> (har 2 soatda, 08:00-24:00)\n",
    ]

    if info.get("run_date"):
        lines.append(f"Oxirgi run: <b>{info['run_date']}</b>")
        total_target = info.get("total_target", 0)
        total_done = info.get("total_done", 0)
        tasks = info.get("tasks", [])
        if total_target:
            bar = progress_bar(total_done, total_target)
            lines.append(f"Natija: {total_done}/{total_target} {bar}")
        if tasks:
            lines.append(f"\nTopilgan postlar ({len(tasks)} ta):")
            for t in tasks[:10]:
                ch = t.get("channel", "").lstrip("@")
                lines.append(
                    f"  • @{ch} — {t.get('views', 0):,} views → "
                    f"<b>{t.get('target_clicks', 0)}</b> visit"
                )
            if len(tasks) > 10:
                lines.append(f"  ... va yana {len(tasks) - 10} ta")
        if info.get("error"):
            lines.append(f"\n❌ Xato: <code>{html.escape(str(info['error'])[:200])}</code>")

    if REKLAMA_CHANNELS:
        channels_list = ", ".join(str(c) for c in REKLAMA_CHANNELS)
    else:
        channels_list = "Hammasi (account a'zo bo'lgan barcha kanallar)"
    lines.append(f"\nKanallar: <code>{html.escape(channels_list)}</code>")

    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=main_kb)


# ==================== STARTUP / MAIN ====================
async def on_startup():
    await init_db()
    print("Bot started. Database initialized.")
    if _TELETHON_READY:
        asyncio.create_task(
            _scheduler.run_scheduler_loop(
                bot=bot,
                admin_ids=AUTHORIZED_USER_IDS,
                api_id=TELETHON_API_ID,
                api_hash=TELETHON_API_HASH,
                session_string=TELETHON_SESSION_STRING,
                channels=REKLAMA_CHANNELS,
            )
        )
        print(f"[scheduler] Auto #reklama enabled. Channels: {REKLAMA_CHANNELS}")
    else:
        print("[scheduler] Telethon not configured — auto #reklama disabled.")


async def main():
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
