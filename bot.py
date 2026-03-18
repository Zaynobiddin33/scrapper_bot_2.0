"""
Telegram bot — Worker Pool orchestration UI.
Mass upload (url:count), dynamic scheduling, global dashboard.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

# Server may be in any timezone — always use Tashkent (UTC+5) explicitly
TASHKENT_TZ = timezone(timedelta(hours=5))

from aiogram import Bot, Router, types, BaseMiddleware
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
from worker import run_workers

# Try importing SERVICE_NAME (optional, not all setups have it)
try:
    from tokens import SERVICE_NAME
except ImportError:
    SERVICE_NAME = None


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
bot = Bot(token=BOT_TOKEN)
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
    if total == 0:
        return "░" * 10 + " 0%"
    pct = min(100, round(current / total * 100))
    filled = pct // 10
    return "█" * filled + "░" * (10 - filled) + f" {pct}%"


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
    """Format all tasks as a dashboard message."""
    if not tasks:
        return "📭 Vazifalar mavjud emas. <b>📝 Vazifalar qo'shish</b> tugmasini bosing."

    lines = ["📊 <b>Dashboard</b>\n"]
    total_target = 0
    total_done = 0

    for t in tasks:
        domain = urlparse(t['url']).netloc or t['url'][:30]
        done = t['current_clicks']
        target = t['target_clicks']
        total_target += target
        total_done += done

        if t['status'] == 'done':
            emoji = "✅"
        elif t['status'] == 'active':
            emoji = "🔄"
        else:
            emoji = "⏳"

        lines.append(
            f"{emoji} <code>{domain}</code>\n"
            f"   {done}/{target} {progress_bar(done, target)}"
        )

    lines.append(f"\n{'🔄 ' if is_running else ''}<b>Jami:</b> {total_done}/{total_target} "
                 f"{progress_bar(total_done, total_target)}")

    return "\n".join(lines)


# ==================== HANDLERS ====================

@router.message(CommandStart())
async def cmd_start(message: types.Message):
    total_target, total_done, task_count = await get_totals()
    if task_count > 0:
        remaining = total_target - total_done
        status = f"\n\n📋 Sizda {task_count} ta vazifa bor ({remaining} click qoldi)"
    else:
        status = ""

    await message.answer(
        f"👋 <b>Salom!</b>\n\n"
        f"Men Yandex Metrica uchun visit simulyatsiya qilaman.\n"
        f"5 ta parallel worker, shuffled queue, stealth browser.{status}",
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

    await state.clear()
    await message.answer("\n".join(lines), parse_mode="HTML", reply_markup=main_kb)


# ---- Start Run ----
@router.message(lambda m: m.text and m.text.startswith("▶️"))
async def start_run_prompt(message: types.Message, state: FSMContext):
    global _active_run_task
    if _active_run_task and not _active_run_task.done():
        await message.answer(
            "⚠️ Jarayon allaqachon ishlayapti!\n"
            "Avval <b>🛑 To'xtatish</b> tugmasini bosing.",
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

    await state.clear()

    # Build the shuffled task queue
    _active_dispatcher = TaskDispatcher()
    total = await _active_dispatcher.build_queue()

    if total == 0:
        await message.answer("📭 Bajarilishi kerak bo'lgan click yo'q.")
        return

    await mark_all_active()

    # Shared list — workers append actual visit durations here
    visit_durations: list[float] = []

    # Dynamic delay calculator — accounts for visit execution time
    def calc_delay() -> float:
        if deadline is None:
            return 8.0  # default 8s per task per worker

        remaining_secs = (deadline - datetime.now(TASHKENT_TZ)).total_seconds()

        # If deadline passed, rush remaining tasks with minimal delay
        if remaining_secs <= 0:
            return 1.0

        remaining_tasks = max(1, _active_dispatcher.remaining)

        # Total cycle time budget per task (visit + delay combined)
        # Formula: time_left / (tasks_left / num_workers)
        #        = time_left * num_workers / tasks_left
        cycle_budget = remaining_secs / remaining_tasks * num_workers

        # Use rolling average of last 30 visits, or 30s estimate initially
        recent = visit_durations[-30:] if visit_durations else []
        avg_visit = sum(recent) / len(recent) if recent else 30.0

        # Delay = cycle budget minus the time the visit itself takes
        delay = cycle_budget - avg_visit

        # Trim visit_durations to prevent unbounded growth (1200+ visits)
        if len(visit_durations) > 60:
            del visit_durations[:-60]

        return max(1.0, min(delay, 600.0))

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
        f"👷 Workerlar: {num_workers}\n"
        f"⏰ Deadline: {deadline_str}{eta}\n\n"
        f"0/{total} {progress_bar(0, total)}",
        parse_mode="HTML",
        reply_markup=stop_kb,
    )

    # Progress callback
    _last_progress_text = {"v": ""}

    async def do_progress_update(completed: int, total_t: int):
        text = (
            f"🔄 <b>Ishlayapti...</b>\n\n"
            f"{completed}/{total_t} {progress_bar(completed, total_t)}\n"
            f"⏰ Deadline: {deadline_str}"
        )
        # Avoid editing with same text (Telegram error)
        if text == _last_progress_text["v"]:
            return
        _last_progress_text["v"] = text
        try:
            await status_msg.edit_text(text, parse_mode="HTML", reply_markup=stop_kb)
        except Exception:
            pass

    # Run workers in background
    async def run():
        try:
            done, total_t = await run_workers(
                _active_dispatcher,
                num_workers=num_workers,
                get_delay=calc_delay,
                on_progress=do_progress_update,
                visit_durations=visit_durations,
            )
            await reset_active_to_pending()
            try:
                await status_msg.edit_text(
                    f"✅ <b>Tugadi!</b>\n\n"
                    f"Bajarildi: {done}/{total_t} {progress_bar(done, total_t)}\n"
                    f"⏰ Deadline: {deadline_str}",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        except Exception as e:
            await reset_active_to_pending()
            try:
                await status_msg.edit_text(
                    f"❌ <b>Xato yuz berdi</b>\n\n<code>{e}</code>",
                    parse_mode="HTML",
                )
            except Exception:
                pass

    _active_run_task = asyncio.create_task(run())


# ---- Stop (button in reply keyboard) ----
@router.message(lambda m: m.text and m.text.startswith("🛑"))
async def stop_from_keyboard(message: types.Message):
    global _active_dispatcher
    if _active_dispatcher and not _active_dispatcher.is_stopped:
        _active_dispatcher.stop()
        await reset_active_to_pending()
        await message.answer("🛑 <b>To'xtatildi.</b> Workerlar tugatilmoqda...", parse_mode="HTML")
    else:
        await message.answer("Hech narsa ishlamayapti.")


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


# ==================== STARTUP / MAIN ====================
async def on_startup():
    await init_db()
    print("Bot started. Database initialized.")


async def main():
    await on_startup()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
