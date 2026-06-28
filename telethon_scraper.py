"""
Telethon scraper: fetches today's #reklama posts from configured channels.
Returns tasks: {url, target_clicks, views, channel, post_id}.
target_clicks = int(views * 0.02), minimum 1.
"""
import re
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.tl.types import (
    MessageEntityTextUrl,
    KeyboardButtonUrl,
    KeyboardButtonUrlAuth,
)

TASHKENT_TZ = timezone(timedelta(hours=5))
_URL_RE = re.compile(r'https?://[^\s<>"\'\]\)\,]+')
# Strict hashtag: exactly #reklama, not #reklamada / #reklama_uz / #reklamapost.
# The negative lookahead rejects any following word char (letters, digits, _).
_REKLAMA_RE = re.compile(r'#reklama(?!\w)', re.IGNORECASE)


def build_telethon_proxy():
    """
    Build a Telethon proxy dict from tokens PROXY_* settings.

    Why: some hosts block Telegram's MTProto data-center IP ranges
    (149.154.x.x), so the user-client (Telethon) cannot connect directly even
    though the Bot API over api.telegram.org still works. Routing Telethon
    through the existing HTTP proxy bypasses that block.

    Returns None when no proxy is configured or python-socks is unavailable,
    in which case Telethon connects directly.
    """
    try:
        import tokens
        from python_socks import ProxyType
    except Exception:
        return None

    host = getattr(tokens, "PROXY_HOST", None)
    port = getattr(tokens, "PROXY_PORT", None)
    if not host or not port:
        return None

    proxy = dict(proxy_type=ProxyType.HTTP, addr=host, port=int(port), rdns=True)
    user = getattr(tokens, "USERNAME", None)
    if user:
        proxy["username"] = user
        proxy["password"] = getattr(tokens, "PASSWORD", None)
    return proxy


async def _resolve_channels(client: TelegramClient, channels: list) -> list:
    """
    Decide which channels to scan.

    If `channels` is non-empty, use that explicit list (usernames or ids).
    If it's empty, auto-discover EVERY broadcast channel the logged-in account
    is subscribed to (broadcast only — that's where #reklama posts with view
    counts live; groups/supergroups are skipped).
    """
    if channels:
        return list(channels)

    discovered = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        # broadcast=True → a channel; megagroup/groups have no per-post views
        if getattr(entity, "broadcast", False):
            discovered.append(entity)
    print(f"[telethon_scraper] Auto-discovered {len(discovered)} channels.")
    return discovered


def _channel_label(entity) -> str:
    """Human-readable channel name for the dashboard/notifications."""
    username = getattr(entity, "username", None)
    if username:
        return "@" + username
    return getattr(entity, "title", None) or str(getattr(entity, "id", entity))


async def fetch_reklama_tasks(client: TelegramClient, channels: list) -> list[dict]:
    """
    Scan channels for today's #reklama posts.
    Returns list of {url, target_clicks, views, channel, post_id}.

    Pass an explicit `channels` list to limit the scan, or an empty list to
    scan every channel the account is subscribed to.
    """
    now = datetime.now(TASHKENT_TZ)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    results = []

    targets = await _resolve_channels(client, channels)

    for ch in targets:
        try:
            # ch is already an entity when auto-discovered; a str/id otherwise
            entity = ch if not isinstance(ch, (str, int)) else await client.get_entity(ch)
            label = _channel_label(entity)
            async for msg in client.iter_messages(entity, limit=300):
                msg_date = msg.date.astimezone(TASHKENT_TZ) if msg.date else None
                if msg_date is None or msg_date < today_start:
                    break
                # Telethon stores caption text in .message/.raw_text too (no .caption attr)
                text = msg.raw_text or msg.message or ""
                if not _REKLAMA_RE.search(text):
                    continue
                urls = _extract_urls(msg, text)
                if not urls:
                    continue
                # One post → one visit task: take only the first URL found.
                url = urls[0]
                views = getattr(msg, "views", 0) or 0
                target = max(1, int(views * 0.02))
                results.append({
                    "url": url,
                    "target_clicks": target,
                    "views": views,
                    "channel": label,
                    "post_id": msg.id,
                })
        except Exception as exc:
            print(f"[telethon_scraper] Channel {ch} error: {exc}")

    return results


def _extract_urls(msg, text: str) -> list[str]:
    """
    Extract URLs from a message. Three sources, all offset-free (entity offsets
    are UTF-16 code units against the raw text — slicing a Python string by them
    breaks on emojis):

      1. Hidden hyperlinks — blue clickable text masking a URL, e.g.
         "Batafsil ➡️"  → MessageEntityTextUrl.url
      2. Visible plain URLs typed in the text → regex over the raw text.
      3. Inline URL buttons under the post (e.g. "🛒 Sotib olish")
         → KeyboardButtonUrl / KeyboardButtonUrlAuth in reply_markup.
    """
    urls: list[str] = []

    def add(u):
        if u and u not in urls:
            urls.append(u)

    # 1. Hidden hyperlinks (blue text → hidden URL)
    if msg.entities:
        for ent in msg.entities:
            if isinstance(ent, MessageEntityTextUrl) and ent.url:
                add(ent.url)

    # 2. Visible URLs typed directly in the text
    for m in _URL_RE.finditer(text):
        add(m.group().rstrip(".,;"))

    # 3. Inline URL buttons under the post
    markup = getattr(msg, "reply_markup", None)
    for row in getattr(markup, "rows", []) or []:
        for btn in getattr(row, "buttons", []) or []:
            if isinstance(btn, (KeyboardButtonUrl, KeyboardButtonUrlAuth)):
                add(getattr(btn, "url", None))

    return urls
