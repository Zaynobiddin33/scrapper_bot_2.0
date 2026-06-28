#!/usr/bin/env python3
"""
Run this ONCE to authenticate Telethon and get a session string.
After it prints the string, paste it into tokens.py as
TELETHON_SESSION_STRING.

Usage:
    python3 auth_telethon.py

Flow:
    1. Sends a login code to your Telegram account.
    2. You type that code here.
    3. (If you have a 2-step password, it asks for that too.)
    4. Prints the session string.
"""
import asyncio
from getpass import getpass

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError

# Fill these in before running (get api_id/api_hash from https://my.telegram.org/apps):
API_ID = 37035416
API_HASH = "54665ad837d95ed6e305c2553780cfd1"
PHONE = "+998944287376"


async def main():
    if not API_ID or not API_HASH or not PHONE:
        print("❌ Fill in API_ID, API_HASH, and PHONE at the top of this file first.")
        return

    # Route through the proxy if configured — needed on hosts that block
    # Telegram's MTProto data-center IPs (direct connect times out there).
    try:
        from telethon_scraper import build_telethon_proxy
        proxy = build_telethon_proxy()
    except Exception:
        proxy = None
    if proxy:
        print("🌐 Using proxy for Telethon connection.")

    client = TelegramClient(StringSession(), API_ID, API_HASH, proxy=proxy)
    await client.connect()

    if not await client.is_user_authorized():
        print(f"📲 Sending login code to {PHONE} (check your Telegram app)...")
        sent = await client.send_code_request(PHONE)

        # Telegram sends the code IN the Telegram app (not SMS, usually).
        code = input("Enter the code you received in Telegram: ").strip()
        try:
            await client.sign_in(phone=PHONE, code=code, phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            # Account has 2-step verification (cloud password) enabled.
            pw = getpass("Two-step (cloud) password: ")
            await client.sign_in(password=pw)
        except PhoneCodeInvalidError:
            print("❌ That code was wrong. Re-run the script and try again.")
            await client.disconnect()
            return

    session_str = client.session.save()
    await client.disconnect()

    print("\n" + "=" * 60)
    print("✅ Authentication successful! Your session string:")
    print("=" * 60)
    print(session_str)
    print("=" * 60)
    print("\nPaste this into tokens.py:")
    print(f'TELETHON_SESSION_STRING = "{session_str}"')


if __name__ == "__main__":
    asyncio.run(main())
