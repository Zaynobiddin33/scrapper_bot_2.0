# Scrapper Bot v2 — Async Worker Pool Architecture

## Overview
Telegram bot that distributes Yandex Metrica visit simulation across 5 parallel
async workers sharing a single headless Playwright browser instance.

## Architecture
```
bot.py (Telegram UI — aiogram 3 + FSM + BaseMiddleware auth)
  ├── db.py (SQLite via aiosqlite — explicit BEGIN IMMEDIATE transactions)
  ├── dispatcher.py (Fisher-Yates shuffled async queue)
  └── worker.py (Playwright stealth visits — manual stealth, no third-party)

tokens.py — secrets: proxy, bot token, auth IDs (gitignored)
scrapper.py — legacy SeleniumBase version (kept for reference)
```

## Files

### db.py — SQLite async layer
- `aiosqlite` with explicit `BEGIN IMMEDIATE` transactions
- Schema: `tasks(id, url, target_clicks, current_clicks, status)`
- Status flow: `pending → active → done`
- `increment_click()` is fully atomic: read + update + status check in one transaction

### dispatcher.py — Shuffled task queue
- Flattens: `siteA(50) + siteB(30)` → 80 individual click-jobs
- Fisher-Yates shuffle → uniform random distribution over time
- asyncio.Queue consumed by N workers

### worker.py — Playwright stealth worker
- 1 browser (headless) + N isolated contexts (~60% less RAM than N browsers)
- Manual stealth injection via `page.add_init_script()` (no playwright_stealth dependency)
  - navigator.webdriver, chrome.runtime, plugins, languages, permissions, WebGL, connection, etc.
- Randomized fingerprints: UA, viewport, hardwareConcurrency, deviceMemory, WebGL renderer
- Bezier curve mouse paths via `page.mouse.move()` (all events isTrusted: true)
- Metrica: detect → wait → simulate 15-22s → flush → verify beacon
- New sticky proxy session per visit = new IP (no API call needed)
- on_progress is async — awaited directly, no cross-thread scheduling

### bot.py — Telegram interface (aiogram 3)
- AuthMiddleware (BaseMiddleware) — doesn't break handler signature introspection
- Mass upload: `url : count` per line (flexible parser)
- Dynamic scheduling: user sets deadline (HH:MM), system auto-calculates delays
- Global dashboard: all tasks with progress bars
- FSM states: Form.waiting_urls, Form.waiting_deadline

## Setup
```bash
pip install -r req.txt
playwright install chromium
python bot.py
```

## Proxy
Smartproxy residential (proxy.smartproxy.net:3120)
IP rotation via session ID in username: `{USERNAME}_session-{uuid}`
No API call needed — each new session ID = fresh IP.
