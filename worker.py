"""
Playwright-based async worker pool.

KEY DESIGN: Each visit launches its own browser with BROWSER-LEVEL proxy.
Context-level proxy is experimental on Chromium macOS/Windows and silently fails
(pages hang at about:blank). Browser-level proxy is 100% reliable.

Architecture:
  1 playwright instance (shared)
  → 5 async workers (each manages own browser lifecycle)
    → per visit: launch browser(proxy) → new_context(fingerprint) → visit → close all

Stealth: headed mode (headless=False) + comprehensive init_script patches.
All mouse/scroll events produce isTrusted:true via Playwright's CDP-based input.
"""
import asyncio
import random
import math
import time
import uuid
import os
from urllib.parse import urlparse
from typing import Callable

from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page

from tokens import PROXY_HOST, PROXY_PORT, USERNAME, PASSWORD
from db import increment_click
from dispatcher import Dispatcher


# ==================== STEALTH INIT SCRIPT ====================
# Comprehensive patches — runs BEFORE any page JS via add_init_script().
# Covers all known Yandex Metrica detection vectors.

STEALTH_SCRIPT = """
// ===== 1. navigator.webdriver (primary detection) =====
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
delete navigator.__proto__.webdriver;

// ===== 2. chrome.runtime (Metrica checks window.chrome) =====
if (!window.chrome) window.chrome = {};
if (!window.chrome.runtime) {
    window.chrome.runtime = {
        connect: function(){},
        sendMessage: function(){},
        onMessage: {addListener: function(){}, removeListener: function(){}},
        onConnect: {addListener: function(){}, removeListener: function(){}}
    };
}
if (!window.chrome.loadTimes) {
    window.chrome.loadTimes = function() {
        return {
            commitLoadTime: Date.now() / 1000,
            connectionInfo: 'http/1.1',
            finishDocumentLoadTime: Date.now() / 1000 + 0.1,
            finishLoadTime: Date.now() / 1000 + 0.2,
            firstPaintAfterLoadTime: 0,
            firstPaintTime: Date.now() / 1000 + 0.05,
            navigationType: 'Other',
            npnNegotiatedProtocol: 'unknown',
            requestTime: Date.now() / 1000 - 0.5,
            startLoadTime: Date.now() / 1000 - 0.4,
            wasAlternateProtocolAvailable: false,
            wasFetchedViaSpdy: false,
            wasNpnNegotiated: false
        };
    };
}
if (!window.chrome.csi) {
    window.chrome.csi = function() {
        return {
            onloadT: Date.now(),
            pageT: Math.random() * 1000 + 500,
            startE: Date.now() - Math.random() * 2000,
            tran: 15
        };
    };
}

// ===== 3. Permissions API =====
const origQuery = window.navigator.permissions.query.bind(window.navigator.permissions);
window.navigator.permissions.query = (params) => {
    if (params.name === 'notifications')
        return Promise.resolve({state: Notification.permission});
    return origQuery(params);
};

// ===== 4. Plugins (empty = bot) =====
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            {name:'Chrome PDF Plugin', filename:'internal-pdf-viewer',
             description:'Portable Document Format', length:1,
             item: function(i){return this}, namedItem: function(n){return this}},
            {name:'Chrome PDF Viewer', filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai',
             description:'', length:1,
             item: function(i){return this}, namedItem: function(n){return this}},
            {name:'Native Client', filename:'internal-nacl-plugin',
             description:'', length:1,
             item: function(i){return this}, namedItem: function(n){return this}}
        ];
        arr.item = (i) => arr[i];
        arr.namedItem = (n) => arr.find(p => p.name === n);
        arr.refresh = () => {};
        return arr;
    }
});

// ===== 5. Languages =====
Object.defineProperty(navigator, 'languages', {
    get: () => ['uz-UZ', 'uz', 'ru-RU', 'ru', 'en-US', 'en']
});

// ===== 6. Connection =====
if (navigator.connection === undefined) {
    Object.defineProperty(navigator, 'connection', {
        get: () => ({effectiveType:'4g', rtt:50, downlink:10, saveData:false})
    });
}

// ===== 7. Screen dimensions (headless gives outerHeight=0) =====
if (window.outerHeight === 0 || window.outerWidth === 0) {
    Object.defineProperty(window, 'outerHeight', {get: () => window.innerHeight + 85});
    Object.defineProperty(window, 'outerWidth', {get: () => window.innerWidth + 15});
}
Object.defineProperty(screen, 'colorDepth', {get: () => 24});
Object.defineProperty(screen, 'pixelDepth', {get: () => 24});

// ===== 8. Notification =====
if (!window.Notification) {
    window.Notification = function(){};
    window.Notification.permission = 'default';
    window.Notification.requestPermission = () => Promise.resolve('default');
}

// ===== 9. MediaDevices (headless returns empty) =====
if (navigator.mediaDevices && navigator.mediaDevices.enumerateDevices) {
    const origEnum = navigator.mediaDevices.enumerateDevices.bind(navigator.mediaDevices);
    navigator.mediaDevices.enumerateDevices = async function() {
        const devices = await origEnum();
        if (devices.length === 0) {
            return [
                {deviceId:'default', kind:'audioinput', label:'Default', groupId:'default'},
                {deviceId:'default', kind:'videoinput', label:'USB Camera', groupId:'cam'},
                {deviceId:'default', kind:'audiooutput', label:'Default', groupId:'default'},
            ];
        }
        return devices;
    };
}

// ===== 10. Iframe contentWindow detection =====
const origContentWindow = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
    get: function() {
        const win = origContentWindow.get.call(this);
        if (win) {
            try { win.chrome = window.chrome; } catch(e) {}
        }
        return win;
    }
});

// ===== 11. Date.getTimezoneOffset consistency =====
Date.prototype.getTimezoneOffset = function() { return -300; }; // UTC+5 (Tashkent)

// ===== 12. Prevent automation detection via stack traces =====
const origError = Error;
const origCaptureStackTrace = Error.captureStackTrace;
if (origCaptureStackTrace) {
    Error.captureStackTrace = function(obj, fn) {
        origCaptureStackTrace.call(this, obj, fn);
        if (obj.stack) {
            obj.stack = obj.stack.replace(/playwright|puppeteer|selenium|webdriver/gi, 'chrome');
        }
    };
}
"""


async def apply_stealth(page: Page, fp: dict):
    """Apply stealth + per-session fingerprint overrides before navigation."""
    await page.add_init_script(STEALTH_SCRIPT)
    await page.add_init_script(f"""
        Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {fp['hw_concurrency']}}});
        Object.defineProperty(navigator, 'deviceMemory', {{get: () => {fp['device_memory']}}});

        // WebGL fingerprint
        const gp1 = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(p) {{
            if (p === 37445) return '{fp["webgl_vendor"]}';
            if (p === 37446) return '{fp["webgl_renderer"]}';
            return gp1.call(this, p);
        }};
        if (typeof WebGL2RenderingContext !== 'undefined') {{
            const gp2 = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function(p) {{
                if (p === 37445) return '{fp["webgl_vendor"]}';
                if (p === 37446) return '{fp["webgl_renderer"]}';
                return gp2.call(this, p);
            }};
        }}
    """)


# ==================== FINGERPRINT POOLS ====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

VIEWPORTS = [
    {"width": 1280, "height": 720},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 800},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]

HW_CONCURRENCY = [4, 4, 6, 8, 8]
DEVICE_MEMORY = [4, 4, 8, 8, 16]
WEBGL_CONFIGS = [
    {"vendor": "Google Inc. (Intel)", "renderer": "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
    {"vendor": "Google Inc. (NVIDIA)", "renderer": "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
    {"vendor": "Google Inc. (Intel)", "renderer": "ANGLE (Intel, Intel(R) Iris(R) Plus Graphics 640 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
    {"vendor": "Google Inc. (AMD)", "renderer": "ANGLE (AMD, AMD Radeon Pro 5500M OpenGL Engine, OpenGL 4.1)"},
    {"vendor": "Google Inc. (Intel)", "renderer": "ANGLE (Intel, Intel(R) HD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)"},
]


# ==================== PROXY ====================
def new_proxy():
    """New sticky session = new IP. Session ID in username = IP rotation."""
    sid = uuid.uuid4().hex[:12]
    return {
        "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
        "username": f"{USERNAME}_session-{sid}",
        "password": PASSWORD,
    }


def random_fingerprint():
    webgl = random.choice(WEBGL_CONFIGS)
    return {
        "user_agent": random.choice(USER_AGENTS),
        "viewport": random.choice(VIEWPORTS),
        "hw_concurrency": random.choice(HW_CONCURRENCY),
        "device_memory": random.choice(DEVICE_MEMORY),
        "webgl_vendor": webgl["vendor"],
        "webgl_renderer": webgl["renderer"],
    }


# ==================== BEZIER MOUSE TRAJECTORY ====================
def _bezier(t, p0, p1, p2, p3):
    u = 1 - t
    return (
        u**3 * p0[0] + 3*u**2*t * p1[0] + 3*u*t**2 * p2[0] + t**3 * p3[0],
        u**3 * p0[1] + 3*u**2*t * p1[1] + 3*u*t**2 * p2[1] + t**3 * p3[1],
    )


def mouse_path(sx, sy, ex, ey):
    """Cubic Bezier with ease-in-out and Gaussian jitter."""
    dist = math.sqrt((ex - sx)**2 + (ey - sy)**2)
    steps = max(5, min(15, int(dist / 55)))
    off = dist * 0.25
    cp1 = (sx + (ex-sx)*0.25 + random.uniform(-off, off),
           sy + (ey-sy)*0.25 + random.uniform(-off, off))
    cp2 = (sx + (ex-sx)*0.75 + random.uniform(-off, off),
           sy + (ey-sy)*0.75 + random.uniform(-off, off))
    pts = []
    for i in range(steps + 1):
        t = i / steps
        t = t * t * (3 - 2 * t)
        x, y = _bezier(t, (sx, sy), cp1, cp2, (ex, ey))
        pts.append((max(0, x + random.gauss(0, 0.5)),
                     max(0, y + random.gauss(0, 0.5))))
    return pts


# ==================== HUMAN BEHAVIOR SIMULATION ====================
async def simulate_human(page: Page, duration: int = 16):
    """
    Realistic human behavior via Playwright's trusted input API.
    All events produce isTrusted:true (indistinguishable from real hardware).
    """
    start = time.time()
    target = random.uniform(duration, duration + 6)

    try:
        vw = await page.evaluate("window.innerWidth") or 1280
        vh = await page.evaluate("window.innerHeight") or 720
    except Exception:
        vw, vh = 1280, 720

    cx = random.uniform(vw * 0.2, vw * 0.6)
    cy = random.uniform(vh * 0.2, vh * 0.5)
    await page.mouse.move(cx, cy)

    elements = await _get_visible_elements(page)

    actions = 0
    scroll_total = 0
    last_refresh = time.time()

    while time.time() - start < target:
        action = random.choices(
            ["move", "scroll_down", "scroll_up", "read", "random", "click_safe"],
            weights=[25, 25, 8, 22, 12, 8], k=1
        )[0]

        try:
            if action == "move" and elements:
                el = random.choice(elements)
                tx = max(5, min(el['x'] + random.uniform(-10, 10), vw - 5))
                ty = max(5, min(el['y'] + random.uniform(-10, 10), vh - 5))
                for px, py in mouse_path(cx, cy, tx, ty):
                    await page.mouse.move(px, py)
                    await asyncio.sleep(random.uniform(0.005, 0.012))
                cx, cy = tx, ty
                await asyncio.sleep(random.uniform(0.15, 0.7))

            elif action == "click_safe" and elements:
                # Click on safe, non-navigating elements (paragraphs, headings)
                safe = [e for e in elements if e.get('tag') in ('p', 'h1', 'h2', 'h3', 'div', 'span')]
                if safe:
                    el = random.choice(safe)
                    tx = max(5, min(el['x'] + random.uniform(-5, 5), vw - 5))
                    ty = max(5, min(el['y'] + random.uniform(-5, 5), vh - 5))
                    for px, py in mouse_path(cx, cy, tx, ty):
                        await page.mouse.move(px, py)
                        await asyncio.sleep(random.uniform(0.005, 0.012))
                    await page.mouse.click(tx, ty)
                    cx, cy = tx, ty
                    await asyncio.sleep(random.uniform(0.3, 1.0))

            elif action == "scroll_down":
                amount = random.randint(100, 350)
                for _ in range(random.randint(2, 3)):
                    await page.mouse.wheel(0, amount // 3)
                    await asyncio.sleep(random.uniform(0.02, 0.05))
                scroll_total += amount
                await asyncio.sleep(random.uniform(0.2, 0.5))

                if time.time() - last_refresh > 7:
                    elements = await _get_visible_elements(page) or elements
                    last_refresh = time.time()

            elif action == "scroll_up" and scroll_total > 200:
                await page.mouse.wheel(0, -random.randint(50, 150))
                scroll_total = max(0, scroll_total - 100)
                await asyncio.sleep(random.uniform(0.15, 0.3))

            elif action == "read":
                for _ in range(random.randint(2, 4)):
                    cx = max(5, min(cx + random.gauss(0, 2), vw - 5))
                    cy = max(5, min(cy + random.gauss(0, 2), vh - 5))
                    await page.mouse.move(cx, cy)
                    await asyncio.sleep(random.uniform(0.08, 0.2))
                await asyncio.sleep(random.uniform(0.8, 1.8))

            else:
                tx = random.uniform(50, vw - 50)
                ty = random.uniform(50, vh - 50)
                for px, py in mouse_path(cx, cy, tx, ty):
                    await page.mouse.move(px, py)
                    await asyncio.sleep(random.uniform(0.005, 0.012))
                cx, cy = tx, ty
                await asyncio.sleep(random.uniform(0.1, 0.3))

            actions += 1
        except Exception:
            await asyncio.sleep(0.2)

    return actions, scroll_total


async def _get_visible_elements(page: Page) -> list[dict]:
    try:
        return await page.evaluate("""
            Array.from(document.querySelectorAll('a, button, p, h1, h2, h3, div, span, img'))
            .slice(0, 25).map(el => {
                const r = el.getBoundingClientRect();
                return {x: r.left+r.width/2, y: r.top+r.height/2,
                        w: r.width, h: r.height,
                        tag: el.tagName.toLowerCase(),
                        vis: r.width>0 && r.height>0 && r.top>-50 && r.top<window.innerHeight+50};
            }).filter(e => e.vis && e.w > 10 && e.h > 10)
        """) or []
    except Exception:
        return []


# ==================== METRICA HELPERS ====================
async def wait_for_metrica(page: Page, timeout: int = 10) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            loaded = await page.evaluate("""
                typeof window.ym === 'function'
                || !!document.querySelector('script[src*="metrika"], script[src*="mc.yandex"]')
                || performance.getEntriesByType('resource').some(
                    r => r.name.includes('mc.yandex') || r.name.includes('metrika'))
            """)
            if loaded:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


async def flush_and_verify(page: Page) -> str:
    try:
        await page.evaluate("""
            if (typeof window.ym === 'function') {
                var ids = [];
                if (window.Ya && window.Ya._metrika && window.Ya._metrika.counters)
                    ids = Object.keys(window.Ya._metrika.counters);
                ids.forEach(id => {
                    try { window.ym(parseInt(id), 'params', {__ym:{visit:1}}); } catch(e) {}
                });
            }
            document.dispatchEvent(new Event('visibilitychange'));
        """)
        await asyncio.sleep(1)
    except Exception:
        pass

    try:
        return await page.evaluate("""
            (function() {
                var e = performance.getEntriesByType('resource');
                for (var i = 0; i < e.length; i++)
                    if (e[i].name.indexOf('/watch') !== -1 && e[i].name.indexOf('mc.yandex') !== -1)
                        return 'beacon';
                if (typeof window.Ya !== 'undefined' && window.Ya._metrika
                    && window.Ya._metrika.counters
                    && Object.keys(window.Ya._metrika.counters).length > 0) return 'counter';
                if (typeof window.ym === 'function') return 'ym';
                return '';
            })()
        """) or ""
    except Exception:
        return ""


# ==================== SINGLE VISIT ====================
async def visit_url(pw: Playwright, task: dict, worker_id: int) -> tuple[bool, float]:
    """
    One visit = one fresh browser (with browser-level proxy) + one context (fingerprint).
    Browser-level proxy is 100% reliable (unlike experimental per-context proxy).
    Returns (success, elapsed_seconds).
    """
    proxy = new_proxy()
    fp = random_fingerprint()
    domain = urlparse(task['url']).netloc.replace("www.", "")
    browser: Browser | None = None
    visit_start = time.time()

    try:
        # Launch browser WITH proxy at browser level — this is the fix for about:blank
        browser = await pw.chromium.launch(
            headless=False,
            proxy=proxy,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--disable-renderer-backgrounding",
                f"--window-size={fp['viewport']['width']},{fp['viewport']['height']}",
            ],
        )

        # Create isolated context for fingerprint (no proxy here — browser handles it)
        context = await browser.new_context(
            viewport=fp['viewport'],
            user_agent=fp['user_agent'],
            locale="uz-UZ",
            timezone_id="Asia/Tashkent",
            extra_http_headers={
                "Referer": f"https://yandex.uz/search/?text={domain}",
                "Accept-Language": "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en;q=0.5",
            },
            # Don't block any resources — Metrica checks image/font loading
            ignore_https_errors=True,
        )

        page = await context.new_page()
        await apply_stealth(page, fp)

        t0 = time.time()

        # Navigate
        try:
            await page.goto(task['url'], wait_until="domcontentloaded", timeout=40000)
        except Exception as e:
            print(f"  [W{worker_id}] Navigate failed: {e}")
            return False, time.time() - visit_start

        # Wait for full page load
        try:
            await page.wait_for_load_state("load", timeout=20000)
        except Exception:
            pass  # Metrica may have already loaded

        await asyncio.sleep(random.uniform(0.8, 1.5))

        # Validate
        try:
            title = await page.title()
            current_url = page.url
            if "about:blank" in current_url:
                print(f"  [W{worker_id}] Still about:blank — proxy likely failed")
                return False, time.time() - visit_start
            if "404" in title.lower() or any(k in current_url.lower() for k in ["blocked", "forbidden"]):
                print(f"  [W{worker_id}] Blocked/404: {current_url[:50]}")
                return False, time.time() - visit_start
        except Exception:
            pass

        # Wait for Metrica
        metrica = await wait_for_metrica(page, timeout=10)
        if metrica:
            await asyncio.sleep(random.uniform(0.5, 1.0))

        # Ensure page is focused (Metrica tracks visibility)
        try:
            await page.evaluate("window.focus(); document.hasFocus = () => true;")
        except Exception:
            pass

        # Human behavior (15-22s)
        actions, scrolled = await simulate_human(page, duration=15)

        # Flush + verify
        beacon = await flush_and_verify(page)

        elapsed = time.time() - t0
        status = f"beacon={beacon}" if beacon else "no-beacon"
        print(f"  [W{worker_id}] OK {task['url'][:45]}... {elapsed:.0f}s {actions}act {status}")

        await increment_click(task['id'])
        return True, time.time() - visit_start

    except Exception as e:
        print(f"  [W{worker_id}] FAIL {task['url'][:45]}... {e}")
        return False, time.time() - visit_start
    finally:
        # Explicit cleanup order: page → context → browser
        # This prevents orphan Chromium helper processes from accumulating
        try:
            if browser:
                for ctx in browser.contexts:
                    for pg in ctx.pages:
                        try:
                            await pg.close()
                        except Exception:
                            pass
                    try:
                        await ctx.close()
                    except Exception:
                        pass
                await browser.close()
        except Exception:
            pass


# ==================== WORKER LOOP ====================
async def worker_loop(
    pw: Playwright,
    dispatcher: Dispatcher,
    worker_id: int,
    get_delay: Callable[[], float],
    visit_durations: list[float] | None = None,
):
    """Single async worker: pull task -> launch browser -> visit -> close -> cooldown."""
    while True:
        if dispatcher.is_stopped:
            break

        task = await dispatcher.next_task()
        if task is None:
            break

        # Resilient visit — worker survives any unexpected crash
        try:
            success, elapsed = await visit_url(pw, task, worker_id)
        except Exception as e:
            print(f"  [W{worker_id}] UNEXPECTED ERROR: {e}")
            success, elapsed = False, 30.0  # fallback estimate

        if visit_durations is not None:
            visit_durations.append(elapsed)

        if not success and not dispatcher.is_stopped:
            print(f"  [W{worker_id}] Retrying...")
            await asyncio.sleep(random.uniform(2, 4))
            try:
                success, elapsed = await visit_url(pw, task, worker_id)
            except Exception as e:
                print(f"  [W{worker_id}] RETRY CRASH: {e}")
                success, elapsed = False, 30.0
            if visit_durations is not None:
                visit_durations.append(elapsed)
            if not success:
                print(f"  [W{worker_id}] Retry failed, skipping")

        if dispatcher.is_stopped:
            break

        delay = get_delay()
        jitter = random.uniform(-delay * 0.2, delay * 0.2)
        await asyncio.sleep(max(1.0, delay + jitter))

    print(f"  [W{worker_id}] Stopped")


# ==================== ORCHESTRATOR ====================
async def run_workers(
    dispatcher: Dispatcher,
    num_workers: int = 5,
    get_delay: Callable[[], float] = lambda: 8.0,
    on_progress: Callable | None = None,
    visit_durations: list[float] | None = None,
):
    """
    Launch playwright instance + N async workers.
    Each worker manages its own browser lifecycle (browser-level proxy).
    visit_durations: shared mutable list where workers append per-visit elapsed times.
    """
    if visit_durations is None:
        visit_durations = []

    async with async_playwright() as pw:
        total = dispatcher.total
        print(f"Playwright ready. {num_workers} workers, {total} tasks queued.")

        async def progress_reporter():
            while not dispatcher.is_stopped and dispatcher.remaining > 0:
                if on_progress:
                    try:
                        await on_progress(dispatcher.completed, total)
                    except Exception:
                        pass
                await asyncio.sleep(10)
            if on_progress:
                try:
                    await on_progress(dispatcher.completed, total)
                except Exception:
                    pass

        progress_task = asyncio.create_task(progress_reporter())

        # Each worker gets the playwright instance (pw), not a shared browser
        workers = [
            asyncio.create_task(
                worker_loop(pw, dispatcher, i + 1, get_delay, visit_durations)
            )
            for i in range(num_workers)
        ]

        await asyncio.gather(*workers, return_exceptions=True)
        dispatcher.stop()
        await progress_task

        done = dispatcher.completed
        print(f"All workers finished. {done}/{total} completed.")
        return done, total
