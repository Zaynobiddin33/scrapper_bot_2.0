"""
Playwright-based async worker pool - OPTIMIZED FOR YANDEX METRIKA

KEY IMPROVEMENTS (v2):
1. Hit verification - confirms Metrika actually counted the visit
2. Enhanced fingerprints - more entropy, better correlation
3. Tab visibility simulation - realistic blur/focus patterns
4. Retry logic - automatic retry on failed hits
5. Better logging - detailed hit verification results

Architecture:
  1 playwright instance (shared)
  → 5 async workers
    → per visit: launch browser(proxy) → verify hit → simulate human → confirm → close

Visit counting verification:
  - Intercepts /watch/{counter_id} request
  - Validates 200 OK response
  - Confirms hittoken present
  - Checks bh cookie set
"""
import asyncio
import random
import math
import time
import uuid
import os
from urllib.parse import urlparse
from typing import Callable, Optional

from playwright.async_api import async_playwright, Playwright, Browser, BrowserContext, Page

from tokens import (
    PROXY_HOST, PROXY_PORT, USERNAME, PASSWORD, 
    METRIKA_COUNTER_ID, METRIKA_COUNTER_OVERRIDES
)
from db import increment_click
from dispatcher import Dispatcher

# Import new Metrika modules
from metrika import (
    HitVerifier, 
    verify_metrika_hit, 
    generate_fingerprint,
    simulate_tab_visibility,
    ensure_visible,
)


# ==================== CONFIGURATION ====================
# Metrika settings - Universal for any website
DEFAULT_COUNTER_ID = METRIKA_COUNTER_ID if METRIKA_COUNTER_ID != 'auto' else None
COUNTER_OVERRIDES = METRIKA_COUNTER_OVERRIDES if METRIKA_COUNTER_OVERRIDES else {}

# Hit verification settings - ALWAYS ON for 100% accuracy
VERIFY_HIT = True
REQUIRE_HITTOKEN = True
HIT_TIMEOUT = 45.0  # Increased for slow proxies

# Visit duration settings - Longer = more realistic, better for Metrika
MIN_VISIT_DURATION = 18  # Minimum 18 seconds
MAX_VISIT_DURATION = 35  # Up to 35 seconds (varies per visit)

# Retry settings - More retries for reliability
MAX_RETRIES = 3  # Try up to 3 times
RETRY_DELAY_MIN = 3.0  # Wait at least 3s between retries
RETRY_DELAY_MAX = 8.0  # Up to 8s for proxy cooldown

# Tab visibility simulation - CRITICAL for Metrika
SIMULATE_VISIBILITY = True

# Stealth settings - Headed = more realistic, harder to detect
HEADED_BROWSER = True  # Always use headed mode (headless=False)


def get_counter_id_for_url(url: str) -> int | None:
    """
    Get Metrika counter ID for a given URL.
    Priority:
    1. Check COUNTER_OVERRIDES for domain match
    2. Return DEFAULT_COUNTER_ID if set
    3. Return None (auto-detect from page)
    """
    domain = urlparse(url).netloc.replace("www.", "").lower()
    
    # Check overrides first
    for override_domain, counter_id in COUNTER_OVERRIDES.items():
        if override_domain.lower() in domain or domain in override_domain.lower():
            return counter_id
    
    # Return default if set
    if DEFAULT_COUNTER_ID and DEFAULT_COUNTER_ID != 'auto':
        return DEFAULT_COUNTER_ID
    
    # Auto-detect
    return None


def get_referer_for_domain(domain: str) -> str:
    """Generate appropriate referer based on domain TLD"""
    tld = domain.split('.')[-1].lower()
    
    referers = {
        'uz': f"https://yandex.uz/search/?text={domain}",
        'ru': f"https://yandex.ru/search/?text={domain}",
        'kz': f"https://yandex.kz/search/?text={domain}",
        'by': f"https://yandex.by/search/?text={domain}",
        'ua': f"https://yandex.ua/search/?text={domain}",
        'com': f"https://www.google.com/search?q={domain}",
        'org': f"https://www.google.com/search?q={domain}",
        'net': f"https://www.google.com/search?q={domain}",
    }
    
    return referers.get(tld, f"https://www.google.com/search?q={domain}")


# ==================== STEALTH INIT SCRIPT ====================
# Optimized - only essential patches (reduced from comprehensive)
STEALTH_SCRIPT = """
// ===== 1. navigator.webdriver =====
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

// ===== 2. chrome.runtime =====
if (!window.chrome) window.chrome = {};
if (!window.chrome.runtime) {
    window.chrome.runtime = {
        connect: function(){},
        sendMessage: function(){},
        onMessage: {addListener: function(){}, removeListener: function(){}},
        onConnect: {addListener: function(){}, removeListener: function(){}}
    };
}

// ===== 3. Permissions API =====
const origQuery = window.navigator.permissions.query.bind(window.navigator.permissions);
window.navigator.permissions.query = (params) => {
    if (params.name === 'notifications')
        return Promise.resolve({state: Notification.permission});
    return origQuery(params);
};

// ===== 4. Plugins =====
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            {name:'Chrome PDF Plugin', filename:'internal-pdf-viewer',
             description:'Portable Document Format', length:1,
             item: function(i){return this}, namedItem: function(n){return this}},
            {name:'Chrome PDF Viewer', filename:'mhjfbmdgcfjbbpaeojofohoefgiehjai',
             description:'', length:1,
             item: function(i){return this}, namedItem: function(n){return this}},
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

// ===== 6. Timezone (Tashkent UTC+5) =====
const origGetTimezoneOffset = Date.prototype.getTimezoneOffset;
Date.prototype.getTimezoneOffset = function() { return -300; };
"""


async def apply_fingerprint(page: Page, fp) -> None:
    """Apply fingerprint to page via init script"""
    # Apply stealth first
    await page.add_init_script(STEALTH_SCRIPT)
    
    # Apply fingerprint-specific overrides
    await page.add_init_script(f"""
        // Hardware
        Object.defineProperty(navigator, 'hardwareConcurrency', {{get: () => {fp.hw_concurrency}}});
        Object.defineProperty(navigator, 'deviceMemory', {{get: () => {fp.device_memory}}});
        Object.defineProperty(navigator, 'platform', {{get: () => '{fp.platform}'}});
        
        // Language
        Object.defineProperty(navigator, 'language', {{get: () => '{fp.language}'}});
        
        // WebGL
        const gp = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(p) {{
            if (p === 37445) return '{fp.webgl_vendor}';
            if (p === 37446) return '{fp.webgl_renderer}';
            return gp.call(this, p);
        }};
        
        // Viewport
        Object.defineProperty(window, 'innerWidth', {{get: () => {fp.viewport['width']}}});
        Object.defineProperty(window, 'innerHeight', {{get: () => {fp.viewport['height']}}});
    """)


# ==================== PROXY ====================
def new_proxy():
    """New sticky session = new IP"""
    sid = uuid.uuid4().hex[:12]
    return {
        "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
        "username": f"{USERNAME}_session-{sid}",
        "password": PASSWORD,
    }


# ==================== BEZIER MOUSE TRAJECTORY ====================
def _bezier(t, p0, p1, p2, p3):
    u = 1 - t
    return (
        u**3 * p0[0] + 3*u**2*t * p1[0] + 3*u*t**2 * p2[0] + t**3 * p3[0],
        u**3 * p0[1] + 3*u**2*t * p1[1] + 3*u*t**2 * p2[1] + t**3 * p3[1],
    )


def mouse_path(sx, sy, ex, ey):
    """Cubic Bezier with ease-in-out and Gaussian jitter"""
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
async def simulate_human(page: Page, duration: int = 18):
    """
    Realistic human behavior via Playwright's trusted input API.
    Optimized for Metrika detection avoidance.
    """
    start = time.time()
    target = random.uniform(duration, duration + 4)

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

    while time.time() - start < target:
        action = random.choices(
            ["move", "scroll_down", "scroll_up", "read", "random"],
            weights=[28, 25, 8, 24, 15], k=1
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

            elif action == "scroll_down":
                amount = random.randint(100, 350)
                for _ in range(random.randint(2, 3)):
                    await page.mouse.wheel(0, amount // 3)
                    await asyncio.sleep(random.uniform(0.02, 0.05))
                scroll_total += amount
                await asyncio.sleep(random.uniform(0.2, 0.5))
                elements = await _get_visible_elements(page) or elements

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


async def _get_visible_elements(page: Page) -> list:
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


# ==================== SINGLE VISIT (OPTIMIZED) ====================
async def visit_url(pw: Playwright, task: dict, worker_id: int) -> tuple[bool, float, dict]:
    """
    One visit with hit verification.
    
    Returns:
        (success, elapsed_seconds, verification_details)
    """
    proxy = new_proxy()
    fp = generate_fingerprint()
    domain = urlparse(task['url']).netloc.replace("www.", "")
    browser: Browser | None = None
    visit_start = time.time()
    hit_result = None

    try:
        # Launch browser with proxy
        browser = await pw.chromium.launch(
            headless=not HEADED_BROWSER,
            proxy=proxy,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-infobars",
                f"--window-size={fp.viewport['width']},{fp.viewport['height']}",
            ],
        )

        # Get appropriate referer for this domain
        referer = get_referer_for_domain(domain)

        context = await browser.new_context(
            viewport=fp.viewport,
            user_agent=fp.user_agent,
            locale=fp.language,
            timezone_id=fp.timezone,
            extra_http_headers={
                "Referer": referer,
                "Accept-Language": f"{fp.language}, en-US;q=0.9, en;q=0.5",
            },
            ignore_https_errors=True,
        )

        page = await context.new_page()
        
        # Apply fingerprint
        await apply_fingerprint(page, fp)

        # Detect counter ID for this URL
        counter_id = get_counter_id_for_url(task['url'])
        if not counter_id:
            # Auto-detect from page after navigation
            counter_id = 'auto'
            print(f"  [W{worker_id}] Counter ID set to auto-detect")
        else:
            print(f"  [W{worker_id}] Using counter ID: {counter_id}")

        # Attach hit verifier BEFORE navigation
        verifier = None
        if VERIFY_HIT:
            verifier = HitVerifier(counter_id=counter_id, require_hittoken=REQUIRE_HITTOKEN)
            verifier.attach(page)

        t0 = time.time()

        # Navigate to target
        try:
            await page.goto(task['url'], wait_until="domcontentloaded", timeout=40000)
        except Exception as e:
            print(f"  [W{worker_id}] Navigate failed: {e}")
            return False, time.time() - visit_start, {'error': str(e)}

        # Wait for full load
        try:
            await page.wait_for_load_state("load", timeout=20000)
        except Exception:
            pass

        await asyncio.sleep(random.uniform(0.8, 1.5))

        # Validate page loaded correctly
        try:
            title = await page.title()
            current_url = page.url
            if "about:blank" in current_url:
                print(f"  [W{worker_id}] Still about:blank — proxy likely failed")
                return False, time.time() - visit_start, {'error': 'about:blank'}
            if "404" in title.lower() or any(k in current_url.lower() for k in ["blocked", "forbidden"]):
                print(f"  [W{worker_id}] Blocked/404: {current_url[:50]}")
                return False, time.time() - visit_start, {'error': 'blocked'}
        except Exception:
            pass

        # CRITICAL: Wait for and verify Metrika hit
        if verifier:
            hit_result = await verifier.wait_for_hit(timeout=HIT_TIMEOUT)
            verifier.detach(page)
            
            if not hit_result.hit_verified:
                print(f"  [W{worker_id}] Hit NOT verified: {hit_result.error}")
                # Don't count as success if hit wasn't verified
                return False, time.time() - visit_start, hit_result.to_dict()
            
            print(f"  [W{worker_id}] Hit verified: hittoken={hit_result.hittoken[:20] if hit_result.hittoken else None}...")

        # Ensure page is focused for Metrika
        await ensure_visible(page)

        # Simulate tab visibility (optional but adds realism)
        if SIMULATE_VISIBILITY:
            duration = random.randint(MIN_VISIT_DURATION, MAX_VISIT_DURATION)
            visibility_task = asyncio.create_task(simulate_tab_visibility(page, duration))
        else:
            duration = random.randint(MIN_VISIT_DURATION, MAX_VISIT_DURATION)
            visibility_task = None

        # Human behavior simulation
        actions, scrolled = await simulate_human(page, duration=duration)

        # Wait for visibility simulation to complete
        if visibility_task:
            await visibility_task

        elapsed = time.time() - t0
        
        # Increment click in database
        await increment_click(task['id'])
        
        verification_details = hit_result.to_dict() if hit_result else {'hit_verified': True}
        verification_details['actions'] = actions
        verification_details['scroll_px'] = scrolled
        verification_details['duration'] = elapsed
        
        print(f"  [W{worker_id}] OK {task['url'][:45]}... {elapsed:.0f}s {actions}act")
        
        return True, elapsed, verification_details

    except Exception as e:
        print(f"  [W{worker_id}] FAIL {task['url'][:45]}... {e}")
        return False, time.time() - visit_start, {'error': str(e)}
    
    finally:
        # Cleanup
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


# ==================== WORKER LOOP WITH RETRY ====================
async def worker_loop(
    pw: Playwright,
    dispatcher: Dispatcher,
    worker_id: int,
    get_delay: Callable[[], float],
    visit_durations: list | None = None,
):
    """Single async worker with retry logic"""
    while True:
        if dispatcher.is_stopped:
            break

        task = await dispatcher.next_task()
        if task is None:
            break

        # Try visit with retries
        success = False
        elapsed = 0
        details = {}
        
        for attempt in range(MAX_RETRIES + 1):
            success, elapsed, details = await visit_url(pw, task, worker_id)
            
            if success:
                break  # Success, no retry needed
            
            if details.get('hit_verified'):
                # Hit was counted but other issue - don't retry
                success = True
                break
            
            # Retry if not last attempt
            if attempt < MAX_RETRIES:
                delay = random.uniform(RETRY_DELAY_MIN, RETRY_DELAY_MAX)
                print(f"  [W{worker_id}] Retrying in {delay:.1f}s... (attempt {attempt + 2}/{MAX_RETRIES + 1})")
                await asyncio.sleep(delay)

        if visit_durations is not None:
            visit_durations.append(elapsed)

        if not success and not dispatcher.is_stopped:
            print(f"  [W{worker_id}] All retries failed, skipping task")

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
    visit_durations: list | None = None,
):
    """Launch workers with shared Playwright instance"""
    if visit_durations is None:
        visit_durations = []

    async with async_playwright() as pw:
        total = dispatcher.total
        print(f"Playwright ready. {num_workers} workers, {total} tasks queued.")
        print(f"Metrika Counter ID: {METRIKA_COUNTER_ID}")
        print(f"Hit verification: {'ON' if VERIFY_HIT else 'OFF'}")

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
