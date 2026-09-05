"""
scrp_v2.py - Human-like browser automation with:
- Realistic scrolling behavior (mimics human movement patterns)
- Smart Metrika hit detection with 15s fallback timeout
- 100-120 second maximum browser lifetime
- Dynamic behavior based on page characteristics
"""
import json
import random
import threading
import time
import uuid
from urllib.parse import urlparse

from seleniumbase import SB

from tokens import PASSWORD, PROXY_HOST, PROXY_PORT, USERNAME

from cookie_store import get_cookie_replay_plan, save_cookies_from_browser
from device_profiles import pick_profile

# Global stop flag - use with caution in async context
STOP_FLAG = False

# ==================== CONFIGURATION ====================
PREWARM_URL = "https://yandex.uz"

# Domain will be extracted from target URL before setting Referer
# This prevents Yandex redirect issues that cause triple counting
# REFERER_URL is now set dynamically per domain in visit_with_proxy
PAGE_LOAD_TIMEOUT = 120
POST_LAUNCH_DELAY = (0.4, 0.8)
PREWARM_DELAY = (1.2, 2.0)
POST_OPEN_SETTLE = (0.8, 1.5)
POST_CAPTCHA_SETTLE = (0.8, 1.4)
CAPTCHA_IFRAME_TIMEOUT = 2
READY_STATE_POLLS = 8
SHORTENER_BLOCK_KEYWORDS = ("404", "blocked", "forbidden")
BLOCK_PAGE_KEYWORDS = (
    "404",
    "not found",
    "forbidden",
    "access denied",
    "page not found",
    "temporarily unavailable",
)

# Session duration: NO hard floor/ceiling — use a natural distribution.
# Real users: 15% bounce in <20s, 50% read 30-90s, 35% stay 90-180s.
# A uniform 30-45s window is statistically impossible for humans and is
# one of the primary signals Metrika's fraud ML detects.
#
# visit_duration() below samples from this weighted distribution each visit.
BROWSER_MIN_LIFETIME = 0    # Unused — kept for import compatibility
BROWSER_MAX_LIFETIME = 180  # Hard ceiling; visit_duration() picks within range

# Metrika hit detection
METRIKA_HIT_TIMEOUT = 15  # Wait up to 15s for Metrika hit
MIN_VISIT_TOTAL = 15       # Absolute minimum seconds before closing
HUMAN_DURATION_VARIANCE = 3  # Additional human-like duration variance

# Timeout for overall visit to prevent hanging
VISIT_TIMEOUT = 200  # Maximum time for a single visit to prevent hanging browsers

SECURITY_VERIFICATION_SELECTOR = "p#jxHnX1.ch-description.spacer-top"
SECURITY_VERIFICATION_TEXT = (
    "This website uses a security service to protect against malicious bots. "
    "This page is displayed while the website verifies you are not a bot."
)


def visit_duration() -> float:
    """
    Sample a session duration that matches real human visit-length distribution.

    Segments (approximate real-user CDF from web analytics data):
      ~20% bounce  :  12 – 22 s   (glanced, left)
      ~45% medium  :  30 – 90 s   (read the article, skimmed)
      ~25% engaged :  90 – 150 s  (read thoroughly, scrolled)
      ~10% deep    : 150 – 200 s  (long read, multiple sections)
    """
    bucket = random.random()
    if bucket < 0.20:
        return random.uniform(12, 22)   # bounce
    elif bucket < 0.65:
        return random.uniform(30, 90)   # medium
    elif bucket < 0.90:
        return random.uniform(90, 150)  # engaged
    else:
        return random.uniform(150, 200) # deep read

# Scrolling patterns for realistic human behavior
SCROLL_PATTERNS = {
    # Pattern: (scroll_amount, delay_range, repetitions)
    "slow_read": [
        (150, (0.4, 0.8)),      # Slow, deliberate scrolling
        (200, (0.5, 0.9)),
        (180, (0.6, 1.0)),
        (220, (0.5, 0.8)),
        (160, (0.4, 0.7)),
    ],
    "fast_scrol": [
        (300, (0.3, 0.5)),
        (400, (0.3, 0.5)),
        (350, (0.3, 0.5)),
        (280, (0.3, 0.5)),
    ],
    "random_exp": [
        (120, (0.5, 1.0)),
        (250, (0.4, 0.8)),
        (180, (0.6, 1.2)),
        (300, (0.3, 0.6)),
        (160, (0.7, 1.3)),
    ],
    "regional": [
        (250, (0.4, 0.6)),      # Scroll down
        (100, (0.8, 1.2)),      # Pause and scroll back up slightly
        (350, (0.3, 0.5)),      # Quick scroll down
        (-80, (0.5, 0.9)),      # Small upward scroll
        (200, (0.4, 0.7)),      # Continue down
    ],
}


def set_stop_flag(value: bool):
    global STOP_FLAG
    STOP_FLAG = value


def cleanup_chrome():
    """Cleanup Chrome processes and temp files."""
    try:
        import psutil
        import os
        import time
        
        # Get current process
        try:
            current = psutil.Process(os.getpid())
            # Kill all child processes (Chrome, chromedriver, etc.)
            for child in current.children(recursive=True):
                try:
                    name = child.name().lower()
                    if any(x in name for x in ["chrome", "chromedriver", "geckodriver", "firefox", "xvfb"]):
                        child.kill()  # Force kill
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception:
            pass
        
        time.sleep(0.5)  # Brief pause to allow cleanup
    except Exception:
        pass


def sticky_proxy() -> dict:
    """Generate sticky proxy with consistent session ID."""
    session_id = uuid.uuid4().hex[:8]
    return {
        "host": PROXY_HOST,
        "port": PROXY_PORT,
        "username": f"{USERNAME}_session-{session_id}",
        "password": PASSWORD,
    }


def _visit_result(success: bool, hit_verified: bool, started_at: float, error: str | None = None) -> dict:
    """Create structured visit result."""
    return {
        "success": success,
        "hit_verified": hit_verified,
        "duration": time.time() - started_at,
        "error": error,
    }


def _sleep_interruptible(duration: float, step: float = 0.2) -> bool:
    """Sleep with interrupt check."""
    end = time.time() + duration
    while time.time() < end:
        if STOP_FLAG:
            return False
        time.sleep(min(step, max(0.0, end - time.time())))
    return not STOP_FLAG


def _sleep_range(delay_range: tuple[float, float]) -> bool:
    """Sleep with random duration from range."""
    return _sleep_interruptible(random.uniform(*delay_range))


def _inject_cookies(sb: SB, cookies: list[dict]) -> int:
    """Inject cookies via CDP and count successful writes."""
    injected = 0
    for cookie in cookies:
        try:
            result = sb.execute_cdp_cmd("Network.setCookie", cookie) or {}
            if result.get("success", True):
                injected += 1
        except Exception:
            pass
    return injected


def _page_looks_blocked(sb: SB) -> bool:
    """Check if page appears blocked."""
    try:
        current_url = (sb.get_current_url() or "").lower()
        title = (sb.get_page_title() or "").lower()
        body = (sb.get_text("body") or "").lower()[:2000]
        if any(keyword in current_url for keyword in SHORTENER_BLOCK_KEYWORDS):
            return True
        return any(keyword in title or keyword in body for keyword in BLOCK_PAGE_KEYWORDS)
    except Exception:
        return False


def _wait_for_ready(sb: SB) -> bool:
    """Wait for page to load."""
    for _ in range(READY_STATE_POLLS):
        if STOP_FLAG:
            return False
        try:
            if sb.execute_script("return document.readyState") in ("interactive", "complete"):
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return True


def _has_challenge(sb: SB) -> bool:
    """Check if captcha challenge is present."""
    selectors = (
        "iframe[title*='challenge'], "
        "iframe[src*='captcha'], "
        "iframe[src*='recaptcha'], "
        "iframe[src*='challenges.cloudflare'], "
        "input[name*='captcha'], "
        "#challenge-form"
    )
    try:
        return bool(sb.is_element_present(selectors, timeout=CAPTCHA_IFRAME_TIMEOUT))
    except Exception:
        return False


def _maybe_handle_challenge(sb: SB, visit_id: int) -> bool:
    """Handle captcha challenge if present."""
    if STOP_FLAG:
        return False
    if not _has_challenge(sb):
        return True

    print(f"[{visit_id}] Challenge detected")

    try:
        sb.uc_gui_click_captcha()
    except Exception:
        pass

    if not _sleep_range(POST_CAPTCHA_SETTLE):
        return False

    try:
        current_url = (sb.get_current_url() or "").lower()
        if "captcha" in current_url or "challenge" in current_url:
            sb.solve_captcha()
            if not _sleep_range(POST_CAPTCHA_SETTLE):
                return False
    except Exception:
        pass

    return True


def _get_content_length(sb: SB) -> int:
    """Estimate content length for dynamic scrolling."""
    try:
        return int(sb.execute_script("return document.body.scrollHeight || 0"))
    except Exception:
        return 0


def _get_viewport_height(sb: SB) -> int:
    """Get viewport height."""
    try:
        return int(sb.execute_script("return window.innerHeight || 0"))
    except Exception:
        return 1080  # Default assumption


def _calculate_scroll_pattern(sb: SB) -> list:
    """Determine optimal scroll pattern based on page characteristics."""
    content_height = _get_content_length(sb)
    viewport_height = _get_viewport_height(sb)
    
    # Calculate how many viewport-heights we need to scroll
    scroll_depth = content_height / viewport_height if viewport_height > 0 else 1
    
    print(f"[SCROLL] Content: {content_height}px, Viewport: {viewport_height}px, Depth: {scroll_depth:.1f}vh")
    
    # Select pattern based on content depth
    if scroll_depth < 2:
        # Short content - minimal scrolling
        return SCROLL_PATTERNS["slow_read"][:2]
    elif scroll_depth < 4:
        # Medium content - moderate scrolling
        return SCROLL_PATTERNS["random_exp"][:3]
    elif scroll_depth < 6:
        # Long content - more scrolling
        return SCROLL_PATTERNS["regional"]
    else:
        # Very long content - extensive scrolling
        return SCROLL_PATTERNS["regional"] + SCROLL_PATTERNS["fast_scrol"][:2]


def _perform_human_like_scroll(sb: SB, scroll_pattern: list) -> bool:
    """
    Perform realistic human-like scrolling with natural variations.
    
    This mimics how humans actually read and scroll:
    - Varying scroll distances (not constant)
    - Natural pauses (reading time)
    - Occasional backward scrolling (re-reading)
    - Random variations in timing
    """
    if STOP_FLAG:
        return False
    
    print(f"[SCROLL] Starting human-like scroll with {len(scroll_pattern)} steps")
    
    # Track scroll position to detect when we hit bottom
    prev_scroll = 0
    scroll_streak = 0  # Consecutive downward scrolls
    
    for amount, delay_range in scroll_pattern:
        if STOP_FLAG:
            return False
        
        # Add natural variance to scroll amount (±15%)
        variance = random.uniform(-0.15, 0.15)
        actual_amount = int(amount * (1 + variance))
        
        # Add natural variance to delay (±20%)
        actual_delay = random.uniform(*delay_range) * random.uniform(0.8, 1.2)
        
        try:
            sb.execute_script(f"window.scrollBy(0,{actual_amount})")
        except Exception:
            return False
        
        # Track scroll direction for streak detection
        current_scroll = sb.execute_script("return window.scrollY || 0")
        if current_scroll > prev_scroll + 50:
            scroll_streak += 1
        else:
            scroll_streak = max(0, scroll_streak - 1)
        prev_scroll = current_scroll
        
        # Natural pause based on scroll streak (reading time)
        # Longer streak = more reading, longer pauses
        reading_pause = 0
        if scroll_streak > 2:
            reading_pause = random.uniform(0.3, 0.7)
        
        if not _sleep_interruptible(actual_delay + reading_pause):
            return False
    
    # Only scroll to the very bottom ~30% of the time.
    # Real users often leave before reaching the bottom — forcing
    # 100% bottom-reach is a statistical bot signal Metrika detects.
    if random.random() < 0.30:
        try:
            max_scroll = sb.execute_script("return document.body.scrollHeight - window.innerHeight")
            current_pos = sb.execute_script("return window.scrollY")
            if max_scroll > 0 and current_pos < max_scroll * 0.8:
                sb.execute_script("window.scrollTo(0, document.body.scrollHeight)")
                _sleep_range((1.0, 2.0))
                print("[SCROLL] Reached bottom of page")
        except Exception:
            pass

    print("[SCROLL] Human-like scroll completed")
    return True


def _simulate_human_read(sb: SB, min_duration: int = 12, max_duration: int = 18) -> bool:
    """
    Simulate human reading behavior with realistic patterns.
    
    This creates a more natural experience by:
    - Reading for variable durations
    - Scrolling based on content engagement
    - Occasional "thinking" pauses
    - Natural timing variations
    """
    target = random.uniform(min_duration, max_duration)
    start = time.time()
    
    # Read speed: faster for short content, slower for long content
    content_length = _get_content_length(sb)
    viewport = _get_viewport_height(sb)
    depth = content_length / viewport if viewport > 0 else 1
    
    # Adjust speed based on content depth
    if depth < 2:
        # Short content - read slower, more engagement
        pause_range = (1.5, 3.0)
        scroll_amount = (100, 200)
    elif depth < 4:
        # Medium content - moderate pace
        pause_range = (0.8, 1.5)
        scroll_amount = (150, 250)
    else:
        # Long content - faster pace
        pause_range = (0.5, 1.0)
        scroll_amount = (200, 400)
    
    while time.time() - start < target:
        if STOP_FLAG:
            return False
        
        action = random.choice(["reading", "scroll", "small_move"])
        
        if action == "reading":
            # Simulate reading time (looking at content)
            pause = random.uniform(*pause_range)
            if not _sleep_interruptible(pause):
                return False
            print(f"[HUMAN] Reading for {pause:.1f}s")
            
        elif action == "scroll":
            # Scroll with natural movement
            amount = random.randint(*scroll_amount)
            try:
                sb.execute_script(f"window.scrollBy(0,{amount})")
            except Exception:
                return False
            
            # Natural scroll delay
            delay = random.uniform(0.3, 0.7)
            if not _sleep_interruptible(delay):
                return False
            print(f"[HUMAN] Scrolled {amount}px")
            
        else:  # small_move
            # Small movement (like checking something)
            amount = random.randint(-100, 100)
            try:
                sb.execute_script(f"window.scrollBy(0,{amount})")
            except Exception:
                return False
            
            # Quick glance pause
            if not _sleep_interruptible(random.uniform(0.2, 0.5)):
                return False
            print(f"[HUMAN] Small movement {amount}px")
    
    print(f"[HUMAN] Human simulation completed for {target:.1f}s")
    return True


def _metrika_loaded(sb: SB) -> bool:
    """Check if Metrika is loaded on the page."""
    try:
        return bool(
            sb.execute_script(
                """
                return typeof window.ym === 'function'
                    || !!document.querySelector('script[src*="mc.yandex"],script[src*="metrika"]');
                """
            )
        )
    except Exception:
        return False


def _trigger_metrika(sb: SB) -> bool:
    """
    Check whether Metrika counters are present on the page.

    NOTE: We deliberately do NOT call ym(id, 'params', {__ym:{visit:1}}).
    That synthetic JS call is a detectable bot signal — Yandex's fraud ML
    recognises it. The page-view hit fires automatically on load; we just
    need to confirm the counter exists, then wait for the natural hit.
    """
    try:
        counter_count = sb.execute_script(
            """
            var ids = new Set();
            if (window.Ya && window.Ya._metrika && window.Ya._metrika.counters) {
                Object.keys(window.Ya._metrika.counters).forEach(function(id) {
                    ids.add(id);
                });
            }
            Object.keys(window).forEach(function(key) {
                if (/^yaCounter\\d+$/.test(key)) ids.add(key);
            });
            return ids.size;
            """
        )
        return bool(counter_count)
    except Exception:
        return False


def _has_metrika_hit_signal(sb: SB) -> bool:
    """
    Check multiple signals that a Yandex Metrika hit fired.

    Signal priority (fastest/most-reliable first):
      1. window._ymHitDetected — set by the JS interceptor injected via
         Page.addScriptToEvaluateOnNewDocument in _apply_cdp_fingerprint().
         Catches fetch/XHR/sendBeacon/Image.src the moment they're called,
         before the request even leaves the browser. Most reliable.
      2. performance.getEntriesByType('resource') — catches mc.yandex /watch
         requests that completed (misses very-fast hits if called too early).
      3. localStorage _ym*_lastHit keys — Metrika writes this after a hit.
      4. document.images noscript pixel — fallback for old-style tracking.
    """
    try:
        return bool(
            sb.execute_script(
                """
                // 1. JS-level interceptor (most reliable)
                if (window._ymHitDetected === true) return true;

                // 2. Performance resource entries
                const resources = performance.getEntriesByType('resource') || [];
                const hasResourceHit = resources.some((entry) => {
                    const name = entry && entry.name ? String(entry.name) : '';
                    return name.includes('mc.yandex') && name.includes('/watch');
                });
                if (hasResourceHit) return true;

                // 3. localStorage marker (written by Metrika after hit)
                try {
                    for (let i = 0; i < localStorage.length; i += 1) {
                        const key = localStorage.key(i) || '';
                        if (/^_ym\\d+_lastHit$/.test(key)) return true;
                    }
                } catch (e) {}

                // 4. Noscript pixel in document images
                return Array.from(document.images || []).some((img) => {
                    const src = img && img.src ? String(img.src) : '';
                    return src.includes('mc.yandex') && src.includes('/watch');
                });
                """
            )
        )
    except Exception:
        return False


def _wait_for_metrika_hit_with_timeout(
    sb: SB, timeout: int = METRIKA_HIT_TIMEOUT, check_interval: float = 0.3
) -> tuple[bool, float]:
    """
    Wait for Metrika hit with timeout.
    
    Returns: (hit_found, time_waited)
    """
    start = time.time()
    
    print(f"[METRIKA] Waiting up to {timeout}s for Metrika hit...")
    
    while time.time() - start < timeout:
        if STOP_FLAG:
            return False, time.time() - start
        
        try:
            has_hit = _has_metrika_hit_signal(sb)
            if has_hit:
                time_taken = time.time() - start
                print(f"[METRIKA] Hit found after {time_taken:.1f}s")
                return True, time_taken
        except Exception:
            pass
        
        time.sleep(check_interval)
    
    time_taken = time.time() - start
    print(f"[METRIKA] Timeout after {time_taken:.1f}s - no hit detected")
    return False, time_taken


def _flush_and_wait_metrika(sb: SB, timeout: int = METRIKA_HIT_TIMEOUT) -> bool:
    """Trigger Metrika and wait for hit with timeout."""
    if not _trigger_metrika(sb):
        print("[METRIKA] Could not trigger Metrika")
        return False
    
    hit_found, _ = _wait_for_metrika_hit_with_timeout(sb, timeout)
    return hit_found


def _wait_for_metrika_hit(sb: SB, timeout: int = METRIKA_HIT_TIMEOUT) -> bool:
    """Check for Metrika hit with timeout (passive detection)."""
    hit_found, _ = _wait_for_metrika_hit_with_timeout(sb, timeout)
    return hit_found


def _has_security_verification_marker(sb: SB) -> bool:
    """Check for the exact security verification marker element."""
    try:
        return bool(sb.execute_script(
            """
            const el = document.querySelector(arguments[0]);
            if (!el) return false;
            const text = (el.textContent || "").replace(/\\s+/g, " ").trim();
            return text === arguments[1];
            """,
            SECURITY_VERIFICATION_SELECTOR,
            SECURITY_VERIFICATION_TEXT,
        ))
    except Exception:
        return False


def _wait_for_security_verification(sb: SB, visit_id: int, timeout: float = 60.0) -> bool:
    """
    Wait for the exact security verification marker to clear before starting
    any human-simulation activity.

    Returns True  — page is clear, human simulation can start.
    Returns False — timed out; caller should treat the visit as blocked.
    """
    if not _has_security_verification_marker(sb):
        return True  # No security gate present

    print(f"[{visit_id}] Security verification detected — waiting up to {timeout:.0f}s")
    start = time.time()

    while time.time() - start < timeout:
        if STOP_FLAG:
            return False
        time.sleep(2.5)
        try:
            if not _has_security_verification_marker(sb):
                elapsed = time.time() - start
                print(f"[{visit_id}] Security verification cleared in {elapsed:.1f}s")
                time.sleep(1.5)  # Brief settle after the check clears
                return True
        except Exception:
            pass

    print(f"[{visit_id}] Security verification did not clear in {timeout:.0f}s — treating as blocked")
    return False


def _accept_language_header(langs: list[str]) -> str:
    """
    Build an Accept-Language header value from a profile's language list.

    e.g. ["uz-UZ","uz","ru-RU","ru","en-US","en"]
      -> "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6"
    """
    if not langs:
        return "en-US,en;q=0.9"
    header = langs[0]
    for i, lang in enumerate(langs[1:5]):
        header += f",{lang};q={round(1.0 - (i + 1) * 0.1, 1)}"
    return header


def _build_ua_metadata(profile: dict, mycdp):
    """
    Build CDP UserAgentMetadata (Client Hints) from a device profile.

    Without this, Chrome leaks the real OS ('Linux' on the server) through the
    Sec-CH-UA-Platform / Sec-CH-UA header set — even when the User-Agent STRING
    says Windows/macOS. Yandex Metrika reads these hints, so we override them to
    match the spoofed device. The underlying engine is always Chromium, so all
    profiles (including the Firefox/Safari UAs) advertise Chromium brands; only
    the OS/platform differs per profile.
    """
    import re

    ua = profile["user_agent"]
    plat = profile.get("platform", "Win32")

    if "Win" in plat:
        ch_platform = "Windows"
        platform_version = "15.0.0"  # Win10/11 report high UA-CH platform versions
        architecture = "x86"
    else:  # Mac
        ch_platform = "macOS"
        m = re.search(r"Mac OS X (\d+[_.]\d+(?:[_.]\d+)?)", ua)
        platform_version = m.group(1).replace("_", ".") if m else "13.5.0"
        renderer = profile.get("webgl", {}).get("renderer", "")
        architecture = "arm" if "Apple M" in renderer else "x86"

    cm = re.search(r"Chrome/(\d+)", ua)
    chrome_major = cm.group(1) if cm else "131"
    full_version = f"{chrome_major}.0.0.0"

    UABV = mycdp.emulation.UserAgentBrandVersion
    not_a = "Not?A_Brand"
    if "Edg/" in ua:
        em = re.search(r"Edg/(\d+)", ua)
        edge_major = em.group(1) if em else chrome_major
        brands = [
            UABV(brand="Chromium", version=chrome_major),
            UABV(brand="Microsoft Edge", version=edge_major),
            UABV(brand=not_a, version="24"),
        ]
        full_version_list = [
            UABV(brand="Chromium", version=full_version),
            UABV(brand="Microsoft Edge", version=f"{edge_major}.0.0.0"),
            UABV(brand=not_a, version="24.0.0.0"),
        ]
    else:
        brands = [
            UABV(brand="Chromium", version=chrome_major),
            UABV(brand="Google Chrome", version=chrome_major),
            UABV(brand=not_a, version="24"),
        ]
        full_version_list = [
            UABV(brand="Chromium", version=full_version),
            UABV(brand="Google Chrome", version=full_version),
            UABV(brand=not_a, version="24.0.0.0"),
        ]

    return mycdp.emulation.UserAgentMetadata(
        platform=ch_platform,
        platform_version=platform_version,
        architecture=architecture,
        model="",
        mobile=False,
        brands=brands,
        full_version_list=full_version_list,
        full_version=full_version,
        bitness="64",
        wow64=False,
    )


def _build_fingerprint_script(profile: dict) -> str:
    """
    Build the persistent JS fingerprint-override script for a device profile.

    Registered via Page.addScriptToEvaluateOnNewDocument on the CDP-mode
    connection so it runs before any page script on every document loaded in the
    session (target + any redirect). It overrides what Yandex Metrica collects:

      - navigator.userAgent / platform / language / languages /
        hardwareConcurrency / deviceMemory / maxTouchPoints  →  per profile
      - screen.width / height / colorDepth  →  per profile
      - window.innerHeight  →  CRITICAL: real browsers subtract ~135px for
        browser chrome; headless bots show innerHeight == screen.height (ratio 1.0)
      - WebGL UNMASKED_VENDOR / RENDERER  →  real GPU strings
    It also installs the Yandex Metrika hit interceptor used for hit detection.
    """
    nav = profile["navigator"]
    scr = profile["screen"]
    vp  = profile["viewport"]
    wgl = profile["webgl"]

    # Real browsers lose ~135 px to Chrome UI on Windows, ~115 px on Mac.
    # innerHeight = screen.height - browser_chrome_px (approx).
    chrome_ui_px = 135 if "Win32" in profile.get("platform", "Win32") else 115
    inner_height = max(500, scr["height"] - chrome_ui_px)

    # Pre-serialise Python values to JSON to avoid f-string brace escaping.
    nav_json = json.dumps({
        "userAgent":           nav["userAgent"],
        "appVersion":          nav["appVersion"],
        "platform":            nav["platform"],
        "language":            nav["language"],
        "languages":           nav["languages"],
        "hardwareConcurrency": nav["hardwareConcurrency"],
        "deviceMemory":        nav["deviceMemory"],
        "maxTouchPoints":      nav["maxTouchPoints"],
    })
    scr_json = json.dumps({
        "width":       scr["width"],
        "height":      scr["height"],
        "availWidth":  scr["width"],
        "availHeight": scr["height"] - 40,   # minus taskbar
        "colorDepth":  scr["colorDepth"],
        "pixelDepth":  scr["pixelDepth"],
    })
    wgl_vendor_js   = json.dumps(wgl["vendor"])
    wgl_renderer_js = json.dumps(wgl["renderer"])

    fp_script = (
        "(function() {\n"
        "  // navigator overrides\n"
        "  const navProps = " + nav_json + ";\n"
        "  for (const [k, v] of Object.entries(navProps)) {\n"
        "    try { Object.defineProperty(navigator, k, { get: () => v, configurable: true }); } catch(e) {}\n"
        "  }\n"
        "  // screen overrides\n"
        "  const scrProps = " + scr_json + ";\n"
        "  for (const [k, v] of Object.entries(scrProps)) {\n"
        "    try { Object.defineProperty(screen, k, { get: () => v, configurable: true }); } catch(e) {}\n"
        "  }\n"
        "  // window.innerHeight: subtract browser chrome so ratio != 1.0\n"
        "  try {\n"
        "    Object.defineProperty(window, 'innerHeight', { get: () => " + str(inner_height) + ", configurable: true });\n"
        "    Object.defineProperty(window, 'innerWidth',  { get: () => " + str(vp["width"]) + ", configurable: true });\n"
        "  } catch(e) {}\n"
        "  // WebGL UNMASKED_VENDOR_WEBGL (37445) and UNMASKED_RENDERER_WEBGL (37446)\n"
        "  const _wv = " + wgl_vendor_js + ";\n"
        "  const _wr = " + wgl_renderer_js + ";\n"
        "  [window.WebGLRenderingContext, window.WebGL2RenderingContext].forEach(function(Ctx) {\n"
        "    if (!Ctx) return;\n"
        "    const orig = Ctx.prototype.getParameter;\n"
        "    Ctx.prototype.getParameter = function(p) {\n"
        "      if (p === 37445) return _wv;\n"
        "      if (p === 37446) return _wr;\n"
        "      return orig.call(this, p);\n"
        "    };\n"
        "  });\n"
        "  // ---- Yandex Metrika hit interceptor ----\n"
        "  // Intercepts fetch/XHR/Image/sendBeacon at the JS level BEFORE the request leaves.\n"
        "  // This is more reliable than performance.getEntriesByType() which can miss fast hits.\n"
        "  window._ymHitDetected = false;\n"
        "  var _capYm = function(u) {\n"
        "    if (u && u.indexOf && u.indexOf('mc.yandex') !== -1) { window._ymHitDetected = true; }\n"
        "  };\n"
        "  if (window.fetch) {\n"
        "    var _oFetch = window.fetch;\n"
        "    window.fetch = function(u) {\n"
        "      try { _capYm(typeof u === 'string' ? u : (u && u.url ? u.url : '')); } catch(e) {}\n"
        "      return _oFetch.apply(this, arguments);\n"
        "    };\n"
        "  }\n"
        "  if (window.XMLHttpRequest) {\n"
        "    var _oOpen = XMLHttpRequest.prototype.open;\n"
        "    XMLHttpRequest.prototype.open = function(m, u) {\n"
        "      try { _capYm(String(u)); } catch(e) {}\n"
        "      return _oOpen.apply(this, arguments);\n"
        "    };\n"
        "  }\n"
        "  if (navigator.sendBeacon) {\n"
        "    var _oBeacon = navigator.sendBeacon.bind(navigator);\n"
        "    navigator.sendBeacon = function(u) {\n"
        "      try { _capYm(String(u)); } catch(e) {}\n"
        "      return _oBeacon.apply(this, arguments);\n"
        "    };\n"
        "  }\n"
        "  var _ImgDesc = Object.getOwnPropertyDescriptor(HTMLImageElement.prototype, 'src');\n"
        "  if (_ImgDesc && _ImgDesc.set) {\n"
        "    Object.defineProperty(HTMLImageElement.prototype, 'src', {\n"
        "      set: function(v) { try { _capYm(String(v)); } catch(e) {} return _ImgDesc.set.call(this, v); },\n"
        "      get: _ImgDesc.get, configurable: true\n"
        "    });\n"
        "  }\n"
        "})();"
    )

    return fp_script


def _apply_cdp_fingerprint(
    sb, profile: dict, accept_language: str
) -> None:
    """
    Apply the device-profile fingerprint on the ACTIVE CDP-mode connection.

    Must run AFTER activate_cdp_mode() and BEFORE opening the target URL.

    Why here (and not before CDP mode): SeleniumBase tears down the webdriver
    DevTools session when it enters CDP mode, which DISCARDS any overrides set
    via sb.execute_cdp_cmd() beforehand. That is exactly why the device profile
    never took effect — Chrome fell back to its real (Linux) identity + en-US.
    These CDP commands are issued on the connection that actually loads the
    target, so they persist for the visit.

    NOTE: We deliberately do NOT set a custom "Referer" via
    Network.setExtraHTTPHeaders here. Doing so in CDP mode aborts the target
    navigation (Chrome returns chrome-error://chromewebdata/). Attribution to
    Yandex is already carried by the yclid/utm_* URL params on the targets, and
    Accept-Language is delivered by the User-Agent override below — so the
    Referer header is both unsafe and unnecessary. (Verified on-server: setting
    Referer -> 100% chrome-error; removing it -> loads + hit detected.)
    """
    import mycdp
    import mycdp.network
    import mycdp.emulation
    import mycdp.page

    loop = sb.cdp.loop
    page = sb.cdp.page

    def send(cmd):
        try:
            loop.run_until_complete(page.send(cmd))
        except Exception:
            pass

    send(mycdp.network.enable())
    send(mycdp.page.enable())

    # 1. User-Agent string + navigator.platform + Client Hints (Sec-CH-UA-*).
    #    The Client Hints metadata is what stops 'Linux' from leaking on the
    #    server regardless of the spoofed User-Agent string.
    try:
        ua_meta = _build_ua_metadata(profile, mycdp)
    except Exception:
        ua_meta = None
    send(mycdp.network.set_user_agent_override(
        user_agent=profile["user_agent"],
        accept_language=accept_language,
        platform=profile["platform"],
        user_agent_metadata=ua_meta,
    ))

    # 2. Timezone (Metrika reads the resolved IANA zone).
    send(mycdp.emulation.set_timezone_override(timezone_id=profile["timezone"]))

    # 3. navigator / screen / WebGL / innerHeight overrides + Metrika interceptor.
    send(mycdp.page.add_script_to_evaluate_on_new_document(
        source=_build_fingerprint_script(profile)
    ))


def _visit_with_proxy_impl(
    proxy: dict,
    target: str,
    visit_id: int,
    secondary_urls: list[str] | None = None,
) -> dict:
    """
    Visit target URL with optimized human-like behavior.
    Browser closes after visit (max ~50 seconds) to prevent hanging.
    
    Returns: structured visit result
    """
    proxy_string = (
        f"{proxy['username']}:{proxy['password']}@"
        f"{proxy['host']}:{proxy['port']}"
    )
    started_at = time.time()
    
    print(f"[{visit_id}] Starting visit with proxy {proxy['host']}")
    
    try:
        # Extract domain from target URL for Referer header
        parsed = urlparse(target)
        domain = parsed.netloc.replace("www.", "")
        tld = domain.split('.')[-1] if '.' in domain else 'com'

        # Pick a device profile for this visit (varies fingerprint across workers)
        profile = pick_profile()

        # Language for this device: primary locale (e.g. "uz-UZ") drives the
        # browser's --lang / navigator.language; the full list builds the
        # Accept-Language header. These are the values Metrika reports as the
        # visitor language — NOT the server's default en-US.
        primary_locale = profile["navigator"].get("language", "uz-UZ")
        accept_language = _accept_language_header(profile["navigator"]["languages"])

        cookie_plan = get_cookie_replay_plan(target)

        print(f"[{visit_id}] Using proxy {proxy['host']}")
        print(f"[{visit_id}] Domain: {domain}, TLD: {tld}")
        print(f"[{visit_id}] Profile: {profile['name']}")
        if cookie_plan["is_returning"]:
            predicted_host = cookie_plan.get("predicted_final_host") or "unknown"
            print(f"[{visit_id}] Returning-cookie profile selected | predicted_host={predicted_host}")

        with SB(
            uc=True,
            proxy=proxy_string,
            headless=False,
            page_load_strategy="eager",
            test=True,
            # Bake the device identity into the browser at launch:
            #   agent       -> --user-agent flag (every request uses this UA)
            #   locale_code -> intl.accept_languages pref + CDP locale override
            # These survive the switch into CDP mode (unlike execute_cdp_cmd
            # overrides), so the server's native Linux UA / en-US never surface.
            agent=profile["user_agent"],
            locale_code=primary_locale,
        ) as sb:
            if STOP_FLAG:
                return _visit_result(False, False, started_at, "stopped")

            print(f"[{visit_id}] Browser session started")

            try:
                sb.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            except Exception:
                pass

            if not _sleep_range(POST_LAUNCH_DELAY):
                return _visit_result(False, False, started_at, "stopped")
            
            # Hide webdriver
            try:
                sb.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
            except Exception:
                pass

            # NOTE: The device fingerprint (UA / navigator / screen / WebGL /
            # timezone / Client Hints / Referer / Accept-Language) is applied
            # AFTER activate_cdp_mode() via _apply_cdp_fingerprint(), because
            # overrides set here (pre-CDP) would be discarded when SeleniumBase
            # tears down the webdriver session to enter CDP mode.

            # Inject Yandex-global cookies before the Yandex prewarm so the
            # prewarm page sees a stable Yandex-side visitor identity.
            try:
                injected = _inject_cookies(sb, cookie_plan["global_cookies"])
                if injected:
                    print(f"[{visit_id}] Injected {injected} Yandex-global cookies before prewarm")
            except Exception:
                pass

            if STOP_FLAG:
                return _visit_result(False, False, started_at, "stopped")

            # Pre-warm
            try:
                sb.activate_cdp_mode(PREWARM_URL)
            except Exception:
                pass

            # Apply the full device fingerprint on the live CDP connection.
            # This is what the target site + its Metrika counter actually see:
            # the profile's UA, platform, language, screen, WebGL, timezone and
            # Client Hints — instead of the server's real Linux / en-US identity.
            try:
                _apply_cdp_fingerprint(sb, profile, accept_language)
                print(f"[{visit_id}] Applied device fingerprint: {profile['name']} "
                      f"| lang={primary_locale}")
            except Exception as _fp_err:
                print(f"[{visit_id}] Fingerprint apply failed: {_fp_err}")

            if not _sleep_range(PREWARM_DELAY):
                return _visit_result(False, False, started_at, "stopped")

            # Inject first-party Metrika cookies immediately before the target
            # open so the landing site sees them on the first request.
            try:
                injected = _inject_cookies(sb, cookie_plan["site_cookies"])
                if injected:
                    predicted_host = cookie_plan.get("predicted_final_host") or domain
                    print(f"[{visit_id}] Injected {injected} first-party Metrika cookies for {predicted_host}")
            except Exception:
                pass

            try:
                sb.open(target)
            except Exception as e:
                print(f"[{visit_id}] Open failed: {e}")
                return _visit_result(False, False, started_at, f"open_failed:{e}")

            if not _wait_for_ready(sb):
                return _visit_result(False, False, started_at, "ready_timeout")

            if not _sleep_range(POST_OPEN_SETTLE):
                return _visit_result(False, False, started_at, "stopped")

            current_url = sb.get_current_url() or ""
            print(f"[{visit_id}] Landed on: {current_url}")
            final_host = urlparse(current_url).netloc.replace("www.", "").split(":", 1)[0].lower()
            predicted_host = cookie_plan.get("predicted_final_host")
            if predicted_host and final_host and predicted_host != final_host:
                print(f"[{visit_id}] Final host resolved to {final_host} (predicted {predicted_host})")

            # Persist the observed landing host immediately so repeated short-links
            # can pre-inject first-party cookies before the next visit.
            try:
                save_cookies_from_browser(target, current_url, [])
            except Exception:
                pass

            # Check if blocked
            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Detected blocked/404 page")
                return _visit_result(False, False, started_at, "blocked")

            if not _maybe_handle_challenge(sb, visit_id):
                return _visit_result(False, False, started_at, "challenge_failed")

            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Blocked after challenge handling")
                return _visit_result(False, False, started_at, "blocked_after_challenge")

            # Wait for any auto-resolving security verification page (Cloudflare WAF,
            # centrum-air 'Performing security verification', etc.) to clear BEFORE
            # starting human simulation. Scrolling/clicking on a security gate page
            # is a strong bot signal since no real user does that.
            if not _wait_for_security_verification(sb, visit_id):
                return _visit_result(False, False, started_at, "security_verification_timeout")

            # Sample this visit's total duration from the natural distribution.
            # ~20% will be short "bounce" visits (12-22s), the rest longer.
            target_duration = visit_duration()
            is_bounce = target_duration < 25
            print(f"[{visit_id}] Target duration: {target_duration:.0f}s ({'bounce' if is_bounce else 'read'})")

            # Wait for Metrika to load — passive, no synthetic JS trigger.
            metrika_ready = _metrika_loaded(sb)
            print(f"[{visit_id}] Metrika {'loaded' if metrika_ready else 'not loaded'}")

            hit_ok = False
            if metrika_ready:
                # Natural hit: just wait; ym() fires automatically on page load.
                hit_ok = _wait_for_metrika_hit(sb, timeout=METRIKA_HIT_TIMEOUT)

            print(f"[{visit_id}] Metrika hit status: {'VERIFIED' if hit_ok else 'NOT FOUND'}")

            # Bounce visits exit immediately after Metrika fires (or times out).
            if is_bounce:
                elapsed = time.time() - started_at
                remaining = max(0.0, target_duration - elapsed)
                if remaining > 0:
                    _sleep_interruptible(remaining)
                # No scrolling for bounce visits — they just leave
            else:
                # Calculate scroll pattern based on content
                scroll_pattern = _calculate_scroll_pattern(sb)

                # Perform human-like scroll
                if not _perform_human_like_scroll(sb, scroll_pattern):
                    return _visit_result(False, hit_ok, started_at, "behavior_failed")

                # Fill remaining time up to target_duration with human reading
                elapsed = time.time() - started_at
                read_time = max(5.0, target_duration - elapsed)
                read_min = max(5, int(read_time * 0.7))
                read_max = max(read_min + 1, int(read_time * 1.1))
                if not _simulate_human_read(sb, read_min, read_max):
                    return _visit_result(False, hit_ok, started_at, "human_sim_failed")

            # Final Metrika check if not yet verified
            if not hit_ok:
                hit_ok = _wait_for_metrika_hit(sb, timeout=METRIKA_HIT_TIMEOUT)
                print(f"[{visit_id}] Final Metrika check: {'VERIFIED' if hit_ok else 'NOT FOUND'}")

            # Final URL
            try:
                current_url = sb.get_current_url()
            except Exception:
                current_url = ""

            final_elapsed = time.time() - started_at
            print(
                f"[{visit_id}] Visit completed | hit={hit_ok} "
                f"| duration={final_elapsed:.1f}s | final_url={current_url}"
            )

            # Persist cookies using the final landing host for `_ym_*` cookies and
            # a shared pool for Yandex-global cookies.
            try:
                raw_cookies = sb.execute_cdp_cmd("Network.getAllCookies", {}).get("cookies", [])
                save_cookies_from_browser(target, current_url, raw_cookies)
            except Exception:
                pass

            return _visit_result(True, hit_ok, started_at)

    except Exception as e:
        print(f"[{visit_id}] Error: {e}")
        return _visit_result(False, False, started_at, str(e))
    finally:
        # Ensure browser cleanup even on unexpected exits
        try:
            # Close all open windows
            try:
                sb.close_current_window()
            except Exception:
                pass
        except Exception:
            pass
        
        try:
            # Force quit the SB instance - this is the critical fix
            # Using quit() instead of just closing windows
            if hasattr(sb, 'driver'):
                try:
                    sb.driver.quit()
                except Exception:
                    pass
        except Exception:
            pass
        
        try:
            # Force quit the SB object itself
            try:
                sb.quit()
            except Exception:
                pass
        except Exception:
            pass
        
        # Final cleanup of any remaining Chrome processes
        try:
            cleanup_chrome()
        except Exception:
            pass

    return _visit_result(False, False, started_at, "unknown_failure")


def visit_with_proxy(
    proxy: dict,
    target: str,
    visit_id: int,
    secondary_urls: list[str] | None = None,
) -> dict:
    """
    Guard the SeleniumBase visit with a hard timeout.

    The inner implementation uses ``with SB(...)``. If SeleniumBase hangs while
    leaving that context manager, control never reaches the outer cleanup code in
    `_visit_with_proxy_impl()`. Running the visit in a daemon thread lets the
    child process return a structured failure instead of keeping the last worker
    stuck forever.
    """
    started_at = time.time()
    result_holder: dict[str, dict] = {
        "result": _visit_result(False, False, started_at, "not_started")
    }

    def _worker():
        try:
            result_holder["result"] = _visit_with_proxy_impl(
                proxy=proxy,
                target=target,
                visit_id=visit_id,
                secondary_urls=secondary_urls,
            )
        except Exception as exc:
            result_holder["result"] = _visit_result(
                False,
                False,
                started_at,
                f"thread_crashed:{str(exc)[:160]}",
            )

    worker = threading.Thread(
        target=_worker,
        daemon=True,
        name=f"visit-{visit_id}",
    )
    worker.start()
    worker.join(timeout=VISIT_TIMEOUT)

    if worker.is_alive():
        print(f"[{visit_id}] Visit timeout after {VISIT_TIMEOUT}s; forcing Chrome cleanup")
        try:
            cleanup_chrome()
        except Exception:
            pass
        time.sleep(0.5)
        return _visit_result(False, False, started_at, f"visit_timeout:{VISIT_TIMEOUT}s")

    return result_holder["result"]


# ==================== CONVENIENCE FUNCTIONS ====================
def get_default_config() -> dict:
    """Get default configuration."""
    return {
        "browser_min_lifetime": BROWSER_MIN_LIFETIME,
        "browser_max_lifetime": BROWSER_MAX_LIFETIME,
        "metrika_timeout": METRIKA_HIT_TIMEOUT,
        "min_visit_total": MIN_VISIT_TOTAL,
    }


# Reset global stop flag on module import
set_stop_flag(False)
