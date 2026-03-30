"""
scrp_v2.py - Human-like browser automation with:
- Realistic scrolling behavior (mimics human movement patterns)
- Smart Metrika hit detection with 15s fallback timeout
- 100-120 second maximum browser lifetime
- Dynamic behavior based on page characteristics
"""
import random
import threading
import time
import uuid
from urllib.parse import urlparse

from seleniumbase import SB

from tokens import PASSWORD, PROXY_HOST, PROXY_PORT, USERNAME

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

# Browser lifetime limits - shorter to prevent hanging
BROWSER_MIN_LIFETIME = 30   # Minimum seconds browser stays alive
BROWSER_MAX_LIFETIME = 45   # Maximum seconds browser stays alive (reduced from 120)
BROWSER_MAX_AGE = BROWSER_MAX_LIFETIME  # Browser restarts after this age

# Metrika hit detection
METRIKA_HIT_TIMEOUT = 15  # Wait up to 15s for Metrika hit
MIN_VISIT_TOTAL = 25       # Minimum total visit time
HUMAN_DURATION_VARIANCE = 3  # Additional human-like duration variance

# Timeout for overall visit to prevent hanging (reduced from 120s to 60s)
VISIT_TIMEOUT = 120  # Maximum time for a single visit to prevent hanging browsers

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
                    if any(x in name for x in ["chrome", "chromedriver", "geckodriver", "firefox"]):
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
    
    # Final scroll to bottom if we haven't reached it
    try:
        max_scroll = sb.execute_script("return document.body.scrollHeight - window.innerHeight")
        current_pos = sb.execute_script("return window.scrollY")
        
        if max_scroll > 0 and current_pos < max_scroll * 0.8:
            # Final scroll to bottom with natural delay
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
    """Trigger Metrika tracking with params."""
    try:
        triggered = sb.execute_script(
            """
            var ids = new Set();
            if (typeof window.ym === 'function') {
                if (window.Ya && window.Ya._metrika && window.Ya._metrika.counters) {
                    Object.keys(window.Ya._metrika.counters).forEach(function(id) {
                        ids.add(parseInt(id, 10));
                    });
                }
            }
            Object.keys(window).forEach(function(key) {
                var match = /^yaCounter(\\d+)$/.exec(key);
                if (match) {
                    ids.add(parseInt(match[1], 10));
                }
            });
            ids.forEach(function(id) {
                try {
                    window.ym(id, 'params', {__ym:{visit:1}});
                } catch(e) {}
            });
            return ids.size;
            """
        )
        return bool(triggered)
    except Exception:
        return False


def _has_metrika_hit_signal(sb: SB) -> bool:
    """Check several browser-visible signals that a Metrika hit fired."""
    try:
        return bool(
            sb.execute_script(
                """
                const resources = performance.getEntriesByType('resource') || [];
                const hasResourceHit = resources.some((entry) => {
                    const name = entry && entry.name ? String(entry.name) : '';
                    return name.includes('mc.yandex') && name.includes('/watch');
                });

                let hasStorageHit = false;
                try {
                    for (let i = 0; i < localStorage.length; i += 1) {
                        const key = localStorage.key(i) || '';
                        if (/^_ym\\d+_lastHit$/.test(key)) {
                            hasStorageHit = true;
                            break;
                        }
                    }
                } catch (e) {}

                const hasNoscriptHit = Array.from(document.images || []).some((img) => {
                    const src = img && img.src ? String(img.src) : '';
                    return src.includes('mc.yandex') && src.includes('/watch');
                });

                return hasResourceHit || hasStorageHit || hasNoscriptHit;
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
        
        # Dynamic Referer based on domain TLD
        if tld == 'uz':
            referer_url = f"https://yandex.uz/search/?text={domain}"
        elif tld == 'ru':
            referer_url = f"https://yandex.ru/search/?text={domain}"
        else:
            referer_url = f"https://www.google.com/search?q={domain}"
        
        print(f"[{visit_id}] Using proxy {proxy['host']}")
        print(f"[{visit_id}] Domain: {domain}, TLD: {tld}, Referer: {referer_url}")

        with SB(
            uc=True,
            proxy=proxy_string,
            headless=False,
            page_load_strategy="eager",
            test=True,
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

            # Set custom headers
            try:
                sb.execute_cdp_cmd(
                    "Network.setExtraHTTPHeaders",
                    {
                        "headers": {
                            "Referer": referer_url,
                            "Accept-Language": "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6,en;q=0.5",
                        }
                    },
                )
            except Exception:
                pass

            if STOP_FLAG:
                return _visit_result(False, False, started_at, "stopped")

            # Pre-warm
            try:
                sb.activate_cdp_mode(PREWARM_URL)
            except Exception:
                pass

            if not _sleep_range(PREWARM_DELAY):
                return _visit_result(False, False, started_at, "stopped")

            try:
                sb.open(target)
            except Exception as e:
                print(f"[{visit_id}] Open failed: {e}")
                return _visit_result(False, False, started_at, f"open_failed:{e}")

            if not _wait_for_ready(sb):
                return _visit_result(False, False, started_at, "ready_timeout")

            if not _sleep_range(POST_OPEN_SETTLE):
                return _visit_result(False, False, started_at, "stopped")

            current_url = sb.get_current_url()
            print(f"[{visit_id}] Landed on: {current_url}")

            # Check if blocked
            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Detected blocked/404 page")
                return _visit_result(False, False, started_at, "blocked")

            if not _maybe_handle_challenge(sb, visit_id):
                return _visit_result(False, False, started_at, "challenge_failed")

            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Blocked after challenge handling")
                return _visit_result(False, False, started_at, "blocked_after_challenge")

            # Calculate scroll pattern based on content
            scroll_pattern = _calculate_scroll_pattern(sb)
            
            # Perform human-like scroll
            if not _perform_human_like_scroll(sb, scroll_pattern):
                return _visit_result(False, False, started_at, "behavior_failed")

            # Wait for Metrika to load, then flush/verify
            metrika_ready = _metrika_loaded(sb)
            print(f"[{visit_id}] Metrika {'loaded' if metrika_ready else 'not loaded'}")
            
            hit_ok = False
            if metrika_ready:
                hit_ok = _flush_and_wait_metrika(sb, timeout=METRIKA_HIT_TIMEOUT)
            else:
                # Still try to wait for hit even if not initially loaded
                hit_ok = _wait_for_metrika_hit(sb, timeout=METRIKA_HIT_TIMEOUT)
            
            print(f"[{visit_id}] Metrika hit status: {'VERIFIED' if hit_ok else 'NOT FOUND'}")

            # Simulate human reading behavior
            if not _simulate_human_read(sb, 8, 12):  # Reduced for faster execution
                return _visit_result(False, hit_ok, started_at, "human_sim_failed")

            # Optional secondary tabs for variety
            if secondary_urls:
                for extra in secondary_urls:
                    if STOP_FLAG:
                        return _visit_result(False, hit_ok, started_at, "stopped")
                    try:
                        sb.open_new_tab(extra)
                        _sleep_range((0.8, 1.2))
                        sb.switch_to_window(0)
                    except Exception:
                        try:
                            sb.close_current_window()
                            sb.switch_to_window(0)
                        except Exception:
                            pass

            # Final Metrika check (may have loaded during human simulation)
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
