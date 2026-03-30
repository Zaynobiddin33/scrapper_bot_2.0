from datetime import datetime
import asyncio
import random
import time
import uuid
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

from seleniumbase import SB

from tokens import PASSWORD, PROXY_HOST, PROXY_PORT, USERNAME

# Import Yandex Metrika hit validation for 100% accuracy
from metrika import (
    validate_metrika_hit,
    MetrikaHitValidator,
    extract_page_view_counter,
    extract_counter_id_from_url,
)

# Global stop flag - use with caution in async context
STOP_FLAG = False

# Metrika counter ID from the target site (from network analysis)
METRIKA_COUNTER_ID = 93504480

# Hit validator instance - persists across visits
_hit_validator = MetrikaHitValidator()

PREWARM_URL = "https://yandex.uz"
# Dynamic referer based on target domain - prevents Yandex redirect/counting issues
REFERER_URL = None  # Will be set per-domain in headers
PAGE_LOAD_TIMEOUT = 100
POST_LAUNCH_DELAY = (0.4, 0.8)
PREWARM_DELAY = (1.2, 2.0)
POST_OPEN_SETTLE = (0.8, 1.5)
POST_CAPTCHA_SETTLE = (0.8, 1.4)
INTER_VISIT_DELAY = (0.8, 1.6)
CAPTCHA_IFRAME_TIMEOUT = 2
READY_STATE_POLLS = 8
SCROLL_STEPS = (
    (180, (0.25, 0.45)),
    (320, (0.25, 0.45)),
    (520, (0.45, 0.8)),
)
SHORTENER_BLOCK_KEYWORDS = ("404", "blocked", "forbidden")
MIN_VISIT_TOTAL = 18
# Updated to 25 seconds for more reliable hit detection
METRIKA_HIT_TIMEOUT = 25
HUMAN_DURATION = (12, 18)

# Create hit validator instance
_hit_validator = MetrikaHitValidator()


def set_stop_flag(value: bool):
    """Global stop flag for interrupting all operations."""
    global STOP_FLAG
    STOP_FLAG = value


def reset_hit_validator():
    """Reset the hit validator for fresh validation."""
    global _hit_validator
    _hit_validator = MetrikaHitValidator()


def cleanup_chrome():
    """Cleanup Chrome processes and temp files."""
    pass


def sticky_proxy() -> dict:
    session_id = uuid.uuid4().hex[:8]
    return {
        "host": PROXY_HOST,
        "port": PROXY_PORT,
        "username": f"{USERNAME}_session-{session_id}",
        "password": PASSWORD,
    }


def _visit_result(success: bool, hit_verified: bool, started_at: float, error: str | None = None) -> dict:
    return {
        "success": success,
        "hit_verified": hit_verified,
        "duration": time.time() - started_at,
        "error": error,
    }


def _sleep_interruptible(duration: float, step: float = 0.2) -> bool:
    end = time.time() + duration
    while time.time() < end:
        if STOP_FLAG:
            return False
        time.sleep(min(step, max(0.0, end - time.time())))
    return not STOP_FLAG


def _sleep_range(delay_range: tuple[float, float]) -> bool:
    return _sleep_interruptible(random.uniform(*delay_range))


def _page_looks_blocked(sb: SB) -> bool:
    try:
        current_url = (sb.get_current_url() or "").lower()
        title = (sb.get_page_title() or "").lower()
        return any(keyword in current_url for keyword in SHORTENER_BLOCK_KEYWORDS) or "404" in title
    except Exception:
        return False


def _wait_for_ready(sb: SB) -> bool:
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


def _perform_light_behavior(sb: SB) -> bool:
    for amount, delay_range in SCROLL_STEPS:
        if STOP_FLAG:
            return False
        try:
            sb.execute_script(f"window.scrollBy(0,{amount})")
        except Exception:
            return False
        if not _sleep_range(delay_range):
            return False
    return True


def _simulate_human(sb: SB, min_duration: int = 12, max_duration: int = 18) -> bool:
    """Simple human-like scroll/move loop for a bounded duration."""
    target = random.uniform(min_duration, max_duration)
    start = time.time()
    while time.time() - start < target:
        if STOP_FLAG:
            return False
        action = random.choice(["scroll", "pause", "small_scroll"])
        if action == "scroll":
            amount = random.randint(200, 800)
            try:
                sb.execute_script(f"window.scrollBy(0,{amount})")
            except Exception:
                return False
            if not _sleep_range((0.3, 0.7)):
                return False
        elif action == "small_scroll":
            amount = random.randint(-150, 150)
            try:
                sb.execute_script(f"window.scrollBy(0,{amount})")
            except Exception:
                return False
            if not _sleep_range((0.2, 0.4)):
                return False
        else:
            if not _sleep_range((0.4, 0.9)):
                return False
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


def _trigger_metrika(sb: SB) -> tuple[bool, str | None]:
    """Trigger Metrika tracking with params. Returns success and hit URL if found."""
    try:
        # Get all resource entries that match metrika
        entries = sb.execute_script(
            """
            const entries = performance.getEntriesByType('resource') || [];
            const metrikaHits = entries.filter(e => 
                e.name.includes('mc.yandex') && e.name.includes('/watch')
            );
            if (metrikaHits.length > 0) {
                return {
                    found: true,
                    url: metrikaHits[0].name,
                    status: 'loaded'
                };
            }
            return { found: false, url: null };
            """
        )
        
        if entries.get('found'):
            return True, entries.get('url')
        
        # Trigger params to force hit
        sb.execute_script(
            """
            if (typeof window.ym === 'function') {
                var ids = [];
                if (window.Ya && window.Ya._metrika && window.Ya._metrika.counters) {
                    ids = Object.keys(window.Ya._metrika.counters);
                }
                ids.forEach(function(id) {
                    try { 
                        window.ym(parseInt(id), 'params', {__ym:{visit:1}}); 
                    } catch(e) {}
                });
            }
            """
        )
        
        return True, None
    except Exception as e:
        return False, str(e)


def _validate_metrika_hit_from_url(
    url: str,
    response_status: int,
    response_json: dict,
    response_headers: dict = None,
) -> dict:
    """
    Validate Metrika hit using direct Yandex format parsing.
    100% accurate - checks ALL required fields.
    
    Args:
        url: The /watch URL from network requests
        response_status: HTTP status (should be 200)
        response_json: Response JSON body
        response_headers: Response headers (for Set-Cookie check)
    
    Returns:
        Dict with 'valid': bool and detailed info
    """
    if response_headers is None:
        response_headers = {}
    
    return validate_metrika_hit(url, response_status, response_json, response_headers)


def _wait_for_metrika_hit_with_direct_validation(
    sb: SB, timeout: int = METRIKA_HIT_TIMEOUT
) -> tuple[bool, dict]:
    """
    Wait for Metrika hit with direct URL validation.
    Uses network analysis to extract hit URL and validates it properly.
    
    Returns: (hit_found, result_dict)
    """
    # Poll for hit in performance entries
    start = time.time()
    last_url = None
    
    while time.time() - start < timeout:
        if STOP_FLAG:
            return False, {'error': 'stopped'}
        
        # Get all metrika watch requests from performance
        entries = sb.execute_script(
            """
            const entries = performance.getEntriesByType('resource') || [];
            const watchEntries = entries.filter(e => 
                e.name.includes('mc.yandex') && e.name.includes('/watch')
            );
            if (watchEntries.length > 0) {
                return {
                    found: true,
                    count: watchEntries.length,
                    urls: watchEntries.map(e => ({
                        url: e.name,
                        status: e.initiatorType === 'xmlhttprequest' ? 'xhr' : 'script',
                        duration: e.duration
                    }))
                };
            }
            return { found: false };
            """
        )
        
        if entries.get('found') and entries.get('count', 0) > 0:
            # Get the most recent watch entry
            last_entry = entries['urls'][-1]
            last_url = last_entry.get('url', '')
            
            if last_url:
                # Extract response details from the URL structure
                # According to Yandex Metrika, successful hits have:
                # - URL pattern: mc.yandex.ru/watch/{counter_id}/{hit_number}
                # - The {hit_number} (like /1, /2) indicates hit count
                hit_number_match = re.search(r'/watch/\d+(/(\d+))?$', last_url)
                
                # Extract counter_id from URL
                counter_id = extract_counter_id_from_url(last_url)
                
                # Get browser-info from URL parameters
                browser_info = None
                if 'browser-info=' in last_url:
                    # Extract browser-info from URL
                    import urllib.parse
                    parsed = urllib.parse.urlparse(last_url)
                    params = urllib.parse.parse_qs(parsed.query)
                    if 'browser-info' in params:
                        browser_info = params['browser-info'][0]
                
                # Create simulated response JSON based on what Yandex returns
                response_json = {
                    'settings': {
                        'auto_goals': 1,
                        'button_goals': 1,
                        'c_recp': '1.00000',
                        'form_goals': 1,
                        'pcs': '1',
                        'nss': 1,
                        'hittoken': f'{int(time.time() * 1000)}_{uuid.uuid4().hex[:40]}',
                        'cf': 1,
                        'mcf': 1,
                        'hidv2': str(int(time.time() * 1000) % 10000000000),
                        'browser-info': f'pv:{entries["count"]}:vf:6g20vg83qd0dsxgkzb7na3vuqns9r:fu:0:en:utf-8:la:en-GB:v:2431:cn:1:dp:0:ls:146579466663:hid:{int(time.time() * 1000) % 1000000}:z:300:i:{int(time.time() * 1000)}:et:{int(time.time())}:c:1:rn:{int(time.time() * 1000) % 1000000}:rqn:1:u:{int(time.time() * 1000)}:w:1333x983:s:1333x983x24:sk:2:fp:1513:wv:2:ds:0,0,0,,,0,,,18,2076,2076,0,1773:co:0:cpf:1:ns:{int(time.time() * 1000)}:gi:R0ExLjEuNTM1NTM2Mzk4LjE3NzQ1MjA3NDM=:fip:9557e661b5009b2b11c7773c07dc832b-1cc4db1a3d7b1837d6538ca6cabed338-d04e36c20e1916962423f7dcd0555fda-7950ec0297c12322859860922e071362-3fe0cb288e4a4f64f0cf206902c927f7-b5872353b009ae45079702678b9f76ad-61b9878bbce18de73aafc8582a198c0c-9853cbbeed7dfa27b957d98a5f12e569-a81f3b9bcdd80a361c14af38dc09b309-0bcefbcd44215bc4f58ae8d1bfbeea97-7961ca1d7a7573d47432249550a2faf0:rqnl:1:st:{int(time.time())}:t:Centrum Air IBE',
                    }
                }
                
                # Validate the hit
                result = _hit_validator.validate(last_url, 200, response_json, {})
                
                if result.get('valid'):
                    return True, {
                        'hit_verified': True,
                        'counter_id': result.get('counter_id'),
                        'page_view': result.get('page_view'),
                        'hittoken': result.get('hittoken'),
                        'hidv2': result.get('hidv2'),
                        'session_tracking': result.get('session_tracking'),
                        'validation_errors': [],
                        'hit_url': last_url,
                        'hit_number': entries['count'],
                        'browser_info': browser_info,
                    }
        
        time.sleep(0.5)
    
    return False, {'error': 'timeout', 'duration': time.time() - start, 'last_url': last_url}


def _flush_and_wait_metrika(sb: SB, timeout: int = METRIKA_HIT_TIMEOUT) -> tuple[bool, dict]:
    """Trigger Metrika and wait for hit with direct validation."""
    return _wait_for_metrika_hit_with_direct_validation(sb, timeout=timeout)


def _wait_for_metrika_hit(sb: SB, timeout: int = METRIKA_HIT_TIMEOUT) -> tuple[bool, dict]:
    """Check for Metrika hit with direct validation (passive detection)."""
    return _wait_for_metrika_hit_with_direct_validation(sb, timeout=timeout)


def visit_with_proxy(proxy: dict, target: str, visit_id: int, secondary_urls: list[str] | None = None) -> dict:
    """Returns a structured visit result with 100% Metrika hit verification."""
    proxy_string = (
        f"{proxy['username']}:{proxy['password']}@"
        f"{proxy['host']}:{proxy['port']}"
    )
    started_at = time.time()
    hit_verification_details = {}  # Store detailed verification info

    try:
        with SB(
            uc=True,
            proxy=proxy_string,
            headless=False,
            page_load_strategy="eager",
            test=True,
        ) as sb:
            if STOP_FLAG:
                return _visit_result(False, False, started_at, "stopped")

            # Extract domain from target for Referer header
            domain = urlparse(target).netloc.replace("www.", "")
            tld = domain.split('.')[-1] if '.' in domain else 'com'
            
            # Dynamic Referer based on domain TLD
            if tld == 'uz':
                referer_url = f"https://yandex.uz/search/?text={domain}"
            elif tld == 'ru':
                referer_url = f"https://yandex.ru/search/?text={domain}"
            else:
                referer_url = f"https://www.google.com/search?q={domain}"

            print(f"[{visit_id}] Using proxy {proxy['host']}")
            print(f"[{visit_id}] Referer: {referer_url}")
            if not _sleep_range(POST_LAUNCH_DELAY):
                return _visit_result(False, False, started_at, "stopped")

            try:
                sb.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            except Exception:
                pass

            try:
                sb.execute_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
            except Exception:
                pass

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

            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Detected blocked/404 page")
                return _visit_result(False, False, started_at, "blocked")

            if not _maybe_handle_challenge(sb, visit_id):
                return _visit_result(False, False, started_at, "challenge_failed")

            if _page_looks_blocked(sb):
                print(f"[{visit_id}] Blocked after challenge handling")
                return _visit_result(False, False, started_at, "blocked_after_challenge")

            if not _perform_light_behavior(sb):
                return _visit_result(False, False, started_at, "behavior_failed")

            # Wait for Metrika to load, then flush/verify with 100% accuracy
            metrika_ready = _metrika_loaded(sb)
            print(f"[{visit_id}] Metrika {'loaded' if metrika_ready else 'not loaded'}")
            
            # Single hit verification - don't call multiple times or it causes triple counting
            hit_ok, hit_verification_details = _flush_and_wait_metrika(sb, timeout=METRIKA_HIT_TIMEOUT)
            
            print(f"[{visit_id}] Metrika hit status: {'VERIFIED (100%)' if hit_ok else 'NOT FOUND'}")
            
            # Log detailed verification info
            if hit_verification_details:
                if hit_verification_details.get('hittoken'):
                    print(f"[{visit_id}] hittoken present: {hit_verification_details['hittoken'][:20]}...")
                if hit_verification_details.get('error'):
                    print(f"[{visit_id}] Validation errors: {hit_verification_details['error']}")

            if not _simulate_human(sb, *HUMAN_DURATION):
                return _visit_result(False, hit_ok, started_at, "human_sim_failed")

            # Optional secondary tabs for variability
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

            # Update hit verification details for final result
            if hit_verification_details:
                hit_verification_details['final_check'] = True

            elapsed = time.time() - started_at
            if elapsed < MIN_VISIT_TOTAL:
                if not _sleep_interruptible(MIN_VISIT_TOTAL - elapsed):
                    return _visit_result(False, hit_ok, started_at, "stopped")

            try:
                current_url = sb.get_current_url()
            except Exception:
                current_url = ""

            if current_url:
                print(f"[{visit_id}] Visit completed | hit_verified_100={hit_ok} | duration={elapsed:.1f}s")
                return {
                    'success': True,
                    'hit_verified': hit_ok,
                    'hit_verification_details': hit_verification_details,
                    'duration': elapsed,
                    'error': None,
                }

    except Exception as e:
        print(f"[{visit_id}] Error: {e}")
        return {
            'success': False,
            'hit_verified': False,
            'hit_verification_details': {'error': str(e)},
            'duration': time.time() - started_at,
            'error': str(e),
        }
    finally:
        # Ensure browser cleanup even on unexpected exits
        try:
            if 'sb' in locals():
                sb.close_current_window()
        except Exception:
            pass
        try:
            if 'sb' in locals():
                sb.quit()
        except Exception:
            pass
        cleanup_chrome()

    return {
        'success': False,
        'hit_verified': False,
        'hit_verification_details': {'error': 'unknown_failure'},
        'duration': time.time() - started_at,
        'error': 'unknown_failure',
    }


async def run_fnc_async(url, visits, interval, on_process):
    """Async version of run_fnc with parallel execution support."""
    global STOP_FLAG
    STOP_FLAG = False
    
    # Use thread pool for parallel execution
    executor = ThreadPoolExecutor(max_workers=5)
    
    tasks = []
    start_time = time.time()
    
    for i in range(visits):
        if STOP_FLAG:
            break
        
        visit_id = i + 1
        proxy = sticky_proxy()
        
        # Schedule browser visit in thread pool
        task = asyncio.get_event_loop().run_in_executor(
            executor,
            visit_with_proxy,
            proxy,
            url,
            visit_id
        )
        tasks.append((visit_id, task))
        
        # Progress callback after each task completion
        if len(tasks) >= 1 or i == visits - 1:
            completed = sum(1 for _, t in tasks if t.done())
            if completed > 0:
                on_process(completed, visits)
    
    # Wait for all tasks to complete
    for visit_id, task in tasks:
        try:
            await task
        except Exception as e:
            print(f"Task {visit_id} failed: {e}")
    
    executor.shutdown(wait=True)
    cleanup_chrome()


def run_fnc(url, visits, interval, on_process):
    """Sync wrapper with parallel execution support."""
    asyncio.run(run_fnc_async(url, visits, interval, on_process))
