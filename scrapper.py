from seleniumbase import SB
import time
import random
import os
import json
import math
from datetime import datetime
from contextlib import contextmanager
import uuid
import psutil
from urllib.parse import urlparse
import tempfile
import shutil
import threading

# ==================== CONFIG & GLOBALS ====================
VISIT_TIMEOUT_SECONDS = 120  # tighter timeout since visits are faster now
STOP_FLAG = False


@contextmanager
def step_timer(visit_id: int, step_name: str):
    """Log how long each pipeline step takes"""
    t0 = time.time()
    print(f"[{visit_id}] ▶ {step_name}...")
    try:
        yield
    finally:
        elapsed = time.time() - t0
        print(f"[{visit_id}] ✔ {step_name} — {elapsed:.1f}s")


# Realistic viewport sizes (common desktop resolutions)
VIEWPORTS = [
    (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
    (1280, 720), (1600, 900), (1280, 800), (1680, 1050),
]


def set_stop_flag(value: bool):
    global STOP_FLAG
    STOP_FLAG = value


def get_unique_profile(visit_id: int) -> str:
    return tempfile.mkdtemp(prefix=f"sb_yandex_{visit_id}_{os.getpid()}_")


def diminish():
    try:
        with open('data.json', 'r') as f:
            data = json.load(f)
        if data and data[0]['times'] > 1:
            data[0]['times'] -= 1
        else:
            data = data[1:] if data else []
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[DIMINISH] Error: {e}")


def cleanup_chrome(aggressive: bool = False):
    killed = 0
    try:
        current = psutil.Process(os.getpid())
        for child in current.children(recursive=True):
            try:
                name = child.name().lower()
                cmd = ' '.join(child.cmdline()).lower()
                if any(x in name for x in ["chrome", "chromedriver"]) and \
                   (any(x in cmd for x in ["selenium", "undetected", "--remote-debugging-port"]) or
                    "sb_yandex" in cmd):
                    if aggressive:
                        child.kill()
                    else:
                        child.terminate()
                        time.sleep(0.3)
                        if child.is_running():
                            child.kill()
                    killed += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except Exception as e:
        print(f"[CLEANUP] Error: {e}")
    if killed > 0:
        time.sleep(1)
    return killed


def nuclear_cleanup(visit_id: int):
    try:
        current = psutil.Process(os.getpid())
        profile_pattern = f"sb_yandex_{visit_id}_"
        for child in current.children(recursive=True):
            try:
                cmd = ' '.join(child.cmdline()).lower()
                if profile_pattern in cmd or "sb_yandex" in cmd:
                    child.kill()
            except:
                pass
        time.sleep(1)
    except Exception:
        pass


# ==================== PROXY HANDLING ====================
from tokens import *


def sticky_proxy() -> dict:
    session_id = uuid.uuid4().hex[:12]
    return {
        "host": PROXY_HOST,
        "port": PROXY_PORT,
        "username": f"{USERNAME}_session-{session_id}",
        "password": PASSWORD,
    }


def get_proxy_string(proxy: dict) -> str:
    return f"{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"


# ==================== BEZIER CURVE MOUSE TRAJECTORY ====================
def bezier_point(t, p0, p1, p2, p3):
    u = 1 - t
    return (
        u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0],
        u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1],
    )


def generate_mouse_path(start_x, start_y, end_x, end_y, steps=None):
    """Bezier curve mouse path -- fewer steps for speed, still natural"""
    distance = math.sqrt((end_x - start_x)**2 + (end_y - start_y)**2)
    if steps is None:
        steps = max(5, min(18, int(distance / 50)))  # fewer steps = faster

    offset = distance * 0.25
    cp1 = (
        start_x + (end_x - start_x) * 0.25 + random.uniform(-offset, offset),
        start_y + (end_y - start_y) * 0.25 + random.uniform(-offset, offset),
    )
    cp2 = (
        start_x + (end_x - start_x) * 0.75 + random.uniform(-offset, offset),
        start_y + (end_y - start_y) * 0.75 + random.uniform(-offset, offset),
    )

    path = []
    for i in range(steps + 1):
        t = i / steps
        t = t * t * (3 - 2 * t)  # ease-in-out
        x, y = bezier_point(t, (start_x, start_y), cp1, cp2, (end_x, end_y))
        x += random.gauss(0, 0.6)
        y += random.gauss(0, 0.6)
        path.append((max(0, x), max(0, y)))
    return path


# ==================== CDP TRUSTED INPUT (isTrusted: true) ====================

def cdp_move_mouse(sb, x, y):
    sb.execute_cdp_cmd("Input.dispatchMouseEvent", {
        "type": "mouseMoved", "x": int(x), "y": int(y),
    })


def cdp_click(sb, x, y, button="left"):
    sb.execute_cdp_cmd("Input.dispatchMouseEvent", {
        "type": "mousePressed", "x": int(x), "y": int(y),
        "button": button, "clickCount": 1,
    })
    time.sleep(random.uniform(0.04, 0.10))
    sb.execute_cdp_cmd("Input.dispatchMouseEvent", {
        "type": "mouseReleased", "x": int(x), "y": int(y),
        "button": button, "clickCount": 1,
    })


def cdp_scroll(sb, x, y, delta_y):
    sb.execute_cdp_cmd("Input.dispatchMouseEvent", {
        "type": "mouseWheel", "x": int(x), "y": int(y),
        "deltaX": 0, "deltaY": delta_y,
    })


def cdp_move_along_path(sb, path, base_delay=0.008):
    """Move mouse along path -- optimized: smaller delays"""
    for i, (x, y) in enumerate(path):
        if STOP_FLAG:
            return
        cdp_move_mouse(sb, x, y)
        progress = i / max(len(path) - 1, 1)
        speed = 0.5 + 1.5 * math.sin(progress * math.pi)
        time.sleep(base_delay / max(speed, 0.3) + random.uniform(0, 0.004))


# ==================== OPTIMIZED HUMAN BEHAVIOR ====================
def simulate_human_behavior(sb, visit_id: int, min_duration: int = 15):
    """
    CDP-based simulation: isTrusted:true events only.
    Optimized: fewer element re-queries, tighter sleeps, same quality signals.
    """
    start = time.time()
    target_duration = random.uniform(min_duration, min_duration + 8)

    try:
        vw = sb.execute_script("return window.innerWidth") or 1200
        vh = sb.execute_script("return window.innerHeight") or 800
    except:
        vw, vh = 1200, 800

    cur_x = random.uniform(vw * 0.2, vw * 0.6)
    cur_y = random.uniform(vh * 0.2, vh * 0.5)
    cdp_move_mouse(sb, cur_x, cur_y)

    # Get visible elements ONCE (avoid repeated DOM queries)
    elements = []
    try:
        elements = sb.execute_script("""
            return Array.from(document.querySelectorAll(
                'a, button, p, h1, h2, h3, img'
            )).slice(0, 20).map(el => {
                const r = el.getBoundingClientRect();
                return {
                    x: r.left + r.width/2, y: r.top + r.height/2,
                    w: r.width, h: r.height,
                    visible: r.width > 0 && r.height > 0 && r.top < window.innerHeight
                };
            }).filter(e => e.visible && e.w > 10 && e.h > 10);
        """) or []
    except:
        pass

    actions = 0
    scroll_total = 0
    last_element_refresh = time.time()

    while time.time() - start < target_duration:
        if STOP_FLAG:
            return

        action = random.choices(
            ["move", "scroll_down", "scroll_up", "read", "random"],
            weights=[28, 28, 8, 22, 14], k=1
        )[0]

        try:
            if action == "move" and elements:
                el = random.choice(elements)
                tx = max(5, min(el['x'] + random.uniform(-10, 10), vw - 5))
                ty = max(5, min(el['y'] + random.uniform(-10, 10), vh - 5))
                path = generate_mouse_path(cur_x, cur_y, tx, ty)
                cdp_move_along_path(sb, path)
                cur_x, cur_y = tx, ty
                time.sleep(random.uniform(0.15, 0.8))

            elif action == "scroll_down":
                amount = random.randint(100, 350)
                # 2-3 incremental scrolls (natural wheel ticks)
                for _ in range(random.randint(2, 3)):
                    if STOP_FLAG: return
                    cdp_scroll(sb, cur_x, cur_y, amount // 3)
                    time.sleep(random.uniform(0.02, 0.05))
                scroll_total += amount
                time.sleep(random.uniform(0.2, 0.5))

                # Refresh elements every ~6s after scrolling (not every scroll)
                if time.time() - last_element_refresh > 6:
                    try:
                        elements = sb.execute_script("""
                            return Array.from(document.querySelectorAll(
                                'a, button, p, h1, h2, h3, img'
                            )).slice(0, 20).map(el => {
                                const r = el.getBoundingClientRect();
                                return {
                                    x: r.left + r.width/2, y: r.top + r.height/2,
                                    w: r.width, h: r.height,
                                    visible: r.width > 0 && r.height > 0
                                        && r.top > -50 && r.top < window.innerHeight + 50
                                };
                            }).filter(e => e.visible && e.w > 10 && e.h > 10);
                        """) or elements
                        last_element_refresh = time.time()
                    except:
                        pass

            elif action == "scroll_up" and scroll_total > 200:
                cdp_scroll(sb, cur_x, cur_y, -random.randint(50, 150))
                scroll_total -= 100
                time.sleep(random.uniform(0.15, 0.35))

            elif action == "read":
                # Micro-jitter (reading) then pause
                for _ in range(random.randint(2, 4)):
                    if STOP_FLAG: return
                    cur_x = max(5, min(cur_x + random.gauss(0, 2), vw - 5))
                    cur_y = max(5, min(cur_y + random.gauss(0, 2), vh - 5))
                    cdp_move_mouse(sb, cur_x, cur_y)
                    time.sleep(random.uniform(0.08, 0.25))
                time.sleep(random.uniform(0.8, 2.0))

            else:  # random
                tx = random.uniform(50, vw - 50)
                ty = random.uniform(50, vh - 50)
                path = generate_mouse_path(cur_x, cur_y, tx, ty)
                cdp_move_along_path(sb, path)
                cur_x, cur_y = tx, ty
                time.sleep(random.uniform(0.1, 0.3))

            actions += 1
        except:
            time.sleep(0.2)

    elapsed = time.time() - start
    print(f"[{visit_id}] Behavior: {elapsed:.0f}s, {actions} actions, {scroll_total}px scrolled")


# ==================== METRICA HELPERS ====================
def wait_for_metrica(sb, visit_id: int, timeout: int = 10) -> bool:
    """Wait for Metrica -- reduced timeout, faster polling"""
    start = time.time()
    while time.time() - start < timeout:
        if STOP_FLAG:
            return False
        try:
            loaded = sb.execute_script("""
                return typeof window.ym === 'function'
                    || !!document.querySelector('script[src*="metrika"], script[src*="mc.yandex"]')
                    || performance.getEntriesByType('resource').some(
                        r => r.name.includes('mc.yandex') || r.name.includes('metrika')
                    );
            """)
            if loaded:
                print(f"[{visit_id}]   Metrica ready ({time.time()-start:.1f}s)")
                return True
        except:
            pass
        time.sleep(0.5)  # poll every 0.5s instead of 1s
    print(f"[{visit_id}]   Metrica not detected after {timeout}s")
    return False


def verify_metrica_beacon(sb, visit_id: int) -> bool:
    """Quick check for Metrica beacon"""
    try:
        result = sb.execute_script("""
            return (function() {
                var e = performance.getEntriesByType('resource');
                for (var i = 0; i < e.length; i++) {
                    if (e[i].name.indexOf('/watch') !== -1 &&
                        (e[i].name.indexOf('mc.yandex') !== -1)) return 'beacon';
                }
                if (typeof window.Ya !== 'undefined' && window.Ya._metrika &&
                    window.Ya._metrika.counters &&
                    Object.keys(window.Ya._metrika.counters).length > 0) return 'counter';
                if (typeof window.ym === 'function') return 'ym';
                return '';
            })();
        """)
        if result:
            print(f"[{visit_id}]   Metrica verified: {result}")
            return True
    except:
        pass
    print(f"[{visit_id}]   Metrica beacon unconfirmed")
    return False


# ==================== CORE VISIT LOGIC ====================
def visit_with_timeout(proxy: dict, target: str, visit_id: int) -> bool:
    result = [False]

    def worker():
        try:
            result[0] = visit_with_proxy(proxy, target, visit_id)
        except Exception as e:
            print(f"[{visit_id}] Worker crashed: {e}")
            result[0] = False

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(timeout=VISIT_TIMEOUT_SECONDS)

    if thread.is_alive():
        print(f"[{visit_id}] TIMEOUT — killing")
        cleanup_chrome(aggressive=True)
        nuclear_cleanup(visit_id)
        time.sleep(2)
        return False

    return result[0]


def visit_with_proxy(proxy: dict, target: str, visit_id: int) -> bool:
    proxy_str = get_proxy_string(proxy)
    is_success = False
    profile_dir = None

    try:
        profile_dir = get_unique_profile(visit_id)
        vw, vh = random.choice(VIEWPORTS)
        domain = urlparse(target).netloc.replace("www.", "")

        visit_t0 = time.time()

        with SB(
            uc=True,
            proxy=proxy_str,
            headless=False,
            page_load_strategy="normal",
            test=True,
            incognito=True,
            user_data_dir=profile_dir,
        ) as sb:
            print(f"[{visit_id}] Browser launched — {time.time() - visit_t0:.1f}s")

            if STOP_FLAG:
                return False

            # ---- STEP 0: Quick browser config ----
            with step_timer(visit_id, "Config"):
                try:
                    sb.execute_cdp_cmd("Emulation.setDeviceMetricsOverride", {
                        "width": vw, "height": vh,
                        "deviceScaleFactor": 1, "mobile": False,
                    })
                except:
                    pass
                sb.driver.set_page_load_timeout(45)

                # Set Referer to Yandex search — this is what Metrica reads as document.referrer
                # No need to actually visit yandex.uz or the search page
                sb.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
                    "headers": {
                        "Referer": f"https://yandex.uz/search/?text={domain}",
                        "Accept-Language": "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6,en;q=0.5"
                    }
                })

            if STOP_FLAG:
                return False

            # ---- STEP 1: Open target DIRECTLY (single page load) ----
            with step_timer(visit_id, "Open target"):
                try:
                    sb.open(target)
                except Exception as e:
                    print(f"[{visit_id}]   Open failed: {e}")
                    return False

            if STOP_FLAG:
                return False

            # ---- STEP 2: Wait for page readyState ----
            with step_timer(visit_id, "Page ready"):
                # sb.open() with page_load_strategy="normal" already waits for load
                # Just verify + short stabilization
                for _ in range(15):
                    if STOP_FLAG: return False
                    try:
                        if sb.execute_script("return document.readyState") == "complete":
                            break
                    except:
                        pass
                    time.sleep(0.5)
                time.sleep(random.uniform(1.0, 2.0))  # brief stabilization

            # Quick validation
            current_url = sb.get_current_url()
            page_title = sb.get_page_title().lower()
            if "404" in page_title or any(k in current_url.lower() for k in ["blocked", "forbidden"]):
                print(f"[{visit_id}] BLOCKED/404: {current_url}")
                return False

            # ---- STEP 2b: Captcha (fast check, timeout=2) ----
            with step_timer(visit_id, "Captcha check"):
                try:
                    if sb.is_element_present(
                        "iframe[title*='challenge'], iframe[src*='captcha'], iframe[src*='recaptcha']",
                        timeout=2  # was 5 — saves 3s when no captcha
                    ):
                        print(f"[{visit_id}]   Captcha! Solving...")
                        sb.uc_gui_click_captcha()
                        time.sleep(2)
                except:
                    pass

            if STOP_FLAG:
                return False

            # ---- STEP 3: Wait for Metrica (max 10s) ----
            with step_timer(visit_id, "Metrica load"):
                metrica_loaded = wait_for_metrica(sb, visit_id, timeout=10)
                if metrica_loaded:
                    time.sleep(random.uniform(0.5, 1.5))

            # Focus window (Metrica cares about tab visibility)
            try:
                sb.execute_script("window.focus();")
            except:
                pass

            # ---- STEP 4: Human behavior (CDP, 15-23s) ----
            with step_timer(visit_id, "Human behavior"):
                simulate_human_behavior(sb, visit_id, min_duration=15)

            if STOP_FLAG:
                return False

            # ---- STEP 5: Flush Metrica + verify ----
            with step_timer(visit_id, "Flush + verify"):
                try:
                    sb.execute_script("""
                        if (typeof window.ym === 'function') {
                            var ids = [];
                            if (window.Ya && window.Ya._metrika && window.Ya._metrika.counters)
                                ids = Object.keys(window.Ya._metrika.counters);
                            ids.forEach(function(id) {
                                try { window.ym(parseInt(id), 'params', {__ym:{visit:1}}); } catch(e) {}
                            });
                        }
                        document.dispatchEvent(new Event('visibilitychange'));
                    """)
                    time.sleep(1)  # was 2
                except:
                    pass

                beacon_ok = verify_metrica_beacon(sb, visit_id)


            is_success = True
            total = time.time() - visit_t0
            status = "CONFIRMED" if beacon_ok else "completed (unconfirmed)"
            print(f"[{visit_id}] ✅ Visit {status} — total {total:.0f}s")

    except Exception as e:
        print(f"[{visit_id}] ERROR: {e}")
        return False
    finally:
        with step_timer(visit_id, "Cleanup"):
            if profile_dir and os.path.exists(profile_dir):
                try:
                    shutil.rmtree(profile_dir, ignore_errors=True)
                except:
                    pass
            cleanup_chrome(aggressive=True)
            time.sleep(0.5)

    return is_success


# ==================== MAIN RUNNER ====================
def run_fnc(url: str, visits: int, interval: int, on_process):
    global STOP_FLAG
    STOP_FLAG = False
    successful_visits = 0
    consecutive_failures = 0

    print(f"Starting {visits} visits to {url} (interval {interval}s)")

    for i in range(visits):
        if STOP_FLAG:
            print("STOP triggered")
            break

        cleanup_chrome()
        start = datetime.now()
        proxy = sticky_proxy()

        success = visit_with_timeout(proxy, url, i + 1)

        # Retry once on failure with new proxy
        if not success and not STOP_FLAG:
            print(f"[{i+1}] Retrying with new proxy...")
            time.sleep(random.uniform(2, 4))
            cleanup_chrome(aggressive=True)
            proxy = sticky_proxy()
            success = visit_with_timeout(proxy, url, i + 1)

        if success:
            successful_visits += 1
            consecutive_failures = 0
            diminish()
            print(f"[{i+1}] SUCCESS | {successful_visits}/{visits}")
        else:
            consecutive_failures += 1
            print(f"[{i+1}] FAILED | {successful_visits}/{visits}")
            if consecutive_failures >= 3:
                cooldown = random.uniform(10, 20)
                print(f"[{i+1}] Cooldown {int(cooldown)}s after {consecutive_failures} failures")
                time.sleep(cooldown)
                consecutive_failures = 0

        on_process(successful_visits, visits)

        elapsed = (datetime.now() - start).total_seconds()
        remain = max(0, int(interval - elapsed))
        remain += random.randint(0, max(1, interval // 5))
        print(f"Sleeping {remain}s...")

        for _ in range(remain):
            if STOP_FLAG:
                cleanup_chrome()
                return
            time.sleep(1)

        if (i + 1) % 3 == 0:
            cleanup_chrome(aggressive=True)

    cleanup_chrome(aggressive=True)
    print(f"Done. {successful_visits}/{visits} successful")
