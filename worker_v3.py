"""
Playwright-based async worker pool - ROBUST VERSION (v3)
Optimized for verified Yandex Metrika visits with per-visit browser sessions.
"""
import asyncio
import json
import os
import random
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse
from typing import Callable, Optional

from playwright.async_api import Response, async_playwright, Browser, BrowserContext, Page, Playwright

from tokens import (
    PROXY_HOST,
    PROXY_PORT,
    USERNAME,
    PASSWORD,
    METRIKA_COUNTER_ID,
    METRIKA_COUNTER_OVERRIDES,
    METRIKA_HEADED_BROWSER,
    METRIKA_VERIFY_HIT,
    METRIKA_REQUIRE_HITTOKEN,
    METRIKA_HIT_TIMEOUT,
    METRIKA_MIN_DURATION,
    METRIKA_MAX_DURATION,
    METRIKA_MAX_RETRIES,
    METRIKA_SIMULATE_VISIBILITY,
)
from db import increment_click
from dispatcher import Dispatcher
from metrika import (
    HitVerifier,
    HumanBehaviorSimulator,
    VisitLog,
    ensure_visible,
    generate_fingerprint,
    get_logger,
    get_stealth_script,
    simulate_tab_visibility,
)

logger = get_logger()

DEFAULT_COUNTER_ID = METRIKA_COUNTER_ID if METRIKA_COUNTER_ID != "auto" else None
COUNTER_OVERRIDES = METRIKA_COUNTER_OVERRIDES if METRIKA_COUNTER_OVERRIDES else {}
VERIFY_HIT = METRIKA_VERIFY_HIT
REQUIRE_HITTOKEN = METRIKA_REQUIRE_HITTOKEN
HIT_TIMEOUT = float(METRIKA_HIT_TIMEOUT)
MIN_VISIT_DURATION = int(METRIKA_MIN_DURATION)
MAX_VISIT_DURATION = int(METRIKA_MAX_DURATION)
MAX_RETRIES = int(METRIKA_MAX_RETRIES)
SIMULATE_VISIBILITY = METRIKA_SIMULATE_VISIBILITY
HEADED_BROWSER = METRIKA_HEADED_BROWSER
PROFILE_LOCALE = "uz-UZ"
PROFILE_TIMEZONE = "Asia/Tashkent"
PROFILE_LANG_HEADER = "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6,en;q=0.5"

LAUNCH_TIMEOUT_MS = 20000
NAVIGATION_TIMEOUT_MS = 40000
NETWORKIDLE_TIMEOUT_MS = 12000
RETRY_DELAY_MIN = 1.5
RETRY_DELAY_MAX = 4.0
BLOCK_LOG_DIR = Path(__file__).resolve().parent / "latest_logs" / "blocks"
SECONDARY_PAGES_ENABLED = True
SECONDARY_PAGE_COUNT = 2
SECONDARY_PAGE_DELAY = (2.5, 5.0)


def get_counter_id_for_url(url: str) -> int | str | None:
    domain = urlparse(url).netloc.replace("www.", "").lower()
    for override_domain, counter_id in COUNTER_OVERRIDES.items():
        override = override_domain.lower()
        if override in domain or domain in override:
            return counter_id
    if DEFAULT_COUNTER_ID and DEFAULT_COUNTER_ID != "auto":
        return DEFAULT_COUNTER_ID
    return "auto"


def get_referer_for_domain(domain: str) -> str:
    tld = domain.split(".")[-1].lower() if "." in domain else "com"
    referers = {
        "uz": f"https://yandex.uz/search/?text={domain}",
        "ru": f"https://yandex.ru/search/?text={domain}",
        "kz": f"https://yandex.kz/search/?text={domain}",
        "by": f"https://yandex.by/search/?text={domain}",
        "ua": f"https://yandex.ua/search/?text={domain}",
        "com": f"https://www.google.com/search?q={domain}",
        "org": f"https://www.google.com/search?q={domain}",
        "net": f"https://www.google.com/search?q={domain}",
    }
    return referers.get(tld, f"https://www.google.com/search?q={domain}")


def new_proxy() -> dict:
    sid = uuid.uuid4().hex[:12]
    return {
        "server": f"http://{PROXY_HOST}:{PROXY_PORT}",
        "username": f"{USERNAME}_session-{sid}",
        "password": PASSWORD,
    }


async def create_browser(pw: Playwright, proxy: dict, viewport: dict) -> Browser:
    args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-infobars",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-site-isolation-trials",
        f"--window-size={viewport['width']},{viewport['height']}",
        "--force-color-profile=srgb",
    ]
    try:
        return await pw.chromium.launch(
            headless=not HEADED_BROWSER,
            proxy=proxy,
            timeout=LAUNCH_TIMEOUT_MS,
            args=args,
        )
    except Exception as e:
        if not HEADED_BROWSER:
            raise
        logger.warning(f"Headed browser launch failed, falling back to headless: {str(e)[:120]}")
        return await pw.chromium.launch(
            headless=True,
            proxy=proxy,
            timeout=LAUNCH_TIMEOUT_MS,
            args=args,
        )


async def setup_context(browser: Browser, fp, referer: str) -> BrowserContext:
    return await browser.new_context(
        viewport=fp.viewport,
        locale=PROFILE_LOCALE,
        timezone_id=PROFILE_TIMEZONE,
        extra_http_headers={
            "Referer": referer,
            "Accept-Language": PROFILE_LANG_HEADER,
        },
        ignore_https_errors=True,
        java_script_enabled=True,
        bypass_csp=True,
    )


async def apply_stealth(page: Page, fp) -> None:
    await page.add_init_script(get_stealth_script())
    await page.add_init_script(
        f"""
        Object.defineProperty(navigator, 'hardwareConcurrency', {{
            get: () => {fp.hw_concurrency},
            configurable: true, enumerable: true
        }});
        Object.defineProperty(navigator, 'deviceMemory', {{
            get: () => {fp.device_memory},
            configurable: true, enumerable: true
        }});
        Object.defineProperty(navigator, 'language', {{
            get: () => '{PROFILE_LOCALE}',
            configurable: true, enumerable: true
        }});
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['uz-UZ', 'uz', 'ru-RU', 'ru', 'en-US', 'en'],
            configurable: true, enumerable: true
        }});
        Date.prototype.getTimezoneOffset = function() {{ return -300; }};
        const gp = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(p) {{
            if (p === 37445) return '{fp.webgl_vendor}';
            if (p === 37446) return '{fp.webgl_renderer}';
            return gp.call(this, p);
        }};
        """
    )


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
                for (var i = 0; i < e.length; i++) {
                    if (e[i].name.indexOf('/watch') !== -1 && e[i].name.indexOf('mc.yandex') !== -1)
                        return 'beacon';
                }
                if (typeof window.Ya !== 'undefined' && window.Ya._metrika
                    && window.Ya._metrika.counters
                    && Object.keys(window.Ya._metrika.counters).length > 0) return 'counter';
                if (typeof window.ym === 'function') return 'ym';
                return '';
            })()
        """) or ""
    except Exception:
        return ""


async def open_secondary_pages(context: BrowserContext, base_url: str) -> None:
    """Open a few extra pages to add variation to a visit."""
    if not SECONDARY_PAGES_ENABLED or SECONDARY_PAGE_COUNT <= 0:
        return
    for i in range(SECONDARY_PAGE_COUNT):
        variant = (
            f"{base_url}&v={i}&r={uuid.uuid4().hex[:6]}"
            if "?" in base_url
            else f"{base_url}?v={i}&r={uuid.uuid4().hex[:6]}"
        )
        page = await context.new_page()
        try:
            await page.goto(variant, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
            try:
                await page.wait_for_load_state("networkidle", timeout=NETWORKIDLE_TIMEOUT_MS)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(*SECONDARY_PAGE_DELAY))
        except Exception:
            pass
        finally:
            try:
                await page.close()
            except Exception:
                pass


async def inspect_block_page(page: Page, response_status: int) -> dict:
    details = {
        "status": response_status,
        "url": "",
        "title": "",
        "server": "",
        "cf_ray": "",
        "snippet": "",
        "is_cloudflare": False,
    }
    try:
        details["url"] = page.url
    except Exception:
        pass
    try:
        details["title"] = await page.title()
    except Exception:
        pass
    try:
        headers = await page.evaluate(
            """() => {
                const entries = performance.getEntriesByType('navigation');
                return entries.length ? {
                    transferSize: entries[0].transferSize || 0,
                    type: entries[0].type || ''
                } : {};
            }"""
        )
        if headers:
            details["nav_type"] = headers.get("type", "")
    except Exception:
        pass
    try:
        snippet = await page.locator("body").inner_text(timeout=2000)
        details["snippet"] = snippet[:400].replace("\n", " ")
    except Exception:
        pass
    if "cloudflare" in details["title"].lower() or "cloudflare" in details["snippet"].lower():
        details["is_cloudflare"] = True
    return details


async def save_block_artifacts(
    page: Page,
    response: Optional[Response],
    worker_id: int,
    block_details: dict,
) -> dict:
    BLOCK_LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    base = BLOCK_LOG_DIR / f"w{worker_id}_{stamp}_{uuid.uuid4().hex[:8]}"

    html_path = str(base.with_suffix(".html"))
    png_path = str(base.with_suffix(".png"))
    json_path = str(base.with_suffix(".json"))

    payload = dict(block_details)
    payload["html_path"] = html_path
    payload["png_path"] = png_path

    if response:
        try:
            payload["response_url"] = response.url
        except Exception:
            pass
        try:
            payload["response_status"] = response.status
        except Exception:
            pass
        try:
            payload["response_headers"] = await response.all_headers()
        except Exception:
            pass

    try:
        await page.screenshot(path=png_path, full_page=True)
    except Exception:
        payload["png_path"] = ""

    try:
        html = await page.content()
        Path(html_path).write_text(html, encoding="utf-8")
    except Exception:
        payload["html_path"] = ""

    Path(json_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["json_path"] = json_path
    return payload


async def visit_url_robust(pw: Playwright, task: dict, worker_id: int) -> tuple[bool, float, dict]:
    visit_start = time.time()
    fp = generate_fingerprint()
    proxy = new_proxy()
    url = task["url"]
    domain = urlparse(url).netloc.replace("www.", "")
    referer = get_referer_for_domain(domain)

    browser: Optional[Browser] = None
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None
    verifier: Optional[HitVerifier] = None
    visibility_task = None

    log_entry = VisitLog(
        worker_id=worker_id,
        url=url,
        domain=domain,
        counter_id=None,
        status="failed",
        hit_verified=False,
        hittoken=None,
        duration=0,
        actions=0,
        scroll_px=0,
    )

    try:
        browser = await create_browser(pw, proxy, fp.viewport)
        context = await setup_context(browser, fp, referer)
        page = await context.new_page()
        await apply_stealth(page, fp)

        counter_id = get_counter_id_for_url(url)
        log_entry.counter_id = str(counter_id) if counter_id else "auto"
        logger.counter_detected(worker_id, counter_id)

        if VERIFY_HIT:
            verifier = HitVerifier(counter_id=counter_id, require_hittoken=REQUIRE_HITTOKEN)
            verifier.attach(page)

        response = await page.goto(url, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
        response_status = response.status if response else 0
        logger.navigation_status(worker_id, response_status, url)

        if response_status not in (0, 200, 304):
            block_details = await inspect_block_page(page, response_status)
            block_details = await save_block_artifacts(page, response, worker_id, block_details)
            if response_status in (403, 404, 503, 520):
                logger.bot_detected(worker_id, block_details.get("url") or url, response_status)
            log_entry.error = f"HTTP {response_status}"
            log_entry.duration = time.time() - visit_start
            logger.warning(
                f"[W{worker_id}] HTTP block artifact: "
                f"title={block_details.get('title', '')[:80]!r} "
                f"cloudflare={block_details.get('is_cloudflare')} "
                f"artifact={block_details.get('json_path', '')}"
            )
            logger.visit_end(log_entry)
            return False, log_entry.duration, {
                "error": f"navigation_failed_{response_status}",
                "block_details": block_details,
            }

        try:
            await page.wait_for_load_state("load", timeout=NETWORKIDLE_TIMEOUT_MS)
        except Exception:
            pass

        await asyncio.sleep(random.uniform(0.8, 1.5))

        try:
            title = await page.title()
            current_url = page.url
            if "about:blank" in current_url:
                log_entry.error = "about:blank"
                log_entry.duration = time.time() - visit_start
                logger.visit_end(log_entry)
                return False, log_entry.duration, {"error": "about:blank"}
            if "404" in title.lower() or any(k in current_url.lower() for k in ["blocked", "forbidden", "/404"]):
                block_details = await inspect_block_page(page, response_status)
                block_details = await save_block_artifacts(page, response, worker_id, block_details)
                logger.bot_detected(worker_id, current_url, response_status)
                log_entry.error = f"blocked:{block_details['title'][:60]} {current_url[:80]}"
                logger.warning(
                    f"[W{worker_id}] Block details: "
                    f"title={block_details['title'][:80]!r} "
                    f"cloudflare={block_details['is_cloudflare']} "
                    f"artifact={block_details.get('json_path', '')} "
                    f"snippet={block_details['snippet'][:140]!r}"
                )
                log_entry.duration = time.time() - visit_start
                logger.visit_end(log_entry)
                return False, log_entry.duration, {
                    "error": "blocked",
                    "block_details": block_details,
                }
        except Exception:
            pass

        metrica_loaded = await wait_for_metrica(page, timeout=10)
        if metrica_loaded:
            await asyncio.sleep(random.uniform(0.5, 1.0))

        await ensure_visible(page)
        try:
            await page.evaluate("window.focus(); document.hasFocus = () => true;")
        except Exception:
            pass

        # Optional extra pages for variability
        try:
            await open_secondary_pages(context, url)
        except Exception:
            pass

        duration = random.randint(MIN_VISIT_DURATION, MAX_VISIT_DURATION)
        if SIMULATE_VISIBILITY:
            visibility_task = asyncio.create_task(simulate_tab_visibility(page, duration))

        behavior_stats = await HumanBehaviorSimulator(page).simulate_visit(duration=duration)

        if visibility_task:
            try:
                await visibility_task
            except Exception:
                pass

        beacon_status = await flush_and_verify(page)
        hit_result = None
        if verifier:
            hit_result = await verifier.wait_for_hit(timeout=min(HIT_TIMEOUT, 8.0))
            log_entry.hit_verified = hit_result.hit_verified if hit_result else False
            log_entry.hittoken = hit_result.hittoken if hit_result else None
            if not hit_result.hit_verified and not beacon_status:
                log_entry.error = hit_result.error or "hit_not_verified"
                log_entry.duration = time.time() - visit_start
                logger.visit_end(log_entry)
                return False, log_entry.duration, hit_result.to_dict()

        accepted = await increment_click(task["id"])
        elapsed = time.time() - visit_start
        if not accepted:
            log_entry.error = "click_not_accepted"
            log_entry.duration = elapsed
            logger.visit_end(log_entry)
            return False, elapsed, {"error": "click_not_accepted"}

        log_entry.status = "success"
        log_entry.duration = elapsed
        log_entry.actions = behavior_stats["actions"]
        log_entry.scroll_px = behavior_stats["scroll_px"]
        logger.visit_end(log_entry)

        details = {
            "success": True,
            "hit_verified": log_entry.hit_verified if VERIFY_HIT else True,
            "actions": behavior_stats["actions"],
            "scroll_px": behavior_stats["scroll_px"],
            "duration": elapsed,
            "beacon_status": beacon_status,
        }
        if hit_result:
            details.update(hit_result.to_dict())
        return True, elapsed, details

    except Exception as e:
        log_entry.error = str(e)[:160]
        log_entry.duration = time.time() - visit_start
        logger.visit_end(log_entry)
        return False, log_entry.duration, {"error": str(e)}

    finally:
        if verifier and page:
            try:
                verifier.detach(page)
            except Exception:
                pass
        if visibility_task and not visibility_task.done():
            visibility_task.cancel()
            try:
                await visibility_task
            except Exception:
                pass
        if page:
            try:
                await page.close()
            except Exception:
                pass
        if context:
            try:
                await context.close()
            except Exception:
                pass
        if browser:
            try:
                await browser.close()
            except Exception:
                pass


async def worker_loop_robust(
    pw: Playwright,
    dispatcher: Dispatcher,
    worker_id: int,
    total_workers: int,
    get_delay: Callable[[], float],
    visit_durations: list,
):
    logger.worker_start(worker_id, total_workers)

    consecutive_none_count = 0
    max_none_before_exit = 2  # Exit only after 2 consecutive None returns

    while True:
        if dispatcher.is_stopped:
            break

        task = await dispatcher.next_task()
        if task is None:
            consecutive_none_count += 1
            if consecutive_none_count >= max_none_before_exit:
                # Exit only after confirming no more tasks are available
                break
            else:
                # Brief pause before checking again
                await asyncio.sleep(0.05)
                continue
        
        consecutive_none_count = 0  # Reset counter when we get a task

        success = False
        elapsed = 0.0

        for attempt in range(MAX_RETRIES + 1):
            logger.visit_start(worker_id, task["url"], attempt + 1, MAX_RETRIES + 1)
            if attempt > 0:
                delay = random.uniform(RETRY_DELAY_MIN, RETRY_DELAY_MAX)
                logger.retry(worker_id, delay, attempt, MAX_RETRIES)
                await asyncio.sleep(delay)

            success, elapsed, details = await visit_url_robust(pw, task, worker_id)
            if success:
                break

        visit_durations.append(elapsed)
        if len(visit_durations) > 120:
            del visit_durations[:-120]

        if not success:
            logger.error(f"[W{worker_id}] All retries failed for {task['url'][:60]}")

        dispatcher.mark_completed()

        if dispatcher.is_stopped:
            break

        delay = get_delay()
        jitter = random.uniform(-delay * 0.2, delay * 0.2) if delay > 0 else 0
        await asyncio.sleep(max(1.0, delay + jitter))

    logger.worker_stop(worker_id)


async def ensure_browser_installed(pw: Playwright):
    executable = pw.chromium.executable_path
    if not executable or not os.path.exists(executable):
        raise RuntimeError(
            "Playwright Chromium is not installed. Run: ./env/bin/python -m playwright install chromium"
        )


async def run_workers(
    dispatcher: Dispatcher,
    num_workers: int = 5,
    get_delay: Callable[[], float] = lambda: 8.0,
    on_progress: Callable | None = None,
    visit_durations: list | None = None,
):
    if visit_durations is None:
        visit_durations = []

    logger.banner("YANDEX METRIKA BOT v3.0 - ROBUST MODE")

    async with async_playwright() as pw:
        await ensure_browser_installed(pw)

        total = dispatcher.total
        logger.info(f"Workers: {num_workers}")
        logger.info(f"Counter: {DEFAULT_COUNTER_ID or 'auto-detect'}")
        logger.info(f"Browser mode: {'headed' if HEADED_BROWSER else 'headless'}")

        async def progress_reporter():
            while True:
                completed = dispatcher.completed
                if on_progress:
                    try:
                        await on_progress(completed, total)
                    except Exception:
                        pass
                logger.progress(completed, total)
                if dispatcher.is_stopped or completed >= total:
                    break
                await asyncio.sleep(10)

        progress_task = asyncio.create_task(progress_reporter())
        workers = [
            asyncio.create_task(
                worker_loop_robust(
                    pw,
                    dispatcher,
                    i + 1,
                    num_workers,
                    get_delay,
                    visit_durations,
                )
            )
            for i in range(num_workers)
        ]

        await asyncio.gather(*workers, return_exceptions=True)
        await progress_task

        done = dispatcher.completed
        logger.stats_summary()
        logger.banner(f"COMPLETED: {done}/{total} visits")
        return done, total
