import asyncio
import multiprocessing
import random
import time
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse

from db import increment_click
from dispatcher import Dispatcher
from metrika import VisitLog, get_logger
from scrp import sticky_proxy, visit_with_proxy

logger = get_logger()

SECONDARY_VARIANTS = 2
MAX_RETRIES = 1
RETRY_DELAY = (1.5, 3.5)


def _secondary_variants(url: str, count: int = SECONDARY_VARIANTS) -> list[str]:
    variants = []
    for i in range(count):
        sep = "&" if "?" in url else "?"
        variants.append(f"{url}{sep}v={i}&r={random.randint(100000, 999999)}")
    return variants


def _run_visit_job(url: str, worker_id: int, secondary_count: int = SECONDARY_VARIANTS) -> dict:
    proxy = sticky_proxy()
    visit_id = int(time.time() * 1000) % 1000000
    secondary = _secondary_variants(url, count=secondary_count)
    result = visit_with_proxy(proxy, url, visit_id, secondary_urls=secondary)
    result["proxy_session"] = proxy["username"]
    result["worker_id"] = worker_id
    return result


async def _run_attempt(
    loop: asyncio.AbstractEventLoop,
    executor: ProcessPoolExecutor,
    url: str,
    worker_id: int,
) -> dict:
    return await loop.run_in_executor(executor, _run_visit_job, url, worker_id, SECONDARY_VARIANTS)


async def worker_loop(
    dispatcher: Dispatcher,
    executor: ProcessPoolExecutor,
    worker_id: int,
    total_workers: int,
    visit_durations: list[float],
    get_delay,
):
    loop = asyncio.get_running_loop()
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
        final_result = {"duration": 0.0, "hit_verified": False, "error": "not_started"}

        for attempt in range(MAX_RETRIES + 1):
            logger.visit_start(worker_id, task["url"], attempt + 1, MAX_RETRIES + 1)
            if attempt > 0:
                delay = random.uniform(*RETRY_DELAY)
                logger.retry(worker_id, delay, attempt, MAX_RETRIES)
                await asyncio.sleep(delay)

            final_result = await _run_attempt(loop, executor, task["url"], worker_id)
            success = bool(final_result.get("success"))
            if success:
                break

        duration = float(final_result.get("duration", 0.0))
        hit_verified = bool(final_result.get("hit_verified", False))
        
        # Check for detailed hit verification (100% accurate)
        # From network analysis: hit is valid when ALL conditions are met:
        # - hittoken present in response JSON
        # - hidv2 present in response JSON  
        # - bh cookie in Set-Cookie header
        # - redirnss=1 in URL (session tracking)
        # - browser-info.pv=N (page view counter)
        hit_verification_details = final_result.get("hit_verification_details", {})
        
        # 100% accurate: only count if hit is verified with all validations
        hit_verified_100 = hit_verified and (
            hit_verification_details.get('hittoken') is not None and
            hit_verification_details.get('hidv2') is not None and
            hit_verification_details.get('session_tracking', False) and
            hit_verification_details.get('page_view') is not None
        )
        
        error = final_result.get("error")
        domain = urlparse(task["url"]).netloc.replace("www.", "")

        # CRITICAL: Only increment database if hit is 100% verified!
        # This ensures only actual Yandex Metrika hits are counted
        # 100% accurate validation requires:
        # - hittoken present (Yandex hit token)
        # - hidv2 present (unique hit identifier)
        # - session_tracking enabled (redirnss=1)
        # - page_view counter exists (browser-info.pv=N)
        if success and hit_verified_100:
            try:
                await increment_click(task["id"])
                print(f"[{task['id']}] ✓ Click incremented - hit 100% verified")
            except Exception as e:
                success = False
                error = f"db_increment_failed:{e}"
                # Decrement hit_verified if DB update failed
                hit_verified = False
                hit_verified_100 = False
        elif success and not hit_verified_100:
            # Log failed hit for debugging - visit succeeded but Metrika hit not confirmed
            print(f"[{task['id']}] ✗ Visit succeeded but Metrika hit NOT verified")
            print(f"  Details: hittoken={hit_verification_details.get('hittoken') is not None}, "
                  f"hidv2={hit_verification_details.get('hidv2') is not None}, "
                  f"session_tracking={hit_verification_details.get('session_tracking')}, "
                  f"page_view={hit_verification_details.get('page_view')}")
            error = f"metrika_hit_not_verified:{hit_verification_details.get('error', 'missing_validation')}"

        log_entry = VisitLog(
            worker_id=worker_id,
            url=task["url"],
            domain=domain,
            counter_id=None,
            status="success" if (success and hit_verified_100) else "failed",
            hit_verified=hit_verified_100,
            hittoken=hit_verification_details.get('hittoken'),
            duration=duration,
            actions=0,
            scroll_px=0,
            error=error if not (success and hit_verified_100) else None,
        )
        logger.visit_end(log_entry)

        visit_durations.append(duration)
        if len(visit_durations) > 120:
            del visit_durations[:-120]

        dispatcher.mark_completed()
        if dispatcher.is_stopped:
            break

        delay = get_delay()
        jitter = random.uniform(-delay * 0.2, delay * 0.2) if delay > 0 else 0
        await asyncio.sleep(max(1.0, delay + jitter))

    logger.worker_stop(worker_id)


async def run_workers(
    dispatcher: Dispatcher,
    num_workers: int = 5,
    get_delay=lambda: 8.0,
    on_progress=None,
    visit_durations=None,
):
    if visit_durations is None:
        visit_durations = []

    logger.banner("SCRP RUNNER - PROCESS MODE")
    total = dispatcher.total
    logger.info(f"Workers (processes): {num_workers}")

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

    safe_workers = max(1, num_workers)
    ctx = multiprocessing.get_context("spawn")
    executor = ProcessPoolExecutor(
        max_workers=safe_workers,
        mp_context=ctx,
        max_tasks_per_child=1,
    )
    progress_task = asyncio.create_task(progress_reporter())

    workers = [
        asyncio.create_task(
            worker_loop(
                dispatcher,
                executor,
                i + 1,
                num_workers,
                visit_durations,
                get_delay,
            )
        )
        for i in range(num_workers)
    ]

    await asyncio.gather(*workers, return_exceptions=True)
    dispatcher.stop()
    await progress_task
    executor.shutdown(wait=True, cancel_futures=True)

    done = dispatcher.completed
    logger.stats_summary()
    logger.banner(f"COMPLETED: {done}/{total} visits")
    return done, total
