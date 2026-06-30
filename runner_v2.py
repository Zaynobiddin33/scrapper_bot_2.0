"""
Runner v2 - spawn-safe worker orchestration for scrp_v2.

This runner keeps the SeleniumBase visit logic in scrp_v2, but executes each
visit in its own child process so five workers can make progress in parallel
without blocking the bot's main asyncio loop.
"""
import asyncio
import multiprocessing
import random
import time
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse

from db import increment_click
from dispatcher import Dispatcher
from metrika import VisitLog, get_logger
from scrp_v2 import sticky_proxy, visit_with_proxy


MAX_RETRIES = 2
RETRY_BASE_DELAY = 2.0
RETRY_BACKOFF = 1.5
SECONDARY_VARIANTS = 2
MAX_VISIT_HISTORY = 120


def _get_secondary_variants(url: str, count: int = SECONDARY_VARIANTS) -> list[str]:
    """Generate lightweight URL variants for secondary tabs."""
    if count <= 0:
        return []

    seed = int(time.time() * 1000) % 1000000
    variants: list[str] = []
    for idx in range(count):
        sep = "&" if "?" in url else "?"
        variants.append(f"{url}{sep}tab={idx + 1}&sid={seed + idx}")
    return variants


def _run_visit_job_v2(
    url: str,
    worker_id: int,
    secondary_count: int = SECONDARY_VARIANTS,
) -> dict:
    """
    Execute one SeleniumBase visit in a child process.

    Every visit gets a fresh sticky proxy session and an isolated browser.
    """
    proxy = sticky_proxy()
    visit_id = int(time.time() * 1000) % 1000000
    secondary = _get_secondary_variants(url, secondary_count)

    try:
        result = visit_with_proxy(
            proxy=proxy,
            target=url,
            visit_id=visit_id,
            secondary_urls=secondary or None,
        )
    except Exception as exc:
        result = {
            "success": False,
            "hit_verified": False,
            "duration": 0.0,
            "error": f"job_failed:{str(exc)[:160]}",
        }

    result["proxy_session"] = proxy["username"]
    result["worker_id"] = worker_id
    return result


async def _run_attempt_v2(
    loop: asyncio.AbstractEventLoop,
    executor: ProcessPoolExecutor,
    url: str,
    worker_id: int,
) -> dict:
    """Run one visit attempt in the process pool."""
    return await loop.run_in_executor(executor, _run_visit_job_v2, url, worker_id)


async def worker_loop_v2(
    dispatcher: Dispatcher,
    worker_id: int,
    total_workers: int,
    visit_durations: list[float],
    get_delay,
    executor: ProcessPoolExecutor,
    pace_first_task: bool = False,
):
    """Async worker loop that submits blocking visits to child processes."""
    logger = get_logger()
    logger.worker_start(worker_id, total_workers)
    should_wait_before_next_task = pace_first_task

    while True:
        if dispatcher.is_stopped:
            break

        if should_wait_before_next_task:
            delay = get_delay()
            if delay > 0:
                await asyncio.sleep(delay)
        should_wait_before_next_task = False

        task = await dispatcher.next_task()
        if task is None:
            # None means either: queue empty OR domain on cooldown.
            # Only exit if the queue is truly exhausted AND we've met
            # our completion count. Otherwise keep waiting.
            if dispatcher.remaining == 0 and dispatcher.completed >= dispatcher.total:
                break
            # Queue has items on cooldown — sleep briefly and retry.
            await asyncio.sleep(1.0)
            continue

        domain = urlparse(task["url"]).netloc.replace("www.", "")

        success = False
        final_result = {
            "duration": 0.0,
            "hit_verified": False,
            "error": "not_started",
        }

        try:
            for attempt in range(MAX_RETRIES + 1):
                logger.visit_start(worker_id, task["url"], attempt + 1, MAX_RETRIES + 1)

                if attempt > 0:
                    delay = RETRY_BASE_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                    jitter = delay * random.uniform(0.05, 0.25)
                    final_delay = delay + jitter
                    logger.retry(worker_id, final_delay, attempt, MAX_RETRIES)
                    await asyncio.sleep(final_delay)

                final_result = await _run_attempt_v2(
                    asyncio.get_running_loop(),
                    executor,
                    task["url"],
                    worker_id,
                )

                success = bool(final_result.get("success"))
                if success:
                    break

        finally:
            # Always release the concurrency slot so the next worker can pick up
            # a task for this domain. This runs even if the visit throws an exception.
            dispatcher.release_domain(dispatcher.rl_key(task["url"]))

        duration = float(final_result.get("duration", 0.0))
        hit_verified = bool(final_result.get("hit_verified"))
        error = final_result.get("error")

        if success:
            try:
                accepted = await increment_click(task["id"])
                if not accepted:
                    success = False
                    error = "db_increment_rejected"
            except Exception as exc:
                success = False
                error = f"db_increment_failed:{exc}"

        log_entry = VisitLog(
            worker_id=worker_id,
            url=task["url"],
            domain=domain,
            counter_id=None,
            status="success" if success else "failed",
            hit_verified=hit_verified,
            hittoken=None,
            duration=duration,
            actions=0,
            scroll_px=0,
            error=error if not success else None,
        )
        logger.visit_end(log_entry)

        visit_durations.append(duration)
        if len(visit_durations) > MAX_VISIT_HISTORY:
            del visit_durations[:-MAX_VISIT_HISTORY]

        dispatcher.mark_completed()
        if dispatcher.is_stopped:
            break

        should_wait_before_next_task = True

    logger.worker_stop(worker_id)


async def run_workers_v2(
    dispatcher: Dispatcher,
    num_workers: int = 5,
    get_delay=lambda: 8.0,
    on_progress=None,
    visit_durations=None,
    pace_first_task: bool = False,
):
    """Run scrp_v2 visits with process-level parallelism."""
    if visit_durations is None:
        visit_durations = []

    logger = get_logger()
    safe_workers = max(1, int(num_workers))
    total = dispatcher.total

    logger.banner("SCRP RUNNER v2 - PROCESS MODE")
    logger.info(f"Workers: {safe_workers} | Runtime: child-process visits")
    logger.info("Scraper: scrp_v2")

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

    ctx = multiprocessing.get_context("spawn")
    executor = ProcessPoolExecutor(
        max_workers=safe_workers,
        mp_context=ctx,
        max_tasks_per_child=1,
    )

    try:
        workers = [
            asyncio.create_task(
                worker_loop_v2(
                    dispatcher,
                    i + 1,
                    safe_workers,
                    visit_durations,
                    get_delay,
                    executor,
                    pace_first_task=pace_first_task,
                )
            )
            for i in range(safe_workers)
        ]
        await asyncio.gather(*workers, return_exceptions=True)
    finally:
        dispatcher.stop()
        await progress_task
        executor.shutdown(wait=True, cancel_futures=True)

    done = dispatcher.completed
    logger.stats_summary()
    logger.banner(f"COMPLETED: {done}/{total} visits")
    return done, total


def get_default_config() -> dict:
    """Get the default runner v2 configuration."""
    return {
        "max_workers": 5,
        "max_retries": MAX_RETRIES,
        "retry_base_delay": RETRY_BASE_DELAY,
        "retry_backoff": RETRY_BACKOFF,
        "secondary_variants": SECONDARY_VARIANTS,
    }


# Keep the bot import simple if it expects `run_workers`.
run_workers = run_workers_v2
