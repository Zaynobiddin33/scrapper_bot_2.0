"""
Runner v3 - Optimized worker orchestration with human-like browser behavior.
Features:
- 100-120 second browser lifetime
- Smart Metrika hit detection with 15s timeout
- Truly human-like scrolling patterns
- Enhanced error handling and recovery
"""
import asyncio
import time
import random
from urllib.parse import urlparse

from db import increment_click
from dispatcher import Dispatcher
from metrika import VisitLog, get_logger
from scrp_v2 import (
    STOP_FLAG,
    set_stop_flag,
    visit_with_proxy,
    sticky_proxy,
    BROWSER_MIN_LIFETIME,
    BROWSER_MAX_LIFETIME,
    METRIKA_HIT_TIMEOUT,
    get_default_config,
)


# ==================== CONFIGURATION ====================
MAX_RETRIES = 2
RETRY_BASE_DELAY = 2.0  # seconds
RETRY_BACKOFF = 1.5  # multiplier


# ==================== VISIT JOB EXECUTION ====================
def _run_visit_job_v3(
    url: str,
    worker_id: int,
    visit_id: int,
    secondary_count: int = 2,
) -> dict:
    """
    Execute a single visit with human-like behavior.
    Browser stays alive for 100-120 seconds with Metrika hit detection.
    """
    try:
        proxy = sticky_proxy()
        
        # Generate secondary variants for variety
        secondary = []
        for i in range(secondary_count):
            sep = "&" if "?" in url else "?"
            secondary.append(f"{url}{sep}v={i}&r={int(time.time() * 1000) % 1000000}")
        
        result = visit_with_proxy(
            proxy=proxy,
            target=url,
            visit_id=visit_id,
            secondary_urls=secondary,
        )
        
        result["proxy_session"] = proxy["username"]
        result["worker_id"] = worker_id
        return result
        
    except Exception as e:
        return {
            "success": False,
            "hit_verified": False,
            "duration": 0.0,
            "error": f"job_failed:{str(e)[:100]}",
            "proxy_session": f"worker-{worker_id}",
            "worker_id": worker_id,
        }


# ==================== WORKER LOOP ====================
async def worker_loop_v3(
    dispatcher: Dispatcher,
    worker_id: int,
    total_workers: int,
    visit_durations: list[float],
    get_delay,
):
    """Worker loop with human-like browser behavior."""
    logger = get_logger()
    logger.worker_start(worker_id, total_workers)
    
    retry_count = 0
    current_delay = RETRY_BASE_DELAY
    
    while True:
        if dispatcher.is_stopped:
            break
        
        task = await dispatcher.next_task()
        if task is None:
            break
        
        success = False
        final_result = {
            "duration": 0.0,
            "hit_verified": False,
            "error": "not_started",
        }
        
        for attempt in range(MAX_RETRIES + 1):
            logger.visit_start(worker_id, task["url"], attempt + 1, MAX_RETRIES + 1)
            
            if attempt > 0:
                # Exponential backoff with jitter
                delay = current_delay * (RETRY_BACKOFF ** attempt)
                jitter = delay * 0.2 * (0.5 + (worker_id % 5) / 10)
                final_delay = delay + jitter
                logger.retry(worker_id, final_delay, attempt, MAX_RETRIES)
                await asyncio.sleep(final_delay)
            
            # Execute visit with human-like behavior
            visit_id = int(time.time() * 1000) % 1000000
            final_result = _run_visit_job_v3(
                task["url"],
                worker_id,
                visit_id,
            )
            
            success = bool(final_result.get("success"))
            if success:
                retry_count = 0  # Reset on success
                current_delay = RETRY_BASE_DELAY
                break
            
            # Increase delay on failure
            retry_count += 1
            current_delay = min(
                current_delay * RETRY_BACKOFF,
                30.0  # Max delay cap
            )
        
        # Extract result data
        duration = float(final_result.get("duration", 0.0))
        hit_verified = bool(final_result.get("hit_verified", False))
        # Check for detailed hit verification (100% accurate)
        hit_verification_details = final_result.get("hit_verification_details", {})
        
        # 100% accurate: only count if hit is verified with all validations
        hit_verified_100 = hit_verified and (
            hit_verification_details.get('hittoken') is not None and
            hit_verification_details.get('hidv2') is not None and
            hit_verification_details.get('bh_cookie') is not None and
            hit_verification_details.get('session_tracking', False) and
            hit_verification_details.get('page_view_counter') is not None
        )
        
        error = final_result.get("error")
        domain = urlparse(task["url"]).netloc.replace("www.", "")
        
        # CRITICAL: Only increment database if hit is 100% verified!
        if success and hit_verified_100:
            try:
                await increment_click(task["id"])
                print(f"[{task['id']}] Click incremented - hit 100% verified")
            except Exception as e:
                success = False
                error = f"db_increment_failed:{e}"
                hit_verified_100 = False
        elif success and not hit_verified_100:
            print(f"[{task['id']}] Visit succeeded but Metrika hit NOT verified: {hit_verification_details.get('error', 'missing_validation')}")
            error = f"metrika_hit_not_verified:{hit_verification_details.get('error', 'missing_validation')}"
        
        # Log visit with 100% verified status
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
        
        # Update durations for dynamic delay calculation
        visit_durations.append(duration)
        if len(visit_durations) > 120:
            del visit_durations[:-120]
        
        dispatcher.mark_completed()
        if dispatcher.is_stopped:
            break
        
        # Dynamic delay with jitter
        delay = get_delay()
        jitter = random.uniform(-delay * 0.2, delay * 0.2) if delay > 0 else 0
        await asyncio.sleep(max(0.5, delay + jitter))
    
    logger.worker_stop(worker_id)


# ==================== MAIN RUNNER ====================
async def run_workers_v3(
    dispatcher: Dispatcher,
    num_workers: int = 5,
    get_delay=lambda: 8.0,
    on_progress=None,
    visit_durations=None,
):
    """Run workers with human-like browser behavior."""
    if visit_durations is None:
        visit_durations = []
    
    logger = get_logger()
    
    logger.banner("SCRP RUNNER v3 - HUMAN-LIKE BEHAVIOR MODE")
    total = dispatcher.total
    logger.info(f"Workers: {num_workers}")
    logger.info(f"Browser lifetime: {BROWSER_MIN_LIFETIME}-{BROWSER_MAX_LIFETIME}s")
    logger.info(f"Metrika timeout: {METRIKA_HIT_TIMEOUT}s")
    
    # Progress reporter
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
    
    # Start workers with human-like behavior
    workers = [
        asyncio.create_task(
            worker_loop_v3(
                dispatcher,
                i + 1,
                num_workers,
                visit_durations,
                get_delay,
            )
        )
        for i in range(num_workers)
    ]
    
    # Wait for all workers to complete
    await asyncio.gather(*workers, return_exceptions=True)
    
    # Cleanup
    dispatcher.stop()
    await progress_task
    
    done = dispatcher.completed
    logger.stats_summary()
    logger.banner(f"COMPLETED: {done}/{total} visits")
    
    return done, total


# ==================== CONFIGURATION ====================
def get_default_config() -> dict:
    """Get default configuration for runner v3."""
    return {
        "max_workers": 5,
        "max_retries": 2,
        "retry_base_delay": 2.0,
        "retry_backoff": 1.5,
        "browser_min_lifetime": BROWSER_MIN_LIFETIME,
        "browser_max_lifetime": BROWSER_MAX_LIFETIME,
        "metrika_timeout": METRIKA_HIT_TIMEOUT,
    }
