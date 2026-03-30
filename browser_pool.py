"""
Browser Pool Manager - Concurrent browser management with unique proxy isolation.
Implements a round-robin proxy strategy and browser reuse optimization.
"""
import asyncio
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from seleniumbase import SB

from tokens import PASSWORD, PROXY_HOST, PROXY_PORT, USERNAME
from scrp_v2 import (
    STOP_FLAG,
    set_stop_flag,
    PREWARM_URL,
    REFERER_URL,
    PAGE_LOAD_TIMEOUT,
    POST_LAUNCH_DELAY,
    PREWARM_DELAY,
    POST_OPEN_SETTLE,
    POST_CAPTCHA_SETTLE,
    CAPTCHA_IFRAME_TIMEOUT,
    READY_STATE_POLLS,
    SHORTENER_BLOCK_KEYWORDS,
    BROWSER_MIN_LIFETIME,
    BROWSER_MAX_LIFETIME,
    METRIKA_HIT_TIMEOUT,
    MIN_VISIT_TOTAL,
    _sleep_interruptible,
    _sleep_range,
    _page_looks_blocked,
    _wait_for_ready,
    _has_challenge,
    _maybe_handle_challenge,
    _perform_human_like_scroll,
    _simulate_human_read,
    _metrika_loaded,
    _flush_and_wait_metrika,
    _wait_for_metrika_hit,
)


# ==================== PROXY MANAGER ====================
class ProxyPool:
    """Manages a pool of unique proxies for parallel browser sessions."""
    
    def __init__(self, count: int = 5):
        self.count = count
        self._proxies: list[dict] = []
        self._current_idx = 0
        self._lock = asyncio.Lock()
        self._initialize_proxies()
    
    def _initialize_proxies(self):
        """Create unique proxy credentials for each session."""
        self._proxies = []
        for i in range(self.count):
            session_id = f"worker-{i}-{uuid.uuid4().hex[:6]}"
            proxy = {
                "host": PROXY_HOST,
                "port": PROXY_PORT,
                "username": f"{USERNAME}_{session_id}",
                "password": PASSWORD,
                "worker_id": i,
            }
            self._proxies.append(proxy)
    
    def get_proxy(self, worker_id: int) -> dict:
        """Get a proxy for the given worker (always same proxy for same worker)."""
        idx = worker_id % self.count
        return self._proxies[idx]
    
    def get_all_proxies(self) -> list[dict]:
        """Get all proxies in the pool."""
        return self._proxies.copy()
    
    async def rotate(self):
        """Rotate proxy credentials for security (optional)."""
        async with self._lock:
            for proxy in self._proxies:
                session_id = uuid.uuid4().hex[:8]
                proxy["username"] = f"{USERNAME}_{proxy['worker_id']}_{session_id}"


# ==================== BROWSER POOL ====================
@dataclass
class BrowserSession:
    """Represents an active browser session with its proxy."""
    worker_id: int
    proxy: dict
    sb: Optional[SB] = None
    last_used: float = field(default_factory=time.time)
    active: bool = False
    visit_count: int = 0
    consecutive_failures: int = 0


class BrowserPool:
    """
    Manages multiple browser sessions with unique proxies.
    Implements automatic restart on failure and load balancing.
    """
    
    def __init__(self, size: int = 5):
        self.size = size
        self.pool: list[BrowserSession] = []
        self.proxy_pool = ProxyPool(size)
        self._initialized = False
        self._lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize all browser sessions in the pool."""
        async with self._lock:
            if self._initialized:
                return
            
            for i in range(self.size):
                proxy = self.proxy_pool.get_proxy(i)
                self.pool.append(BrowserSession(
                    worker_id=i,
                    proxy=proxy,
                ))
            
            self._initialized = True
            print(f"✅ Browser pool initialized with {self.size} browsers")
    
    async def acquire(self, worker_id: int) -> BrowserSession:
        """Acquire a browser session for the given worker."""
        async with self._lock:
            # Find or create session for this worker
            for session in self.pool:
                if session.worker_id == worker_id:
                    if not session.active or session.consecutive_failures >= 3:
                        # Need to restart this browser
                        if session.sb:
                            try:
                                session.sb.quit()
                            except:
                                pass
                        session.sb = None
                        session.consecutive_failures = 0
                        session.visit_count = 0
                    
                    session.active = True
                    session.last_used = time.time()
                    return session
            
            # Create new session if not found
            proxy = self.proxy_pool.get_proxy(worker_id)
            session = BrowserSession(
                worker_id=worker_id,
                proxy=proxy,
            )
            self.pool.append(session)
            return session
    
    async def release(self, worker_id: int, success: bool = True):
        """Release a browser session after use."""
        async with self._lock:
            for session in self.pool:
                if session.worker_id == worker_id:
                    session.active = False
                    if success:
                        session.consecutive_failures = 0
                    else:
                        session.consecutive_failures += 1
                    session.visit_count += 1
    
    async def shutdown(self):
        """Clean up all browser sessions."""
        async with self._lock:
            for session in self.pool:
                if session.sb:
                    try:
                        session.sb.quit()
                    except:
                        pass
            self.pool.clear()
            self._initialized = False
            print("🛑 Browser pool shutdown complete")
    
    def get_stats(self) -> dict:
        """Get pool statistics."""
        return {
            "total": self.size,
            "active": sum(1 for s in self.pool if s.active),
            "visits": sum(s.visit_count for s in self.pool),
            "avg_failures": sum(s.consecutive_failures for s in self.pool) / self.size if self.size > 0 else 0,
        }


# ==================== OPTIMIZED VISIT FUNCTION ====================
async def visit_with_parallel_browsers(
    proxy: dict,
    target: str,
    visit_id: int,
    browser_session: BrowserSession,
    secondary_urls: list[str] | None = None,
) -> dict:
    """
    Visit target URL with optimized parallel browser support.
    Returns structured visit result with enhanced error handling.
    """
    proxy_string = (
        f"{proxy['username']}:{proxy['password']}@"
        f"{proxy['host']}:{proxy['port']}"
    )
    started_at = time.time()
    
    try:
        # Check global stop flag
        if STOP_FLAG:
            return {
                "success": False,
                "hit_verified": False,
                "duration": time.time() - started_at,
                "error": "stopped",
            }
        
        print(f"[{visit_id}] Starting visit with proxy {proxy['host']}")
        
        # Create new browser for this visit (isolated for safety)
        with SB(
            uc=True,
            proxy=proxy_string,
            headless=False,
            page_load_strategy="eager",
            test=True,
        ) as sb:
            browser_session.sb = sb
            
            # Apply global stop check
            if STOP_FLAG:
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "stopped",
                }
            
            # Set page load timeout
            try:
                sb.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            except Exception:
                pass
            
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
                            "Referer": REFERER_URL,
                            "Accept-Language": "uz-UZ,uz;q=0.9,ru-RU;q=0.8,ru;q=0.7,en-US;q=0.6,en;q=0.5",
                        }
                    },
                )
            except Exception:
                pass
            
            # Pre-warm with Yandex
            try:
                sb.activate_cdp_mode(PREWARM_URL)
            except Exception:
                pass
            
            if not await asyncio.to_thread(_sleep_range, POST_LAUNCH_DELAY):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "stopped",
                }
            
            # Navigate to target
            try:
                sb.open(target)
            except Exception as e:
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": f"open_failed:{str(e)[:50]}",
                }
            
            if not await asyncio.to_thread(_wait_for_ready, sb):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "ready_timeout",
                }
            
            # Wait for page to settle
            if not await asyncio.to_thread(_sleep_range, POST_OPEN_SETTLE):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "stopped",
                }
            
            current_url = sb.get_current_url()
            print(f"[{visit_id}] Landed on: {current_url}")
            
            # Check if blocked
            if await asyncio.to_thread(_page_looks_blocked, sb):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "blocked",
                }
            
            # Handle challenge
            if not await asyncio.to_thread(_maybe_handle_challenge, sb, visit_id):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "challenge_failed",
                }
            
            # Check again after challenge
            if await asyncio.to_thread(_page_looks_blocked, sb):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "blocked_after_challenge",
                }
            
            # Perform human-like scroll
            if not await asyncio.to_thread(_perform_human_like_scroll, sb, None):
                return {
                    "success": False,
                    "hit_verified": False,
                    "duration": time.time() - started_at,
                    "error": "behavior_failed",
                }
            
            # Handle Metrika
            metrika_ready = await asyncio.to_thread(_metrika_loaded, sb)
            hit_ok = False
            
            if metrika_ready:
                hit_ok = await asyncio.to_thread(
                    _flush_and_wait_metrika, sb, METRIKA_HIT_TIMEOUT
                )
            
            # Simulate human reading behavior
            if not await asyncio.to_thread(_simulate_human_read, sb, 8, 12):
                return {
                    "success": False,
                    "hit_verified": hit_ok,
                    "duration": time.time() - started_at,
                    "error": "human_sim_failed",
                }
            
            # Secondary tabs (optional)
            if secondary_urls:
                for extra in secondary_urls:
                    if STOP_FLAG:
                        return {
                            "success": False,
                            "hit_verified": hit_ok,
                            "duration": time.time() - started_at,
                            "error": "stopped",
                        }
                    try:
                        sb.open_new_tab(extra)
                        await asyncio.to_thread(_sleep_range, (0.8, 1.2))
                        sb.switch_to_window(0)
                    except Exception:
                        try:
                            sb.close_current_window()
                            sb.switch_to_window(0)
                        except Exception:
                            pass
            
            # Final Metrika check
            hit_ok = await asyncio.to_thread(
                _wait_for_metrika_hit, sb, METRIKA_HIT_TIMEOUT
            )
            
            # Ensure minimum browser lifetime
            elapsed = time.time() - started_at
            if elapsed < BROWSER_MIN_LIFETIME:
                if not await asyncio.to_thread(
                    _sleep_interruptible, BROWSER_MIN_LIFETIME - elapsed
                ):
                    return {
                        "success": False,
                        "hit_verified": hit_ok,
                        "duration": time.time() - started_at,
                        "error": "stopped",
                    }
            
            # Verify browser stayed alive for minimum time
            final_elapsed = time.time() - started_at
            if final_elapsed < BROWSER_MIN_LIFETIME:
                min_remaining = BROWSER_MIN_LIFETIME - final_elapsed
                if not await asyncio.to_thread(
                    _sleep_interruptible, min_remaining
                ):
                    return {
                        "success": False,
                        "hit_verified": hit_ok,
                        "duration": time.time() - started_at,
                        "error": "stopped",
                    }
            
            # Get final URL
            try:
                current_url = sb.get_current_url()
            except Exception:
                current_url = ""
            
            print(f"[{visit_id}] Visit successful | hit={hit_ok}")
            return {
                "success": True,
                "hit_verified": hit_ok,
                "duration": time.time() - started_at,
                "error": None,
            }
            
    except Exception as e:
        print(f"[{visit_id}] Error: {e}")
        return {
            "success": False,
            "hit_verified": False,
            "duration": time.time() - started_at,
            "error": str(e),
        }


# ==================== CONVENIENCE FUNCTIONS ====================
def get_browser_pool(size: int = 5) -> BrowserPool:
    """Get or create a global browser pool instance."""
    if not hasattr(get_browser_pool, "_instance"):
        get_browser_pool._instance = BrowserPool(size)
    return get_browser_pool._instance


async def cleanup_browser_pool():
    """Clean up the global browser pool."""
    if hasattr(get_browser_pool, "_instance"):
        await get_browser_pool._instance.shutdown()
        del get_browser_pool._instance


# Reset global stop flag on module import
set_stop_flag(False)
