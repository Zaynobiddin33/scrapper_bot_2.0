"""
Shuffled Task Dispatcher.
Flattens all pending DB tasks into individual click-jobs,
applies Fisher-Yates shuffle, and serves them to workers via async queue.

Per-LINK rate limiting: enforces a minimum gap + concurrency cap per unique
URL (not per domain). The ad links are all Yandex shortlinks (ya.cc/t/<id>)
that resolve to the same domain (yandex.ru), so keying on domain would force
every ad through one lane. Keying on the unique link lets different ads run in
parallel while still pacing repeated visits to the SAME ad/counter.
"""
import random
import time
import asyncio
from urllib.parse import urlparse
from db import get_pending_tasks

# Concurrency-slot model, keyed per unique link:
#   MAX_CONCURRENT_PER_DOMAIN — up to N visits to the SAME link may run at once.
#   DOMAIN_DISPATCH_MIN_GAP  — minimum stagger between STARTING visits to the
#                              same link, so repeated hits to one ad/counter are
#                              spread out rather than all firing at once.
# Different links are independent → many ads run concurrently (bounded only by
# the worker count), which is what clears a large queue within the day.
MAX_CONCURRENT_PER_DOMAIN = 3
DOMAIN_DISPATCH_MIN_GAP   = 15    # seconds between dispatching to the same link


class Dispatcher:
    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stop = False
        self._total = 0
        self._completed = 0
        # link-key → monotonic timestamp of last dispatch (for stagger gap)
        self._domain_last_dispatched: dict[str, float] = {}
        # link-key → count of visits currently in-flight (for concurrency cap)
        self._domain_in_flight: dict[str, int] = {}

    async def build_queue(self) -> int:
        """
        Fetch pending tasks, flatten into individual clicks, shuffle.
        Returns total number of click-jobs queued.
        Example: siteA(50) + siteB(30) → 80 shuffled jobs
        """
        tasks = await get_pending_tasks()
        flat: list[dict] = []

        for task in tasks:
            remaining = task['target_clicks'] - task['current_clicks']
            for _ in range(remaining):
                flat.append({'id': task['id'], 'url': task['url']})

        # Fisher-Yates shuffle: O(n), unbiased uniform permutation
        for i in range(len(flat) - 1, 0, -1):
            j = random.randint(0, i)
            flat[i], flat[j] = flat[j], flat[i]

        # Load into async queue
        for item in flat:
            await self._queue.put(item)

        self._total = len(flat)
        self._completed = 0
        self._domain_last_dispatched.clear()
        self._domain_in_flight.clear()
        return self._total

    def rl_key(self, url: str) -> str:
        """
        Rate-limit key = the unique link (fragment stripped). Each distinct ad
        link is its own lane; identical URLs (repeat visits to one ad) share a
        lane. Used by both dispatch and the runner's release call so they match.
        """
        return (url or "").split("#")[0].strip()

    def _extract_domain(self, url: str) -> str:
        """Bare domain — kept for logging/back-compat (not used for keying)."""
        try:
            return urlparse(url).netloc.replace("www.", "").lower()
        except Exception:
            return url

    async def next_task(self) -> dict | None:
        """
        Get the next click-job that satisfies the concurrency-slot model,
        or None if all slots are full / queue empty / stopped.

        A task is dispatchable when BOTH conditions hold for its link-key:
          1. in_flight[key] < MAX_CONCURRENT_PER_DOMAIN  (concurrency cap)
          2. now - last_dispatched[key] >= DOMAIN_DISPATCH_MIN_GAP  (stagger gap)
        """
        if self._stop:
            return None

        # Drain up to 200 items to find one that passes both gates.
        # All asyncio Queue get_nowait / put calls here are non-blocking
        # (unlimited queue), so this entire section is effectively atomic
        # within the event loop — no other coroutine can interleave.
        pending: list[dict] = []
        try:
            while len(pending) < 200:
                item = self._queue.get_nowait()
                pending.append(item)
        except asyncio.QueueEmpty:
            pass

        if not pending:
            await asyncio.sleep(0.05)
            return None

        now = time.monotonic()
        chosen_idx: int | None = None

        for idx, item in enumerate(pending):
            key = self.rl_key(item["url"])
            in_flight = self._domain_in_flight.get(key, 0)
            last_dispatched = self._domain_last_dispatched.get(key, 0.0)

            slot_free  = in_flight < MAX_CONCURRENT_PER_DOMAIN
            gap_ok     = (now - last_dispatched) >= DOMAIN_DISPATCH_MIN_GAP

            if slot_free and gap_ok:
                chosen_idx = idx
                break

        if chosen_idx is None:
            # No task is dispatchable right now — put everything back and signal None.
            for item in pending:
                await self._queue.put(item)
            # Sleep briefly so workers don't spin-loop at 100% CPU.
            await asyncio.sleep(1.0)
            return None

        chosen = pending[chosen_idx]
        # Return all unchosen items to the queue
        for idx, item in enumerate(pending):
            if idx != chosen_idx:
                await self._queue.put(item)

        # Claim the concurrency slot and record dispatch time
        key = self.rl_key(chosen["url"])
        self._domain_in_flight[key] = self._domain_in_flight.get(key, 0) + 1
        self._domain_last_dispatched[key] = time.monotonic()
        return chosen

    def release_domain(self, key: str) -> None:
        """
        Release one concurrency slot for a link-key.
        Called by the runner after a visit (success or failure) completes,
        using dispatcher.rl_key(url) so it matches the dispatch claim.
        """
        self._domain_in_flight[key] = max(
            0, self._domain_in_flight.get(key, 0) - 1
        )

    def stop(self):
        """Signal all workers to stop after current task."""
        self._stop = True
        # Drain the queue so workers don't block
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    @property
    def is_stopped(self) -> bool:
        return self._stop

    @property
    def remaining(self) -> int:
        return self._queue.qsize()

    @property
    def total(self) -> int:
        return self._total

    @property
    def in_flight_total(self) -> int:
        """Total visits currently running across all links."""
        return sum(self._domain_in_flight.values())

    def mark_completed(self):
        """Record one finished click-job."""
        self._completed += 1

    @property
    def completed(self) -> int:
        return self._completed

    def all_tasks_done(self) -> bool:
        """Check if all tasks have been completed."""
        return self._completed >= self._total
