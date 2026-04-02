"""
Shuffled Task Dispatcher.
Flattens all pending DB tasks into individual click-jobs,
applies Fisher-Yates shuffle, and serves them to workers via async queue.

Per-domain rate limiting: enforces a minimum gap between visits to the
same domain so Yandex Metrika fraud detection is not triggered by a burst
of visits to a single counter in a short window.
"""
import random
import time
import asyncio
from urllib.parse import urlparse
from db import get_pending_tasks

# Concurrency-slot model (replaces the old 120s time-gate).
#
# OLD approach (broke parallelism): one task dispatched per domain per 120 seconds.
# With centrum-air.com being the only domain, only 1 browser ran at a time.
#
# NEW approach:
#   MAX_CONCURRENT_PER_DOMAIN — up to N browsers may work the same domain simultaneously.
#   DOMAIN_DISPATCH_MIN_GAP  — minimum stagger between STARTING visits to the same domain,
#                               so the 3 parallel starts are spread 15s apart rather than
#                               all hitting the counter at the exact same millisecond.
#
# Effect: with 5 workers and centrum-air.com:
#   T=0s  → Worker 1 dispatched
#   T=15s → Worker 2 dispatched  (15s stagger)
#   T=30s → Worker 3 dispatched  (at MAX_CONCURRENT=3, workers 4 & 5 wait)
#   T~90s → Worker 1 finishes, slot released → Worker 4 dispatched
#   T~105s → Worker 5 dispatched
MAX_CONCURRENT_PER_DOMAIN = 3
DOMAIN_DISPATCH_MIN_GAP   = 15    # seconds between dispatching to same domain


class Dispatcher:
    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stop = False
        self._total = 0
        self._completed = 0
        # domain → monotonic timestamp of last dispatch (for stagger gap)
        self._domain_last_dispatched: dict[str, float] = {}
        # domain → count of visits currently in-flight (for concurrency cap)
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

    def _extract_domain(self, url: str) -> str:
        """Normalize URL to bare domain for rate-limit keying."""
        try:
            return urlparse(url).netloc.replace("www.", "").lower()
        except Exception:
            return url

    async def next_task(self) -> dict | None:
        """
        Get the next click-job that satisfies the concurrency-slot model,
        or None if all slots are full / queue empty / stopped.

        A task is dispatchable when BOTH conditions hold:
          1. in_flight[domain] < MAX_CONCURRENT_PER_DOMAIN  (concurrency cap)
          2. now - last_dispatched[domain] >= DOMAIN_DISPATCH_MIN_GAP  (stagger gap)

        The stagger gap prevents all workers from hammering the same counter at the
        exact same second on start-up. Once a visit finishes and releases its slot,
        the next visit for that domain is eligible immediately (gap already elapsed).
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
            domain = self._extract_domain(item["url"])
            in_flight = self._domain_in_flight.get(domain, 0)
            last_dispatched = self._domain_last_dispatched.get(domain, 0.0)

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
        domain = self._extract_domain(chosen["url"])
        self._domain_in_flight[domain] = self._domain_in_flight.get(domain, 0) + 1
        self._domain_last_dispatched[domain] = time.monotonic()
        return chosen

    def release_domain(self, domain: str) -> None:
        """
        Release one concurrency slot for domain.
        Called by the runner after a visit (success or failure) completes.
        This is what allows the next worker to pick up a task for that domain.
        """
        self._domain_in_flight[domain] = max(
            0, self._domain_in_flight.get(domain, 0) - 1
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

    def mark_completed(self):
        """Record one finished click-job."""
        self._completed += 1

    @property
    def completed(self) -> int:
        return self._completed

    def all_tasks_done(self) -> bool:
        """Check if all tasks have been completed."""
        return self._completed >= self._total
