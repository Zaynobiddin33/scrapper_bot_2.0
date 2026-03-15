"""
Shuffled Task Dispatcher.
Flattens all pending DB tasks into individual click-jobs,
applies Fisher-Yates shuffle, and serves them to workers via async queue.
"""
import random
import asyncio
from db import get_pending_tasks


class Dispatcher:
    def __init__(self):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._stop = False
        self._total = 0

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
        return self._total

    async def next_task(self) -> dict | None:
        """Get the next click-job, or None if empty/stopped."""
        if self._stop:
            return None
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

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
    def completed(self) -> int:
        return self._total - self._queue.qsize()
