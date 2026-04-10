"""
Deadline-aware pacing helpers.

The bot uses five parallel workers, so "average delay per worker" is not enough
to stay close to a user-selected deadline. This pacer reserves future completion
slots and spaces new visits against the full run window, while continuously
correcting itself using real visit durations.
"""
from __future__ import annotations

import time
from typing import Callable, Protocol


class DispatcherLike(Protocol):
    @property
    def completed(self) -> int: ...

    @property
    def in_flight_total(self) -> int: ...


class DeadlinePacer:
    """Reserve future completion slots so workers do not finish far too early."""

    def __init__(
        self,
        deadline_ts: float | None,
        total_tasks: int,
        dispatcher: DispatcherLike,
        visit_durations: list[float],
        *,
        default_delay: float = 8.0,
        default_visit_duration: float = 75.0,
        min_delay: float = 0.5,
        history_size: int = 40,
        now_fn: Callable[[], float] | None = None,
    ):
        self.deadline_ts = deadline_ts
        self.total_tasks = max(1, int(total_tasks))
        self.dispatcher = dispatcher
        self.visit_durations = visit_durations
        self.default_delay = max(0.0, float(default_delay))
        self.default_visit_duration = max(1.0, float(default_visit_duration))
        self.min_delay = max(0.0, float(min_delay))
        self.history_size = max(1, int(history_size))
        self._now_fn = now_fn or time.time
        self.start_ts = self._now_fn()
        self._last_reserved_slot = 0

    @property
    def enabled(self) -> bool:
        return self.deadline_ts is not None

    def _estimate_visit_duration(self) -> float:
        recent = [
            float(duration)
            for duration in self.visit_durations[-self.history_size:]
            if duration is not None and float(duration) >= 1.0
        ]
        if not recent:
            return self.default_visit_duration

        recent.sort()
        trim = len(recent) // 10
        if trim and len(recent) > trim * 2:
            recent = recent[trim:-trim]

        mid = len(recent) // 2
        if len(recent) % 2:
            median = recent[mid]
        else:
            median = (recent[mid - 1] + recent[mid]) / 2
        mean = sum(recent) / len(recent)

        # Lean slightly toward the median to avoid one-off outliers from
        # making the whole schedule drift.
        return max(8.0, (median * 0.7) + (mean * 0.3))

    def _reserve_completion_slot(self) -> int:
        baseline_slot = min(
            self.total_tasks,
            self.dispatcher.completed + self.dispatcher.in_flight_total + 1,
        )
        next_slot = max(
            baseline_slot,
            min(self.total_tasks, self._last_reserved_slot + 1),
        )
        self._last_reserved_slot = next_slot
        return next_slot

    def get_delay(self) -> float:
        """Return how long the next worker should wait before starting a visit."""
        if self.deadline_ts is None:
            return self.default_delay

        unfinished = max(0, self.total_tasks - self.dispatcher.completed)
        if unfinished <= 0:
            return 0.0

        now = self._now_fn()
        remaining_secs = self.deadline_ts - now
        if remaining_secs <= 0:
            return self.min_delay

        total_window = max(1.0, self.deadline_ts - self.start_ts)
        completion_spacing = total_window / self.total_tasks
        visit_estimate = self._estimate_visit_duration()

        slot_number = self._reserve_completion_slot()
        target_completion_ts = self.start_ts + (slot_number * completion_spacing)
        target_start_ts = max(self.start_ts, target_completion_ts - visit_estimate)

        # Never wait so long that the remaining visits become impossible to fit.
        latest_reasonable_start = max(self.start_ts, self.deadline_ts - visit_estimate)
        delay = min(
            max(0.0, target_start_ts - now),
            max(0.0, latest_reasonable_start - now),
        )

        if delay <= 0:
            return 0.0
        return max(self.min_delay, delay)
