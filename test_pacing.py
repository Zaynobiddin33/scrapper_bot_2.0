#!/usr/bin/env python3
import heapq
import os
import random
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pacing import DeadlinePacer


class FakeClock:
    def __init__(self):
        self.now = 0.0


class FakeDispatcher:
    def __init__(self):
        self.completed = 0
        self.in_flight_total = 0


class DeadlinePacerTests(unittest.TestCase):
    def test_long_deadlines_are_not_capped_to_ten_minutes(self):
        dispatcher = FakeDispatcher()
        pacer = DeadlinePacer(
            deadline_ts=12 * 3600,
            total_tasks=50,
            dispatcher=dispatcher,
            visit_durations=[],
            now_fn=lambda: 0.0,
        )

        delay = pacer.get_delay()
        self.assertGreater(delay, 600.0)

    def test_parallel_schedule_finishes_close_to_deadline(self):
        random.seed(7)
        workers = 5
        total_tasks = 120
        deadline_ts = 12 * 3600

        clock = FakeClock()
        dispatcher = FakeDispatcher()
        visit_durations: list[float] = []
        pacer = DeadlinePacer(
            deadline_ts=deadline_ts,
            total_tasks=total_tasks,
            dispatcher=dispatcher,
            visit_durations=visit_durations,
            now_fn=lambda: clock.now,
        )

        queue: list[tuple[float, int, float]] = []
        for worker_id in range(workers):
            start_at = clock.now + pacer.get_delay()
            dispatcher.in_flight_total += 1
            duration = random.uniform(28.0, 65.0)
            heapq.heappush(queue, (start_at + duration, worker_id, duration))

        while dispatcher.completed < total_tasks:
            finished_at, worker_id, duration = heapq.heappop(queue)
            clock.now = finished_at
            dispatcher.in_flight_total -= 1
            dispatcher.completed += 1
            visit_durations.append(duration)

            if dispatcher.completed >= total_tasks:
                break

            start_at = clock.now + pacer.get_delay()
            dispatcher.in_flight_total += 1
            duration = random.uniform(28.0, 65.0)
            heapq.heappush(queue, (start_at + duration, worker_id, duration))

        self.assertLess(abs(clock.now - deadline_ts), 5 * 60)


if __name__ == "__main__":
    unittest.main()
