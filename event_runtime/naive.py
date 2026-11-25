"""Naive baseline runtime: one global FIFO, no coalescing, no fairness.

This is deliberately the simple implementation that the smart runtime is
compared against. It keeps the same safety guarantees (per-key serialization,
version CAS, backoff retries) but offers no protection against a noisy tenant:
every event, including redundant consecutive updates, occupies its own FIFO
slot, so one storm tenant can fill the queue and delay every healthy tenant.
"""

from __future__ import annotations

from collections import deque

from .events import Event
from .base import BaseRuntime


class NaiveRuntime(BaseRuntime):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._queue: deque[Event] = deque()

    def _enqueue_locked(self, event: Event) -> None:
        self._queue.append(event)

    def _next_event_locked(self) -> Event | None:
        skipped: list[Event] = []
        chosen: Event | None = None
        while self._queue:
            event = self._queue.popleft()
            if event.key in self._inflight or \
                    self._tenant_at_capacity_locked(event.tenant):
                # Honor per-key serialization (and any tenant cap) by parking
                # the event for a later scheduling pass.
                skipped.append(event)
                continue
            chosen = event
            break
        # Reinsert parked events at the front, preserving their order.
        for event in reversed(skipped):
            self._queue.appendleft(event)
        return chosen

    def _tenant_backlog_locked(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in self._queue:
            counts[event.tenant] = counts.get(event.tenant, 0) + 1
        return counts

    def _idle_locked(self) -> bool:
        return not self._queue and not self._inflight and not self._delay_heap
