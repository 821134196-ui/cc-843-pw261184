"""Smart runtime: coalescing + per-key serialization + DRR tenant fairness.

Queue topology
--------------
* One FIFO per tenant; each tenant's queue holds at most one pending event
  per key. A new event for a key that already has a pending event is *merged*
  into it (coalescing), so a continuous storm collapses to one pending update
  per object instead of an unbounded FIFO.
* A global retry heap holds events waiting out their backoff. Due retries are
  promoted back into the tenant queue and re-enter the same fairness round.
* Deficit Round Robin picks which tenant's head event is dispatched next.
  A noisy tenant can only spend its quantum before healthy tenants get a turn,
  so a sustained storm cannot starve them.
"""

from __future__ import annotations

from collections import deque

from .events import Event
from .base import BaseRuntime


class SmartRuntime(BaseRuntime):
    def __init__(self, *args, quantum: int = 4, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.default_quantum = quantum
        self._tenant_queues: dict[str, deque[Event]] = {}
        # keys that currently have a pending (queued) event
        self._pending_keys: set[str] = set()
        # DRR bookkeeping: stable ordered tenant list + per-tenant deficit
        self._tenants: list[str] = []
        self._deficit: dict[str, int] = {}
        self._cursor = 0

    def set_quantum(self, tenant: str, quantum: int) -> None:
        """Optional per-tenant weight for DRR."""
        self.tenant_quantums[tenant] = max(1, quantum)

    def _quantum(self, tenant: str) -> int:
        return self.tenant_quantums.get(tenant, self.default_quantum)

    # ------------------------------------------------------------- enqueue
    def _enqueue_locked(self, event: Event) -> None:
        tq = self._tenant_queues.get(event.tenant)
        if tq is None:
            tq = deque()
            self._tenant_queues[event.tenant] = tq
            self._tenants.append(event.tenant)
            self._deficit[event.tenant] = 0

        pending_key = f"{event.tenant}/{event.key}"
        if pending_key in self._pending_keys:
            # Same object already waiting in this tenant's queue: merge.
            for queued in tq:
                if queued.key == event.key:
                    queued.coalesce(event)
                    return
        tq.append(event)
        self._pending_keys.add(pending_key)

    # ------------------------------------------------------------ scheduling
    def _next_event_locked(self) -> Event | None:
        n = len(self._tenants)
        if n == 0:
            return None

        # At most two passes over the tenant ring: after one full round every
        # non-empty tenant has received a quantum, so a second pass that still
        # finds nothing means every queued key is currently in flight.
        start = self._cursor
        for _ in range(2):
            for step in range(n):
                idx = (start + step) % n
                tenant = self._tenants[idx]
                tq = self._tenant_queues[tenant]
                if not tq or self._tenant_at_capacity_locked(tenant):
                    continue

                # Find the tenant's first event whose key isn't in flight.
                # Coalescing guarantees at most one pending event per key, so
                # blocked heads are rotated behind later keys at most once.
                head = None
                for _ in range(len(tq)):
                    candidate = tq[0]
                    if candidate.key not in self._inflight:
                        head = candidate
                        break
                    tq.rotate(-1)
                if head is None:
                    continue  # all of this tenant's keys are in flight

                if self._deficit[tenant] <= 0:
                    self._deficit[tenant] += self._quantum(tenant)
                if self._deficit[tenant] <= 0:
                    continue

                tq.popleft()
                self._pending_keys.discard(f"{tenant}/{head.key}")
                self._deficit[tenant] -= 1
                # Next dispatch starts scanning from the following tenant,
                # giving each tenant an equal share of the ring.
                self._cursor = (idx + 1) % n
                return head

            start = self._cursor
        return None

    def _tenant_backlog_locked(self) -> dict[str, int]:
        return {t: len(q) for t, q in self._tenant_queues.items() if q}

    def _idle_locked(self) -> bool:
        return (
            not any(self._tenant_queues.values())
            and not self._inflight
            and not self._delay_heap
        )
