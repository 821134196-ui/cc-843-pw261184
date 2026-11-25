"""Common worker machinery shared by the naive and smart runtimes.

The two runtimes differ only in their queue topology (the ``_enqueue_locked``
/ ``_next_event_locked`` hooks). The worker loop, retry/backoff handling,
per-key serialization and stale guard are identical and live here.
"""

from __future__ import annotations

import heapq
import itertools
import random
import threading
import time
from typing import Callable

from .backoff import BackoffPolicy
from .events import Event
from .metrics import Metrics
from .store import StaleVersionError, StateStore

Handler = Callable[[Event], None]


class BaseRuntime:
    def __init__(
        self,
        handler: Handler,
        store: StateStore | None = None,
        workers: int = 4,
        backoff: BackoffPolicy | None = None,
        max_attempts: int = 3,
        seed: int = 7,
    ) -> None:
        self.handler = handler
        self.store = store or StateStore()
        self.workers = workers
        self.backoff = backoff or BackoffPolicy()
        self.max_attempts = max_attempts
        self._rng = random.Random(seed)

        self.metrics = Metrics()
        self._lock = threading.Condition()
        self._stopped = False
        self._threads: list[threading.Thread] = []

        # retry delay heap: (ready_at, seq, event)
        self._delay_heap: list[tuple[float, int, Event]] = []
        self._seq = itertools.count()

        # per-key serialization: keys currently being handled by a worker
        self._inflight: set[str] = set()
        # per-tenant count of in-flight handlers (for fair worker sharing)
        self._tenant_inflight: dict[str, int] = {}

        # highest version ever enqueued per key (runtime watermark). Unlike
        # the store's committed version, this advances on submission, so a
        # failed attempt can tell when a newer version has overtaken it.
        self._seen_version: dict[str, int] = {}

        # optional cap on concurrent handlers per tenant; None = unlimited
        self.tenant_concurrency: int | None = None
        self.tenant_quantums: dict[str, int] = {}

    # ------------------------------------------------------------------ API
    def start(self) -> "BaseRuntime":
        if self._threads:
            return self
        self._threads = [
            threading.Thread(
                target=self._worker_loop, name=f"{type(self).__name__}-{i}",
                daemon=True,
            )
            for i in range(self.workers)
        ]
        for t in self._threads:
            t.start()
        return self

    def submit(self, event: Event) -> None:
        if self._stopped:
            raise RuntimeError("runtime is stopped")
        event.touch()
        with self._lock:
            self.metrics.submitted += 1
            self._seen_version[event.key] = max(
                self._seen_version.get(event.key, 0), event.version
            )
            self._enqueue_locked(event)
            self._lock.notify_all()

    def drain(self, timeout: float | None = None) -> bool:
        """Wait until all queued/in-flight/retry work is gone."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while not self._idle_locked():
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._lock.wait(timeout=remaining)
        return True

    def stop(self, timeout: float = 5.0) -> None:
        with self._lock:
            self._stopped = True
            self._lock.notify_all()
        for t in self._threads:
            t.join(timeout=timeout)

    def backlog(self) -> dict[str, object]:
        """Current queue depth, per tenant, plus in-flight/delayed counts."""
        with self._lock:
            per_tenant = self._tenant_backlog_locked()
            return {
                "total": sum(per_tenant.values())
                + len(self._inflight)
                + len(self._delay_heap),
                "queued": sum(per_tenant.values()),
                "inflight": len(self._inflight),
                "delayed": len(self._delay_heap),
                "per_tenant": per_tenant,
                "tenant_inflight": dict(self._tenant_inflight),
            }

    def _tenant_at_capacity_locked(self, tenant: str) -> bool:
        if self.tenant_concurrency is None:
            return False
        return self._tenant_inflight.get(tenant, 0) >= self.tenant_concurrency

    # -------------------------------------------------------------- hooks
    def _enqueue_locked(self, event: Event) -> None:
        raise NotImplementedError

    def _next_event_locked(self) -> Event | None:
        raise NotImplementedError

    def _tenant_backlog_locked(self) -> dict[str, int]:
        raise NotImplementedError

    def _idle_locked(self) -> bool:
        raise NotImplementedError

    # ------------------------------------------------------------- worker
    def _worker_loop(self) -> None:
        while True:
            with self._lock:
                event = None
                while not self._stopped:
                    self._promote_due_retries_locked()
                    event = self._next_event_locked()
                    if event is not None:
                        break
                    if self._stopped:
                        break
                    wait = 0.05
                    if self._delay_heap:
                        wait = min(wait, max(0.0, self._delay_heap[0][0]
                                             - time.monotonic()))
                    self._lock.wait(timeout=wait)
                if event is None:
                    return
                self._inflight.add(event.key)
                self._tenant_inflight[event.tenant] = (
                    self._tenant_inflight.get(event.tenant, 0) + 1
                )

            self._process(event)

            with self._lock:
                self._inflight.discard(event.key)
                remaining = self._tenant_inflight.get(event.tenant, 1) - 1
                if remaining <= 0:
                    self._tenant_inflight.pop(event.tenant, None)
                else:
                    self._tenant_inflight[event.tenant] = remaining
                self._lock.notify_all()

    def _process(self, event: Event) -> None:
        event.attempt += 1
        try:
            # Cheap pre-check: never invoke the handler for an event that
            # cannot possibly win because a newer version already committed.
            # The store commit re-checks under its own lock as the real CAS.
            if event.version <= self.store.version(event.key):
                with self._lock:
                    self.metrics.stale_dropped += 1
                return

            self.handler(event)
            self.store.commit(event)
        except StaleVersionError:
            with self._lock:
                self.metrics.stale_dropped += 1
            return
        except Exception as exc:  # handler failure -> retry with backoff
            with self._lock:
                self.metrics.failed += 1
                # A newer version of this object has been enqueued while this
                # attempt ran: retrying the old version is pointless. Drop it
                # as stale instead of burning backoff/worker capacity on work
                # that can never become the latest state.
                if event.version < self._seen_version.get(event.key, 0):
                    self.metrics.stale_dropped += 1
                    self._lock.notify_all()
                    return
                if event.attempt >= self.max_attempts:
                    self.metrics.dead_lettered += 1
                    self.metrics.record_dead_letter(
                        event.tenant, event.key, event.version,
                        event.attempt, type(exc).__name__,
                    )
                    self._lock.notify_all()
                    return
                delay = self.backoff.delay(event.attempt, self._rng)
                ready = time.monotonic() + delay
                heapq.heappush(self._delay_heap, (ready, next(self._seq), event))
                self.metrics.retried += 1
                self._lock.notify_all()
            return

        with self._lock:
            self.metrics.processed += 1
            self.metrics.coalesced += event.merged_count
            self.metrics.add_latency(
                time.monotonic() - event.enqueued_at, event.tenant
            )
            self._lock.notify_all()

    def _promote_due_retries_locked(self) -> None:
        now = time.monotonic()
        while self._delay_heap and self._delay_heap[0][0] <= now:
            _, _, event = heapq.heappop(self._delay_heap)
            event.redelivered = True
            self._enqueue_locked(event)
