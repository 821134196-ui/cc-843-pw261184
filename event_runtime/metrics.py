"""Thread-safe metrics: counters, latency samples, recent dead letters."""

from __future__ import annotations

import threading
from collections import deque
from typing import Any


def percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile of an already-sorted list."""
    if not sorted_values:
        return 0.0
    k = max(1, min(len(sorted_values), round(len(sorted_values) * pct / 100)))
    return sorted_values[k - 1]


class Metrics:
    def __init__(self, max_dead_letters: int = 50) -> None:
        self._lock = threading.Lock()
        self.submitted = 0
        self.processed = 0
        self.failed = 0
        self.retried = 0
        self.coalesced = 0
        self.stale_dropped = 0
        self.dead_lettered = 0
        self.latencies: list[float] = []
        self.tenant_latencies: dict[str, list[float]] = {}
        self._dead_letters: deque[dict[str, Any]] = deque(maxlen=max_dead_letters)

    def add_latency(self, seconds: float, tenant: str = "") -> None:
        with self._lock:
            self.latencies.append(seconds)
            self.tenant_latencies.setdefault(tenant, []).append(seconds)

    def record_dead_letter(self, tenant: str, key: str, version: int,
                           attempts: int, error: str) -> None:
        with self._lock:
            self._dead_letters.append(
                {
                    "tenant": tenant,
                    "key": key,
                    "version": version,
                    "attempts": attempts,
                    "error": error,
                }
            )

    def summary(self) -> dict[str, Any]:
        with self._lock:
            ordered = sorted(self.latencies)

            def stats(values: list[float]) -> dict[str, float]:
                ordered = sorted(values)
                k = len(ordered)
                if not k:
                    return {"mean": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
                return {
                    "mean": 1000 * sum(ordered) / k,
                    "p50": 1000 * percentile(ordered, 50),
                    "p95": 1000 * percentile(ordered, 95),
                    "p99": 1000 * percentile(ordered, 99),
                    "max": 1000 * ordered[-1],
                }

            return {
                "submitted": self.submitted,
                "processed": self.processed,
                "failed": self.failed,
                "retried": self.retried,
                "coalesced": self.coalesced,
                "stale_dropped": self.stale_dropped,
                "dead_lettered": self.dead_lettered,
                "latency_ms": stats(ordered),
                "tenant_latency_ms": {
                    tenant: stats(values)
                    for tenant, values in sorted(self.tenant_latencies.items())
                },
                "dead_letters": list(self._dead_letters),
            }
