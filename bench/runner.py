"""Run the same fixed schedule against both runtimes and collect metrics."""

from __future__ import annotations

import threading
import time
from typing import Any

from event_runtime import BackoffPolicy, NaiveRuntime, SmartRuntime, StateStore

from .workload import ScheduledEvent, make_handler, to_event


class BacklogSampler:
    """Samples runtime.backlog() on a fixed wall-clock interval."""

    def __init__(self, runtime, interval: float = 0.05) -> None:
        self.runtime = runtime
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.samples: list[dict[str, Any]] = []

    def _run(self) -> None:
        start = time.monotonic()
        while not self._stop.is_set():
            snapshot = self.runtime.backlog()
            self.samples.append({
                "t": round(time.monotonic() - start, 4),
                "total": snapshot["total"],
                "queued": snapshot["queued"],
                "inflight": snapshot["inflight"],
                "delayed": snapshot["delayed"],
                "per_tenant": dict(snapshot["per_tenant"]),
            })
            self._stop.wait(self.interval)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)


def _inject(runtime, schedule: list[ScheduledEvent]) -> None:
    """Replay the schedule in real time (one feeder thread)."""
    start = time.monotonic()
    for item in schedule:
        target = start + item.offset
        wait = target - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        runtime.submit(to_event(item))


def run_once(runtime_cls, schedule, cfg, label: str,
             workers: int | None = None) -> dict[str, Any]:
    store = StateStore()
    handler = make_handler(cfg)
    handler.configure(schedule)
    runtime = runtime_cls(
        handler=handler,
        store=store,
        workers=workers or cfg.workers,
        backoff=BackoffPolicy(base=cfg.backoff_base, factor=2.0,
                              cap=cfg.backoff_cap, jitter=0.2),
        max_attempts=cfg.max_attempts,
        seed=7,
    )
    if runtime_cls is SmartRuntime:
        runtime.set_quantum("tenant-storm", cfg.quantum_storm)
        # Fair worker sharing: one tenant may not occupy every worker.
        runtime.tenant_concurrency = max(1, cfg.workers // 2)
    runtime.start()

    sampler = BacklogSampler(runtime, interval=cfg.sample_interval)
    sampler.start()
    wall_start = time.monotonic()
    feeder = threading.Thread(target=_inject, args=(runtime, schedule),
                              daemon=True)
    feeder.start()
    feeder.join()
    injection_done = time.monotonic() - wall_start

    drained = runtime.drain(timeout=15.0)
    sampler.stop()
    runtime.stop()

    metrics = runtime.metrics.summary()
    backlog_series = sampler.samples
    totals = [s["total"] for s in backlog_series] or [0]
    queued = [s["queued"] for s in backlog_series] or [0]

    # Per-tenant latency requires re-deriving latencies from samples is not
    # possible from global metrics, so track via tenant backlog area instead.
    return {
        "label": label,
        "drained": drained,
        "injection_seconds": round(injection_done, 3),
        "metrics": metrics,
        "backlog": {
            "series": backlog_series,
            "peak_total": max(totals),
            "peak_queued": max(queued),
            "avg_total": round(sum(totals) / len(totals), 2),
        },
        "final_state": store.snapshot(),
    }


def compare(cfg=None) -> dict[str, Any]:
    from .workload import WorkloadConfig, build_schedule

    cfg = cfg or WorkloadConfig()
    schedule = build_schedule(cfg)
    naive = run_once(NaiveRuntime, schedule, cfg, "naive-fifo")
    smart = run_once(SmartRuntime, schedule, cfg, "smart-merged-drr")
    return {
        "workload": {
            "events": len(schedule),
            "duration": cfg.duration,
            "burst_size": cfg.burst_size,
            "burst_gap": cfg.burst_gap,
            "healthy_interval": cfg.healthy_interval,
            "workers": cfg.workers,
            "handler_cost_ms": cfg.handler_cost * 1000,
            "tenants": ["tenant-storm", *cfg.healthy_tenants],
        },
        "runs": [naive, smart],
    }
