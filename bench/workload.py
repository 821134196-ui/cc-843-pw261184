"""Fixed, seeded workload: one storm tenant plus several healthy tenants.

The schedule is built once and replayed against every runtime so comparisons
are apples-to-apples. Everything is deterministic given ``seed``.

Traffic shape
-------------
* ``tenant-storm`` - a few hot objects. Updates arrive in *bursts*: several
  consecutive versions of the same key within a sub-millisecond window, then
  a short pause. This mirrors "the same object receives a flood of updates"
  and is exactly what coalescing collapses. Its handlers intermittently fail,
  forcing retries with backoff. Two deterministic scenarios are embedded:
    - ``storm-key-0`` v1 fails once; v2 lands while v1 waits in the backoff
      heap, so the retried v1 is provably stale and must be dropped by the
      version CAS instead of overwriting v2.
    - ``storm-key-1`` v1 always fails until attempts are exhausted, producing
      one dead letter.
* ``tenant-a/b/c`` - healthy tenants with slow, regular, always-successful
  updates. Their latency is the fairness indicator: under a storm they should
  not be starved.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from event_runtime import Event


@dataclass(frozen=True)
class ScheduledEvent:
    offset: float          # injection time relative to run start (seconds)
    tenant: str
    key: str
    version: int
    fail: bool = False


@dataclass
class WorkloadConfig:
    duration: float = 6.0
    storm_keys: int = 8
    burst_size: int = 6              # consecutive updates per object per burst
    burst_gap: float = 0.004         # pause between bursts
    burst_spread: float = 0.0005     # intra-burst arrival spread
    storm_fail_rate: float = 0.05
    healthy_tenants: tuple[str, ...] = ("tenant-a", "tenant-b", "tenant-c")
    healthy_keys_per_tenant: int = 3
    healthy_interval: float = 0.30
    seed: int = 20261126
    # bench / runtime parameters
    workers: int = 4
    handler_cost: float = 0.003      # seconds of work per handled event
    sample_interval: float = 0.05
    quantum_storm: int = 2
    backoff_base: float = 0.03
    backoff_cap: float = 0.4
    max_attempts: int = 5


def build_schedule(cfg: WorkloadConfig | None = None) -> list[ScheduledEvent]:
    cfg = cfg or WorkloadConfig()
    rng = random.Random(cfg.seed)
    schedule: list[ScheduledEvent] = []

    storm_keys = [f"storm-key-{i}" for i in range(cfg.storm_keys)]
    versions = {key: 0 for key in storm_keys}

    # --- storm tenant: repeated bursts of updates to hot objects ----------
    t = 0.0
    while t < cfg.duration:
        key = storm_keys[rng.randrange(cfg.storm_keys)]
        for _ in range(cfg.burst_size):
            if t >= cfg.duration:
                break
            versions[key] += 1
            schedule.append(
                ScheduledEvent(
                    offset=t,
                    tenant="tenant-storm",
                    key=key,
                    version=versions[key],
                    fail=rng.random() < cfg.storm_fail_rate,
                )
            )
            t += cfg.burst_spread
        t += cfg.burst_gap

    # --- healthy tenants: sparse, regular, always-successful updates ------
    for tenant in cfg.healthy_tenants:
        for k in range(cfg.healthy_keys_per_tenant):
            key = f"{tenant}-key-{k}"
            version = 0
            offset = rng.uniform(0.05, cfg.healthy_interval)
            while offset < cfg.duration:
                version += 1
                schedule.append(
                    ScheduledEvent(offset=offset, tenant=tenant, key=key,
                                   version=version, fail=False)
                )
                offset += cfg.healthy_interval

    # --- deterministic stale-retry scenario on storm-key-0 ----------------
    # Remove every early update for this key so v1 is the newest version when
    # it fails and genuinely enters the backoff heap. v2 then lands inside the
    # backoff window and commits first; when the retried v1 is promoted back it
    # must be rejected by the version CAS instead of overwriting v2.
    demo_stale = storm_keys[0]
    schedule = [
        item for item in schedule
        if not (item.key == demo_stale and item.offset < 0.5)
    ]
    versions[demo_stale] = max(versions[demo_stale], 2)
    schedule.extend([
        ScheduledEvent(0.05, "tenant-storm", demo_stale, 1, fail=True),
        ScheduledEvent(0.40, "tenant-storm", demo_stale, 2, fail=False),
    ])

    # --- deterministic dead-letter scenario on storm-key-1 ----------------
    if cfg.storm_keys >= 2:
        demo_dl = storm_keys[1]
        schedule = [
            item for item in schedule
            if not (item.key == demo_dl and item.version == 1)
        ]
        versions[demo_dl] = max(versions[demo_dl], 1)
        schedule.append(
            ScheduledEvent(0.08, "tenant-storm", demo_dl, 1, fail=True)
        )

    schedule.sort(key=lambda item: item.offset)
    return schedule


def make_handler(cfg: WorkloadConfig | None = None):
    """Handler that fails on scheduled failures and simulates fixed work."""
    import time as _time

    cfg = cfg or WorkloadConfig()
    fail_marks: set[tuple[str, int]] = set()
    # Keys whose failure is permanent (every attempt fails -> dead letter)
    permanent_fail: set[str] = set()

    def configure(schedule: list[ScheduledEvent]) -> None:
        fail_marks.clear()
        permanent_fail.clear()
        for item in schedule:
            if item.fail:
                fail_marks.add((item.key, item.version))
        if cfg.storm_keys >= 2:
            permanent_fail.add("storm-key-1")

    def handler(event: Event) -> None:
        _time.sleep(cfg.handler_cost)  # fixed cost -> bounded service capacity
        if event.key in permanent_fail:
            raise RuntimeError(f"permanent failure on {event.key}")
        if (event.key, event.version) in fail_marks:
            raise RuntimeError(
                f"injected failure on {event.key} v{event.version}")

    handler.configure = configure  # type: ignore[attr-defined]
    return handler


def to_event(item: ScheduledEvent) -> Event:
    return Event(tenant=item.tenant, key=item.key, version=item.version,
                 payload={"value": item.version})
