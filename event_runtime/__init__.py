"""Lightweight event-processing runtime.

Public objects:
    Event            - a versioned update targeting (tenant, key)
    StateStore       - keyed store with compare-and-swap by version
    BackoffPolicy    - exponential backoff with jitter
    NaiveRuntime     - baseline: single FIFO, no coalescing, no fairness
    SmartRuntime     - coalescing + per-key serialization + DRR tenant fairness
    Metrics          - counters and latency samples
"""

from .events import Event
from .store import StateStore, StaleVersionError
from .backoff import BackoffPolicy
from .metrics import Metrics
from .naive import NaiveRuntime
from .smart import SmartRuntime

__all__ = [
    "Event",
    "StateStore",
    "StaleVersionError",
    "BackoffPolicy",
    "Metrics",
    "NaiveRuntime",
    "SmartRuntime",
]
