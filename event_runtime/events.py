"""Event definition and coalescing rule."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass
class Event:
    """A versioned update for one object (key) owned by a tenant.

    ``version`` is monotonically increasing per key. The runtime guarantees
    that only the highest version seen so far can land in the state store.
    """

    tenant: str
    key: str
    version: int
    payload: Any = None
    enqueued_at: float = 0.0
    attempt: int = 0
    merged_count: int = 0
    redelivered: bool = False

    def touch(self) -> None:
        """Stamp the first arrival time; preserved across coalescing."""
        if self.enqueued_at == 0.0:
            self.enqueued_at = time.monotonic()

    def coalesce(self, newer: "Event") -> None:
        """Merge ``newer`` into ``self`` while both refer to the same key.

        The newest version wins (an older redelivery never downgrades state),
        the earliest arrival timestamp is kept so end-to-end latency reflects
        the full waiting time, and the merge count accumulates.
        """
        if newer.version > self.version:
            self.version = newer.version
            self.payload = newer.payload
        self.merged_count += 1 + newer.merged_count
        self.redelivered = self.redelivered or newer.redelivered
