"""Exponential backoff with symmetric jitter."""

from __future__ import annotations

import random


class BackoffPolicy:
    """delay(attempt) = min(base * factor**(attempt-1), cap) * jitter factor."""

    def __init__(
        self,
        base: float = 0.05,
        factor: float = 2.0,
        cap: float = 0.5,
        jitter: float = 0.25,
    ) -> None:
        if base <= 0 or factor < 1 or not 0 <= jitter < 1:
            raise ValueError("invalid backoff parameters")
        self.base = base
        self.factor = factor
        self.cap = cap
        self.jitter = jitter

    def delay(self, attempt: int, rng: random.Random) -> float:
        raw = min(self.base * self.factor ** (attempt - 1), self.cap)
        low, high = 1.0 - self.jitter, 1.0 + self.jitter
        return max(0.0, raw * rng.uniform(low, high))
