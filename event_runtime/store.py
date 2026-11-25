"""Versioned keyed state store with compare-and-swap."""

from __future__ import annotations

import threading
from typing import Any

from .events import Event


class StaleVersionError(RuntimeError):
    """Raised when an event's version is not newer than the stored one."""


class StateStore:
    """Holds the latest committed payload and version for each key.

    Every commit is a compare-and-swap on the version: a delayed retry of an
    old event can never overwrite a state produced by a newer event.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._versions: dict[str, int] = {}
        self._state: dict[str, Any] = {}

    def version(self, key: str) -> int:
        """Highest *committed* version for a key."""
        with self._lock:
            return self._versions.get(key, 0)

    def commit(self, event: Event) -> None:
        with self._lock:
            current = self._versions.get(event.key, 0)
            if event.version <= current:
                # Stale retry / redelivery: the newer state must win.
                raise StaleVersionError(
                    f"stale v{event.version} for {event.tenant}/{event.key}, "
                    f"store already at v{current}"
                )
            self._versions[event.key] = event.version
            self._state[event.key] = event.payload

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {
                key: {"version": self._versions[key], "payload": self._state[key]}
                for key in self._versions
            }
