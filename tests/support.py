"""Test support: object store with fault injection hooks."""

from __future__ import annotations

import asyncio
import socket

from cascadq.errors import ConflictError
from cascadq.storage.memory import InMemoryObjectStore
from cascadq.storage.protocol import VersionToken


def find_free_port() -> int:
    """Bind to an ephemeral port and return its number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class FaultInjectingStore(InMemoryObjectStore):
    """InMemoryObjectStore with fault injection for tests.

    Adds hooks to inject CAS conflicts, transient errors, and write
    delays on a per-key basis. Each injection is consumed on use.
    """

    def __init__(self) -> None:
        super().__init__()
        self._conflict_keys: set[str] = set()
        self._error_keys: dict[str, int] = {}
        self._write_delays: dict[str, float] = {}

    async def write(
        self, key: str, data: bytes, expected_version: VersionToken
    ) -> VersionToken:
        delay = self._write_delays.pop(key, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)
        remaining = self._error_keys.get(key, 0)
        if remaining > 0:
            if remaining == 1:
                del self._error_keys[key]
            else:
                self._error_keys[key] = remaining - 1
            raise OSError(f"injected transient error for key={key!r}")
        if key in self._conflict_keys:
            self._conflict_keys.discard(key)
            raise ConflictError(f"injected conflict for key={key!r}")
        return await super().write(key, data, expected_version)

    def inject_conflict(self, key: str) -> None:
        """Mark a key so the next write() raises ConflictError."""
        self._conflict_keys.add(key)

    def inject_transient_error(self, key: str, count: int = 1) -> None:
        """Make the next *count* write() calls for *key* raise OSError."""
        self._error_keys[key] = count

    def inject_write_delay(self, key: str, delay_seconds: float) -> None:
        """Make the next write() for *key* sleep before executing."""
        self._write_delays[key] = delay_seconds
