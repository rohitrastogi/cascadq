"""In-memory object store for testing and development."""

from __future__ import annotations

import asyncio

from cascadq.errors import ConflictError
from cascadq.storage.protocol import VersionToken


class InMemoryObjectStore:
    """Dict-backed object store with version counter per key for CAS."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[bytes, int]] = {}
        self._conflict_keys: set[str] = set()
        self._error_keys: dict[str, int] = {}
        self._write_delays: dict[str, float] = {}

    async def read(self, key: str) -> tuple[bytes, VersionToken]:
        if key not in self._store:
            raise KeyError(key)
        data, version = self._store[key]
        return data, version

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
        if key not in self._store:
            raise KeyError(key)
        _, current_version = self._store[key]
        if current_version != expected_version:
            raise ConflictError(
                f"version mismatch for key={key!r}: "
                f"expected={expected_version}, current={current_version}"
            )
        new_version = current_version + 1
        self._store[key] = (data, new_version)
        return new_version

    async def write_new(self, key: str, data: bytes) -> VersionToken:
        if key in self._store:
            raise ConflictError(f"key already exists: {key!r}")
        self._store[key] = (data, 1)
        return 1

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def list_prefix(self, prefix: str) -> list[str]:
        return [k for k in self._store if k.startswith(prefix)]

    def inject_conflict(self, key: str) -> None:
        """Mark a key so the next write() raises ConflictError.

        Useful for testing broker fencing without two real brokers.
        """
        self._conflict_keys.add(key)

    def inject_transient_error(self, key: str, count: int = 1) -> None:
        """Make the next *count* write() calls for *key* raise OSError.

        Useful for testing the flush coordinator's transient-failure path.
        """
        self._error_keys[key] = count

    def inject_write_delay(self, key: str, delay_seconds: float) -> None:
        """Make the next write() for *key* sleep before executing.

        The delay is consumed on first use. Useful for testing client
        timeout + retry idempotency.
        """
        self._write_delays[key] = delay_seconds
