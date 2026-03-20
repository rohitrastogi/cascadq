"""In-memory object store for testing and development."""

from __future__ import annotations

from cascadq.errors import ConflictError
from cascadq.storage.protocol import VersionToken


class InMemoryObjectStore:
    """Dict-backed object store with version counter per key for CAS."""

    def __init__(self) -> None:
        self._store: dict[str, tuple[bytes, int]] = {}
        self._conflict_keys: set[str] = set()

    async def read(self, key: str) -> tuple[bytes, VersionToken]:
        if key not in self._store:
            raise KeyError(key)
        data, version = self._store[key]
        return data, version

    async def write(
        self, key: str, data: bytes, expected_version: VersionToken
    ) -> None:
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
        self._store[key] = (data, current_version + 1)

    async def write_new(self, key: str, data: bytes) -> None:
        if key in self._store:
            raise ConflictError(f"key already exists: {key!r}")
        self._store[key] = (data, 1)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

    async def list_prefix(self, prefix: str) -> list[str]:
        return [k for k in self._store if k.startswith(prefix)]

    def inject_conflict(self, key: str) -> None:
        """Mark a key so the next write() raises ConflictError.

        Useful for testing broker fencing without two real brokers.
        """
        self._conflict_keys.add(key)
