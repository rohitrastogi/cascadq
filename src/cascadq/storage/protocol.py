"""Object storage protocol for CAScadq."""

from __future__ import annotations

from typing import Protocol

VersionToken = str | int


class ObjectStore(Protocol):
    """Abstract interface over object storage with CAS support."""

    async def read(self, key: str) -> tuple[bytes, VersionToken]:
        """Read an object. Returns its data and an opaque version token."""
        ...

    async def write(
        self, key: str, data: bytes, expected_version: VersionToken
    ) -> VersionToken:
        """Write an object conditionally. Returns the new version token.

        Raises ConflictError if the version token doesn't match
        the current version (another writer intervened).
        """
        ...

    async def write_new(self, key: str, data: bytes) -> VersionToken:
        """Write an object that must not already exist. Returns the version token.

        Raises ConflictError if it does.
        """
        ...

    async def delete(self, key: str) -> None:
        """Delete an object."""
        ...

    async def list_prefix(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        ...
