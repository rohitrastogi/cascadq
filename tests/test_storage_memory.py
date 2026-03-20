"""Tests for InMemoryObjectStore CAS semantics."""

import pytest

from cascadq.errors import ConflictError
from cascadq.storage.memory import InMemoryObjectStore


@pytest.fixture
def store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


class TestCASSemantics:
    async def test_conditional_write_with_correct_version(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("k", b"v1")
        _, version = await store.read("k")
        await store.write("k", b"v2", version)
        data, _ = await store.read("k")
        assert data == b"v2"

    async def test_conditional_write_with_stale_version_raises(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("k", b"v1")
        _, stale = await store.read("k")
        await store.write("k", b"v2", stale)
        with pytest.raises(ConflictError):
            await store.write("k", b"v3", stale)

    async def test_write_new_collision_raises(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("k", b"v1")
        with pytest.raises(ConflictError):
            await store.write_new("k", b"v2")

    async def test_version_increments_on_each_write(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("k", b"v1")
        _, v1 = await store.read("k")
        await store.write("k", b"v2", v1)
        _, v2 = await store.read("k")
        assert v2 == v1 + 1

    async def test_inject_conflict_fires_once(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("k", b"v1")
        _, version = await store.read("k")
        store.inject_conflict("k")
        with pytest.raises(ConflictError, match="injected conflict"):
            await store.write("k", b"v2", version)
        # Conflict consumed — next write succeeds
        await store.write("k", b"v2", version)

    async def test_list_prefix_filters_correctly(
        self, store: InMemoryObjectStore
    ) -> None:
        await store.write_new("queues/a.json", b"")
        await store.write_new("queues/b.json", b"")
        await store.write_new("broker.json", b"")
        keys = await store.list_prefix("queues/")
        assert sorted(keys) == ["queues/a.json", "queues/b.json"]
