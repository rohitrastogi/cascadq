"""Tests for hedged write tail-latency mitigation."""

import pytest

from cascadq.errors import ConflictError
from cascadq.storage.hedging import hedged_write
from tests.support import FaultInjectingStore


class TestHedgedWrite:
    async def test_fast_primary_no_hedge(self) -> None:
        """When the primary completes before the hedge threshold,
        no hedge is fired and the result is returned directly."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")

        result = await hedged_write(store.write, "k", b"v2", 1, hedge_after=1.0)

        assert result == 2
        data, version = await store.read("k")
        assert data == b"v2"
        assert version == 2

    async def test_slow_primary_hedge_wins(self) -> None:
        """When the primary is slow, the hedge fires and wins.
        The primary's self-inflicted CAS conflict is suppressed."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")
        store.inject_write_delay("k", 0.5)

        result = await hedged_write(store.write, "k", b"v2", 1, hedge_after=0.1)

        data, version = await store.read("k")
        assert data == b"v2"
        assert version == result

    async def test_real_conflict_propagates(self) -> None:
        """When the version is wrong, both writes fail with CAS conflict
        and the error propagates — it's a real conflict, not a self-conflict."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")
        store.inject_write_delay("k", 0.2)

        with pytest.raises(ConflictError):
            await hedged_write(store.write, "k", b"v2", 999, hedge_after=0.05)

    async def test_returns_correct_version_after_hedge(self) -> None:
        """The returned version token must match the store's current
        version so the next CAS write succeeds."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")
        store.inject_write_delay("k", 0.5)

        version = await hedged_write(store.write, "k", b"v2", 1, hedge_after=0.1)

        # Use the returned version for a follow-up write — must succeed
        next_version = await store.write("k", b"v3", version)
        assert next_version == version + 1
