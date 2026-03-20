"""Tests for hedged write tail-latency mitigation."""

import asyncio

import pytest

from cascadq.errors import ConflictError
from cascadq.storage.hedging import hedged_write
from cascadq.storage.protocol import VersionToken
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

    async def test_primary_conflict_does_not_cancel_hedge(self) -> None:
        """If the primary completes with a CAS conflict but the hedge
        hasn't finished yet, the hedge must not be cancelled — it may
        still succeed (the conflict was self-inflicted)."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")

        call_count = 0

        async def slow_then_fast(
            key: str, data: bytes, version: VersionToken,
        ) -> VersionToken:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Primary: sleep, then succeed (but by then hedge
                # already wrote, so this gets a CAS conflict)
                await asyncio.sleep(0.3)
                return await store.write(key, data, version)
            # Hedge: sleep briefly, then succeed
            await asyncio.sleep(0.05)
            return await store.write(key, data, version)

        result = await hedged_write(slow_then_fast, "k", b"v2", 1, hedge_after=0.1)

        data, version = await store.read("k")
        assert data == b"v2"
        assert version == result

    async def test_non_conflict_error_propagates(self) -> None:
        """A write failure that isn't a CAS conflict (e.g., network error)
        must propagate as the original exception, not an AssertionError."""
        store = FaultInjectingStore()
        await store.write_new("k", b"v1")
        # Both primary and hedge will hit transient errors
        store.inject_transient_error("k", count=2)
        store.inject_write_delay("k", 0.2)

        with pytest.raises(OSError, match="injected transient error"):
            await hedged_write(store.write, "k", b"v2", 1, hedge_after=0.05)
