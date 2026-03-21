"""Flush coordination: write buffer, generation tracking, and waiters."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


class FlushWaiter:
    """A waiter that blocks a client request until the next successful flush.

    The flush loop resolves it by calling set_result() or set_error().
    Callers check the result via the event and error slot.
    """

    __slots__ = ("_error", "_event")

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._error: Exception | None = None

    def set_result(self) -> None:
        self._event.set()

    def set_error(self, error: Exception) -> None:
        self._error = error
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()
        if self._error is not None:
            raise self._error


@dataclass(frozen=True, slots=True)
class FlushBatch:
    """Snapshot of the write buffer at the start of a flush attempt."""

    waiters: list[FlushWaiter]
    generation: int
    is_dirty: bool


class FlushBuffer:
    """Coordinates flush lifecycle: write buffer, generation tracking, waiters.

    Separates flush coordination from domain state so that QueueState
    can be pure domain logic.
    """

    def __init__(self) -> None:
        self._write_buffer: list[FlushWaiter] = []
        self._generation = 0
        self._flushed_generation = 0

    @property
    def needs_flush(self) -> bool:
        return (
            self._generation != self._flushed_generation
            or len(self._write_buffer) > 0
        )

    @property
    def generation(self) -> int:
        return self._generation

    def record_mutation(self) -> FlushWaiter:
        """Record a state-changing mutation: mark dirty and append waiter."""
        self.mark_dirty()
        waiter = FlushWaiter()
        self._write_buffer.append(waiter)
        return waiter

    def record_waiter(self) -> FlushWaiter:
        """Record a waiter without marking dirty (idempotent replays)."""
        waiter = FlushWaiter()
        self._write_buffer.append(waiter)
        return waiter

    def mark_dirty(self) -> None:
        """Mark the buffer as dirty without creating a waiter (fire-and-forget)."""
        self._generation += 1

    def begin_flush(self) -> FlushBatch:
        """Atomically swap the write buffer and snapshot the current state."""
        waiters = self._write_buffer
        self._write_buffer = []
        return FlushBatch(
            waiters=waiters,
            generation=self._generation,
            is_dirty=self._generation != self._flushed_generation,
        )

    def complete_flush(self, batch: FlushBatch) -> None:
        """Resolve all waiters in the batch and advance flushed generation."""
        for waiter in batch.waiters:
            waiter.set_result()
        assert batch.generation >= self._flushed_generation
        self._flushed_generation = batch.generation

    def fail_flush(self, batch: FlushBatch) -> None:
        """Re-queue waiters after a transient flush failure."""
        self._write_buffer = batch.waiters + self._write_buffer

    def reject_all(self, error: Exception) -> None:
        """Fail all buffered waiters and drain the buffer (fencing/recovery)."""
        for waiter in self._write_buffer:
            waiter.set_error(error)
        self._write_buffer = []
