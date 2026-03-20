"""Double-buffered flush loop and CAS write coordination."""

from __future__ import annotations

import asyncio
import logging

from cascadq.broker import queue_key
from cascadq.broker.queue_state import FlushWaiter, QueueState
from cascadq.errors import BrokerFencedError, ConflictError
from cascadq.models import serialize_queue_file
from cascadq.storage.protocol import ObjectStore, VersionToken

logger = logging.getLogger(__name__)


class FlushCoordinator:
    """Continuous flush loop that flushes all dirty queues concurrently.

    Owns the double-buffer swap: swaps all buffers upfront so that on
    failure, every waiter can be resolved with an error. CAS failure
    on ANY queue triggers global fencing.

    Holds a reference to the broker's queue_states dict — no separate
    registration needed.
    """

    def __init__(
        self,
        store: ObjectStore,
        prefix: str,
        queue_states: dict[str, QueueState],
        max_consecutive_failures: int = 3,
        retry_delay_seconds: float = 1.0,
    ) -> None:
        self._store = store
        self._prefix = prefix
        self._queue_states = queue_states
        self._max_consecutive_failures = max_consecutive_failures
        self._retry_delay = retry_delay_seconds
        self._consecutive_failures = 0
        self._fenced = False
        self._task: asyncio.Task[None] | None = None
        self._flush_event = asyncio.Event()

    @property
    def is_fenced(self) -> bool:
        return self._fenced

    def notify(self) -> None:
        """Signal that there's dirty data to flush."""
        self._flush_event.set()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            await self._flush_event.wait()
            self._flush_event.clear()
            if self._fenced:
                return
            await self._flush_all()

    async def _flush_all(self) -> None:
        """Flush all dirty queues concurrently.

        Buffer swap happens upfront so the coordinator owns every waiter.
        If any CAS write fails, all waiters (in-flight + newly buffered)
        are failed and the broker enters fenced state.
        """
        all_waiters: list[FlushWaiter] = []
        jobs: list[tuple[str, QueueState, bytes, VersionToken]] = []

        for state in self._queue_states.values():
            if not state.is_dirty and not state.has_pending_waiters:
                continue
            all_waiters.extend(state.swap_write_buffer())
            if state.is_dirty:
                snapshot = state.snapshot()
                data = serialize_queue_file(snapshot)
                jobs.append((state.name, state, data, state.version))

        if not jobs:
            for waiter in all_waiters:
                waiter.set_result()
            return

        try:
            async with asyncio.TaskGroup() as tg:
                results: dict[str, asyncio.Task[VersionToken]] = {}
                for name, _state, data, version in jobs:
                    key = queue_key(self._prefix, name)
                    results[name] = tg.create_task(
                        self._store.write(key, data, version)
                    )
            # All succeeded — update versions, mark clean, resolve waiters
            for name, state, _data, _version in jobs:
                state.version = results[name].result()
                state.mark_clean()
            self._consecutive_failures = 0
            for waiter in all_waiters:
                waiter.set_result()
        except* ConflictError as eg:
            logger.error(
                "CAS conflict detected — broker is fenced: %s",
                eg.exceptions[0],
            )
            self._fence(all_waiters)
        except* Exception as eg:
            self._consecutive_failures += 1
            logger.warning(
                "Flush failure %d/%d: %s",
                self._consecutive_failures,
                self._max_consecutive_failures,
                eg.exceptions[0],
            )
            if self._consecutive_failures >= self._max_consecutive_failures:
                logger.error(
                    "Max consecutive flush failures reached, fencing broker"
                )
                self._fence(all_waiters)
            else:
                # Fail current waiters — their mutations are in-memory but
                # not persisted. They'll need to retry at the HTTP level.
                error = BrokerFencedError(
                    "flush failed, client should retry"
                )
                for waiter in all_waiters:
                    waiter.set_error(error)
                await asyncio.sleep(self._retry_delay)
                self._flush_event.set()

    def _fence(self, in_flight_waiters: list[FlushWaiter]) -> None:
        """Enter fenced state: fail all in-flight and buffered waiters."""
        self._fenced = True
        error = BrokerFencedError("broker has been fenced by another instance")
        for waiter in in_flight_waiters:
            waiter.set_error(error)
        # Also drain any waiters that accumulated during the flush
        for state in self._queue_states.values():
            for waiter in state.swap_write_buffer():
                waiter.set_error(error)
