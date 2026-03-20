"""Double-buffered flush loop and CAS write coordination."""

from __future__ import annotations

import asyncio
import logging

from cascadq.broker import queue_key
from cascadq.broker.queue_state import FlushWaiter, QueueState
from cascadq.errors import (
    BrokerFencedError,
    CascadqError,
    ConflictError,
    FlushExhaustedError,
)
from cascadq.models import serialize_queue_file
from cascadq.storage.protocol import ObjectStore, VersionToken

logger = logging.getLogger(__name__)


class FlushCoordinator:
    """Continuous flush loop that flushes all dirty queues concurrently.

    Owns the double-buffer swap: swaps all buffers upfront so that on
    failure, every waiter can be resolved with an error. CAS failure
    on ANY queue triggers global fencing.

    On transient (non-CAS) write failures, waiters are kept pending
    and the flush is retried internally. Clients are never told a
    mutation failed while the broker still intends to complete it.
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
        self._shutdown_error: CascadqError | None = None
        self._task: asyncio.Task[None] | None = None
        self._flush_event = asyncio.Event()

    @property
    def is_fenced(self) -> bool:
        return self._fenced

    @property
    def shutdown_error(self) -> CascadqError | None:
        """The error that caused the broker to stop, or None if healthy."""
        return self._shutdown_error

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
        On success all waiters are resolved. On CAS conflict the broker
        fences. On transient failure waiters are returned to their queues
        and the flush retries after a delay.
        """
        waiters_by_queue: dict[str, list[FlushWaiter]] = {}
        jobs: list[tuple[str, QueueState, bytes, VersionToken]] = []

        for state in self._queue_states.values():
            if not state.is_dirty and not state.has_pending_waiters:
                continue
            swapped = state.swap_write_buffer()
            if swapped:
                waiters_by_queue[state.name] = swapped
            if state.is_dirty:
                snapshot = state.snapshot()
                data = serialize_queue_file(snapshot)
                jobs.append((state.name, state, data, state.version))

        all_waiters = [
            w for ws in waiters_by_queue.values() for w in ws
        ]

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
            self._fence(
                BrokerFencedError("broker has been fenced by another instance"),
                all_waiters,
            )
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
                    "Max consecutive flush failures reached — broker stopping"
                )
                self._fence(
                    FlushExhaustedError(
                        "flush retries exhausted, broker cannot persist state"
                    ),
                    all_waiters,
                )
            else:
                # Keep waiters pending — put them back so the next
                # flush cycle re-collects and re-snapshots them.
                for name, waiters in waiters_by_queue.items():
                    self._queue_states[name].prepend_waiters(waiters)
                await asyncio.sleep(self._retry_delay)
                self._flush_event.set()

    def _fence(
        self, error: CascadqError, in_flight_waiters: list[FlushWaiter],
    ) -> None:
        """Enter terminal state: fail all in-flight and buffered waiters."""
        self._fenced = True
        self._shutdown_error = error
        for waiter in in_flight_waiters:
            waiter.set_error(error)
        # Also drain any waiters that accumulated during the flush
        for state in self._queue_states.values():
            for waiter in state.swap_write_buffer():
                waiter.set_error(error)
