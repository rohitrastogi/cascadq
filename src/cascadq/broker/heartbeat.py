"""Background heartbeat timeout detection."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from uuid import uuid4

from cascadq.broker.flush import FlushCoordinator
from cascadq.broker.queue_state import QueueState

logger = logging.getLogger(__name__)


class HeartbeatWorker:
    """Periodically scans for expired claims and re-queues them."""

    def __init__(
        self,
        queue_states: dict[str, QueueState],
        coordinator: FlushCoordinator,
        timeout_seconds: float,
        check_interval_seconds: float,
        clock: Callable[[], float],
    ) -> None:
        self._queue_states = queue_states
        self._coordinator = coordinator
        self._timeout = timeout_seconds
        self._interval = check_interval_seconds
        self._clock = clock
        self._task: asyncio.Task[None] | None = None

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
            await asyncio.sleep(self._interval)
            if self._coordinator.is_fenced:
                return
            now = self._clock()
            any_expired = False
            for state in self._queue_states.values():
                before_dirty = state.is_dirty
                state.timeout_expired_claims(
                    now=now,
                    timeout_seconds=self._timeout,
                    next_task_id_fn=lambda: uuid4().hex,
                )
                if not before_dirty and state.is_dirty:
                    any_expired = True
            if any_expired:
                self._coordinator.notify()
