"""Background completed-task cleanup."""

from __future__ import annotations

import asyncio
import logging

from cascadq.broker.flush import FlushCoordinator
from cascadq.broker.queue_state import QueueState

logger = logging.getLogger(__name__)


class CompactionWorker:
    """Periodically removes completed tasks from queue state."""

    def __init__(
        self,
        queue_states: dict[str, QueueState],
        coordinator: FlushCoordinator,
        interval_seconds: float,
    ) -> None:
        self._queue_states = queue_states
        self._coordinator = coordinator
        self._interval = interval_seconds
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
            any_compacted = False
            for state in self._queue_states.values():
                before = state.is_dirty
                state.compact()
                if not before and state.is_dirty:
                    any_compacted = True
            if any_compacted:
                self._coordinator.notify()
