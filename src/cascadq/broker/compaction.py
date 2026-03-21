"""Background completed-task cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from time import time

from cascadq.broker.flusher import Flusher


class CompactionWorker:
    """Periodically removes completed tasks from queue state."""

    def __init__(
        self,
        flushers: dict[str, Flusher],
        interval_seconds: float,
        clock: Callable[[], float] = time,
    ) -> None:
        self._flushers = flushers
        self._interval = interval_seconds
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
            now = self._clock()
            for flusher in self._flushers.values():
                if not flusher.is_healthy:
                    continue
                flusher.compact(now)
