"""Background lease timeout detection."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import uuid4

from cascadq.broker.flusher import Flusher


class LeaseChecker:
    """Periodically scans for expired leases and re-queues them."""

    def __init__(
        self,
        flushers: dict[str, Flusher],
        timeout_seconds: float,
        check_interval_seconds: float,
        clock: Callable[[], float],
    ) -> None:
        self._flushers = flushers
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
            now = self._clock()
            for flusher in self._flushers.values():
                if not flusher.is_healthy:
                    continue
                flusher.requeue_expired(
                    now=now,
                    timeout_seconds=self._timeout,
                    next_task_id_fn=lambda: uuid4().hex,
                )
