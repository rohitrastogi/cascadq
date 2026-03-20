"""Background heartbeat timeout detection."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import uuid4

from cascadq.broker.queue_flusher import QueueFlusher


class HeartbeatWorker:
    """Periodically scans for expired claims and re-queues them."""

    def __init__(
        self,
        queue_flushers: dict[str, QueueFlusher],
        timeout_seconds: float,
        check_interval_seconds: float,
        clock: Callable[[], float],
    ) -> None:
        self._queue_flushers = queue_flushers
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
            for flusher in self._queue_flushers.values():
                if not flusher.is_healthy:
                    continue
                flusher.timeout_expired_claims(
                    now=now,
                    timeout_seconds=self._timeout,
                    next_task_id_fn=lambda: uuid4().hex,
                )
