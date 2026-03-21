"""Top-level Broker class: lifecycle, startup/shutdown, public API."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from time import time
from uuid import uuid4

from cascadq import metrics
from cascadq.broker import queue_key
from cascadq.broker.compaction import CompactionWorker
from cascadq.broker.flusher import Flusher
from cascadq.broker.heartbeat import LeaseChecker
from cascadq.broker.queue import TaskQueue
from cascadq.config import BrokerConfig
from cascadq.errors import (
    ConflictError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
)
from cascadq.models import (
    FlusherStatus,
    QueueMeta,
    Snapshot,
    Task,
    deserialize_snapshot,
    serialize_snapshot,
)
from cascadq.storage.protocol import ObjectStore

logger = logging.getLogger(__name__)


class Broker:
    """Top-level broker orchestrator.

    Manages queue flushers, compaction, and heartbeat detection.
    All queue mutations go through the flusher so notify is automatic.
    """

    def __init__(
        self,
        store: ObjectStore,
        config: BrokerConfig | None = None,
        clock: Callable[[], float] = time,
    ) -> None:
        self._store = store
        self._config = config or BrokerConfig()
        self._clock = clock
        self._broker_id = uuid4().hex
        self._prefix = self._config.storage_prefix
        self._flushers: dict[str, Flusher] = {}
        self._compaction: CompactionWorker | None = None
        self._lease_checker: LeaseChecker | None = None
        self._started = False

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def is_fenced(self) -> bool:
        return any(f.is_fenced for f in self._flushers.values())

    async def start(self) -> None:
        """Start the broker: discover queues, start background tasks."""
        keys = await self._store.list_prefix(f"{self._prefix}queues/")
        reads = [self._store.read(key) for key in keys]
        results = await asyncio.gather(*reads)
        for key, (data, version) in zip(keys, results, strict=True):
            name = key.removeprefix(f"{self._prefix}queues/").removesuffix(
                ".json"
            )
            qf = deserialize_snapshot(data)
            state = TaskQueue(
                name=name, queue_file=qf, version=version,
                push_key_ttl_seconds=self._config.push_key_ttl_seconds,
            )
            self._flushers[name] = self._make_flusher(state)
            metrics.reset_queue_gauges(name, qf)
            metrics.set_queue_status(name, FlusherStatus.healthy)
            logger.info("Discovered queue %s with %d tasks", name, len(qf.tasks))

        self._compaction = CompactionWorker(
            self._flushers,
            self._config.compaction_interval_seconds,
            clock=self._clock,
        )
        self._lease_checker = LeaseChecker(
            self._flushers,
            self._config.heartbeat_timeout_seconds,
            self._config.heartbeat_check_interval_seconds,
            self._clock,
        )
        for flusher in self._flushers.values():
            flusher.start()
        self._compaction.start()
        self._lease_checker.start()
        self._started = True
        logger.info("Broker %s started", self._broker_id)

    async def stop(self) -> None:
        """Stop all background tasks."""
        self._started = False
        if self._lease_checker:
            await self._lease_checker.stop()
        if self._compaction:
            await self._compaction.stop()
        for flusher in self._flushers.values():
            await flusher.stop()
        logger.info("Broker %s stopped", self._broker_id)

    async def create_queue(
        self, name: str, payload_schema: dict | None = None
    ) -> None:
        """Create a new queue. Bypasses the flush loop (single atomic write)."""
        schema = payload_schema or {}
        qf = Snapshot(
            metadata=QueueMeta(
                created_at=self._clock(), payload_schema=schema
            ),
        )
        key = queue_key(self._prefix, name)
        try:
            version = await self._store.write_new(key, serialize_snapshot(qf))
        except ConflictError as e:
            raise QueueAlreadyExistsError(
                f"queue {name!r} already exists"
            ) from e
        state = TaskQueue(
            name=name, queue_file=qf, version=version,
            push_key_ttl_seconds=self._config.push_key_ttl_seconds,
        )
        flusher = self._make_flusher(state)
        self._flushers[name] = flusher
        flusher.start()
        logger.info("Created queue %s", name)

    async def delete_queue(self, name: str) -> None:
        """Delete a queue. Bypasses the flush loop (single atomic delete)."""
        flusher = self._flushers.pop(name, None)
        if flusher is None:
            raise QueueNotFoundError(f"queue {name!r} not found")
        await flusher.stop()
        key = queue_key(self._prefix, name)
        await self._store.delete(key)
        logger.info("Deleted queue %s", name)

    async def push(
        self,
        queue_name: str,
        payload: dict,
        push_key: str,
    ) -> None:
        """Push a task to a queue. Blocks until the mutation is flushed."""
        flusher = self._get_flusher(queue_name)
        task_id = uuid4().hex
        now = self._clock()
        waiter = flusher.push(task_id, payload, now, push_key)
        await waiter.wait()

    async def claim(
        self,
        queue_name: str,
        claim_key: str,
        timeout_seconds: float | None = None,
    ) -> Task:
        """Claim the next pending task.

        Blocks until a task is available (up to *timeout_seconds*).
        If *timeout_seconds* expires with no task, raises QueueEmptyError.
        Without a timeout, blocks indefinitely.
        """
        flusher = self._get_flusher(queue_name)
        deadline = (
            self._clock() + timeout_seconds
            if timeout_seconds is not None
            else None
        )
        # Loop because multiple consumers may be long-polling the same
        # queue.  A push wakes all of them, but only one wins the
        # claim — the rest get QueueEmptyError and must wait again.
        while True:
            now = self._clock()
            try:
                return await flusher.claim(now, claim_key)
            except QueueEmptyError as empty:
                remaining = None
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        raise
                try:
                    await flusher.wait_claimable(remaining)
                except TimeoutError:
                    raise empty from None
                flusher = self._get_flusher(queue_name)

    async def heartbeat(self, queue_name: str, task_id: str) -> None:
        """Update heartbeat for a claimed task.

        Returns immediately after the in-memory update. The updated
        timestamp is flushed to storage as part of the next normal
        flush cycle for recovery, but does not block the RPC.
        """
        flusher = self._get_flusher(queue_name)
        flusher.heartbeat(task_id, self._clock())

    async def finish(
        self,
        queue_name: str,
        task_id: str,
        sequence: int,
    ) -> None:
        """Mark a claimed task as completed. Blocks until flushed."""
        flusher = self._get_flusher(queue_name)
        waiter = flusher.finish(task_id, sequence)
        await waiter.wait()

    def _get_flusher(self, queue_name: str) -> Flusher:
        flusher = self._flushers.get(queue_name)
        if flusher is None:
            raise QueueNotFoundError(f"queue {queue_name!r} not found")
        flusher.ensure_healthy()
        return flusher

    def _make_flusher(self, state: TaskQueue) -> Flusher:
        return Flusher(
            store=self._store,
            prefix=self._prefix,
            state=state,
            max_consecutive_failures=self._config.max_consecutive_flush_failures,
            retry_delay_seconds=self._config.flush_retry_delay_seconds,
            recovery_interval_seconds=self._config.flush_recovery_interval_seconds,
            push_key_ttl_seconds=self._config.push_key_ttl_seconds,
        )
