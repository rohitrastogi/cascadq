"""Top-level Broker class: lifecycle, startup/shutdown, public API."""

from __future__ import annotations

import logging
from collections.abc import Callable
from time import time
from uuid import uuid4

from cascadq.broker import queue_key
from cascadq.broker.compaction import CompactionWorker
from cascadq.broker.flush import FlushCoordinator
from cascadq.broker.heartbeat import HeartbeatWorker
from cascadq.broker.queue_state import QueueState
from cascadq.config import BrokerConfig
from cascadq.errors import (
    ConflictError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
)
from cascadq.models import (
    QueueFile,
    QueueMetadata,
    Task,
    deserialize_queue_file,
    serialize_queue_file,
)
from cascadq.storage.protocol import ObjectStore

logger = logging.getLogger(__name__)


class Broker:
    """Top-level broker orchestrator.

    Manages queue state, the flush loop, compaction, and heartbeat
    detection. Provides the public API consumed by HTTP routes.
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
        self._queue_states: dict[str, QueueState] = {}
        self._coordinator: FlushCoordinator | None = None
        self._compaction: CompactionWorker | None = None
        self._heartbeat: HeartbeatWorker | None = None

    @property
    def is_fenced(self) -> bool:
        return self._coordinator is not None and self._coordinator.is_fenced

    async def start(self) -> None:
        """Start the broker: discover queues, start background tasks."""
        # Discover existing queues
        keys = await self._store.list_prefix(f"{self._prefix}queues/")
        for key in keys:
            name = key.removeprefix(f"{self._prefix}queues/").removesuffix(
                ".json"
            )
            data, version = await self._store.read(key)
            qf = deserialize_queue_file(data)
            state = QueueState(
                name=name, queue_file=qf, version=version,
                idempotency_ttl_seconds=self._config.idempotency_ttl_seconds,
            )
            self._queue_states[name] = state
            logger.info("Discovered queue %s with %d tasks", name, len(qf.tasks))

        self._coordinator = FlushCoordinator(
            store=self._store,
            prefix=self._prefix,
            queue_states=self._queue_states,
            max_consecutive_failures=self._config.max_consecutive_flush_failures,
            retry_delay_seconds=self._config.flush_retry_delay_seconds,
        )
        self._compaction = CompactionWorker(
            self._queue_states,
            self._coordinator,
            self._config.compaction_interval_seconds,
            clock=self._clock,
        )
        self._heartbeat = HeartbeatWorker(
            self._queue_states,
            self._coordinator,
            self._config.heartbeat_timeout_seconds,
            self._config.heartbeat_check_interval_seconds,
            self._clock,
        )
        self._coordinator.start()
        self._compaction.start()
        self._heartbeat.start()
        logger.info("Broker %s started", self._broker_id)

    async def stop(self) -> None:
        """Stop all background tasks."""
        if self._heartbeat:
            await self._heartbeat.stop()
        if self._compaction:
            await self._compaction.stop()
        if self._coordinator:
            await self._coordinator.stop()
        logger.info("Broker %s stopped", self._broker_id)

    def _check_fenced(self) -> None:
        if self._coordinator is not None and self._coordinator.is_fenced:
            raise self._coordinator.shutdown_error  # type: ignore[misc]

    def _get_coordinator(self) -> FlushCoordinator:
        if self._coordinator is None:
            raise RuntimeError("broker has not been started")
        return self._coordinator

    def _get_state(self, queue_name: str) -> QueueState:
        state = self._queue_states.get(queue_name)
        if state is None:
            raise QueueNotFoundError(f"queue {queue_name!r} not found")
        return state

    async def create_queue(
        self, name: str, payload_schema: dict | None = None
    ) -> None:
        """Create a new queue. Bypasses the flush loop (single atomic write)."""
        self._check_fenced()
        schema = payload_schema or {}
        qf = QueueFile(
            metadata=QueueMetadata(
                created_at=self._clock(), payload_schema=schema
            ),
        )
        key = queue_key(self._prefix, name)
        try:
            version = await self._store.write_new(key, serialize_queue_file(qf))
        except ConflictError as e:
            raise QueueAlreadyExistsError(
                f"queue {name!r} already exists"
            ) from e
        state = QueueState(
            name=name, queue_file=qf, version=version,
            idempotency_ttl_seconds=self._config.idempotency_ttl_seconds,
        )
        self._queue_states[name] = state
        logger.info("Created queue %s", name)

    async def delete_queue(self, name: str) -> None:
        """Delete a queue. Bypasses the flush loop (single atomic delete)."""
        self._check_fenced()
        if name not in self._queue_states:
            raise QueueNotFoundError(f"queue {name!r} not found")
        key = queue_key(self._prefix, name)
        await self._store.delete(key)
        del self._queue_states[name]
        logger.info("Deleted queue %s", name)

    async def push(
        self,
        queue_name: str,
        payload: dict,
        idempotency_key: str,
    ) -> None:
        """Push a task to a queue. Blocks until the mutation is flushed."""
        self._check_fenced()
        state = self._get_state(queue_name)
        task_id = uuid4().hex
        now = self._clock()
        waiter = state.push(task_id, payload, now, idempotency_key)
        self._get_coordinator().notify()
        await waiter.wait()

    async def claim(
        self,
        queue_name: str,
        idempotency_key: str,
        timeout_seconds: float | None = None,
    ) -> Task:
        """Claim the next pending task.

        Blocks until a task is available (up to *timeout_seconds*).
        If *timeout_seconds* expires with no task, raises QueueEmptyError.
        Without a timeout, blocks indefinitely.
        """
        self._check_fenced()
        state = self._get_state(queue_name)
        deadline = (
            self._clock() + timeout_seconds
            if timeout_seconds is not None
            else None
        )
        while True:
            now = self._clock()
            try:
                task, waiter = state.claim(idempotency_key)
                self._get_coordinator().notify()
                await waiter.wait()
                state.activate_lease(task.task_id, self._clock())
                return task
            except QueueEmptyError as empty:
                remaining = None
                if deadline is not None:
                    remaining = deadline - now
                    if remaining <= 0:
                        raise
                try:
                    await state.wait_for_push(remaining)
                except TimeoutError:
                    raise empty from None
                self._check_fenced()

    async def heartbeat(self, queue_name: str, task_id: str) -> None:
        """Update heartbeat for a claimed task.

        Returns immediately after the in-memory update. The updated
        timestamp is flushed to storage as part of the next normal
        flush cycle for recovery, but does not block the RPC.
        """
        self._check_fenced()
        state = self._get_state(queue_name)
        now = self._clock()
        state.heartbeat(task_id, now)
        self._get_coordinator().notify()

    async def finish(
        self,
        queue_name: str,
        task_id: str,
        sequence: int,
    ) -> None:
        """Mark a claimed task as completed. Blocks until flushed."""
        self._check_fenced()
        state = self._get_state(queue_name)
        waiter = state.finish(task_id, sequence)
        self._get_coordinator().notify()
        await waiter.wait()
