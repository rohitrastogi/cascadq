"""Top-level Broker class: lifecycle, startup/shutdown, public API."""

from __future__ import annotations

import logging
import socket
from collections.abc import Callable
from time import time
from uuid import uuid4

from cascadq.broker.compaction import CompactionWorker
from cascadq.broker.flush import FlushCoordinator, _queue_key
from cascadq.broker.heartbeat import HeartbeatWorker
from cascadq.broker.queue_state import QueueState
from cascadq.config import BrokerConfig
from cascadq.errors import (
    BrokerFencedError,
    ConflictError,
    QueueAlreadyExistsError,
    QueueNotFoundError,
)
from cascadq.models import (
    BrokerInfo,
    QueueFile,
    QueueMetadata,
    Task,
    deserialize_queue_file,
    serialize_broker_info,
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
        """Start the broker: write identity, discover queues, start background tasks."""
        info = BrokerInfo(
            broker_id=self._broker_id,
            host=socket.gethostname(),
            started_at=self._clock(),
        )
        await self._store.write_new(
            f"{self._prefix}broker.json", serialize_broker_info(info)
        )

        # Discover existing queues
        keys = await self._store.list_prefix(f"{self._prefix}queues/")
        for key in keys:
            name = key.removeprefix(f"{self._prefix}queues/").removesuffix(
                ".json"
            )
            data, version = await self._store.read(key)
            qf = deserialize_queue_file(data)
            state = QueueState(name=name, queue_file=qf, version=version)
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
        if self.is_fenced:
            raise BrokerFencedError("broker has been fenced by another instance")

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
        key = _queue_key(self._prefix, name)
        try:
            version = await self._store.write_new(key, serialize_queue_file(qf))
        except ConflictError as e:
            raise QueueAlreadyExistsError(
                f"queue {name!r} already exists"
            ) from e
        state = QueueState(name=name, queue_file=qf, version=version)
        self._queue_states[name] = state
        logger.info("Created queue %s", name)

    async def delete_queue(self, name: str) -> None:
        """Delete a queue. Bypasses the flush loop (single atomic delete)."""
        self._check_fenced()
        if name not in self._queue_states:
            raise QueueNotFoundError(f"queue {name!r} not found")
        key = _queue_key(self._prefix, name)
        await self._store.delete(key)
        del self._queue_states[name]
        logger.info("Deleted queue %s", name)

    async def push(self, queue_name: str, payload: dict) -> str:
        """Push a task to a queue. Blocks until the mutation is flushed.

        Returns the task_id assigned to the new task.
        """
        self._check_fenced()
        state = self._get_state(queue_name)
        task_id = uuid4().hex
        now = self._clock()
        waiter = state.push(task_id, payload, now)
        self._coordinator.notify()
        await waiter.wait()
        return task_id

    async def claim(self, queue_name: str, consumer_id: str) -> Task:
        """Claim the next pending task. Blocks until the mutation is flushed."""
        self._check_fenced()
        state = self._get_state(queue_name)
        now = self._clock()
        task, waiter = state.claim(consumer_id, now)
        self._coordinator.notify()
        await waiter.wait()
        return task

    async def heartbeat(self, queue_name: str, task_id: str) -> None:
        """Update heartbeat for a claimed task. Blocks until flushed."""
        self._check_fenced()
        state = self._get_state(queue_name)
        now = self._clock()
        waiter = state.heartbeat(task_id, now)
        self._coordinator.notify()
        await waiter.wait()

    async def finish(self, queue_name: str, task_id: str) -> None:
        """Mark a claimed task as completed. Blocks until flushed."""
        self._check_fenced()
        state = self._get_state(queue_name)
        waiter = state.finish(task_id)
        self._coordinator.notify()
        await waiter.wait()
