"""Queue-local flush loop and CAS write coordination."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from enum import StrEnum

from cascadq import metrics
from cascadq.broker import queue_key
from cascadq.broker.queue_state import ClaimResult, FlushWaiter, QueueState
from cascadq.errors import (
    BrokerFencedError,
    CascadqError,
    ConflictError,
    FlushExhaustedError,
)
from cascadq.models import TaskStatus
from cascadq.queue_codec import decode_queue_file, encode_queue_file
from cascadq.storage.protocol import ObjectStore

logger = logging.getLogger(__name__)


class FlusherStatus(StrEnum):
    healthy = "healthy"
    recovering = "recovering"
    fenced = "fenced"


class QueueFlusher:
    """Queue-local durability owner for a single queue.

    All mutations go through this class so that ``_notify()`` is
    called automatically.  Callers never need to remember to wake
    the flush loop.

    Three states:
      - **healthy**: serving requests, flush loop running.
      - **recovering**: flush exhaustion occurred, background task
        periodically tries to reload from durable state. Requests
        get ``FlushExhaustedError`` until recovery succeeds.
      - **fenced**: CAS conflict detected, terminal. Another writer
        owns this queue. No automatic recovery.
    """

    def __init__(
        self,
        store: ObjectStore,
        prefix: str,
        state: QueueState,
        max_consecutive_failures: int = 3,
        retry_delay_seconds: float = 1.0,
        recovery_interval_seconds: float = 5.0,
        idempotency_ttl_seconds: float = 300.0,
        compress_snapshots: bool = False,
    ) -> None:
        self._store = store
        self._prefix = prefix
        self._state = state
        self._max_consecutive_failures = max_consecutive_failures
        self._retry_delay = retry_delay_seconds
        self._recovery_interval = recovery_interval_seconds
        self._idempotency_ttl = idempotency_ttl_seconds
        self._compress_snapshots = compress_snapshots
        self._consecutive_failures = 0
        self._status = FlusherStatus.healthy
        self._shutdown_error: CascadqError | None = None
        self._task: asyncio.Task[None] | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._flush_event = asyncio.Event()

    # -- Properties ------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._state.name

    @property
    def status(self) -> FlusherStatus:
        return self._status

    @property
    def is_fenced(self) -> bool:
        return self._status == FlusherStatus.fenced

    @property
    def is_recovering(self) -> bool:
        return self._status == FlusherStatus.recovering

    @property
    def is_healthy(self) -> bool:
        return self._status == FlusherStatus.healthy

    @property
    def shutdown_error(self) -> CascadqError | None:
        return self._shutdown_error

    def ensure_healthy(self) -> None:
        """Raise if the queue is not in healthy state."""
        if self._status != FlusherStatus.healthy:
            error = self._shutdown_error
            assert error is not None
            raise error

    # -- Lifecycle -------------------------------------------------------------

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._recovery_task is not None:
            self._recovery_task.cancel()
            try:
                await self._recovery_task
            except asyncio.CancelledError:
                pass
            self._recovery_task = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # -- Mutation API (delegates to QueueState + auto-notifies) ----------------

    def push(
        self,
        task_id: str,
        payload: dict,
        now: float,
        idempotency_key: str,
    ) -> FlushWaiter:
        """Push a task and schedule a flush."""
        waiter = self._state.push(task_id, payload, now, idempotency_key)
        metrics.tasks_pushed_total.labels(queue=self.name).inc()
        metrics.queue_pending_tasks.labels(queue=self.name).inc()
        self._notify()
        return waiter

    def claim(self, now: float, idempotency_key: str) -> ClaimResult:
        """Claim the next pending task and schedule a flush.

        The lease timestamp is set immediately so it's durable in the
        same flush.  The task is pending delivery until
        ``confirm_delivery`` is called.
        """
        result = self._state.claim(now, idempotency_key)
        if result.mutated:
            metrics.tasks_claimed_total.labels(queue=self.name).inc()
            metrics.queue_dwell_seconds.labels(queue=self.name).observe(
                now - result.task.created_at,
            )
            metrics.queue_pending_tasks.labels(queue=self.name).dec()
            metrics.queue_claimed_tasks.labels(queue=self.name).inc()
        self._notify()
        return result

    def confirm_delivery(self, task_id: str) -> None:
        """Mark a claim as delivered — makes it timeout-eligible."""
        self._state.confirm_delivery(task_id)

    def heartbeat(self, task_id: str, now: float) -> None:
        """Renew the heartbeat lease (fire-and-forget, no waiter)."""
        self._state.heartbeat(task_id, now)
        self._notify()

    def finish(self, task_id: str, sequence: int) -> FlushWaiter:
        """Mark a task as completed and schedule a flush."""
        result = self._state.finish(task_id, sequence)
        if result.mutated:
            metrics.queue_claimed_tasks.labels(queue=self.name).dec()
        self._notify()
        return result.waiter

    async def wait_for_push(self, timeout: float | None) -> None:
        """Block until a push or re-queue signals new pending work."""
        await self._state.wait_for_push(timeout)

    def compact(self, now: float) -> None:
        """Remove completed tasks and expired idempotency keys."""
        before_dirty = self._state.is_dirty
        removed = self._state.compact(now)
        if removed > 0:
            metrics.compaction_tasks_removed_total.labels(
                queue=self.name,
            ).inc(removed)
        if not before_dirty and self._state.is_dirty:

            self._notify()

    def timeout_expired_claims(
        self,
        now: float,
        timeout_seconds: float,
        next_task_id_fn: Callable[[], str],
    ) -> None:
        """Re-queue tasks whose heartbeat has expired."""
        before_dirty = self._state.is_dirty
        requeued = self._state.timeout_expired_claims(
            now, timeout_seconds, next_task_id_fn,
        )
        if requeued > 0:
            metrics.tasks_requeued_total.labels(queue=self.name).inc(requeued)
            metrics.queue_claimed_tasks.labels(queue=self.name).dec(requeued)
            metrics.queue_pending_tasks.labels(queue=self.name).inc(requeued)
        if not before_dirty and self._state.is_dirty:
            self._notify()

    # -- Flush loop ------------------------------------------------------------

    def _notify(self) -> None:
        self._flush_event.set()

    async def _run(self) -> None:
        while True:
            await self._flush_event.wait()
            self._flush_event.clear()
            if self._status != FlusherStatus.healthy:
                return
            await self._flush_once()

    async def _flush_once(self) -> None:
        """Flush one queue's current dirty generation and waiter buffer."""
        t_start = time.monotonic()
        waiters = self._state.swap_write_buffer()
        if not self._state.is_dirty and not waiters:
            return

        if not self._state.is_dirty:
            for waiter in waiters:
                waiter.set_result()
            return

        generation = self._state.generation
        version = self._state.version
        data = encode_queue_file(
            self._state.snapshot(),
            compress=self._compress_snapshots,
        )

        key = queue_key(self._prefix, self._state.name)
        name = self._state.name
        try:
            next_version = await self._store.write(key, data, version)
            elapsed = time.monotonic() - t_start
            metrics.flush_duration_seconds.labels(queue=name).observe(elapsed)
            self._state.version = next_version
            self._state.acknowledge_flush(generation)
            self._consecutive_failures = 0
            for waiter in waiters:
                waiter.set_result()
            if elapsed > 0.5:
                logger.info(
                    "Flush slow for queue %s: %d waiters, %.0fms",
                    name, len(waiters), elapsed * 1000,
                )
            if self._state.is_dirty or self._state.has_pending_waiters:
                self._flush_event.set()
        except ConflictError as exc:
            # A ConflictError means another broker wrote to this queue's
            # S3 object with a different ETag.  This broker's in-memory
            # state is stale; fencing is terminal.
            #
            # Production scenarios where this happens:
            #   1. Network partition — orchestrator thinks this pod is
            #      dead and starts a replacement on another node, but
            #      this pod is still running.  Both write; one loses.
            #   2. Slow shutdown — old pod hasn't finished draining
            #      before the new pod starts writing.
            #   3. Operator error — two brokers pointed at the same
            #      S3 prefix.
            #
            # In all cases the correct response is to stop writing.
            # CAS already prevented corruption; fencing ensures this
            # broker doesn't keep competing.  The readyz probe returns
            # 503 so the orchestrator can kill and replace this pod.
            #
            # Deploy with a "recreate" strategy (kill old, start new)
            # to avoid routine CAS conflicts during restarts.
            metrics.flush_errors_total.labels(queue=name, error="conflict").inc()
            logger.error(
                "CAS conflict detected for queue %s — queue is fenced: %s",
                name, exc,
            )
            self._fence(
                BrokerFencedError("queue has been fenced by another instance"),
                waiters,
            )
        except Exception as exc:
            metrics.flush_errors_total.labels(queue=name, error="transient").inc()
            self._consecutive_failures += 1
            logger.warning(
                "Flush failure for queue %s %d/%d: %s",
                name,
                self._consecutive_failures,
                self._max_consecutive_failures,
                exc,
            )
            self._state.prepend_waiters(waiters)
            if self._consecutive_failures >= self._max_consecutive_failures:
                logger.error(
                    "Max consecutive flush failures reached for queue %s, "
                    "entering recovery",
                    self._state.name,
                )
                self._enter_recovering(
                    FlushExhaustedError(
                        "flush retries exhausted, queue cannot persist state"
                    ),
                    waiters,
                )
                return
            await asyncio.sleep(self._retry_delay)
            self._flush_event.set()

    # -- State transitions -----------------------------------------------------

    def _fence(
        self, error: CascadqError, in_flight_waiters: list[FlushWaiter],
    ) -> None:
        """Enter terminal fenced state — no automatic recovery."""
        self._status = FlusherStatus.fenced
        self._shutdown_error = error
        metrics.set_queue_status(self.name, self._status)
        self._fail_waiters(in_flight_waiters)

    def _enter_recovering(
        self, error: CascadqError, in_flight_waiters: list[FlushWaiter],
    ) -> None:
        """Enter recovering state — background task will try to reload."""
        self._status = FlusherStatus.recovering
        self._shutdown_error = error
        metrics.set_queue_status(self.name, self._status)
        self._fail_waiters(in_flight_waiters)
        self._recovery_task = asyncio.create_task(self._recovery_loop())

    def _fail_waiters(self, in_flight_waiters: list[FlushWaiter]) -> None:
        error = self._shutdown_error
        assert error is not None
        for waiter in in_flight_waiters:
            waiter.set_error(error)
        for waiter in self._state.swap_write_buffer():
            waiter.set_error(error)
        self._state.wake_blocked_claims()

    # -- Background recovery ---------------------------------------------------

    async def _recovery_loop(self) -> None:
        """Periodically try to reload from durable state."""
        while self._status == FlusherStatus.recovering:
            await asyncio.sleep(self._recovery_interval)
            if self._status != FlusherStatus.recovering:
                return
            try:
                await self._try_recover()
                return
            except Exception as exc:
                metrics.recovery_events_total.labels(
                    queue=self._state.name, outcome="failed",
                ).inc()
                logger.warning(
                    "Recovery attempt failed for queue %s: %s",
                    self._state.name,
                    exc,
                )

    async def _try_recover(self) -> None:
        """Reload queue state from durable storage and resume flushing.

        Compares the durable version against the last successfully
        flushed version.  If they differ, another writer intervened
        and the queue transitions to fenced instead of recovering.
        """
        name = self._state.name
        expected_version = self._state.version
        key = queue_key(self._prefix, name)
        data, version = await self._store.read(key)
        if version != expected_version:
            metrics.recovery_events_total.labels(
                queue=name, outcome="fenced",
            ).inc()
            logger.error(
                "Version mismatch during recovery for queue %s: "
                "expected %s, found %s — fencing",
                name, expected_version, version,
            )
            self._fence(
                BrokerFencedError(
                    "queue version changed during recovery"
                ),
                [],
            )
            return
        queue_file = decode_queue_file(data)
        new_state = QueueState(
            name=name,
            queue_file=queue_file,
            version=version,
            idempotency_ttl_seconds=self._idempotency_ttl,
        )
        self._state = new_state
        self._status = FlusherStatus.healthy
        self._shutdown_error = None
        self._consecutive_failures = 0
        self._flush_event.clear()
        self.start()
        # Reset gauges from the reloaded state
        pending = sum(
            1 for t in queue_file.tasks if t.status == TaskStatus.pending
        )
        claimed = sum(
            1 for t in queue_file.tasks if t.status == TaskStatus.claimed
        )
        metrics.queue_pending_tasks.labels(queue=name).set(pending)
        metrics.queue_claimed_tasks.labels(queue=name).set(claimed)
        metrics.set_queue_status(name, self._status)
        metrics.recovery_events_total.labels(queue=name, outcome="succeeded").inc()
        logger.info("Queue %s recovered from flush exhaustion", name)
