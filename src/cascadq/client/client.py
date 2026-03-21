"""Async reference client for CAScadq with retry and heartbeat support."""

from __future__ import annotations

import asyncio
import logging
import os
import random
from types import TracebackType
from typing import Any
from uuid import uuid4

import httpx

from cascadq.config import ClientConfig
from cascadq.errors import (
    BrokerFencedError,
    CascadqError,
    FlushExhaustedError,
    PayloadValidationError,
    QueueAlreadyExistsError,
    QueueNotFoundError,
    TaskNotClaimedError,
    TaskNotFoundError,
)

logger = logging.getLogger(__name__)

_TRACE_TASK_LIFECYCLE = os.environ.get("CASCADQ_TRACE_TASK_LIFECYCLE") == "1"


class CascadqClient:
    """Async client for the CAScadq broker HTTP API.

    Translates HTTP errors into domain exceptions:
      - 404 → QueueNotFoundError or TaskNotFoundError
      - 409 → QueueAlreadyExistsError or TaskNotClaimedError
      - 422 → PayloadValidationError
      - 503 → BrokerFencedError (after retries exhausted)
      - 5xx → retried with exponential backoff + jitter
      - Connection errors → retried with exponential backoff + jitter
    """

    def __init__(
        self,
        config: ClientConfig | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = config or ClientConfig()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            base_url=self._config.base_url
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def create_queue(
        self, name: str, payload_schema: dict | None = None
    ) -> None:
        """Create a new queue.

        Raises:
            QueueAlreadyExistsError: Queue with this name already exists.
            BrokerFencedError: Broker is fenced (after retries).
        """
        await self._request(
            "POST",
            "/queues",
            json={"name": name, "payload_schema": payload_schema or {}},
            expected_status=201,
            error_map=_QUEUE_CREATE_ERROR_MAP,
        )

    async def delete_queue(self, name: str) -> None:
        """Delete a queue.

        Raises:
            QueueNotFoundError: Queue does not exist.
            BrokerFencedError: Broker is fenced (after retries).
        """
        await self._request(
            "DELETE", f"/queues/{name}", expected_status=204
        )

    async def push(self, queue_name: str, payload: dict) -> None:
        """Push a task to a queue.

        Raises:
            QueueNotFoundError: Queue does not exist.
            PayloadValidationError: Payload doesn't match the queue's schema.
            BrokerFencedError: Broker is fenced (after retries).
        """
        await self._request(
            "POST",
            f"/queues/{queue_name}/push",
            json={
                "payload": payload,
                "idempotency_key": uuid4().hex,
            },
            expected_status=204,
        )

    async def claim(
        self,
        queue_name: str,
        timeout_seconds: float | None = None,
    ) -> ClaimedTask | None:
        """Claim the next pending task.

        Blocks until a task is available (up to *timeout_seconds*).
        Returns ``None`` when *timeout_seconds* expires with no task.
        Without a timeout, blocks indefinitely.

        Returns a ClaimedTask context manager that sends heartbeats
        in the background.

        Raises:
            QueueNotFoundError: Queue does not exist.
            BrokerFencedError: Broker is fenced (after retries).
        """
        idempotency_key = uuid4().hex
        body: dict[str, Any] = {"idempotency_key": idempotency_key}
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        resp = await self._request(
            "POST",
            f"/queues/{queue_name}/claim",
            json=body,
            expected_status=(200, 204),
        )
        if resp.status_code == 204:
            return None
        data = resp.json()
        return ClaimedTask(
            _client=self,
            _queue_name=queue_name,
            task_id=data["task_id"],
            sequence=data["sequence"],
            payload=data["payload"],
            _heartbeat_interval=self._config.heartbeat_interval_seconds,
        )

    async def _heartbeat(self, queue_name: str, task_id: str) -> None:
        await self._request(
            "POST",
            f"/queues/{queue_name}/heartbeat",
            json={"task_id": task_id},
            expected_status=204,
            error_map=_TASK_ERROR_MAP,
        )

    async def _finish(
        self,
        queue_name: str,
        task_id: str,
        sequence: int,
    ) -> None:
        await self._request(
            "POST",
            f"/queues/{queue_name}/finish",
            json={"task_id": task_id, "sequence": sequence},
            expected_status=204,
            error_map=_TASK_ERROR_MAP,
        )

    async def _request(
        self,
        method: str,
        path: str,
        expected_status: int | tuple[int, ...] = 200,
        error_map: dict[int, type[CascadqError]] | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send an HTTP request with retry for transient failures."""
        expected = (
            (expected_status,)
            if isinstance(expected_status, int)
            else expected_status
        )
        emap = error_map or _DEFAULT_ERROR_MAP
        last_error: Exception | None = None
        last_resp: httpx.Response | None = None
        max_retries = self._config.max_retries

        for attempt in range(max_retries + 1):
            try:
                resp = await self._client.request(method, path, **kwargs)
            except httpx.HTTPError as e:
                last_error = e
                if attempt < max_retries:
                    await self._backoff(attempt)
                continue

            if resp.status_code in expected:
                return resp

            if resp.status_code >= 500:
                last_resp = resp
                if attempt < max_retries:
                    await self._backoff(attempt)
                continue

            # 4xx — translate to domain error immediately
            _raise_for_status(resp, emap)

        # Retries exhausted
        if last_resp is not None:
            _raise_for_status(last_resp, emap)
        raise last_error  # type: ignore[misc]

    async def _backoff(self, attempt: int) -> None:
        delay = min(
            self._config.retry_base_delay_seconds * (2**attempt),
            self._config.retry_max_delay_seconds,
        )
        jitter = delay * random.random() * 0.5
        await asyncio.sleep(delay + jitter)


class ClaimedTask:
    """Handle for a claimed task. Use as an async context manager.

    Sends heartbeats in the background while the task is held.
    Call finish() or let the context manager exit to complete the task.
    """

    def __init__(
        self,
        *,
        _client: CascadqClient,
        _queue_name: str,
        task_id: str,
        sequence: int,
        payload: dict,
        _heartbeat_interval: float,
    ) -> None:
        self.task_id = task_id
        self.sequence = sequence
        self.payload = payload
        self._client = _client
        self._queue_name = _queue_name
        self._heartbeat_interval = _heartbeat_interval
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._finishing = False
        self._finished = False

    @property
    def finish_acknowledged(self) -> bool:
        """Whether the broker durably acknowledged task completion."""
        return self._finished

    async def __aenter__(self) -> ClaimedTask:
        if _TRACE_TASK_LIFECYCLE:
            logger.info(
                "task_trace enter task_id=%s queue=%s heartbeat_interval=%.3fs",
                self.task_id,
                self._queue_name,
                self._heartbeat_interval,
            )
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None and not self._finished:
                try:
                    await self.finish()
                except TaskNotFoundError:
                    logger.warning(
                        "Task %s was re-queued before finish; "
                        "another consumer will process it",
                        self.task_id,
                    )
        finally:
            await self._stop_heartbeat()

    async def finish(self) -> None:
        """Mark the task as completed.

        Heartbeats continue running until the finish RPC is durably
        acknowledged, so the broker does not re-queue the task while
        the completion flush is in flight.
        """
        if self._finished or self._finishing:
            return
        self._finishing = True
        try:
            if _TRACE_TASK_LIFECYCLE:
                logger.info(
                    "task_trace finish_start task_id=%s queue=%s sequence=%s",
                    self.task_id,
                    self._queue_name,
                    self.sequence,
                )
            await self._client._finish(
                self._queue_name,
                self.task_id,
                self.sequence,
            )
            self._finished = True
            if _TRACE_TASK_LIFECYCLE:
                logger.info(
                    "task_trace finish_done task_id=%s queue=%s",
                    self.task_id,
                    self._queue_name,
                )
        finally:
            self._finishing = False
            await self._stop_heartbeat()

    async def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                if _TRACE_TASK_LIFECYCLE:
                    logger.info(
                        "task_trace heartbeat_send task_id=%s queue=%s",
                        self.task_id,
                        self._queue_name,
                    )
                await self._client._heartbeat(self._queue_name, self.task_id)
                if _TRACE_TASK_LIFECYCLE:
                    logger.info(
                        "task_trace heartbeat_ack task_id=%s queue=%s",
                        self.task_id,
                        self._queue_name,
                    )
            except TaskNotClaimedError:
                if self._finished or self._finishing:
                    return
                if _TRACE_TASK_LIFECYCLE:
                    logger.info(
                        "task_trace heartbeat_not_claimed task_id=%s queue=%s",
                        self.task_id,
                        self._queue_name,
                    )
                logger.warning(
                    "Heartbeat rejected for task %s, stopping heartbeat",
                    self.task_id,
                )
                return
            except TaskNotFoundError:
                if self._finished or self._finishing:
                    return
                if _TRACE_TASK_LIFECYCLE:
                    logger.info(
                        "task_trace heartbeat_not_found task_id=%s queue=%s",
                        self.task_id,
                        self._queue_name,
                    )
                logger.warning(
                    "Task %s not found during heartbeat, stopping heartbeat",
                    self.task_id,
                )
                return
            except (httpx.HTTPError, CascadqError):
                if _TRACE_TASK_LIFECYCLE:
                    logger.info(
                        "task_trace heartbeat_error task_id=%s queue=%s",
                        self.task_id,
                        self._queue_name,
                        exc_info=True,
                    )
                logger.warning(
                    "Heartbeat failed for task %s", self.task_id,
                    exc_info=True,
                )


# Private helpers — error maps and status-to-exception translation

_DEFAULT_ERROR_MAP: dict[int, type[CascadqError]] = {
    404: QueueNotFoundError,
    422: PayloadValidationError,
    503: BrokerFencedError,
}

_QUEUE_CREATE_ERROR_MAP: dict[int, type[CascadqError]] = {
    **_DEFAULT_ERROR_MAP,
    409: QueueAlreadyExistsError,
}

_TASK_ERROR_MAP: dict[int, type[CascadqError]] = {
    **_DEFAULT_ERROR_MAP,
    404: TaskNotFoundError,
    409: TaskNotClaimedError,
}

# Maps the "code" field in the server's JSON error body to a client
# exception. Used to disambiguate status codes that map to multiple
# domain errors (e.g., 503 can be flush_exhausted or broker_fenced).
_ERROR_CODE_OVERRIDE: dict[str, type[CascadqError]] = {
    "broker_fenced": BrokerFencedError,
    "flush_exhausted": FlushExhaustedError,
}


def _raise_for_status(
    resp: httpx.Response,
    error_map: dict[int, type[CascadqError]],
) -> None:
    """Translate an HTTP error response into a domain exception."""
    error_cls = error_map.get(resp.status_code)
    if error_cls is None:
        raise httpx.HTTPStatusError(
            f"Unexpected status {resp.status_code}: {resp.text}",
            request=resp.request,
            response=resp,
        )
    try:
        body = resp.json()
    except ValueError:
        raise error_cls(resp.text) from None
    detail = body.get("error", resp.text)
    code = body.get("code", "")
    error_cls = _ERROR_CODE_OVERRIDE.get(code, error_cls)
    raise error_cls(detail)
