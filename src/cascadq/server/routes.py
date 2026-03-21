"""HTTP route handlers for the CAScadq broker API."""

from __future__ import annotations

import logging
import time

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from cascadq.broker.broker import Broker
from cascadq.errors import (
    BrokerFencedError,
    CascadqError,
    FlushExhaustedError,
    PayloadValidationError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
    TaskNotClaimedError,
    TaskNotFoundError,
)
from cascadq.metrics import rpc_duration_seconds
from cascadq.server.schemas import (
    ClaimRequest,
    CreateQueueRequest,
    FinishRequest,
    HeartbeatRequest,
    PushRequest,
)

logger = logging.getLogger(__name__)

# Ordered most-specific first: if exception subclasses are added,
# children must appear before their parents so isinstance matches
# the most specific handler.
_ERROR_MAP: dict[type[Exception], tuple[int, str]] = {
    QueueNotFoundError: (404, "queue_not_found"),
    TaskNotFoundError: (404, "task_not_found"),
    QueueAlreadyExistsError: (409, "queue_already_exists"),
    TaskNotClaimedError: (409, "task_not_claimed"),
    PayloadValidationError: (422, "payload_validation_error"),
    BrokerFencedError: (503, "broker_fenced"),
    FlushExhaustedError: (503, "flush_exhausted"),
}


def _error_response(exc: Exception) -> Response:
    for cls, (status, code) in _ERROR_MAP.items():
        if isinstance(exc, cls):
            return JSONResponse(
                {"error": str(exc), "code": code}, status_code=status
            )
    return JSONResponse(
        {"error": str(exc), "code": "internal_error"}, status_code=500
    )


def healthz(request: Request) -> Response:
    """Liveness probe — returns 200 if the event loop is responsive."""
    return Response(status_code=200)


def readyz(request: Request) -> Response:
    """Readiness probe — returns 200 when the broker can serve traffic."""
    broker: Broker | None = getattr(request.app.state, "broker", None)
    if broker is None:
        return JSONResponse(
            {"error": "broker not started", "code": "not_ready"},
            status_code=503,
        )
    if broker.is_fenced:
        return JSONResponse(
            {"error": "broker is fenced", "code": "broker_fenced"},
            status_code=503,
        )
    return Response(status_code=200)


def _get_broker(request: Request) -> Broker:
    return request.app.state.broker


async def create_queue(request: Request) -> Response:
    try:
        body = CreateQueueRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    try:
        broker = _get_broker(request)
        await broker.create_queue(body.name, body.payload_schema)
        return Response(status_code=201)
    except CascadqError as e:
        return _error_response(e)


async def delete_queue(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        broker = _get_broker(request)
        await broker.delete_queue(name)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)


async def push(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = PushRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    t0 = time.monotonic()
    try:
        broker = _get_broker(request)
        await broker.push(name, body.payload, body.push_key)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)
    finally:
        rpc_duration_seconds.labels(operation="push").observe(
            time.monotonic() - t0,
        )


async def claim(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = ClaimRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    t0 = time.monotonic()
    try:
        broker = _get_broker(request)
        task = await broker.claim(
            name, body.claim_key, body.timeout_seconds,
        )
        return JSONResponse(
            {
                "task_id": task.task_id,
                "sequence": task.sequence,
                "payload": task.payload,
            },
            status_code=200,
        )
    except QueueEmptyError:
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)
    finally:
        rpc_duration_seconds.labels(operation="claim").observe(
            time.monotonic() - t0,
        )


async def heartbeat(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = HeartbeatRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    t0 = time.monotonic()
    try:
        broker = _get_broker(request)
        await broker.heartbeat(name, body.task_id)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)
    finally:
        rpc_duration_seconds.labels(operation="heartbeat").observe(
            time.monotonic() - t0,
        )


async def finish(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = FinishRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    t0 = time.monotonic()
    try:
        broker = _get_broker(request)
        await broker.finish(name, body.task_id, body.sequence)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)
    finally:
        rpc_duration_seconds.labels(operation="finish").observe(
            time.monotonic() - t0,
        )
