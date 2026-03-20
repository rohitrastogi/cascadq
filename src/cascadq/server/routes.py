"""HTTP route handlers for the CAScadq broker API."""

from __future__ import annotations

import logging

from pydantic import ValidationError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from cascadq.broker.broker import Broker
from cascadq.errors import (
    BrokerFencedError,
    CascadqError,
    FlushFailedError,
    PayloadValidationError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
    TaskNotClaimedError,
    TaskNotFoundError,
)
from cascadq.server.schemas import (
    ClaimRequest,
    CreateQueueRequest,
    FinishRequest,
    HeartbeatRequest,
    PushRequest,
)

logger = logging.getLogger(__name__)

_ERROR_MAP: dict[type[Exception], tuple[int, str]] = {
    QueueNotFoundError: (404, "queue_not_found"),
    TaskNotFoundError: (404, "task_not_found"),
    QueueAlreadyExistsError: (409, "queue_already_exists"),
    TaskNotClaimedError: (409, "task_not_claimed"),
    PayloadValidationError: (422, "payload_validation_error"),
    FlushFailedError: (503, "flush_failed"),
    BrokerFencedError: (503, "broker_fenced"),
}


def _error_response(exc: Exception) -> Response:
    status, code = _ERROR_MAP.get(type(exc), (500, "internal_error"))
    return JSONResponse(
        {"error": str(exc), "code": code}, status_code=status
    )


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
    try:
        broker = _get_broker(request)
        await broker.push(name, body.payload, body.idempotency_key)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)


async def claim(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = ClaimRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    try:
        broker = _get_broker(request)
        task = await broker.claim(name, body.consumer_id)
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


async def heartbeat(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = HeartbeatRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    try:
        broker = _get_broker(request)
        await broker.heartbeat(name, body.task_id)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)


async def finish(request: Request) -> Response:
    name = request.path_params["name"]
    try:
        body = FinishRequest.model_validate_json(await request.body())
    except ValidationError as e:
        return JSONResponse({"error": str(e)}, status_code=422)
    try:
        broker = _get_broker(request)
        await broker.finish(name, body.task_id, body.sequence)
        return Response(status_code=204)
    except CascadqError as e:
        return _error_response(e)
