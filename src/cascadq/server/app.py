"""Starlette app factory with lifespan managing broker start/stop."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.routing import Route

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.server import routes
from cascadq.storage.protocol import ObjectStore


def create_app(
    store: ObjectStore,
    config: BrokerConfig | None = None,
    broker: Broker | None = None,
) -> Starlette:
    """Create the Starlette application with broker lifecycle.

    If a broker is provided, it is used directly and the caller is
    responsible for its lifecycle. Otherwise one is created and
    managed by the app lifespan.
    """
    owns_broker = broker is None
    if broker is None:
        broker = Broker(store=store, config=config)

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncGenerator[None]:
        if owns_broker:
            await broker.start()
        app.state.broker = broker
        yield
        if owns_broker:
            await broker.stop()

    app = Starlette(
        routes=[
            Route("/queues", routes.create_queue, methods=["POST"]),
            Route("/queues/{name}", routes.delete_queue, methods=["DELETE"]),
            Route("/queues/{name}/push", routes.push, methods=["POST"]),
            Route("/queues/{name}/claim", routes.claim, methods=["POST"]),
            Route("/queues/{name}/heartbeat", routes.heartbeat, methods=["POST"]),
            Route("/queues/{name}/finish", routes.finish, methods=["POST"]),
        ],
        lifespan=lifespan,
    )
    return app
