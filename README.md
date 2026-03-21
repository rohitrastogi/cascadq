# CAScadq

CAScadq is a single-broker task queue backed by object storage.

The broker is stateless across restarts. Durable queue state lives in S3-compatible object storage as CAS-protected JSON snapshots.

The core design is heavily inspired by turbopuffer's ["Building a Queue on Object Storage"](https://turbopuffer.com/blog/object-storage-queue): queue snapshots stored in object storage, compare-and-swap writes for consistency, a single active broker, and heartbeat-based recovery of abandoned work. CAScadq is a Python implementation of that approach with an HTTP API, Python client, and pluggable storage backends.

## Overview

CAScadq is designed around a small number of components:

- one broker process
- one object-store prefix
- one JSON snapshot per queue at `queues/{name}.json`

It does not require:

- a database
- local disk for durable state
- a coordination service

The broker keeps queue state in memory, serves the HTTP API, and flushes full queue snapshots to object storage.

## Architecture

This is a single active broker design.

The broker:

- serves `create_queue`, `delete_queue`, `push`, `claim`, `heartbeat`, and `finish`
- keeps queue state in memory
- flushes queue snapshots with compare-and-swap writes
- retries transient flush failures internally
- fences itself on CAS conflict
- runs background heartbeat-timeout recovery
- runs background compaction of completed tasks

Each queue snapshot contains:

- queue metadata
- task list
- next sequence number
- compaction watermark
- bounded idempotency state for producer retries

## Semantics

CAScadq provides at-least-once task processing.

Tasks move through:

- `pending`
- `claimed`
- `completed`

A claimed task may be re-queued if its lease expires.

`push`, `claim`, and `finish` succeed only after the corresponding queue mutation is durably flushed to object storage.

`claim` is blocking:

- if work is available, it returns a task
- if no work is available, it waits
- an optional timeout returns `204` / `None`

If a claimed task stops heartbeating:

- the task is re-queued automatically

## Tradeoffs

This design favors operational simplicity over horizontal scaling.

Constraints:

- one active broker
- object-store latency affects `push`, `claim`, and `finish`
- at-least-once processing rather than exactly-once processing
- automatic retries are handled by the first-party client rather than exposed as part of the normal usage model

## HTTP API

All endpoints accept and return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/queues` | Create a queue |
| `DELETE` | `/queues/{name}` | Delete a queue |
| `POST` | `/queues/{name}/push` | Enqueue a payload |
| `POST` | `/queues/{name}/claim` | Block until a task is available, optionally with timeout |
| `POST` | `/queues/{name}/heartbeat` | Renew a claim lease |
| `POST` | `/queues/{name}/finish` | Mark a task completed |

Common status codes:

| Status | Meaning |
|--------|---------|
| `404` | Queue or task not found |
| `409` | Queue already exists, or task not in claimed state |
| `422` | Payload validation failed |
| `503` | Broker fenced or unable to persist state |

## Python Example

```python
import asyncio

from cascadq_client import CascadqClient, ClientConfig


async def main() -> None:
    client = CascadqClient(ClientConfig(base_url="http://localhost:8000"))

    await client.create_queue("emails")
    await client.push("emails", {"to": "user@example.com", "subject": "Hello"})

    claimed = await client.claim("emails", timeout_seconds=5.0)
    if claimed is None:
        return

    async with claimed:
        print(claimed.payload)

    await client.close()


asyncio.run(main())
```

## Installation

Requires Python 3.13+. The repo is a uv workspace with two packages:
`cascadq` (server) and `cascadq-client` (client).

```bash
uv sync --all-extras
```

Run the broker:

```bash
uv run python -m cascadq                       # in-memory storage, default config
uv run python -m cascadq --config config.yaml  # custom config
```

## Development

```bash
uv run pytest
uv run ruff check src/ client/src/ tests/
uv run ty check src/ client/src/
```
