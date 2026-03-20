# CAScadq

Object-storage-backed task queue using compare-and-swap (CAS) for consistency.

CAScadq is a lightweight, async task queue that persists queue state as JSON objects in any S3-compatible object store. Instead of relying on a traditional database, it uses conditional writes (ETags / `If-Match`) to ensure only one broker is actively writing at a time, providing consistency without coordination infrastructure.

The design is heavily inspired by -- and in many places directly adapted from -- the approach described in turbopuffer's [*Building a Queue on Object Storage*](https://turbopuffer.com/blog/object-storage-queue) blog post by Dan Harrison. The core ideas are theirs: storing the entire queue as a single CAS-guarded JSON file, using a broker with a group-commit flush loop so write throughput scales with bandwidth rather than latency, fencing stale brokers on CAS conflict, and reclaiming dead work via heartbeat timeouts. CAScadq is essentially a from-scratch Python implementation of that design, repackaged as a general-purpose task queue with an HTTP API and pluggable storage backends.

## How it works

- **Queue state is a single JSON object per queue**, stored at `queues/{name}.json`. Each object contains the full task list, metadata, and a monotonic sequence counter.
- **Mutations are buffered in memory** and periodically flushed to object storage via a double-buffered flush loop. Client requests block until their mutation is persisted.
- **CAS writes** ensure consistency: if another broker writes first, the flush detects a version mismatch and the broker **fences itself**, refusing further mutations.
- **Heartbeat detection** re-queues claimed tasks whose consumers have gone silent, and **compaction** periodically removes completed tasks from state.

## Architecture

```
+-----------+         +-----------------------------------+
|  Client   |--HTTP-->|            Broker                  |
+-----------+         |  +------------+ +---------------+  |
                      |  |QueueState  | |QueueState     |  |
                      |  |  (tasks)   | |  (tasks)      |  |
                      |  +-----+------+ +------+--------+  |
                      |        +--------+------+            |
                      |        FlushCoordinator             |
                      |         (CAS writes)                |
                      |   CompactionWorker  HeartbeatWorker  |
                      +----------+--------------------------+
                                 |
                      +----------v------------+
                      |    Object Store       |
                      |  (S3 / R2 / MinIO)    |
                      +-----------------------+
```

## Installation

Requires Python 3.13+.

```bash
# Core install
uv pip install -e .

# With S3 backend support
uv pip install -e ".[s3]"

# With dev dependencies (pytest, ruff)
uv pip install -e ".[dev]"
```

## Quickstart

### Running the server

```python
import uvicorn
from cascadq.config import BrokerConfig
from cascadq.server.app import create_app
from cascadq.storage.memory import InMemoryObjectStore

store = InMemoryObjectStore()
app = create_app(store=store, config=BrokerConfig(port=8000))

uvicorn.run(app, host="0.0.0.0", port=8000)
```

With S3-compatible storage:

```python
from cascadq.config import S3Config
from cascadq.storage.s3 import S3ObjectStore

s3_config = S3Config(
    bucket="my-queue-bucket",
    access_key_id="...",
    secret_access_key="...",
    endpoint_url="https://my-r2-endpoint.example.com",  # omit for AWS S3
)
store = S3ObjectStore(
    bucket=s3_config.bucket,
    access_key_id=s3_config.access_key_id,
    secret_access_key=s3_config.secret_access_key,
    endpoint_url=s3_config.endpoint_url,
)
```

### Using the client

```python
import asyncio
from cascadq.client.client import CascadqClient
from cascadq.config import ClientConfig

async def main():
    client = CascadqClient(ClientConfig(base_url="http://localhost:8000"))

    # Create a queue (optionally with a JSON Schema for payload validation)
    await client.create_queue("emails")

    # Push a task
    task_id = await client.push("emails", {"to": "user@example.com", "subject": "Hello"})

    # Claim and process a task (heartbeats are sent automatically)
    claimed = await client.claim("emails")
    if claimed is not None:
        async with claimed:
            print(f"Processing task {claimed.task_id}: {claimed.payload}")
            # Task is automatically finished when the context manager exits

    await client.close()

asyncio.run(main())
```

## HTTP API

All endpoints accept and return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/queues` | Create a queue. Body: `{"name": "...", "payload_schema": {...}}` |
| `DELETE` | `/queues/{name}` | Delete a queue |
| `POST` | `/queues/{name}/push` | Push a task. Body: `{"payload": {...}}` |
| `POST` | `/queues/{name}/claim` | Claim the next pending task. Returns `204` if empty |
| `POST` | `/queues/{name}/heartbeat` | Send heartbeat for a claimed task. Body: `{"task_id": "..."}` |
| `POST` | `/queues/{name}/finish` | Mark a claimed task as completed. Body: `{"task_id": "..."}` |

### Error responses

| Status | Meaning |
|--------|---------|
| `404` | Queue or task not found |
| `409` | Queue already exists, or task not in claimed state |
| `422` | Payload validation failed (against queue's JSON Schema) |
| `503` | Broker is fenced (another broker took over) |

## Storage backends

CAScadq uses a pluggable `ObjectStore` protocol with two built-in implementations:

- **`InMemoryObjectStore`** -- dict-backed, for testing and development.
- **`S3ObjectStore`** -- works with AWS S3, Cloudflare R2, MinIO, and any S3-compatible service. Uses `aiobotocore` for async access and conditional writes via `If-Match` / `If-None-Match` headers.

## Configuration

Configuration uses frozen Pydantic models.

**`BrokerConfig`** -- server-side settings:
- `host` / `port` -- bind address (default `0.0.0.0:8000`)
- `heartbeat_timeout_seconds` -- time before a claimed task is re-queued (default `30`)
- `compaction_interval_seconds` -- how often completed tasks are removed (default `60`)
- `storage_prefix` -- key prefix in object storage (default `""`)

**`ClientConfig`** -- client-side settings:
- `base_url` -- broker URL (default `http://localhost:8000`)
- `heartbeat_interval_seconds` -- how often the client sends heartbeats (default `10`)
- `max_retries` -- retry count for transient failures (default `3`)

**`S3Config`** -- S3 backend settings:
- `bucket`, `access_key_id`, `secret_access_key`, `endpoint_url`, `region`

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
pytest

# Lint
ruff check src/ tests/
```
