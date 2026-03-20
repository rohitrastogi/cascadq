# CAScadq

CAScadq (CAS -> Compare and Swap) is a simple object-storage-backed task queue for sharing work across producers and consumers.

## Goals and non-goals

**Goals:**
- FIFO ordering with at-least-once consumption guarantees
- Support long-running tasks (minutes)
- Robust to worker and broker failures
- Support thousands of tasks and QPS across producers and consumers
- Support multiple logical queues
- Leverage durability and consistency guarantees of object storage, specifically CAS primitives

**Non-goals:**
- Not a log for high-throughput streaming
- Not a bulk data transport — payloads are small task descriptors (pointers/notifications, typically under 1KB), not bulk data

## Client API

HTTP transport. Designed to be trivial to implement clients in any language.

### Management API
- `create_queue(name, payload_schema)` — creates a new queue with a JSON Schema defining the accepted payload format
- `delete_queue(name)` — hard deletes the queue and all its tasks immediately. In-flight claims fail on the consumer's next heartbeat or finish call

### Producer API
- `push(queue_id, payload)` — enqueues a task. Payload is validated server-side against the queue's JSON Schema

### Consumer API
- `claim(queue_id) -> (task_id, payload)` — claims the task at the head of the queue (lowest sequence number among unclaimed tasks). The client library transparently sends periodic heartbeats in the background while the task is held
- `finish(queue_id, task_id)` — marks a claimed task as completed

## Reference client

- Async Python
- Transparently handles heartbeats for claimed tasks in the background
- Configurable retry policy with exponential backoff. After exhausting retries, fails permanently for the caller to handle
- Designed as a reference for building clients in other languages

## Data model

The state is a **materialized snapshot**, not an event log. Each flush writes the full current state of a queue.

### Storage layout

- **`queues/{name}.json`** — one self-contained file per queue. Contains queue metadata (schema, created_at) and all task state. CAS'd on every flush.
- **`broker.json`** — written once on broker startup. Contains broker identity info (id, host, started_at) for debuggability. Not CAS'd, purely informational.
- **No separate registry.** The broker discovers queues by listing the `queues/` prefix on startup.

One file per queue allows independent CAS operations per queue, enabling parallel flushes and eliminating cross-queue contention.

### Queue operations and atomicity

Because each queue is a single file, queue operations are single atomic object storage calls:
- **Create queue**: write-if-not-exists for `queues/{name}.json`
- **Delete queue**: delete `queues/{name}.json`
- **Push/claim/heartbeat/finish**: CAS on `queues/{name}.json`

No cross-file coordination is needed.

### Schema sketch

```python
@dataclass
class QueueFile:
    metadata: QueueMetadata
    next_sequence: int  # monotonically increasing, assigned on push
    tasks: list[Task]

@dataclass
class QueueMetadata:
    created_at: float
    payload_schema: dict  # JSON Schema

@dataclass
class BrokerInfo:
    """Written to broker.json on startup. Purely for debuggability."""
    broker_id: str
    host: str
    started_at: float

@dataclass
class Task:
    task_id: str
    sequence: int  # assigned on push, determines FIFO order
    created_at: float
    status: TaskStatus  # pending | claimed | completed
    payload: dict
    # set on claim, updated on heartbeat, used for timeout detection
    last_heartbeat: float | None
    claimed_by: str | None  # consumer identifier
```

### Task lifecycle

```
Pending -> Claimed -> Completed
                  \-> Pending (on heartbeat timeout, re-queued to front)
```

- **Pending**: task is waiting to be claimed. Consumers claim the pending task with the lowest sequence number
- **Claimed**: a consumer is actively working on the task. The consumer's client library sends periodic heartbeats that update `last_heartbeat`
- **Completed**: the consumer called `finish()`. Completed tasks remain in the state file until cleaned up by the background compaction worker

### Heartbeat timeout and re-queueing

If a claimed task has not received a heartbeat within a configurable duration, it is timed out:
1. The original claim is invalidated (subsequent `finish()` calls for the old claim are rejected)
2. A new task is created with a fresh `task_id` and the same payload, placed at the **front** of the queue (assigned a sequence number lower than all current pending tasks)

This preserves FIFO semantics — failed tasks are retried before new work.

## Object storage abstraction

Abstract interface over S3, GCS, and R2. The CAS mechanism uses each provider's native conditional write support:

- **S3**: `If-Match` header on PutObject, comparing against the object's ETag
- **GCS**: `if_generation_match` precondition, comparing against the object's generation number
- **R2**: `If-Match` header on PutObject (S3-compatible)

### Interface sketch

```python
class ObjectStore(Protocol):
    async def read(self, key: str) -> tuple[bytes, VersionToken]:
        """Read an object. Returns its data and an opaque version token."""
        ...

    async def write(self, key: str, data: bytes, expected_version: VersionToken) -> None:
        """Write an object conditionally. Raises ConflictError if the version
        token doesn't match the current version (another writer intervened)."""
        ...

    async def write_new(self, key: str, data: bytes) -> None:
        """Write an object that must not already exist. Raises ConflictError if it does."""
        ...

    async def delete(self, key: str) -> None:
        """Delete an object."""
        ...

    async def list_prefix(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        ...
```

`VersionToken` is opaque to the broker — an ETag string for S3/R2, a generation int for GCS.

## Broker

### Architecture

Single-threaded async event loop (`asyncio`). The broker is a cache and write buffer in front of object storage. It is stateless on disk — all durable state lives in object storage.

### Startup

A broker starts from just an **object storage path**:
1. Write `broker.json` with identity info
2. List `queues/` prefix to discover all queues
3. Read each queue state file, reconstruct in-memory state
4. Start serving client requests

No coordination with any previous broker instance. CAS handles fencing.

### Per-queue state management

Each queue has a lightweight in-memory state object:
- Current materialized state (task list, metadata, sequence counter)
- Write buffer (pending mutations not yet flushed)
- Version token from last successful read/write (for CAS)

No separate threads or actors per queue — all managed by the single event loop.

### Flush strategy — continuous double-buffering

Flushing is continuous, not time-based or size-based:

1. Client requests (push, claim, heartbeat, finish) are applied to the in-memory state and appended to the active write buffer
2. A background flush task runs in a loop: grab the current buffer, start flushing to object storage
3. While the flush is in flight, new writes accumulate in a fresh buffer
4. When the flush completes successfully, ack all clients whose writes were in the flushed batch
5. Immediately start flushing the next buffer

Throughput is naturally bounded by object storage latency. Batching happens organically — more writes accumulate during slower flushes.

### Concurrent flushes across queues

Each queue flushes independently via `asyncio.TaskGroup`. Queue A's CAS does not contend with queue B's CAS. This allows parallel PUT operations to object storage.

### CAS and leader fencing

Every flush performs a conditional write using the version token from the last successful operation. The broker is a global leader across all queues. If a CAS fails on **any** queue:
- Another broker has written — this broker has been fenced out
- **All buffered and in-flight client requests across all queues are failed**
- The broker logs the fencing event and should shut down so the orchestrator can restart it cleanly

If the broker is still leader but a write fails (transient object storage error):
- Retry the write, continue buffering new writes
- If too many consecutive failures, fail all buffered writes across all queues

### Client request lifecycle

1. Client sends HTTP request to broker
2. Broker validates the request (e.g., schema validation for push)
3. Broker applies the mutation to in-memory state
4. Mutation is appended to the queue's write buffer
5. Client request **blocks until the next successful flush** that includes its mutation
6. On flush success: client receives success response
7. On flush failure (CAS conflict or exhausted retries): client receives error response

### Background compaction

A periodic background task cleans up completed tasks from per-queue state files to prevent unbounded growth. This goes through the same CAS flush path.

### Heartbeat timeout detection

A periodic background task scans claimed tasks and checks `last_heartbeat` against the configured timeout. Timed-out tasks are re-queued to the front of the queue with a new `task_id` (so the old claim's `finish()` is rejected).

## Broker failover

1. Broker A is running, flushing with CAS
2. Broker A becomes unresponsive
3. Orchestrator (K8s, systemd, manual) starts Broker B
4. Broker B reads state from object storage, starts serving
5. Broker A wakes up, tries to flush — CAS fails on a queue, it knows it's been fenced
6. Broker A fails all buffered client requests across all queues and shuts down

**Clients may experience transient failures during the handoff.** At-least-once semantics mean they retry and succeed against the new broker. No data corruption, just brief unavailability.

### Service discovery

Not a concern of cascadq. Clients are configured with the broker's URL. How that URL maps to a live broker is a deployment concern:
- In Kubernetes: a Service endpoint
- Outside Kubernetes: a load balancer, DNS entry, systemd-managed process, or any mechanism that ensures the broker is reachable at a stable address

## Configuration

Key tuning parameters (with sensible defaults):
- Heartbeat interval (client-side)
- Heartbeat timeout (broker-side, must be > heartbeat interval)
- Max consecutive object storage failures before failing all buffered writes
- Client retry policy (max retries, backoff parameters)
- Compaction interval
