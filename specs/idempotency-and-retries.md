# Idempotency And Retry Semantics

## Purpose

This document defines the retry and idempotency model for CAScadq.

It covers:

- what problem we are solving
- what guarantees we want to provide
- how `push`, `claim`, `finish`, and `heartbeat` should behave
- how broker-side flush retries differ from request idempotency
- the concrete implementation shape to build

This is a final-form design document, not a brainstorming note.

## Desired Guarantees

CAScadq should provide:

- durable success semantics: a mutating request succeeds only after the updated queue state is flushed to object storage
- safe automatic retries in the first-party Python client for supported mutating operations
- at-least-once task processing, not exactly-once processing
- no requirement for application code using the Python client to manually provide idempotency keys

CAScadq does **not** need to provide:

- exactly-once task processing
- permanent deduplication for arbitrarily delayed user retries
- a guarantee that every ambiguous crash window is eliminated

## The Two Retry Problems

There are two different reasons a client may retry after a timeout, and they need different solutions.

### Problem 1: The Original Request Is Still In Flight

Example:

1. the broker accepts a `push`
2. the mutation is applied in memory
3. the flush to object storage is very slow
4. the client times out locally before receiving a response
5. the broker is still working on the original request

This is **not primarily an idempotency problem**. The original request is still in progress.

Required solution:

- the broker must keep retrying or waiting internally
- the client must not be told the mutation failed while the broker still intends to complete it

### Problem 2: The Original Request May Already Have Succeeded

Example:

1. the broker accepts a `push`
2. the flush succeeds
3. the response is lost, delayed past the client timeout, or the connection breaks
4. the client retries

This **is** an idempotency problem. The original mutation may already be durable, but the client cannot know that.

Required solution:

- repeated logical requests must be recognized as the same operation
- the broker must return the original outcome instead of applying the mutation again

## Root Cause In The Current Code

The current broker model is:

1. apply a mutation to in-memory queue state
2. flush the updated snapshot to object storage
3. return success after the flush succeeds

That is a good durability model, but it makes one thing dangerous:

- once a mutation has been applied in memory, the broker must not surface an error that encourages the client to re-issue the operation unless the broker has rolled the mutation back or abandoned that in-memory state

The current transient flush failure path violates that rule.

Today, on a transient object-store write failure in [flush.py](../src/cascadq/broker/flush.py):

1. the mutation remains applied in memory
2. the waiter receives `FlushFailedError`
3. the broker schedules another flush attempt

That is unsafe because the client sees a retryable failure even though the original mutation may still commit later.

## Core Design Decisions

### 1. Broker-Side Flush Retries And Request Idempotency Are Different Mechanisms

They solve different problems and should not be conflated.

- **Broker-side flush retries** solve the case where the original request is still in flight.
- **Request idempotency** solves the case where the original request may already have succeeded.

### 2. Transient Flush Failures Must Not Be Surfaced Mid-Retry

On a transient object-store write failure:

- the broker retries internally up to a configurable limit
- the original request remains pending during those retries
- no intermediate retryable request error is returned to the client

If retries are exhausted:

- the broker treats itself as unhealthy
- it abandons volatile in-memory state
- in-flight requests fail terminally
- recovery happens by restarting from durable object-store state

This means `FlushFailedError` should not be used as an intermediate client-visible signal while the broker still intends to flush the same in-memory mutation batch.

### 3. All Supported Mutating Operations Use One Retry Concept: `idempotency_key`

The HTTP API should expose an optional `idempotency_key` for:

- `push`
- `claim`
- `finish`

The first-party Python client should generate it automatically. Application code using the Python client should not need to provide it manually.

This keeps the protocol explicit for non-Python clients while keeping the normal client experience simple.

### 4. `push` Should Not Return `task_id`

`push` should return acknowledgement only.

Reasoning:

- producers do not need a broker-generated `task_id` for normal queue usage
- returning `task_id` exposes an internal identifier without providing much producer-side value
- idempotent `push` becomes simpler if the stable result is just "accepted once"

Decision:

- `push` returns success with no `task_id`
- internal task ids still exist for consumer-side operations

### 5. `consumer_id` Should Be Removed

The current implementation does not need `consumer_id` for correctness.

Today:

- heartbeat is keyed by `task_id`
- finish is keyed by `task_id` plus `sequence`
- timeout recovery is driven by `last_heartbeat`
- no correctness check validates that a specific consumer identity sends heartbeat or finish

Decision:

- remove `consumer_id` from the public API and internal task model
- claim retries should be deduped by `idempotency_key`, not by consumer identity

### 6. `sequence` Remains Required For `finish`

`sequence` is still needed for correctness.

Reasoning:

- `task_id` alone is not enough once compaction can remove completed tasks
- heartbeat timeout recovery can replace a claimed task with a new task id
- `finish` needs a stable witness of which logical task attempt is being acknowledged

Decision:

- `finish` continues to require `task_id` and `sequence`
- `sequence` is a correctness field, not the retry identity

## Per-Operation Design

### Push

HTTP request fields:

- `payload`
- `idempotency_key` optional

Client behavior:

- `CascadqClient.push()` generates one idempotency key per logical call
- the same key is reused across transport retries for that call
- the client returns `None` on success

Broker behavior:

- the first request with key `K` enqueues one task
- repeated requests with the same key are treated as the same logical enqueue
- no duplicate task is created

Durable state shape:

- `push` needs durable idempotency records in queue state
- this is necessary because producer retries may arrive after restart or after a lost response

### Claim

HTTP request fields:

- `idempotency_key` optional

Client behavior:

- `CascadqClient.claim()` generates one idempotency key per logical call
- the same key is reused across transport retries for that call

Broker behavior:

- the first request with key `K` claims the next pending task
- the same key retried later returns the same claimed task

Important design choice:

- `claim` should **not** use a separate durable replay table by default
- instead, the claimed task itself should store the claim idempotency key that produced it

Reasoning:

- the claimed task already materializes the claim result
- the queue snapshot already contains the fields needed to replay the response:
  - `task_id`
  - `sequence`
  - `payload`
  - `status=claimed`
- a separate replay map would duplicate state already present in the queue file

Implementation note:

- an in-memory index from claim idempotency key to task may be added for efficiency
- the durable source of truth should still be the task in queue state

### Finish

HTTP request fields:

- `task_id`
- `sequence`
- `idempotency_key` optional

Client behavior:

- `ClaimedTask.finish()` generates one idempotency key the first time finish is attempted
- the same key is reused across transport retries for that finish call

Broker behavior:

- if the task is currently claimed, finish it
- if the same finish is retried, return success
- if the task was already compacted and `sequence` is at or below the compaction watermark, return success

Important design choice:

- `finish` should rely primarily on task state plus `sequence` and the compaction watermark
- do not introduce a separate finish replay table unless later implementation proves it is necessary

### Heartbeat

Heartbeat is not part of the general idempotency mechanism.

Why:

- duplicate heartbeats are mostly harmless
- the real heartbeat problem is timing, not duplicate logical mutation

Current timing rule:

- the effective server-observed heartbeat cadence is approximately `heartbeat_interval + request_latency + flush_latency`

Therefore:

- `heartbeat_timeout_seconds` must be comfortably larger than the worst expected cadence under load
- stress tests should validate this explicitly

Future improvements may change heartbeat scheduling, but heartbeat should stay outside the main idempotency design unless a concrete need appears.

## Durable State Rules

### Idempotency Information Must Be Durable

If idempotency is meant to survive:

- broker restart
- lost responses
- retries after the original flush succeeded

then the idempotency information must be flushed as part of queue state.

Broker memory alone is not enough.

### Idempotency Information Does Not Need To Live Forever

CAScadq only needs a bounded deduplication window.

That window must be long enough to cover:

- first-party client timeout and retry behavior
- normal network delays
- short post-restart retry windows

It does not need to cover:

- arbitrary user retries much later

Decision:

- idempotency information is durable
- retention is bounded rather than permanent
- the retention window is configurable via `BrokerConfig.idempotency_ttl_seconds`, defaulting to 300 (5 minutes)
- cleanup runs during compaction: keys older than the TTL are removed

Time-based retention is preferred over flush-generation-based retention because the client's retry behavior is time-based (backoff delays in seconds). Under high flush rates, a generation-based window would shrink in wall-clock time, potentially expiring keys before client retries arrive — exactly the scenario where retries are most likely.

## Failure Semantics

### Transient Object-Store Failure

- broker retries internally
- request remains pending
- no intermediate retryable request error is surfaced

### Retry Exhaustion

- broker transitions to terminal unavailable state
- volatile in-memory state is abandoned
- waiters fail terminally
- recovery is by restart from durable state

### CAS Conflict

- broker is fenced immediately
- in-flight and buffered waiters fail
- recovery is by restart from durable state

### Client Transport Retry

If the first-party client retries the same logical operation automatically:

- it reuses the same `idempotency_key`
- the broker returns the original logical result

### Explicit User Retry After A Surfaced Error

If application code calls the client method again later, that is a new logical operation unless the same idempotency key is reused intentionally.

This is acceptable because CAScadq targets:

- at-least-once processing
- near-exactly-once behavior for automatic retries

not

- permanent exactly-once semantics for arbitrary user behavior

## Build Plan

1. Change [flush.py](../src/cascadq/broker/flush.py) so transient flush failures keep waiters pending during broker-side retries.
2. Remove intermediate client-visible `FlushFailedError` behavior for transient failures.
3. Change `push` so the Python client auto-generates `idempotency_key` and the public API no longer returns `task_id`.
4. Add `idempotency_key` to `claim` and store the claim key on the claimed task so retries can return the same task.
5. Add `idempotency_key` to `finish`, while keeping `sequence` as the correctness witness across compaction and stale retries.
6. Remove `consumer_id` from the API and task model.
7. Add tests for client transport retries of `push`, `claim`, and `finish`.
8. Validate heartbeat timeout safety separately under elevated flush latency.

## Summary

The design is:

- broker-side flush retries are internal and keep requests pending
- `idempotency_key` is the one shared retry concept for `push`, `claim`, and `finish`
- the Python client manages idempotency keys automatically
- `push` no longer returns `task_id`
- `consumer_id` is removed
- `sequence` stays only for `finish` correctness
- `claim` idempotency is stored on the claimed task itself
- durable idempotency information is retained for a bounded retry window, not forever

This keeps the model consistent, close to the existing materialized-snapshot architecture, and straightforward to implement.
