# Stress Test Harness

## Motivation

CAScadq has 73 unit and integration tests, all running against `InMemoryObjectStore`. This gives good coverage of individual components and the happy-path task lifecycle, but it leaves three significant gaps:

1. **No real object store.** The in-memory backend is a faithful implementation of the `ObjectStore` protocol, but it doesn't exercise real network round-trips, real ETag-based CAS semantics, or real latency. A bug in how we format `If-Match` headers, handle pagination in `list_objects_v2`, or parse ETags could ship undetected.

2. **No concurrency.** Existing integration tests drive a single client through sequential push/claim/finish sequences. CAScadq's value proposition is that many producers and consumers can operate concurrently while the broker batches their mutations through the flush loop. We have never tested that claim.

3. **No sustained load.** The flush loop, heartbeat worker, and compaction worker are long-lived background tasks. The existing tests exercise at most a handful of flush cycles. We need tests that run enough cycles to surface timing races, memory growth, or state corruption that only appear under sustained operation.

A stress test harness that runs against a real S3-compatible object store with concurrent load would close all three gaps.

## Goals

### Correctness

The primary goal is **not** performance measurement. It is proving that the system's correctness invariants hold under concurrent, sustained load against real storage:

- **No lost tasks.** Every task that is pushed must eventually be claimed and finished. The set of pushed payload identifiers must equal the set of finished payload identifiers.
- **No duplicate delivery.** At any point in time, at most one consumer holds a claim on a given task. Re-delivery after heartbeat timeout is expected, but overlapping claims on the same logical task are not.
- **FIFO ordering preserved per queue.** Within a single queue, tasks should be claimed in push order (sequence-number order), not in arbitrary order.
- **Queue isolation.** Tasks pushed to queue A must never appear in claims from queue B.
- **Compaction correctness.** After compaction runs, the persisted queue file must contain no completed tasks while preserving all pending and claimed tasks.
- **Heartbeat timeout recovery.** Tasks abandoned by a failing consumer must be re-queued and eventually completed by a healthy consumer.

### Performance Baseline

A key motivation for this harness is establishing quantitative baselines for the Python broker so we can evaluate whether a Rust rewrite of the server layer would yield meaningful improvements. The harness should produce repeatable, comparable metrics across runs:

- **Push latency** -- p50, p95, p99 for the round-trip from client push call to flush confirmation. This measures the flush loop's batching efficiency and the object store's write latency.
- **Claim-to-finish latency** -- p50, p95, p99 for the full lifecycle of a claimed task. This captures broker overhead plus any queuing delay.
- **Throughput** -- total tasks/sec (pushes and completions) at varying concurrency levels. This is the number most directly comparable across language implementations.
- **Flush cycle time** -- wall-clock time per flush cycle (swap + serialize + CAS write + waiter resolution). This isolates the broker's hot path from client-side overhead and network variance.
- **Heartbeat-triggered re-queues** -- count per scenario, to confirm timeout behavior and quantify recovery overhead.

These metrics do not gate pass/fail. They are logged in a structured format that supports comparison across runs (e.g., same scenario, same object store, Python vs. Rust).

## Non-Goals

- **Controlled benchmarking.** The harness runs against a real object store over a real network, so absolute numbers will vary between runs and environments. Results are useful for relative comparisons (e.g., Python vs. Rust on the same machine against the same store) but should not be treated as controlled benchmarks.
- **Multi-broker fencing tests.** Testing that a second broker correctly fences the first requires running two broker instances against the same storage prefix. This is a distinct scenario with its own coordination complexity and should be specified separately.
- **Fault injection at the storage layer.** Simulating network partitions, throttling, or storage outages against a real object store is hard to do reliably. The existing unit tests cover the broker's reaction to `ConflictError` and consecutive flush failures. Stress tests should exercise the happy path under load, not failure injection.
- **Performance regression gates.** Latency numbers are logged for human review. Automated thresholds that gate CI are not in scope -- they would be noisy and environment-dependent.

## Scenarios

### 1. Throughput Under Moderate Concurrency

Multiple producers push tasks concurrently to a single queue while multiple consumers claim and finish tasks concurrently. The workload should be large enough to span many flush cycles (hundreds of tasks, not tens).

**Validates:** no lost tasks, no duplicate delivery, FIFO ordering, flush loop batching works correctly under concurrent mutation pressure.

### 2. High-Contention Flush Batching

A larger number of producers and consumers than scenario 1, creating more concurrent mutations per flush cycle. This exercises the double-buffer swap path where many waiters accumulate between flushes and must all be resolved correctly.

**Validates:** same invariants as scenario 1, with emphasis on the coordinator's ability to batch and resolve many waiters atomically.

### 3. Heartbeat Timeout and Re-Queuing

Some consumers claim tasks but never send heartbeats or finish them, simulating worker crashes. Healthy consumers run alongside them. After the heartbeat timeout, abandoned tasks should be re-queued and picked up by healthy consumers.

**Validates:** heartbeat timeout recovery, no lost tasks despite worker failure, re-queued tasks are claimable and completable.

### 4. Multi-Queue Isolation

Multiple queues operate simultaneously, each with its own producers and consumers. Payload identifiers are partitioned by queue so that cross-queue leakage is detectable.

**Validates:** queue isolation, per-queue correctness invariants hold independently, no cross-contamination of task payloads.

### 5. Compaction Under Sustained Load

Tasks are pushed, claimed, and finished over an extended period while the compaction worker runs on a short interval. After all tasks are processed and compaction has had time to run, the persisted queue state is read directly from the object store and inspected.

**Validates:** compaction removes completed tasks from persisted state, compaction does not drop pending or claimed tasks, compaction works correctly against real storage (not just in-memory).

## Environment Requirements

The harness must run against a real S3-compatible object store. Supported targets include AWS S3, Cloudflare R2, and MinIO (including a local MinIO container for development).

Credentials and bucket configuration should be provided via environment variables. When credentials are absent, tests should be skipped, not fail. This keeps the default `pytest` invocation fast and avoids CI failures in environments without S3 access.

Tests should be marked so they can be selected or excluded independently from the fast unit test suite (e.g., via pytest markers).

## Test Isolation

Each test run must use an isolated storage namespace (e.g., a unique key prefix) so that:

- Concurrent test runs against the same bucket do not interfere with each other.
- Leftover state from a previous run does not affect the current run.
- Teardown can clean up all objects created during the test by deleting everything under the prefix.

## Success Criteria

A stress test run passes if and only if:

1. All correctness invariants hold for every scenario (no lost tasks, no overlapping claims, queue isolation, compaction correctness).
2. No unhandled exceptions, broker fencing events, or unexpected error responses occur during normal operation (i.e., outside the intentional failure scenarios).
3. All created objects are cleaned up from the object store after the test completes.

Latency and throughput numbers are logged but do not determine pass/fail.
