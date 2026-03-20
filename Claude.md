### Architecture

CAScadq is an object-storage-backed task queue. Clients push tasks, claim them, send heartbeats while processing, and finish them. All durable state lives in S3-compatible object storage as JSON files (one per queue), updated via CAS (conditional writes with ETags).

**Layers** (top to bottom):
- `server/routes.py` — HTTP handlers, parse request, call broker, return response
- `broker/broker.py` — orchestrator, maps queue names to flushers, exposes push/claim/heartbeat/finish. Never touches QueueState directly.
- `broker/queue_flusher.py` — single point of contact per queue. All mutations go through it (push, claim, finish, heartbeat, activate_lease, compact, timeout_expired_claims). Each method delegates to QueueState and auto-notifies the flush loop — callers never call notify manually. Owns the flush loop, retry state, and three-state lifecycle: healthy → recovering (flush exhaustion, auto-reloads from durable state) → fenced (CAS conflict, terminal).
- `broker/queue_state.py` — pure in-memory state: task lifecycle (pending → claimed → completed), write buffer, generation-based dirty tracking. No I/O, no flush scheduling.
- `storage/` — `ObjectStore` protocol with `S3ObjectStore` (production) and `InMemoryObjectStore` (tests). S3 backend supports hedged writes for tail latency.

**Durability model**: mutations are applied to in-memory state via the flusher, which schedules a flush automatically. Mutations that block the caller (push, claim, finish) return a `FlushWaiter`. Fire-and-forget mutations (heartbeat, activate_lease) schedule a flush for crash recovery but don't block. If the broker crashes before a flush, unflushed mutations are lost — but no client was told they succeeded.

**Background workers** (call flusher methods, never touch QueueState directly):
- `HeartbeatWorker` — calls `flusher.timeout_expired_claims()` on healthy queues
- `CompactionWorker` — calls `flusher.compact()` on healthy queues

**Lease lifecycle**: `Task.claim()` sets status to claimed but does NOT set `last_heartbeat`. The broker calls `flusher.activate_lease()` after the claim is durably flushed, which sets `last_heartbeat` and schedules a second flush. This prevents the heartbeat checker from re-queuing a task whose claim response hasn't reached the client yet. The activate_lease flush is needed for crash recovery — without it, durable state shows `last_heartbeat=None` and the task is never timed out.

**Client** (`client/client.py`): `CascadqClient` with HTTP retry. `ClaimedTask` context manager sends heartbeats in the background until `finish()` is durably acknowledged. Heartbeats continue through the finish RPC so slow flushes don't cause re-queues.

**Key invariants**:
- Generation-based dirty tracking: `acknowledge_flush(g)` only advances to generation `g`, so mutations arriving during a slow S3 write are not silently dropped.
- Per-queue isolation: each queue has its own flush loop, retry counter, and fencing state. A slow or failing queue does not affect others.
- Recovery version check: when reloading from durable state after flush exhaustion, the flusher verifies the ETag matches the last known version. A mismatch means another broker wrote → fence instead of recovering.

### Readability
- Optimize for code that is easy to review, not clever.
- Prefer early returns over deeply nested control flow.
- Define variables as close as practical to where they are used, unless lifting them earlier improves control flow, naming, or avoids repeated work.
- As a default file layout, keep public types and public functions near the top; keep private helpers lower in the file. Break this rule when local placement materially improves readability.
- Non-obvious algorithms should include comments that explain invariants, tradeoffs, edge cases, or implementation details that would not be clear from the code alone.
- Add docstrings for public classes, functions, and modules when they define behavior or contracts that are not obvious from the type signature alone.
- In Python, use a single leading underscore for non-public functions, methods, and module-level helpers.
- All functions and methods should have type annotations. Local variable annotations are only needed when the type would otherwise be unclear, such as empty containers, `None` initialization, or complex inferred types.
- Avoid `hasattr`, `getattr`, reflection, and type-based branching unless the problem is genuinely dynamic, such as plugin systems, metaprogramming, or boundary normalization of untyped data.
- Avoid broad polymorphic argument shapes. Prefer explicit types, enums, unions with clear semantics, or separate functions over APIs that accept many unrelated input forms.
- Avoid runtime dispatch on container types like `dict` vs `list` except at external boundaries where untyped data must be normalized.
- Use a logger instead of `print()` for application code.
- For stdlib logging, prefer deferred interpolation like `logger.info("x=%s", x)` over eager string formatting.
- Prefer top-level imports. Use local imports only for optional dependencies or to defer unusually expensive imports on rare code paths. Do not use local imports to work around circular dependencies.

### Design
- Before committing a public-facing module, review the public API surface: are internal details leaked? Are errors documented and translated into domain types? Would a caller find the API obvious without reading the implementation?
- Use Pydantic models at untrusted boundaries where data must be parsed and validated, such as CLI input, config files, HTTP payloads, LLM outputs, or environment-derived configuration.
- Prefer dataclasses for trusted internal state where the code can maintain invariants without repeated validation.
- Design abstractions to be easy to test. Favor lightweight dependency injection so collaborators can be replaced in tests without patching internals.
- Avoid circular dependencies. When multiple modules need shared helpers or shared types, move them to a descriptively named neutral module rather than creating ad hoc coupling.
- Make impossible states impossible to represent.
- Prefer enums, tagged unions, or distinct types over collections of loosely related optional fields when the object has a finite set of valid states.
- Do not use `Optional[...]` for stable internal state unless absence is a real and meaningful domain state.
- Once data has been validated at a trusted boundary, do not defensively revalidate it again inside the same trusted flow unless it crosses a new boundary or is materially transformed.
- Prefer strongly typed configuration loading for CLI and YAML-based configuration. Configuration should parse into typed objects early rather than being passed around as raw dictionaries.
- When sharing behavior across backends, prefer protocols for interface-only polymorphism. Use abstract base classes when shared implementation or shared state management materially reduces duplication and preserves invariants.
- Prefer pure functions where practical. Prefer immutable data by default, especially for values shared across layers or passed across boundaries. Use frozen dataclasses or frozen Pydantic models when immutability improves correctness or API clarity. Keep mutation localized to clearly stateful classes, owned internal data structures, and performance-critical paths.

### Testing
- Test behavior through public interfaces by default.
- Do not write tests that mock or assert against private state; use dependency injection so public behavior can be tested cleanly.
- Write tests for code with meaningful branching, algorithmic complexity, or subtle invariants.
- Avoid tests for trivial passthrough code unless that code encodes important business behavior or has broken before.
- Each test should have one primary reason to fail. Prefer one test per invariant, edge case, or business rule.
- Avoid duplicate tests that assert the same contract through nearly identical scenarios.
- It is acceptable to mock/patch lightweight ambient state such as environment variables, time, or filesystem temp paths when introducing an abstraction would add more complexity than value.
- Slow tests are acceptable when they document real integration assumptions. Mark them explicitly as slow and keep them limited in scope.

### Error handling
- Fail early on invalid configuration or unrecoverable invalid internal state.
- Raise specific exceptions when callers can reasonably handle them; use built-in exceptions consistently when a custom exception would add no value.
- Catch exceptions at external boundaries, such as API calls, SDK calls, subprocess execution, file I/O, parsing, or LLM calls, where you can add context, translate the failure, retry, or return a controlled error.
- Within trusted internal code paths, prefer letting invariant violations fail loudly rather than masking them with broad exception handling.
- As the system grows, define custom exception types where finer-grained handling or clearer error semantics are useful.

### Async and Concurrency
- Prefer structured concurrency. For request-scoped parallel work, use `asyncio.TaskGroup`. For long-lived background work, tasks must still be explicitly owned by a component with defined startup, cancellation, and shutdown behavior, such as `start()` / `stop()` methods or a context-managed lifecycle API.
- Never block the event loop with blocking I/O or CPU-heavy work; offload that work explicitly.

### Workflow
- Make one logical change per commit. Separate unrelated fixes, refactors, and feature work into different commits.
- When adding or materially changing production code, add or update tests in the same commit unless the change is purely mechanical and does not alter behavior.
- Run /simplify before each commit and apply any changes needed to keep the code easy to review.
- Run `uv run ruff check src/ tests/` and `uv run ty check src/` before each commit. Fix all errors before committing.
- Write clear commit messages that describe what changed and why.
- Use pyproject.toml for project metadata and dependency configuration.
- Use uv for package and environment management.