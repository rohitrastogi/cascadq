CAScadq (CAS -> Compare and Swap) is a simple object store backed task queue to share work across several producers and consumers.
- fifo with at least once consumption guarantees
- tasks may be long running (minutes)
- persistence layer must be robust to worker failures
- consumers only read from the head of the queue
- support thousands of tasks and QPS across producers and consumers
- explicitly not a log for high throughput streaming
- support different logical queues
- rely on durability and consistency guarantees of object storage - aggressively leverage object storage CAS primitives

client sketch
- management api:
    - create_queue(name, payload schema)
    - delete_queue(name)
- producer api
    - push(queue_id, per queue scoped payload schema)
- consumer api
    - claim(queue_id) -> per queue scoped payload (when a consumer has started processing an item)
        - while a consumer is working on a task it periodically sends heartbeats to the backend
    - finish(queue_id, task_id) -> (when a consumer has completed processing of an item)

client
- should be trivial to build libraries in different programming languagees
- start with a reference client using async Python
- transparently handles heartbeats and retries on server failures (after too many retries, fails permanently for programer to retry)
- retries should be configurable

storage
- use object storage
- must be abstract over different object storage implementations (AWS S3, Google GCS, Cloudflare R2)
- state of queue is a simple well typed JSON file on object storage
- very rough sketch like:

```
@dataclass
class State:
    # metadata on currently active broker
    current_broker: Broker
    queue_registry: dict[str, Queue]
    # key is queue name, value is queue contents -> blobs must conform to schema in schema registry
    messages: dict[str, list[Message]]

@dataclass
class Queue:
    create_timestamp: float
    live: true
    payload_schema: some serialized represenation of the schema


@dataclass
class Broker:
    host: todo
    ip: todo
    id: str # maybe required?

@dataclass
class Message:
    task_id: str
    create_timestamp: float
    type: Union[Push, Claim, Complete, Heartbeat]

@dataclass
class Push
    payload: msgpack?

broker
- all client apis (management and producer/consumer) are modeled as writes to log on broker
- broker buffers writes in memory and flushes writes durably to object storage to mitigate slow object storage writes
- `push()` requests apply schema validation on write (can consider doing this client side, but server side validation provides more guarantees)
- client requests are not acked until their update is flushed to object storage
- while object storage update is in flight, broker applies updates to in memory representation, maintains watermark of last flush
- if object storage update succeeds - all write requests associated with flush watermark are acked
- every write does a object storage compare and swap (CAS) - if CAS fails, another broker wrote - we are not the leader, we need to fail all requests (current design assumes one leader)
- if still leader, but write failed - retry write to object storage, continue buffering writes
- if too many object store failures in a row, fail all buffered writes (before and after watermark)
- introduce periodic background worker to clean up completed tasks and deleted queues to prevent file from growing indefinitely over time
- batching/stepping behavior is ok since tasks are relatively long running
- tasks that have not had a heartbeat within some configurable duration are transitioned back from claimed to pushed
    - may be simple to mark as closed so it cannot be claimed and then create new pushed object
    - this way, close requests associated with the old claimed requests can be denied
- if the broker dies, kubernetes/some orchestrator should just start a new instance which starts from the persisted state. new instance updates the log file with its broker metadata
    - becuase every flush does CAS using object storage api, old broker will realize something has changed and fail all of its client requests
- we will probably want to locking on the in memory state as much as possible for best throughput - perhaps we can start with finegrained locking on the in memory state
    - may be ok to serialize writes in memory within a queue since object storage will be the bottleneck
- consider using event loop style async architecture
    - server endpoints append to in memory queue
    - background async actors merge state, handle flushing, notifying requests of success, failures