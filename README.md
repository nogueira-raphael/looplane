# Looplane

A lightweight, async-first task queue for Python focused on simplicity and pluggable backends.

> **Warning**: This project is in early development and not yet production-ready.

---

## Why Looplane?

Most task queues require external services like Redis, RabbitMQ, or Celery workers. Looplane takes a different approach:

- **Local-first**: No external dependencies required for basic usage
- **Async-native**: Built from the ground up with `asyncio`
- **Simple API**: Decorator-based task registration, minimal boilerplate
- **Pluggable backends**: Easy to swap storage and result backends

Looplane is ideal for:
- Local development and testing of async workflows
- Small-scale internal tools and scripts
- Learning async task queue concepts
- Prototyping before scaling to distributed systems

---

## Features

- **Async-first task queue** using `asyncio`
- **Decorator-based task registration** for clean, readable code
- **Producer/Consumer architecture** with clear separation of concerns
- **Pluggable storage backends**: InMemory (transient) and LMDB (persistent)
- **Pluggable result backends**: Store and query task execution results
- **Built-in retry mechanism** with attempt tracking
- **Concurrent task execution** with configurable parallelism
- **CLI monitor** for real-time task inspection

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Code                         │
│                                                                 │
│   @register_task                    producer.send(task, args)   │
│   async def my_task(): ...                    │                 │
└───────────────────────────────────────────────┼─────────────────┘
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          PRODUCER                               │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │ Serializer  │───▶│   Task      │───▶│  StorageBackend     │  │
│  │ (JSON, etc) │    │  Creation   │    │  (LMDB, Memory, ..) │  │
│  └─────────────┘    └─────────────┘    └─────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                                │
                                                ▼
                                    ┌───────────────────┐
                                    │   Task Storage    │
                                    │  (Queue/Broker)   │
                                    └───────────────────┘
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                          CONSUMER                               │
│  ┌─────────────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  StorageBackend     │───▶│   Task      │───▶│ Serializer  │  │
│  │  (LMDB, Memory, ..) │    │  Fetching   │    │ (JSON, etc) │  │
│  └─────────────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │  Consumer   │───▶│   Task      │───▶│  ResultBackend      │  │
│  │  .receive() │    │  Execution  │    │  (LMDB, Memory, ..) │  │
│  └─────────────┘    └─────────────┘    └─────────────────────┘  │
│                            │                                    │
│                     ack() / nack()                              │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

| Component | Responsibility | Location |
|-----------|---------------|----------|
| **Producer** | Creates and sends tasks to the queue | `looplane/broker/producer.py` |
| **Consumer** | Fetches tasks from the queue | `looplane/broker/consumer.py` |
| **Orchestrator** | Executes tasks and manages workers | `looplane/orchestrator.py` |
| **Serializer** | Converts tasks to/from bytes | `looplane/broker/serializer.py` |

### Pluggable Backends

| Backend Type | Interface | Implementations |
|--------------|-----------|-----------------|
| **Storage** | `StorageBackend` | `InMemoryStorage`, `LMDBStorage` |
| **Results** | `ResultBackend` | `InMemoryResultBackend`, `LMDBResultBackend` |
| **Serializer** | `Serializer` | `JSONSerializer` |

---

## Installation

```bash
# Clone the repository
git clone https://github.com/your-username/looplane.git
cd looplane

# Install with Poetry (includes CLI)
poetry install --extras cli
```

---

## Quick Start

```python
import asyncio
from looplane.task import register_task
from looplane.storage import InMemoryStorage
from looplane.result import InMemoryResultBackend
from looplane.broker import StorageProducer, StorageConsumer
from looplane.orchestrator import Orchestrator

# 1. Register tasks
@register_task
async def greet(name: str) -> str:
    return f"Hello, {name}!"

async def main():
    # 2. Setup backends
    storage = InMemoryStorage()
    result_backend = InMemoryResultBackend()

    # 3. Create Producer and send tasks
    producer = StorageProducer(storage=storage)
    await producer.send(greet, "Alice", retries=3)
    await producer.send(greet, "Bob", retries=3)

    # 4. Create Consumer and Orchestrator
    consumer = StorageConsumer(storage=storage)
    orchestrator = Orchestrator(
        consumer=consumer,
        result_backend=result_backend,
        max_concurrent_tasks=5,
    )

    # 5. Process tasks
    await orchestrator.start()
    await asyncio.sleep(2)
    await orchestrator.stop()

    # 6. Check results
    for result in await result_backend.results():
        print(f"{result.func_name}: {result.value}")

asyncio.run(main())
```

---

## Producer

The **Producer** is responsible for creating and sending tasks to the queue.

```python
from looplane.storage import LMDBStorage
from looplane.broker import StorageProducer

storage = LMDBStorage(path="/tmp/my-queue")
producer = StorageProducer(storage=storage)

# Send a single task
task = await producer.send(my_task, "arg1", "arg2", retries=3, timeout=30)

# Send multiple tasks
tasks = await producer.send_batch([
    (task1, ("arg1",), {}),
    (task2, ("arg2",), {"key": "value"}),
], retries=3)

# Close when done
await producer.close()
```

### Implementing a Custom Producer

```python
from looplane.broker import Producer, Serializer
from looplane.task import Task

class RedisProducer(Producer):
    def __init__(self, redis_url: str, serializer: Serializer):
        self._redis = Redis.from_url(redis_url)
        self._serializer = serializer

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    async def send(self, func, *args, retries=3, timeout=None, **kwargs) -> Task:
        task = self._create_task(func, args, kwargs, retries, timeout)
        data = self.serializer.serialize(task)
        await self._redis.lpush("tasks", data)
        return task

    async def send_batch(self, tasks, retries=3, timeout=None) -> List[Task]:
        # Implementation...

    async def close(self) -> None:
        await self._redis.close()
```

---

## Consumer

The **Consumer** is responsible for fetching tasks from the queue.

```python
from looplane.storage import LMDBStorage
from looplane.broker import StorageConsumer

storage = LMDBStorage(path="/tmp/my-queue")
consumer = StorageConsumer(storage=storage)

# Receive a single task (blocking with timeout)
task = await consumer.receive(timeout=5.0)

# Receive multiple tasks
tasks = await consumer.receive_batch(batch_size=10, timeout=1.0)

# Acknowledge success
await consumer.ack(task)

# Negative acknowledge (requeue for retry)
await consumer.nack(task, requeue=True)

# Close when done
await consumer.close()
```

### Implementing a Custom Consumer

```python
from looplane.broker import Consumer, Serializer
from looplane.task import Task

class RedisConsumer(Consumer):
    def __init__(self, redis_url: str, serializer: Serializer):
        self._redis = Redis.from_url(redis_url)
        self._serializer = serializer

    @property
    def serializer(self) -> Serializer:
        return self._serializer

    async def receive(self, timeout=None) -> Optional[Task]:
        data = await self._redis.brpop("tasks", timeout=timeout or 0)
        if data:
            return self.serializer.deserialize(data[1])
        return None

    async def receive_batch(self, batch_size=10, timeout=None) -> List[Task]:
        # Implementation...

    async def ack(self, task: Task) -> None:
        # Remove from processing set
        pass

    async def nack(self, task: Task, requeue=True) -> None:
        if requeue:
            await self._redis.lpush("tasks", self.serializer.serialize(task))

    async def close(self) -> None:
        await self._redis.close()
```

---

## Orchestrator

The **Orchestrator** coordinates task execution using a Consumer and ResultBackend.

```python
from looplane.orchestrator import Orchestrator

orchestrator = Orchestrator(
    consumer=consumer,
    result_backend=result_backend,
    max_concurrent_tasks=10,
)

# Start processing
await orchestrator.start()

# Check status
print(f"Active tasks: {orchestrator.active_tasks}")
print(f"Capacity: {orchestrator.capacity}")

# Stop gracefully (waits for running tasks)
await orchestrator.stop(wait=True)
```

---

## Serializer

The **Serializer** handles converting tasks to/from bytes for transport.

```python
from looplane.broker import JSONSerializer

serializer = JSONSerializer(encoding="utf-8")

# Serialize
data = serializer.serialize(task)  # -> bytes

# Deserialize
task = serializer.deserialize(data)  # -> Task
```

### Implementing a Custom Serializer

```python
from looplane.broker import Serializer
from looplane.task import Task
import msgpack

class MsgPackSerializer(Serializer):
    def serialize(self, task: Task) -> bytes:
        return msgpack.packb(task_to_dict(task))

    def deserialize(self, data: bytes) -> Task:
        return dict_to_task(msgpack.unpackb(data))
```

---

## CLI

Looplane includes a CLI for running workers and monitoring tasks.

### Worker

Start a worker to process tasks from the queue:

```bash
# Start worker with LMDB storage (default)
looplane worker -A myapp.tasks

# Start with custom concurrency
looplane worker -A myapp.tasks -c 20

# Start with in-memory storage (for testing)
looplane worker -A myapp.tasks --storage memory

# Custom database path
looplane worker -A myapp.tasks --db-path /var/lib/looplane
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-A, --app` | - | Application module to load (e.g., `myapp.tasks`) |
| `-c, --concurrency` | 10 | Number of concurrent tasks to process |
| `--storage` | lmdb | Storage backend: `memory` or `lmdb` |
| `--db-path` | /tmp/looplane | Database path (for LMDB storage) |

**Startup Banner:**

When started, the worker displays system information, configuration, and registered tasks:

```
╭──────────────────────────────────────────────────────────────────────────────╮
│ ╦  ╔═╗╔═╗╔═╗╦  ╔═╗╔╗╔╔═╗                                                     │
│ ║  ║ ║║ ║╠═╝║  ╠═╣║║║║╣                                                      │
│ ╩═╝╚═╝╚═╝╩  ╩═╝╩ ╩╝╚╝╚═╝                                                     │
│ Async Task Queue for Python                                                  │
╰──────────────────────────────────────────────────────────────────────────────╯
              System Information
┌──────────────────┬──────────────────────┐
│ Looplane Version │ 0.0.1                │
│ Python Version   │ 3.13.5               │
│ Platform         │ Linux-6.12...        │
│ Process ID       │ 12345                │
│ Started At       │ 2026-02-03 14:45:37  │
└──────────────────┴──────────────────────┘

              Configuration
┌─────────────────┬──────────────────────┐
│ Concurrency     │ 10                   │
│ Storage Backend │ LMDBStorage (...)    │
│ Result Backend  │ LMDBResultBackend    │
│ App Module      │ myapp.tasks          │
└─────────────────┴──────────────────────┘

           Registered Tasks (4)
┌───┬─────────────────┬─────────────────┐
│ # │ Task Name       │ Module          │
├───┼─────────────────┼─────────────────┤
│ 1 │ send_email      │ myapp.tasks     │
│ 2 │ process_image   │ myapp.tasks     │
│ 3 │ generate_report │ myapp.tasks     │
│ 4 │ flaky_task      │ myapp.tasks     │
└───┴─────────────────┴─────────────────┘

Worker ready. Press Ctrl+C to stop.
```

**Graceful Shutdown:**

Press `Ctrl+C` to stop the worker. It will finish processing current tasks before exiting.

### Monitor

View real-time task status and results with pagination:

```bash
# Monitor with in-memory backend
looplane monitor

# Monitor with LMDB backend
looplane monitor --storage lmdb --db-path /tmp/my-queue
```

**Features:**
- Real-time updates (1 second refresh)
- Tasks sorted by timestamp (newest first)
- Datetime columns: Fetched, Started, Finished
- Pagination for large task lists

**Keyboard Controls:**

| Key | Action |
|-----|--------|
| `→` / `j` / `Space` | Next page |
| `←` / `k` | Previous page |
| `g` | First page |
| `G` | Last page |
| `q` | Quit |

---

## Examples

| Example | Description |
|---------|-------------|
| `myapp/` | Complete worker CLI example with tasks and enqueue script |
| `example_producer_consumer.py` | Producer/Consumer architecture demo |
| `example_main_1.py` | Basic in-memory queue |
| `example_retry_1.py` | Retry mechanism demo |
| `example_retry_lmdb.py` | LMDB with retries |
| `example_monitor_demo.py` | Monitor UI demo |

### Running the Worker Example

```bash
# Terminal 1: Start the worker
looplane worker -A examples.myapp.tasks --storage lmdb

# Terminal 2: Enqueue tasks
python examples/myapp/enqueue.py

# Terminal 3 (optional): Monitor tasks
looplane monitor --storage lmdb
```

### Running Standalone Examples

```bash
poetry run python examples/example_producer_consumer.py
```

---

## API Reference

### Task Registration

```python
from looplane.task import register_task

@register_task
async def my_task(arg1: str, arg2: int) -> str:
    return f"Result: {arg1}, {arg2}"
```

### TaskResult Properties

```python
result.success      # bool: True if succeeded
result.value        # Any: Return value
result.error        # str: Error message
result.attempt      # int: Attempt number (1, 2, 3...)
result.max_retries  # int: Max attempts configured
result.is_retry     # bool: True if attempt > 1
result.duration     # float: Execution time (seconds)
```

---

## Known Limitations

- **Timeout not enforced**: The `timeout` field exists but is not enforced
- **Single-process only**: No distributed execution across machines
- **No task priorities**: FIFO processing only
- **No task dependencies**: Cannot define task chains or DAGs

---

## Roadmap

### High Priority
- [ ] Implement timeout enforcement
- [ ] Add unit tests
- [ ] Retry backoff strategies

### Medium Priority
- [ ] Redis storage/result backends
- [ ] Task dependencies and chaining
- [ ] CLI commands for enqueue and inspect

### Future
- [ ] RabbitMQ backend
- [ ] Web UI dashboard
- [ ] Task priorities
- [ ] DAG-style pipelines

---

## License

MIT License

---

## Contributing

Contributions welcome! Open an issue or PR to start a discussion.
