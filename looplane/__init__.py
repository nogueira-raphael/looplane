"""
Looplane - Async-first task queue for Python.

A lightweight, local-first task queue focused on simplicity and pluggable backends.

Quick Start:
    from looplane.task import register_task
    from looplane.storage import InMemoryStorage
    from looplane.broker import StorageProducer, StorageConsumer
    from looplane.orchestrator import Orchestrator

    @register_task
    async def my_task(name: str) -> str:
        return f"Hello, {name}!"

    # Producer side (enqueue tasks)
    storage = InMemoryStorage()
    producer = StorageProducer(storage=storage)
    await producer.send(my_task, "World", retries=3)

    # Consumer side (process tasks)
    consumer = StorageConsumer(storage=storage)
    orchestrator = Orchestrator(consumer=consumer)
    await orchestrator.start()
"""

__version__ = "0.0.1"
