"""
Broker module for task queue communication.

This module provides abstractions for:
- Producer: Sends tasks to the queue
- Consumer: Receives tasks from the queue
- Serializer: Handles task serialization/deserialization

These abstractions allow swapping the underlying transport (LMDB, Redis, RabbitMQ)
without changing application code.

Example usage:
    from looplane.storage import LMDBStorage
    from looplane.broker import StorageProducer, StorageConsumer

    storage = LMDBStorage(path="/tmp/my-queue")

    # Producer (used by application to enqueue tasks)
    producer = StorageProducer(storage=storage)
    task = await producer.send(my_task, "arg1", retries=3)

    # Consumer (used by worker to fetch tasks)
    consumer = StorageConsumer(storage=storage)
    task = await consumer.receive(timeout=5.0)
"""

from .base import Consumer, Producer, Serializer
from .consumer import StorageConsumer
from .producer import StorageProducer
from .serializer import JSONSerializer

__all__ = [
    # Abstract interfaces
    "Producer",
    "Consumer",
    "Serializer",
    # Implementations
    "StorageProducer",
    "StorageConsumer",
    "JSONSerializer",
]
