"""Task consumer implementations."""

import asyncio
from typing import List, Optional

from looplane.broker.base import Consumer, Serializer
from looplane.broker.serializer import JSONSerializer
from looplane.storage.base import StorageBackend
from looplane.task import Task


class StorageConsumer(Consumer):
    """Consumer implementation using StorageBackend.

    This consumer fetches tasks from any StorageBackend implementation
    (InMemoryStorage, LMDBStorage, etc.).

    The consumer handles:
    - Fetching pending tasks
    - Marking tasks as RUNNING when received
    - Acknowledging completion (deletes task)
    - Negative acknowledgment (requeues or deletes based on retries)

    Example:
        from looplane.storage import LMDBStorage
        from looplane.broker import StorageConsumer

        storage = LMDBStorage(path="/tmp/my-queue")
        consumer = StorageConsumer(storage=storage)

        # Receive tasks
        task = await consumer.receive(timeout=5.0)
        if task:
            try:
                # Process task...
                await consumer.ack(task)
            except Exception:
                await consumer.nack(task, requeue=True)
    """

    def __init__(
        self,
        storage: StorageBackend,
        serializer: Optional[Serializer] = None,
        poll_interval: float = 0.1,
    ):
        """Initialize the storage consumer.

        Args:
            storage: The storage backend to use.
            serializer: Optional serializer (default: JSONSerializer).
            poll_interval: Seconds between polls when waiting for tasks.
        """
        self._storage = storage
        self._serializer = serializer or JSONSerializer()
        self._poll_interval = poll_interval
        self._closed = False

    @property
    def storage(self) -> StorageBackend:
        """The underlying storage backend."""
        return self._storage

    @property
    def serializer(self) -> Serializer:
        """The serializer used for task deserialization."""
        return self._serializer

    async def receive(self, timeout: Optional[float] = None) -> Optional[Task]:
        """Receive the next task from the queue.

        Fetches a pending task from storage and marks it as RUNNING.

        Args:
            timeout: Optional timeout in seconds to wait for a task.
                     None means wait indefinitely, 0 means non-blocking.

        Returns:
            The next Task, or None if no task is available (on timeout).
        """
        if self._closed:
            raise RuntimeError("Consumer is closed")

        start_time = asyncio.get_event_loop().time()

        while True:
            tasks = await self._storage.fetch_batch(batch_size=1)

            if tasks:
                task = tasks[0]
                task.status = Task.RUNNING
                await self._storage.update(task)
                return task

            # Non-blocking mode
            if timeout == 0:
                return None

            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    return None

            # Wait before next poll
            await asyncio.sleep(self._poll_interval)

    async def receive_batch(
        self,
        batch_size: int = 10,
        timeout: Optional[float] = None,
    ) -> List[Task]:
        """Receive multiple tasks from the queue.

        Fetches pending tasks from storage and marks them as RUNNING.

        Args:
            batch_size: Maximum number of tasks to receive.
            timeout: Optional timeout in seconds.

        Returns:
            List of tasks, may be less than batch_size.
        """
        if self._closed:
            raise RuntimeError("Consumer is closed")

        start_time = asyncio.get_event_loop().time()

        while True:
            tasks = await self._storage.fetch_batch(batch_size=batch_size)

            if tasks:
                for task in tasks:
                    task.status = Task.RUNNING
                    await self._storage.update(task)
                return tasks

            # Non-blocking mode
            if timeout == 0:
                return []

            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout:
                    return []

            # Wait before next poll
            await asyncio.sleep(self._poll_interval)

    async def ack(self, task: Task) -> None:
        """Acknowledge successful task completion.

        Removes the task from the queue permanently.

        Args:
            task: The completed task.
        """
        task.status = Task.DONE
        await self._storage.delete(task.id)

    async def nack(self, task: Task, requeue: bool = True) -> None:
        """Negative acknowledge - task processing failed.

        Args:
            task: The failed task.
            requeue: If True, put task back in queue for retry.
                     If False, delete the task (used when retries exhausted).
        """
        if requeue and task.retries_left > 0:
            task.retries_left -= 1
            task.status = Task.PENDING
            await self._storage.update(task)
        else:
            task.status = Task.FAILED
            await self._storage.delete(task.id)

    async def close(self) -> None:
        """Close the consumer.

        After closing, receive operations will raise RuntimeError.
        """
        self._closed = True


__all__ = ["StorageConsumer"]
