"""Task producer implementations."""

import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from looplane.broker.base import Producer, Serializer
from looplane.broker.serializer import JSONSerializer
from looplane.storage.base import StorageBackend
from looplane.task import Task, get_task


class StorageProducer(Producer):
    """Producer implementation using StorageBackend.

    This producer sends tasks to any StorageBackend implementation
    (InMemoryStorage, LMDBStorage, etc.).

    Example:
        from looplane.storage import LMDBStorage
        from looplane.broker import StorageProducer

        storage = LMDBStorage(path="/tmp/my-queue")
        producer = StorageProducer(storage=storage)

        # Send a task
        task = await producer.send(my_task, "arg1", "arg2", retries=3)
    """

    def __init__(
        self,
        storage: StorageBackend,
        serializer: Optional[Serializer] = None,
    ):
        """Initialize the storage producer.

        Args:
            storage: The storage backend to use.
            serializer: Optional serializer (default: JSONSerializer).
                        Used for any pre-processing of task data.
        """
        self._storage = storage
        self._serializer = serializer or JSONSerializer()

    @property
    def storage(self) -> StorageBackend:
        """The underlying storage backend."""
        return self._storage

    @property
    def serializer(self) -> Serializer:
        """The serializer used for task serialization."""
        return self._serializer

    def _create_task(
        self,
        func: Union[str, Callable],
        args: Tuple,
        kwargs: Dict,
        retries: int = 3,
        timeout: Optional[int] = None,
    ) -> Task:
        """Create a Task object from function and arguments.

        Args:
            func: The registered task function or its name.
            args: Positional arguments.
            kwargs: Keyword arguments.
            retries: Number of retry attempts.
            timeout: Optional timeout in seconds.

        Returns:
            A new Task object.

        Raises:
            ValueError: If func is not a valid task.
        """
        if callable(func):
            func_name = func.__name__
        elif isinstance(func, str):
            func_name = func
        else:
            raise ValueError("func must be a registered function or its name as string")

        # Validate function is registered
        resolved_func = get_task(func_name)

        return Task(
            id=str(uuid.uuid4()),
            func_name=func_name,
            func=resolved_func,
            args=args,
            kwargs=kwargs,
            max_retries=retries,
            retries_left=retries,
            timeout=timeout,
        )

    async def send(
        self,
        func: Union[str, Callable],
        *args: Any,
        retries: int = 3,
        timeout: Optional[int] = None,
        **kwargs: Any,
    ) -> Task:
        """Send a task to the queue.

        Args:
            func: The registered task function or its name.
            *args: Positional arguments to pass to the function.
            retries: Number of retry attempts on failure.
            timeout: Optional timeout in seconds.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The created Task object with assigned ID.

        Raises:
            ValueError: If func is not a registered task.
        """
        task = self._create_task(func, args, kwargs, retries, timeout)
        await self._storage.save(task)
        return task

    async def send_batch(
        self,
        tasks: List[Tuple[Union[str, Callable], Tuple, Dict]],
        retries: int = 3,
        timeout: Optional[int] = None,
    ) -> List[Task]:
        """Send multiple tasks to the queue.

        Args:
            tasks: List of (func, args, kwargs) tuples.
            retries: Number of retry attempts on failure (applies to all).
            timeout: Optional timeout in seconds (applies to all).

        Returns:
            List of created Task objects.
        """
        created_tasks = []
        for func, args, kwargs in tasks:
            task = self._create_task(func, args, kwargs, retries, timeout)
            await self._storage.save(task)
            created_tasks.append(task)
        return created_tasks

    async def close(self) -> None:
        """Close the producer.

        For StorageProducer, this is a no-op as the storage
        lifecycle is managed externally.
        """
        pass


__all__ = ["StorageProducer"]
