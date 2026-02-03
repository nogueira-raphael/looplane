"""Abstract base classes for broker components."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from looplane.task import Task


class Serializer(ABC):
    """Abstract base class for task serialization.

    Serializers handle converting Task objects to bytes for transport
    and back to Task objects when received.

    To implement a custom serializer:
        1. Subclass Serializer
        2. Implement serialize() and deserialize() methods

    Example:
        class MsgPackSerializer(Serializer):
            def serialize(self, task: Task) -> bytes:
                return msgpack.packb(task_to_dict(task))

            def deserialize(self, data: bytes) -> Task:
                return dict_to_task(msgpack.unpackb(data))
    """

    @abstractmethod
    def serialize(self, task: Task) -> bytes:
        """Serialize a Task to bytes for transport.

        Args:
            task: The Task object to serialize.

        Returns:
            Bytes representation of the task.
        """
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Task:
        """Deserialize bytes back to a Task object.

        Args:
            data: Bytes to deserialize.

        Returns:
            Reconstructed Task object.
        """
        ...


class Producer(ABC):
    """Abstract base class for task producers.

    Producers are responsible for:
    - Creating Task objects from function calls
    - Serializing tasks for transport
    - Sending tasks to the broker/queue

    The Producer is used by application code to schedule tasks.

    To implement a custom producer:
        1. Subclass Producer
        2. Implement all abstract methods
        3. Configure with appropriate serializer and connection settings

    Example:
        class RedisProducer(Producer):
            def __init__(self, redis_url: str, serializer: Serializer):
                self.redis = Redis.from_url(redis_url)
                self.serializer = serializer

            async def send(self, func, *args, **kwargs) -> Task:
                task = self._create_task(func, args, kwargs)
                data = self.serializer.serialize(task)
                await self.redis.lpush("tasks", data)
                return task
    """

    @property
    @abstractmethod
    def serializer(self) -> Serializer:
        """The serializer used for task serialization."""
        ...

    @abstractmethod
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
        ...

    @abstractmethod
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
        ...

    @abstractmethod
    async def close(self) -> None:
        """Close the producer and release resources."""
        ...


class Consumer(ABC):
    """Abstract base class for task consumers.

    Consumers are responsible for:
    - Fetching tasks from the broker/queue
    - Deserializing tasks
    - Acknowledging or rejecting task completion

    The Consumer is used by the worker/orchestrator to receive tasks.

    To implement a custom consumer:
        1. Subclass Consumer
        2. Implement all abstract methods
        3. Configure with appropriate serializer and connection settings

    Example:
        class RedisConsumer(Consumer):
            def __init__(self, redis_url: str, serializer: Serializer):
                self.redis = Redis.from_url(redis_url)
                self.serializer = serializer

            async def receive(self) -> Optional[Task]:
                data = await self.redis.brpop("tasks", timeout=1)
                if data:
                    return self.serializer.deserialize(data[1])
                return None
    """

    @property
    @abstractmethod
    def serializer(self) -> Serializer:
        """The serializer used for task deserialization."""
        ...

    @abstractmethod
    async def receive(self, timeout: Optional[float] = None) -> Optional[Task]:
        """Receive the next task from the queue.

        Args:
            timeout: Optional timeout in seconds to wait for a task.
                     None means wait indefinitely, 0 means non-blocking.

        Returns:
            The next Task, or None if no task is available.
        """
        ...

    @abstractmethod
    async def receive_batch(
        self,
        batch_size: int = 10,
        timeout: Optional[float] = None,
    ) -> List[Task]:
        """Receive multiple tasks from the queue.

        Args:
            batch_size: Maximum number of tasks to receive.
            timeout: Optional timeout in seconds.

        Returns:
            List of tasks, may be less than batch_size.
        """
        ...

    @abstractmethod
    async def ack(self, task: Task) -> None:
        """Acknowledge successful task completion.

        Removes the task from the queue permanently.

        Args:
            task: The completed task.
        """
        ...

    @abstractmethod
    async def nack(self, task: Task, requeue: bool = True) -> None:
        """Negative acknowledge - task processing failed.

        Args:
            task: The failed task.
            requeue: If True, put task back in queue for retry.
                     If False, move to dead letter queue or discard.
        """
        ...

    @abstractmethod
    async def close(self) -> None:
        """Close the consumer and release resources."""
        ...


__all__ = [
    "Serializer",
    "Producer",
    "Consumer",
]
