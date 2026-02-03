from abc import ABC, abstractmethod
from typing import List, Optional

from looplane.task import Task


class StorageBackend(ABC):
    """Abstract base class for task storage backends.

    Storage backends are responsible for persisting pending tasks
    and providing them to the queue for execution. Implementations
    may be transient (in-memory) or persistent (LMDB, Redis, etc.).

    To implement a custom storage backend:
        1. Subclass StorageBackend
        2. Set _IS_TRANSIENT class attribute (True for in-memory, False for persistent)
        3. Implement all abstract methods

    Example:
        class RedisStorage(StorageBackend):
            _IS_TRANSIENT = False

            async def save(self, task: Task) -> None:
                # Save task to Redis
                ...
    """

    _IS_TRANSIENT: bool

    @property
    def is_transient(self) -> bool:
        """Returns True if data is lost when the process ends."""
        return self._IS_TRANSIENT

    @abstractmethod
    async def save(self, task: Task) -> None:
        """Persist a new task to storage.

        Args:
            task: The task to save.
        """
        ...

    @abstractmethod
    async def get_by_id(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by its ID.

        Args:
            task_id: The unique identifier of the task.

        Returns:
            The task if found, None otherwise.
        """
        ...

    @abstractmethod
    async def update(self, task: Task) -> None:
        """Update an existing task in storage.

        Args:
            task: The task with updated fields.
        """
        ...

    @abstractmethod
    async def delete(self, task_id: str) -> None:
        """Remove a task from storage.

        Args:
            task_id: The unique identifier of the task to delete.
        """
        ...

    @abstractmethod
    async def fetch_batch(self, batch_size: int = 10) -> List[Task]:
        """Fetch a batch of pending tasks for processing.

        Should return tasks with status PENDING, ordered by creation time.

        Args:
            batch_size: Maximum number of tasks to return.

        Returns:
            List of pending tasks, up to batch_size.
        """
        ...

    @abstractmethod
    async def count(self) -> int:
        """Return the total number of tasks in storage.

        Returns:
            Total count of tasks (all statuses).
        """
        ...

    @abstractmethod
    async def clear(self) -> None:
        """Remove all tasks from storage.

        Use with caution. Primarily intended for testing.
        """
        ...
