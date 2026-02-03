from abc import ABC, abstractmethod
from typing import List, Optional

from looplane.task.core import TaskResult


class ResultBackend(ABC):
    """Abstract base class for task result storage backends.

    Result backends store TaskResult objects after task execution,
    allowing inspection of task outcomes, timing metrics, and errors.

    To implement a custom result backend:
        1. Subclass ResultBackend
        2. Implement all abstract methods

    Example:
        class RedisResultBackend(ResultBackend):
            async def save(self, result: TaskResult) -> None:
                # Save result to Redis
                ...
    """

    @abstractmethod
    async def save(self, result: TaskResult) -> None:
        """Persist a task result.

        Args:
            result: The TaskResult to save.
        """
        ...

    @abstractmethod
    async def get_by_id(self, result_id: str) -> Optional[TaskResult]:
        """Retrieve a specific result by its ID.

        Args:
            result_id: The unique identifier of the result.

        Returns:
            The TaskResult if found, None otherwise.
        """
        ...

    @abstractmethod
    async def get(self, task_id: str) -> List[TaskResult]:
        """Retrieve all results for a specific task.

        A task may have multiple results if it was retried.

        Args:
            task_id: The unique identifier of the task.

        Returns:
            List of TaskResults for the task, ordered by finished_at.
        """
        ...

    @abstractmethod
    async def results(self) -> List[TaskResult]:
        """Retrieve all stored results.

        Returns:
            List of all TaskResults, ordered by finished_at.
        """
        ...

    @abstractmethod
    async def count(self) -> int:
        """Return the total number of results stored.

        Returns:
            Total count of results.
        """
        ...

    @abstractmethod
    async def clear(self) -> None:
        """Remove all results from storage.

        Use with caution. Primarily intended for testing.
        """
        ...


# Backward compatibility alias
AbstractResultBackend = ResultBackend
