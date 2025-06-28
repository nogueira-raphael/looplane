from abc import ABC, abstractmethod
from typing import List

from looplane.task.core import TaskResult


class AbstractResultBackend(ABC):
    @abstractmethod
    async def store_result(self, task_id: str, result: TaskResult): ...

    @abstractmethod
    async def get_result(self, task_id: str) -> List[TaskResult]: ...
