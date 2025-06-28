from abc import ABC, abstractmethod
from typing import List

from looplane.task.core import TaskResult


class AbstractResultBackend(ABC):
    @abstractmethod
    async def save(self, result: TaskResult): ...

    @abstractmethod
    async def results(self) -> List[TaskResult]: ...

    @abstractmethod
    async def get(self, task_id: str) -> List[TaskResult]: ...
