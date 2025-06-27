from abc import ABC, abstractmethod
from typing import List, Optional

from looplane.task import Task


class StorageBackend(ABC):
    @abstractmethod
    async def get_by_id(self, task_id: str) -> Optional[Task]: ...

    @abstractmethod
    async def save(self, task: Task) -> None: ...

    @abstractmethod
    async def update(self, task: Task) -> None: ...

    @abstractmethod
    async def delete(self, task_id: str) -> None: ...

    @abstractmethod
    async def fetch_batch(self, batch_size: int = 10) -> List[Task]: ...
