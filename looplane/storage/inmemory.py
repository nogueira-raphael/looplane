from asyncio import Lock
from datetime import datetime
from typing import Dict, List, Optional

from looplane.storage.base import StorageBackend
from looplane.task import Task


class InMemoryStorage(StorageBackend):
    def __init__(self):
        self._tasks: Dict[str, Task] = {}
        self.lock = Lock()

    async def save(self, task: Task) -> None:
        async with self.lock:
            self._tasks[task.id] = task

    async def get_by_id(self, task_id: str) -> Optional[Task]:
        async with self.lock:
            return self._tasks.get(task_id)

    async def delete(self, task_id: str) -> None:
        async with self.lock:
            if task_id in self._tasks:
                del self._tasks[task_id]

    async def update(self, task: Task) -> None:
        async with self.lock:
            if task.id in self._tasks:
                task.updated_at = datetime.utcnow()
                self._tasks[task.id] = task

    async def fetch_batch(self, batch_size: int = 10) -> List[Task]:
        async with self.lock:
            pending_tasks = [
                t for t in self._tasks.values() if t.status == Task.PENDING
            ]
            return pending_tasks[:batch_size]


__all__ = ["InMemoryStorage"]
