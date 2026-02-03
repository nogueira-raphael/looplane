from collections import defaultdict
from typing import Dict, List, Optional

from looplane.result.base import ResultBackend
from looplane.task.core import TaskResult


class InMemoryResultBackend(ResultBackend):
    """In-memory result backend for storing task execution results.

    Stores results in dictionaries indexed by result ID and task ID.
    Data is lost when the process ends. Suitable for development and testing.
    """

    def __init__(self):
        self._by_task: Dict[str, List[TaskResult]] = defaultdict(list)
        self._by_id: Dict[str, TaskResult] = {}

    async def save(self, result: TaskResult) -> None:
        self._by_id[result.id] = result
        self._by_task[result.task_id].append(result)

    async def get_by_id(self, result_id: str) -> Optional[TaskResult]:
        return self._by_id.get(result_id)

    async def get(self, task_id: str) -> List[TaskResult]:
        return sorted(self._by_task.get(task_id, []), key=lambda r: r.finished_at)

    async def results(self) -> List[TaskResult]:
        return sorted(self._by_id.values(), key=lambda r: r.finished_at)

    async def count(self) -> int:
        return len(self._by_id)

    async def clear(self) -> None:
        self._by_id.clear()
        self._by_task.clear()
