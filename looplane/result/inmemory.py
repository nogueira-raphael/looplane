from collections import defaultdict
from typing import Dict, List

from looplane.result.base import AbstractResultBackend
from looplane.task.core import TaskResult


class InMemoryResultBackend(AbstractResultBackend):
    def __init__(self):
        self._by_task: Dict[str, List[TaskResult]] = defaultdict(list)
        self._by_id: dict[str, TaskResult] = {}

    async def save(self, result: TaskResult):
        self._by_id[result.id] = result
        if not self._by_task[result.id]:
            self._by_task[result.id] = []
        self._by_task[result.task_id].append(result)

    async def get(self, task_id: str) -> List[TaskResult]:
        return sorted(self._by_task.get(task_id, []), key=lambda r: r.finished_at)

    async def results(self) -> List[TaskResult]:
        return sorted(self._by_id.values(), key=lambda r: r.finished_at)
