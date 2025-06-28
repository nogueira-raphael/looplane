from collections import defaultdict
from typing import Dict, List

from looplane.result.base import AbstractResultBackend
from looplane.task.core import TaskResult


class InMemoryResultBackend(AbstractResultBackend):
    def __init__(self):
        self.results: Dict[str, List[TaskResult]] = defaultdict(list)

    async def store_result(self, task_id: str, result: TaskResult):
        self.results[task_id].append(result)

    async def get_result(self, task_id: str) -> List[TaskResult]:
        return self.results.get(task_id, [])
