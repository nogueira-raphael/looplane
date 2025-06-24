import json
from datetime import datetime
from typing import List

import lmdb

from looplane.task import Task, get_task


class LMDBStorage:
    def __init__(self, path: str = "./task_queue_lmdb", map_size: int = 10485760):
        self.env = lmdb.open(path, map_size=map_size, max_dbs=1)

    def _serialize_task(self, task: Task) -> bytes:
        data = task.__dict__.copy()
        data.pop("func", None)
        data["created_at"] = data["created_at"].isoformat()
        data["updated_at"] = data["updated_at"].isoformat()
        return json.dumps(data).encode("utf-8")

    def _deserialize_task(self, data: bytes) -> Task:
        obj = json.loads(data.decode("utf-8"))
        obj["created_at"] = datetime.fromisoformat(obj["created_at"])
        obj["updated_at"] = datetime.fromisoformat(obj["updated_at"])
        func_name = obj.get("func_name")
        obj["func"] = get_task(func_name)
        return Task(**obj)

    async def save(self, task: Task):
        with self.env.begin(write=True) as txn:
            txn.put(task.id.encode("utf-8"), self._serialize_task(task))

    async def load_all(self) -> List[Task]:
        tasks = []
        with self.env.begin() as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                tasks.append(self._deserialize_task(value))
        return tasks


__all__ = ["LMDBStorage"]
