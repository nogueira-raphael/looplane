import json
from asyncio import Lock
from datetime import datetime
from typing import List, Optional

import lmdb

from looplane.storage.base import StorageBackend
from looplane.task import Task, get_task


class LMDBStorage(StorageBackend):
    def __init__(self, path: str = "./task_queue_lmdb", map_size: int = 10485760):
        self.env = lmdb.open(path, map_size=map_size, max_dbs=1, subdir=True, lock=True)
        self.db = self.env.open_db(b"tasks")
        self.lock = Lock()

    # noinspection PyMethodMayBeStatic
    def _serialize_task(self, task: Task) -> bytes:
        data = task.__dict__.copy()
        data.pop("func", None)
        data["created_at"] = data["created_at"].isoformat()
        data["updated_at"] = data["updated_at"].isoformat()
        return json.dumps(data).encode("utf-8")

    # noinspection PyMethodMayBeStatic
    def _deserialize_task(self, data: bytes) -> Task:
        obj = json.loads(data.decode("utf-8"))
        obj["created_at"] = datetime.fromisoformat(obj["created_at"])
        obj["updated_at"] = datetime.fromisoformat(obj["updated_at"])
        func_name = obj.get("func_name")
        obj["func"] = get_task(func_name)
        return Task(**obj)

    async def save(self, task: Task) -> None:
        async with self.lock:
            with self.env.begin(write=True, db=self.db) as txn:
                txn.put(task.id.encode("utf-8"), self._serialize_task(task))

    async def get_by_id(self, task_id: str) -> Optional[Task]:
        async with self.lock:
            with self.env.begin(db=self.db) as txn:
                raw = txn.get(task_id.encode("utf-8"))
                return self._deserialize_task(raw) if raw else None

    async def delete(self, task_id: str) -> None:
        async with self.lock:
            with self.env.begin(write=True, db=self.db) as txn:
                txn.delete(task_id.encode("utf-8"))

    async def update(self, task: Task) -> None:
        async with self.lock:
            task.updated_at = datetime.utcnow()
            with self.env.begin(write=True, db=self.db) as txn:
                txn.put(task.id.encode("utf-8"), self._serialize_task(task))

    async def fetch_batch(self, batch_size: int = 10) -> List[Task]:
        results = []
        async with self.lock:
            with self.env.begin(db=self.db) as txn:
                cursor = txn.cursor()
                for _, raw in cursor:
                    task = self._deserialize_task(raw)
                    if task.status == Task.PENDING:
                        results.append(task)
                        if len(results) >= batch_size:
                            break
        return results


__all__ = ["LMDBStorage"]
