import json
from asyncio import Lock
from dataclasses import fields
from datetime import datetime
from typing import List, Optional

import lmdb

from looplane.storage.base import StorageBackend
from looplane.task import Task, get_task


class LMDBStorage(StorageBackend):
    """Persistent storage backend using LMDB.

    Stores tasks in an LMDB database, providing durability across restarts.
    Tasks are serialized as JSON with datetime fields converted to ISO format.
    """

    _IS_TRANSIENT = False

    def __init__(self, path: str = "./task_queue_lmdb", map_size: int = 10485760):
        self.env = lmdb.open(path, map_size=map_size, max_dbs=1, subdir=True, lock=True)
        self.db = self.env.open_db(b"tasks")
        self.lock = Lock()

    # noinspection PyMethodMayBeStatic
    def _serialize_task(self, task: Task) -> bytes:
        def convert(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return obj

        raw = {
            f.name: convert(getattr(task, f.name))
            for f in fields(task)
            if f.init and f.name != "func"
        }
        return json.dumps(raw).encode("utf-8")

    # noinspection PyMethodMayBeStatic
    def _deserialize_task(self, raw: bytes) -> Task:
        def try_parse_datetime(value):
            try:
                return datetime.fromisoformat(value)
            except (ValueError, TypeError):
                return value

        data = json.loads(raw.decode("utf-8"))
        data = {k: try_parse_datetime(v) for k, v in data.items()}
        func_name = data.get("func_name")
        try:
            if func_name and get_task(func_name):
                data["func"] = get_task(func_name)
        except ValueError:  # TODO: it needs to be solved
            pass
        return Task(**data)

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

    async def count(self) -> int:
        async with self.lock:
            with self.env.begin(db=self.db) as txn:
                return txn.stat()["entries"]

    async def clear(self) -> None:
        async with self.lock:
            with self.env.begin(write=True, db=self.db) as txn:
                txn.drop(self.db, delete=False)


__all__ = ["LMDBStorage"]
