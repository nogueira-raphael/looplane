import json
from datetime import datetime
from typing import List, Optional

import lmdb

from looplane.result.base import ResultBackend
from looplane.task.core import TaskResult


class LMDBResultBackend(ResultBackend):
    """Persistent result backend using LMDB.

    Stores task results in an LMDB database with two indices:
    - by_id: Direct lookup by result ID
    - by_task: Lookup all results for a given task ID
    """

    def __init__(self, path: str, map_size: int = 10 * 1024 * 1024):
        self.env = lmdb.open(
            path,
            map_size=map_size,
            subdir=True,
            max_dbs=2,
            readonly=False,
            create=True,
            lock=True,
        )
        self._db_by_id = self.env.open_db(b"by_id")
        self._db_by_task = self.env.open_db(b"by_task")

    def _serialize_result(self, result: TaskResult) -> bytes:
        def convert(obj: object) -> str:
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

        raw = result.__dict__.copy()
        for key in ["started_at", "finished_at", "fetched_from_storage_at"]:
            if raw[key]:
                raw[key] = raw[key].isoformat()

        return json.dumps(raw, default=convert).encode("utf-8")

    def _deserialize_result(self, data: bytes) -> TaskResult:
        raw = json.loads(data.decode("utf-8"))

        for key in ["started_at", "finished_at", "fetched_from_storage_at"]:
            if raw.get(key):
                raw[key] = datetime.fromisoformat(raw[key])

        return TaskResult(**raw)

    async def save(self, result: TaskResult) -> None:
        task_key = result.task_id.encode("utf-8")
        result_key = result.id.encode("utf-8")
        serialized = self._serialize_result(result)

        with self.env.begin(write=True) as txn:
            txn.put(result_key, serialized, db=self._db_by_id)
            existing = txn.get(task_key, db=self._db_by_task)
            task_result_ids = json.loads(existing.decode()) if existing else []
            if result.id not in task_result_ids:
                task_result_ids.append(result.id)
                txn.put(task_key, json.dumps(task_result_ids).encode(), db=self._db_by_task)

    async def get_by_id(self, result_id: str) -> Optional[TaskResult]:
        with self.env.begin(db=self._db_by_id) as txn:
            data = txn.get(result_id.encode("utf-8"))
            if data:
                return self._deserialize_result(data)
            return None

    async def get(self, task_id: str) -> List[TaskResult]:
        results = []
        task_key = task_id.encode("utf-8")

        with self.env.begin(db=self._db_by_task) as txn:
            data = txn.get(task_key)
            if not data:
                return []

            result_ids = json.loads(data.decode())
            for result_id in result_ids:
                result_data = txn.get(result_id.encode(), db=self._db_by_id)
                if result_data:
                    results.append(self._deserialize_result(result_data))

        return sorted(results, key=lambda r: r.finished_at or "")

    async def results(self) -> List[TaskResult]:
        result_list = []

        with self.env.begin(db=self._db_by_id) as txn:
            cursor = txn.cursor()
            for _, val in cursor:
                result = self._deserialize_result(val)
                result_list.append(result)

        return sorted(result_list, key=lambda r: r.finished_at or "")

    async def count(self) -> int:
        with self.env.begin(db=self._db_by_id) as txn:
            return txn.stat()["entries"]

    async def clear(self) -> None:
        with self.env.begin(write=True) as txn:
            txn.drop(self._db_by_id, delete=False)
            txn.drop(self._db_by_task, delete=False)


__all__ = ["LMDBResultBackend"]
