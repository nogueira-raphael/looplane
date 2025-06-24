import asyncio
from typing import Callable, Optional

from looplane.storages.lmdb_storage import LMDBStorage
from looplane.task import Task


class TaskQueue:
    def __init__(self, use_memory_queue: bool = True, persist: bool = True):
        self._use_memory_queue = use_memory_queue
        self._persist = persist
        self._queue: Optional[asyncio.Queue] = (
            asyncio.Queue() if use_memory_queue else None
        )
        self.storage = LMDBStorage() if persist else None

    async def enqueue(
        self, func: Callable, *args, retries=3, timeout: Optional[int] = None, **kwargs
    ) -> Task:
        task = Task.create(
            func=func, args=args, kwargs=kwargs, retries=retries, timeout=timeout
        )

        if self._persist and self.storage:
            await self.storage.save(task)

        if self._use_memory_queue and self._queue:
            await self._queue.put(task)

        return task

    async def get_next_task(self) -> Optional[Task]:
        if self._use_memory_queue and self._queue and not self._queue.empty():
            return await self._queue.get()

        if self._persist and self.storage:
            tasks = await self.storage.load_all()
            for task in tasks:
                if task.status == Task.PENDING:
                    return task

        return None

    async def run_immediately(self, func: Callable, *args, **kwargs):
        task = Task.create(func=func, args=args, kwargs=kwargs)
        return await self._run_task(task)

    async def _run_task(self, task: Task):
        try:
            task.status = Task.RUNNING
            if task.func:  # TODO: improve this
                if task.timeout:
                    await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs), timeout=task.timeout
                    )
                else:
                    await task.func(*task.args, **task.kwargs)
            else:
                raise Exception("Task func must have function.")  # TODO: improve this
            task.status = Task.DONE
        except Exception:  # noqa
            task.retries_left -= 1
            task.status = Task.FAILED
            if task.retries_left > 0:
                if self._persist and self.storage:
                    await self.storage.save(task)
                if self._use_memory_queue and self._queue:
                    await self._queue.put(task)
        else:
            if self._persist and self.storage:
                await self.storage.save(task)

    async def run_task(self, task: Task):
        return await self._run_task(task)


__all__ = ["TaskQueue"]
