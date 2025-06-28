import asyncio
import logging
import uuid
from datetime import datetime
from typing import Callable, Optional, Union

from looplane.exceptions import TaskExecutionError, TaskRetryExhaustedError
from looplane.result import AbstractResultBackend
from looplane.storage.base import StorageBackend
from looplane.task import Task, TaskResult, get_task

_logger = logging.getLogger(__name__)


class TaskQueue:
    def __init__(
        self,
        storage: StorageBackend,
        result_backend: Optional[AbstractResultBackend] = None,
        batch_size: int = 10,
    ):
        self._queue: asyncio.Queue = asyncio.Queue()
        self.storage = storage
        self.result_backend = result_backend
        self.batch_size = batch_size

    async def enqueue(
        self,
        func: Union[str, Callable],
        *args,
        retries: int = 3,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> Task:
        if callable(func):
            func_name = func.__name__
        elif isinstance(func, str):
            func_name = func
        else:
            raise ValueError("func must be a registered function or its name as string")

        task = Task(
            id=str(uuid.uuid4()),
            func_name=func_name,
            func=get_task(func_name),
            args=args,
            kwargs=kwargs,
            retries_left=retries,
            timeout=timeout,
        )
        await self.storage.save(task)
        return task

    async def get_next_task(self) -> Optional[Task]:
        if self._queue.empty():
            tasks = await self.storage.fetch_batch(self.batch_size)
            for task in tasks:
                task.status = Task.RUNNING
                await self.storage.update(task)
                await self._queue.put(task)

        return await self._queue.get()

    async def run_immediately(self, func: Callable, *args, **kwargs):
        task = Task.create(func=func, args=args, kwargs=kwargs)
        return await self._run_task(task)

    async def run_task(self, task: Task):
        return await self._run_task(task)

    async def _run_task(self, task: Task):
        start_time = datetime.utcnow()
        try:
            task.status = Task.RUNNING
            task.updated_at = start_time
            await self.storage.update(task)
            # TODO: implement else handling or improve this design
            if not callable(task.func):
                raise TypeError(f"Task {task.id} has a non-callable func: {task.func}")
            result = await task.func(*task.args, **task.kwargs)
            task.results.append(
                TaskResult(
                    success=True,
                    value=result,
                    started_at=start_time,
                    fetched_from_storage_at=start_time,  # TODO: it's not correct yet
                    finished_at=datetime.utcnow(),
                )
            )
            task.status = Task.DONE
            if self.result_backend:
                await self.result_backend.store_result(task.id, task.results[-1])
            await self.storage.delete(task.id)

        except Exception as error:  # noqa
            task.retries_left -= 1
            task.status = Task.FAILED
            task.updated_at = datetime.utcnow()
            task.results.append(
                TaskResult(
                    success=False,
                    error=str(error),
                    started_at=start_time,
                    fetched_from_storage_at=start_time,  # TODO: it's not correct yet
                    finished_at=datetime.utcnow(),
                )
            )

            if task.retries_left > 0:
                task.status = Task.PENDING
                await self.storage.update(task)
                if self.result_backend:
                    await self.result_backend.store_result(task.id, task.results[-1])
                _logger.warning(
                    f"[WARN] Task {task.id} failed. Retrying... ({task.retries_left} retries left)"
                )
                raise TaskExecutionError(str(error)) from error
            else:
                await self.storage.delete(task.id)
                if self.result_backend:
                    await self.result_backend.store_result(task.id, task.results[-1])
                _logger.error(f"[ERROR] Task {task.id} permanently failed: {error}")
                raise TaskRetryExhaustedError(str(error)) from error


__all__ = ["TaskQueue"]
