import asyncio
import logging

from looplane.queue import TaskQueue
from looplane.task import Task

_logger = logging.getLogger(__name__)


class TaskWorker:
    def __init__(self, queue: TaskQueue, max_concurrent_tasks: int = 10):
        self.queue = queue
        self.max_concurrent_tasks = max_concurrent_tasks
        self._running = False
        self._main_loop_task: asyncio.Task | None = None
        self._running_tasks: set[asyncio.Task] = set()

    @property
    def running(self):
        return self._running

    @running.setter
    def running(self, value):
        self._running = value

    @property
    def capacity(self):
        return self.max_concurrent_tasks - len(self._running_tasks)

    async def wait_until_done(self):
        if self._main_loop_task:
            await self._main_loop_task

    async def _run_single_task(self, task: Task):
        try:
            await self.queue.run_task(task)
        except Exception as error:  # noqa
            _logger.error(f"[WORKER] Error while running task {task.id}: {error}")

    async def _run_loop(self):
        while self.running or self._running_tasks:
            if self.capacity > 0:
                tasks = await self.queue.fetch_up_to(self.capacity)
                for task in tasks:
                    coro = self._run_single_task(task)
                    self._running_tasks.add(asyncio.create_task(coro))

            if not self._running_tasks:
                await asyncio.sleep(0.1)
                continue

            done, _ = await asyncio.wait(
                self._running_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            self._running_tasks.difference_update(done)

    def start(self):
        self._running = True
        self._main_loop_task = asyncio.create_task(self._run_loop())


    def stop(self):
        self.running = False


__all__ = ["TaskWorker"]
