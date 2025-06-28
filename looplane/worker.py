import asyncio
import logging

from looplane.queue import TaskQueue

_logger = logging.getLogger(__name__)


class TaskWorker:
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self._running = False

    @property
    def running(self):
        return self._running

    @running.setter
    def running(self, value):
        self._running = value

    async def start(self):
        self.running = True
        while self.running:
            task = await self.queue.get_next_task()
            if task is None:
                await asyncio.sleep(0.1)
                continue
            try:
                await self.queue.run_task(task)
            except Exception as error:  # noqa
                _logger.error(f"[WORKER] Error while running task {task.id}: {error}")

    def stop(self):
        self.running = False


__all__ = ["TaskWorker"]
