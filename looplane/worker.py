from looplane.queue import TaskQueue


class TaskWorker:
    def __init__(self, queue: TaskQueue):
        self.queue = queue
        self.running = False

    async def start(self):
        self.running = True
        while self.running:
            task = await self.queue.get_next_task()
            if task is None:
                continue
            await self.queue.run_task(task)

    def stop(self):
        self.running = False


__all__ = ["TaskWorker"]
