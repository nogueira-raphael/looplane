import asyncio
from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage.inmemory import InMemoryStorage



@register_task
async def sample_task(name: str):
    print(f"Processing: {name}")
    await asyncio.sleep(1)
    print(f"Finished: {name}")


async def main():
    queue = TaskQueue(storage=InMemoryStorage())
    worker = TaskWorker(queue)

    await queue.enqueue(sample_task, "task 1", retries=2)
    await queue.enqueue("sample_task", "task 2", retries=1)

    worker.start()
    await asyncio.sleep(3)
    worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
