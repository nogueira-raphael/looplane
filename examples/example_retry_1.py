import asyncio

from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage.inmemory import InMemoryStorage


attempts = {"count": 0}

@register_task
async def flaky_task():
    attempts["count"] += 1
    print(f"Attempt #{attempts['count']}")

    await asyncio.sleep(1)

    if attempts["count"] < 3:
        raise Exception(f"Failure {attempts.get('count')}")

    print("Task succeeded!")

async def main():
    queue = TaskQueue(storage=InMemoryStorage())
    worker = TaskWorker(queue)

    await queue.enqueue(flaky_task, retries=3)

    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
