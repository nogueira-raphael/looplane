import asyncio
import os

from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage import LMDBStorage
from looplane.result.lmdb import LMDBResultBackend


DB_PATH = "/tmp/looplane"
os.makedirs(DB_PATH, exist_ok=True)

attempts = {"count": 0}


@register_task
async def flaky_task():
    """A task that fails twice before succeeding."""
    attempts["count"] += 1
    print(f"Attempt #{attempts['count']}")
    await asyncio.sleep(0.5)

    if attempts["count"] < 3:
        raise Exception(f"Simulated failure #{attempts['count']}")

    return "Success after retries!"


async def main():
    storage = LMDBStorage(path=DB_PATH)
    result_backend = LMDBResultBackend(path=DB_PATH)
    queue = TaskQueue(storage=storage, result_backend=result_backend)
    worker = TaskWorker(queue)

    print("Enqueueing flaky task (will fail 2x, succeed on 3rd attempt)...")
    task = await queue.enqueue(flaky_task, retries=3)

    worker.start()
    await asyncio.sleep(5)
    worker.stop()
    await worker.wait_until_done()

    print("\n--- Results ---")
    results = await result_backend.get(task.id)
    for result in results:
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
