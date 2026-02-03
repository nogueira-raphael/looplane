import asyncio
import os
from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage import LMDBStorage
from looplane.result.lmdb import LMDBResultBackend


DB_PATH = "/tmp/looplane"


os.makedirs(DB_PATH, exist_ok=True)


@register_task
async def hello_task(name: str):
    print(f"Running task for {name}")
    await asyncio.sleep(1)
    return f"Hello, {name}!"

async def main():
    storage = LMDBStorage(path=DB_PATH)
    result_backend = LMDBResultBackend(path=DB_PATH)

    queue = TaskQueue(storage=storage, result_backend=result_backend)
    worker = TaskWorker(queue)

    task = await queue.enqueue(hello_task, "LMDB Test", retries=2)

    worker.start()

    await asyncio.sleep(3)
    worker.stop()

    await worker.wait_until_done()

    results = await result_backend.get(task.id)
    for r in results:
        print(f"[RESULT] Success={r.success}, Value={r.value}, Error={r.error}")

if __name__ == "__main__":
    asyncio.run(main())
