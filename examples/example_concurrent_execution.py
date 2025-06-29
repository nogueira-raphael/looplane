import asyncio
from datetime import datetime

from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage.inmemory import InMemoryStorage
from looplane.result.inmemory import InMemoryResultBackend


def log(msg: str):
    print(f"{datetime.utcnow().time()} | {msg}")


@register_task
async def dummy_task(name: str, delay: float):
    log(f"[o] Start task {name} (delay {delay}s)")
    await asyncio.sleep(delay)
    log(f"[x] End task {name} (delay {delay}s)")
    return f"Finished {name}"


async def main():
    storage = InMemoryStorage()
    result_backend = InMemoryResultBackend()
    queue = TaskQueue(storage=storage, result_backend=result_backend)
    worker = TaskWorker(queue)

    task_a = await queue.enqueue(dummy_task, "A", 12)
    task_b = await queue.enqueue(dummy_task, "B", 11)
    task_c = await queue.enqueue(dummy_task, "C", 10)
    task_d = await queue.enqueue(dummy_task, "D", 9)
    task_e = await queue.enqueue(dummy_task, "E", 8)
    task_f = await queue.enqueue(dummy_task, "F", 7)
    task_g = await queue.enqueue(dummy_task, "G", 6)
    task_h = await queue.enqueue(dummy_task, "H", 5)
    task_i = await queue.enqueue(dummy_task, "I", 4)
    task_j = await queue.enqueue(dummy_task, "J", 3)
    task_k = await queue.enqueue(dummy_task, "K", 2)
    task_l = await queue.enqueue(dummy_task, "L", 1)
    tasks = [task_a, task_b, task_c, task_d, task_e, task_f, task_g, task_h, task_i, task_j, task_k, task_l]

    worker.start()
    await asyncio.sleep(1)
    worker.stop()
    await worker.wait_until_done()

    print("\nTask Results:")
    for task in tasks:
        results = await result_backend.get(task.id)
        for result in results:
            print(result)


if __name__ == "__main__":
    asyncio.run(main())
