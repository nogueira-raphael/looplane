import asyncio

from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage.inmemory import InMemoryStorage


attempts = {"count": 0}
task_id_holder = {"id": None}


@register_task
async def flaky_task():
    attempts["count"] += 1
    print(f"Attempt #{attempts['count']}")

    await asyncio.sleep(1)

    if attempts["count"] < 3:
        raise Exception(f"Failure {attempts.get('count')}")

    return "Success!"


async def main():
    storage = InMemoryStorage()
    queue = TaskQueue(storage=storage)
    worker = TaskWorker(queue)

    task = await queue.enqueue(flaky_task, retries=3)
    task_id_holder["id"] = task.id  # type: ignore

    asyncio.create_task(worker.start())
    await asyncio.sleep(5)
    worker.stop()

    results = await queue.result_backend.get(task_id_holder["id"])  # noqa
    for result in results:
        print(result)


if __name__ == "__main__":
    asyncio.run(main())
