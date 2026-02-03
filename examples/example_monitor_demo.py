"""
Demo script to test the monitor UI with pending tasks.

Run this script in one terminal, then run the monitor in another:
    Terminal 1: poetry run python examples/example_monitor_demo.py
    Terminal 2: poetry run looplane monitor --storage lmdb --db-path /tmp/looplane
"""

import asyncio
import os
import random

from looplane.task import register_task
from looplane.queue import TaskQueue
from looplane.worker import TaskWorker
from looplane.storage import LMDBStorage
from looplane.result.lmdb import LMDBResultBackend


DB_PATH = "/tmp/looplane"
os.makedirs(DB_PATH, exist_ok=True)


@register_task
async def slow_task(task_num: int):
    """A slow task that takes 2-4 seconds."""
    delay = random.uniform(2, 4)
    print(f"Task {task_num}: starting (will take {delay:.1f}s)")
    await asyncio.sleep(delay)
    print(f"Task {task_num}: completed")
    return f"Result from task {task_num}"


@register_task
async def flaky_task(task_num: int):
    """A task that fails randomly."""
    if random.random() < 0.5:
        raise Exception(f"Random failure in task {task_num}")
    await asyncio.sleep(1)
    return f"Success from flaky task {task_num}"


async def main():
    storage = LMDBStorage(path=DB_PATH)
    result_backend = LMDBResultBackend(path=DB_PATH)
    queue = TaskQueue(storage=storage, result_backend=result_backend)

    # Use only 2 concurrent workers so tasks queue up
    worker = TaskWorker(queue, max_concurrent_tasks=2)

    print("Enqueueing 10 tasks...")
    for i in range(1, 11):
        if i % 3 == 0:
            await queue.enqueue(flaky_task, i, retries=3)
            print(f"  Enqueued flaky_task({i})")
        else:
            await queue.enqueue(slow_task, i, retries=2)
            print(f"  Enqueued slow_task({i})")

    print("\nStarting worker (2 concurrent tasks)...")
    print("Open another terminal and run:")
    print("  poetry run looplane monitor --storage lmdb --db-path /tmp/looplane\n")

    worker.start()

    # Run for 30 seconds
    await asyncio.sleep(30)

    worker.stop()
    await worker.wait_until_done()

    print("\n--- Final Results ---")
    results = await result_backend.results()
    for r in results:
        status = "OK" if r.success else "FAIL"
        print(f"  [{status}] {r.func_name} attempt {r.attempt}/{r.max_retries}")


if __name__ == "__main__":
    asyncio.run(main())
