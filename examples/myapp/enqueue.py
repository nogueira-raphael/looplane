"""Script to enqueue tasks for the worker.

First start the worker:
    looplane worker -A examples.myapp.tasks --storage lmdb

Then run this script to enqueue tasks:
    python examples/myapp/enqueue.py
"""

import asyncio

# Import tasks to register them
from examples.myapp.tasks import (
    flaky_task,
    generate_report,
    process_image,
    send_email,
)

from looplane.broker import StorageProducer
from looplane.storage import LMDBStorage


async def main():
    # Use the same storage as the worker
    storage = LMDBStorage(path="/tmp/looplane")
    producer = StorageProducer(storage=storage)

    print("Enqueueing tasks...")

    # Enqueue various tasks
    await producer.send(send_email, "user@example.com", "Welcome!", retries=2)
    await producer.send(send_email, "admin@example.com", "Report Ready", retries=2)

    await producer.send(process_image, 101, filters="blur,resize", retries=3)
    await producer.send(process_image, 102, retries=3)
    await producer.send(process_image, 103, filters="grayscale", retries=3)

    await producer.send(generate_report, "monthly", 42, retries=2)

    # Enqueue some flaky tasks to test retries
    await producer.send(flaky_task, "task_a", retries=3)
    await producer.send(flaky_task, "task_b", retries=3)

    await producer.close()

    print(f"Enqueued {await storage.count()} tasks")
    print("Tasks will be processed by the worker")


if __name__ == "__main__":
    asyncio.run(main())
