"""
Example demonstrating the Producer/Consumer architecture.

This example shows how to:
1. Use a Producer to enqueue tasks
2. Use a Consumer with Orchestrator to process tasks
3. Store results in a ResultBackend

Run with: poetry run python examples/example_producer_consumer.py
"""

import asyncio

from looplane.task import register_task
from looplane.storage import InMemoryStorage
from looplane.result import InMemoryResultBackend
from looplane.broker import StorageProducer, StorageConsumer
from looplane.orchestrator import Orchestrator


# === Task Definitions ===

@register_task
async def process_item(item_id: int) -> str:
    """Simulate processing an item."""
    print(f"  Processing item {item_id}...")
    await asyncio.sleep(0.5)
    return f"Item {item_id} processed"


@register_task
async def flaky_task(name: str) -> str:
    """A task that fails sometimes."""
    import random
    if random.random() < 0.5:
        raise ValueError(f"Random failure for {name}")
    return f"Success: {name}"


# === Producer Example ===

async def producer_example():
    """Demonstrate using a Producer to enqueue tasks."""
    print("\n=== Producer Example ===\n")

    storage = InMemoryStorage()
    producer = StorageProducer(storage=storage)

    # Send individual tasks
    print("Sending individual tasks...")
    task1 = await producer.send(process_item, 1, retries=2)
    task2 = await producer.send(process_item, 2, retries=2)
    print(f"  Created task: {task1.id[:8]} - {task1.func_name}")
    print(f"  Created task: {task2.id[:8]} - {task2.func_name}")

    # Send batch of tasks
    print("\nSending batch of tasks...")
    batch = [
        (process_item, (3,), {}),
        (process_item, (4,), {}),
        (flaky_task, ("test",), {}),
    ]
    tasks = await producer.send_batch(batch, retries=3)
    for task in tasks:
        print(f"  Created task: {task.id[:8]} - {task.func_name}")

    await producer.close()
    print(f"\nTotal tasks in queue: {await storage.count()}")
    return storage


# === Consumer Example ===

async def consumer_example(storage: InMemoryStorage):
    """Demonstrate using a Consumer with Orchestrator to process tasks."""
    print("\n=== Consumer Example ===\n")

    result_backend = InMemoryResultBackend()
    consumer = StorageConsumer(storage=storage)

    orchestrator = Orchestrator(
        consumer=consumer,
        result_backend=result_backend,
        max_concurrent_tasks=2,  # Process 2 tasks at a time
    )

    print("Starting orchestrator...")
    await orchestrator.start()

    # Wait for tasks to complete (simple timeout approach)
    print("Processing tasks...\n")
    await asyncio.sleep(5)  # Give time for tasks to complete

    await orchestrator.stop()
    await consumer.close()

    # Show results
    print("\n=== Results ===\n")
    results = await result_backend.results()
    for result in results:
        status = "SUCCESS" if result.success else "FAILED"
        print(
            f"  [{status}] {result.func_name} "
            f"(attempt {result.attempt}/{result.max_retries}) - "
            f"{result.value or result.error}"
        )

    return result_backend


# === Main ===

async def main():
    # Run producer
    storage = await producer_example()

    # Run consumer
    await consumer_example(storage)

    print("\n=== Done ===")


if __name__ == "__main__":
    asyncio.run(main())
