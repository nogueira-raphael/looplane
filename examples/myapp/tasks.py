"""Example application tasks for testing the worker CLI.

Run the worker with:
    looplane worker -A examples.myapp.tasks --storage lmdb

Then enqueue tasks from another terminal with:
    python examples/myapp/enqueue.py
"""

import asyncio
import random

from looplane.task import register_task


@register_task
async def send_email(to: str, subject: str) -> str:
    """Simulate sending an email."""
    print(f"  [send_email] Sending email to {to}: {subject}")
    await asyncio.sleep(0.5)
    return f"Email sent to {to}"


@register_task
async def process_image(image_id: int, filters: str = "none") -> str:
    """Simulate processing an image."""
    print(f"  [process_image] Processing image {image_id} with filters: {filters}")
    await asyncio.sleep(random.uniform(0.5, 1.5))
    return f"Image {image_id} processed"


@register_task
async def generate_report(report_type: str, user_id: int) -> dict:
    """Simulate generating a report."""
    print(f"  [generate_report] Generating {report_type} report for user {user_id}")
    await asyncio.sleep(1.0)
    return {"report_type": report_type, "user_id": user_id, "status": "complete"}


@register_task
async def flaky_task(name: str) -> str:
    """A task that fails randomly (for testing retries)."""
    print(f"  [flaky_task] Processing {name}...")
    await asyncio.sleep(0.3)

    if random.random() < 0.5:
        raise ValueError(f"Random failure for {name}")

    return f"Success: {name}"
