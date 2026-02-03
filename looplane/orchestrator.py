"""Task orchestrator for coordinating task execution."""

import asyncio
import logging
from datetime import datetime
from typing import Optional, Set

from looplane.broker.base import Consumer
from looplane.result.base import ResultBackend
from looplane.task import Task, TaskResult

_logger = logging.getLogger(__name__)


class Orchestrator:
    """Coordinates task execution using a Consumer and ResultBackend.

    The Orchestrator is responsible for:
    - Fetching tasks from the Consumer
    - Executing tasks concurrently
    - Recording results to the ResultBackend
    - Handling retries via Consumer.ack/nack

    This is the main component used to run a worker process.

    Example:
        from looplane.storage import LMDBStorage
        from looplane.result import LMDBResultBackend
        from looplane.broker import StorageConsumer
        from looplane.orchestrator import Orchestrator

        storage = LMDBStorage(path="/tmp/my-queue")
        result_backend = LMDBResultBackend(path="/tmp/my-queue")
        consumer = StorageConsumer(storage=storage)

        orchestrator = Orchestrator(
            consumer=consumer,
            result_backend=result_backend,
            max_concurrent_tasks=10,
        )

        # Run the orchestrator
        await orchestrator.start()
        await asyncio.sleep(60)  # Run for 60 seconds
        await orchestrator.stop()
    """

    def __init__(
        self,
        consumer: Consumer,
        result_backend: Optional[ResultBackend] = None,
        max_concurrent_tasks: int = 10,
    ):
        """Initialize the orchestrator.

        Args:
            consumer: The consumer to fetch tasks from.
            result_backend: Optional backend to store task results.
            max_concurrent_tasks: Maximum number of concurrent task executions.
        """
        self._consumer = consumer
        self._result_backend = result_backend
        self._max_concurrent = max_concurrent_tasks
        self._running = False
        self._main_task: Optional[asyncio.Task] = None
        self._running_tasks: Set[asyncio.Task] = set()

    @property
    def consumer(self) -> Consumer:
        """The consumer used to fetch tasks."""
        return self._consumer

    @property
    def result_backend(self) -> Optional[ResultBackend]:
        """The result backend for storing task outcomes."""
        return self._result_backend

    @property
    def running(self) -> bool:
        """Whether the orchestrator is currently running."""
        return self._running

    @property
    def capacity(self) -> int:
        """Number of additional tasks that can be executed concurrently."""
        return self._max_concurrent - len(self._running_tasks)

    @property
    def active_tasks(self) -> int:
        """Number of currently executing tasks."""
        return len(self._running_tasks)

    async def _execute_task(self, task: Task) -> None:
        """Execute a single task and handle the result.

        Args:
            task: The task to execute.
        """
        start_time = datetime.utcnow()
        current_attempt = task.attempt

        try:
            if not callable(task.func):
                raise TypeError(f"Task {task.id} has a non-callable func: {task.func}")

            # Execute the task
            result = await task.func(*task.args, **task.kwargs)

            # Create success result
            task_result = TaskResult(
                task_id=task.id,
                func_name=task.func_name,
                success=True,
                value=result,
                started_at=start_time,
                fetched_from_storage_at=start_time,
                finished_at=datetime.utcnow(),
                attempt=current_attempt,
                max_retries=task.max_retries,
            )

            # Save result and acknowledge
            if self._result_backend:
                await self._result_backend.save(task_result)
            await self._consumer.ack(task)

            _logger.debug(f"Task {task.id} completed successfully")

        except Exception as error:
            # Create failure result
            task_result = TaskResult(
                task_id=task.id,
                func_name=task.func_name,
                success=False,
                error=str(error),
                started_at=start_time,
                fetched_from_storage_at=start_time,
                finished_at=datetime.utcnow(),
                attempt=current_attempt,
                max_retries=task.max_retries,
            )

            # Save result
            if self._result_backend:
                await self._result_backend.save(task_result)

            # Handle retry logic
            if task.retries_left > 1:
                await self._consumer.nack(task, requeue=True)
                _logger.warning(
                    f"Task {task.id} failed, requeueing. "
                    f"({task.retries_left - 1} retries left)"
                )
            else:
                await self._consumer.nack(task, requeue=False)
                _logger.error(f"Task {task.id} permanently failed: {error}")

    async def _run_loop(self) -> None:
        """Main orchestrator loop."""
        while self._running or self._running_tasks:
            # Fetch tasks up to capacity
            if self._running and self.capacity > 0:
                tasks = await self._consumer.receive_batch(
                    batch_size=self.capacity,
                    timeout=0.1,
                )

                for task in tasks:
                    coro = self._execute_task(task)
                    self._running_tasks.add(asyncio.create_task(coro))

            # Wait for at least one task to complete
            if self._running_tasks:
                done, _ = await asyncio.wait(
                    self._running_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                self._running_tasks.difference_update(done)
            else:
                await asyncio.sleep(0.1)

    async def start(self) -> None:
        """Start the orchestrator.

        The orchestrator will begin fetching and executing tasks.
        Call stop() to gracefully shut down.
        """
        if self._running:
            raise RuntimeError("Orchestrator is already running")

        self._running = True
        self._main_task = asyncio.create_task(self._run_loop())
        _logger.info(
            f"Orchestrator started with max_concurrent_tasks={self._max_concurrent}"
        )

    async def stop(self, wait: bool = True) -> None:
        """Stop the orchestrator.

        Args:
            wait: If True, wait for all running tasks to complete.
                  If False, cancel running tasks immediately.
        """
        self._running = False

        if wait and self._main_task:
            await self._main_task
        elif self._main_task:
            self._main_task.cancel()
            try:
                await self._main_task
            except asyncio.CancelledError:
                pass

        _logger.info("Orchestrator stopped")

    async def wait(self) -> None:
        """Wait for the orchestrator to finish.

        Blocks until the orchestrator is stopped and all tasks complete.
        """
        if self._main_task:
            await self._main_task


__all__ = ["Orchestrator"]
