import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum  # type: ignore
from os import getpid
from typing import Any, Callable, Dict, List, Optional, Tuple


class TaskStatus(StrEnum):
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    PENDING = "PENDING"


@dataclass(frozen=True, order=True)
class TaskResult:
    """Immutable record of a task execution outcome.

    Captures timing information, success/failure status, return value or error,
    and metadata about the execution context (PID, function name).
    """

    finished_at: datetime
    started_at: datetime = field(compare=False)
    fetched_from_storage_at: datetime = field(compare=False)
    success: bool = field(compare=False)
    task_id: str = field(compare=False)
    func_name: str = field(default="-", compare=False)
    pid: int = field(default_factory=getpid, compare=False)

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    value: Optional[Any] = field(default=None, compare=False)
    error: Optional[str] = field(default=None, compare=False)
    attempt: int = field(default=1, compare=False)
    max_retries: int = field(default=3, compare=False)

    def __str__(self) -> str:
        return (
            f"TaskResult {self.task_id[:8]} | "
            f"Success: {self.success} | "
            f"Attempt: {self.attempt}/{self.max_retries} | "
            f"Duration: {self.duration:.2f}s | "
            f"Value: {self.value} | "
            f"Error: {self.error or '-'}"
        )

    @property
    def is_retry(self) -> bool:
        """Returns True if this result is from a retry attempt."""
        return self.attempt > 1

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def duration(self) -> Optional[float]:
        """Time in seconds between start and finish time"""
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def wait_time(self) -> Optional[float]:
        """Time in seconds between fetch from storage and execution start"""
        if self.fetched_from_storage_at and self.started_at:
            return (self.started_at - self.fetched_from_storage_at).total_seconds()
        return None

    @property
    def total_time(self) -> Optional[float]:
        """Time in seconds between start and finish time"""
        if self.fetched_from_storage_at and self.finished_at:
            return (self.finished_at - self.fetched_from_storage_at).total_seconds()
        return None


@dataclass(order=True)
class Task:
    """Represents a unit of work to be executed by the task queue.

    Contains the callable function, its arguments, retry configuration,
    and status tracking. Tasks are ordered by creation time.
    """

    RUNNING = TaskStatus(TaskStatus.RUNNING)
    DONE = TaskStatus(TaskStatus.DONE)
    FAILED = TaskStatus(TaskStatus.FAILED)
    PENDING = TaskStatus(TaskStatus.PENDING)

    id: str

    sort_index: datetime = field(init=False, repr=False)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    func: Optional[Callable] = field(repr=False, default=None, compare=False)
    func_name: str = field(default="", compare=False)
    args: Tuple = field(default=(), compare=False)
    kwargs: Dict = field(default_factory=dict, compare=False)
    status: TaskStatus = field(default=PENDING, compare=False)
    max_retries: int = field(default=3, compare=False)
    retries_left: int = field(default=3, compare=False)
    timeout: Optional[int] = field(default=None, compare=False)
    results: List[TaskResult] = field(default_factory=list, compare=False)

    def __post_init__(self):
        self.sort_index = self.created_at

    @property
    def attempt(self) -> int:
        """Current attempt number (1 = first try, 2 = first retry, etc.)."""
        return self.max_retries - self.retries_left + 1

    @staticmethod
    def create(func, args=(), kwargs=None, retries=3, timeout=None):
        return Task(
            id=str(uuid.uuid4()),
            func=func,
            func_name=func.__name__,
            args=args,
            kwargs=kwargs or {},
            max_retries=retries,
            retries_left=retries,
            timeout=timeout,
        )


__all__ = [
    "Task",
    "TaskResult",
]
