import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum  # type: ignore
from typing import Callable, Dict, Optional, Tuple


class TaskStatus(StrEnum):
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    PENDING = "PENDING"


@dataclass
class Task:
    RUNNING = TaskStatus(TaskStatus.RUNNING)
    DONE = TaskStatus(TaskStatus.DONE)
    FAILED = TaskStatus(TaskStatus.FAILED)
    PENDING = TaskStatus(TaskStatus.PENDING)

    id: str
    func: Optional[Callable] = field(repr=False, default=None)
    func_name: str = ""
    args: Tuple = ()
    kwargs: Dict = field(default_factory=dict)
    status: TaskStatus = PENDING
    retries_left: int = 3
    timeout: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    @staticmethod
    def create(func, args=(), kwargs=None, retries=3, timeout=None):
        return Task(
            id=str(uuid.uuid4()),
            func=func,
            func_name=func.__name__,
            args=args,
            kwargs=kwargs or {},
            retries_left=retries,
            timeout=timeout,
        )


__all__ = [
    "Task",
]
