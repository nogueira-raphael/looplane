from .core import Task, TaskResult
from .register import TaskRegistry

__all__ = [
    "Task",
    "TaskResult",
    "register_task",
    "get_task",
    "task_registry",
]

task_registry = TaskRegistry()


def register_task(func):
    """Register a function to be used in Task."""
    return task_registry.register(func)


def get_task(name: str):
    """Get a registered task function by name."""
    return task_registry.get(name)
