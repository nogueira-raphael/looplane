class LooplaneError(Exception):
    """Base exception for Looplane"""
    pass

class TaskExecutionError(LooplaneError):
    """Raised when task execution fails but has retries left"""
    pass

class TaskRetryExhaustedError(LooplaneError):
    """Raised when a task fails and no retries are left"""
    pass


__all__ = [
    'TaskExecutionError',
    'TaskRetryExhaustedError',
]
