from typing import Callable, Dict


class TaskRegistry:
    def __init__(self):
        self._registry: Dict[str, Callable] = {}

    def register(self, func: Callable) -> Callable:
        self._registry[func.__name__] = func
        return func

    def get(self, name: str) -> Callable:
        task_function = self._registry.get(name)
        if task_function is None:
            raise ValueError(f"Task function '{name}' is not registered.")
        return task_function

    def all(self) -> Dict[str, Callable]:
        return self._registry
