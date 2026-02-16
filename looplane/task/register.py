from typing import Callable, Dict


class TaskRegistry:
    def __init__(self):
        self._registry: Dict[str, Callable] = {}

    def _fqn(self, func: Callable) -> str:
        return f"{func.__module__}.{func.__qualname__}"

    def register(self, func: Callable) -> Callable:
        # primary key
        fqn = self._fqn(func)
        self._registry[fqn] = func

        # backward compatibility alias
        short_name = func.__name__

        # IMPORTANT: use _registry (with underscore)
        if short_name not in self._registry:
            self._registry[short_name] = func

        return func

    def get(self, name: str) -> Callable:
        task_function = self._registry.get(name)
        if task_function is None:
            raise ValueError(f"Task function '{name}' is not registered.")
        return task_function

    def all(self) -> Dict[str, Callable]:
        return dict(self._registry)
