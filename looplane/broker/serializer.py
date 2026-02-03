"""Task serializer implementations."""

import json
from dataclasses import fields
from datetime import datetime
from typing import Any, Dict

from looplane.broker.base import Serializer
from looplane.task import Task, get_task


class JSONSerializer(Serializer):
    """JSON-based task serializer.

    Serializes Task objects to JSON bytes. Handles datetime conversion
    and excludes non-serializable fields (like the func callable).

    This is the default serializer suitable for most use cases.
    For better performance with large payloads, consider implementing
    a MsgPack or Pickle serializer.
    """

    def __init__(self, encoding: str = "utf-8"):
        """Initialize the JSON serializer.

        Args:
            encoding: Character encoding for JSON bytes.
        """
        self.encoding = encoding

    def serialize(self, task: Task) -> bytes:
        """Serialize a Task to JSON bytes.

        Args:
            task: The Task object to serialize.

        Returns:
            UTF-8 encoded JSON bytes.
        """
        def convert(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return {"__datetime__": obj.isoformat()}
            return obj

        # Extract serializable fields (exclude func which is a callable)
        data: Dict[str, Any] = {}
        for field in fields(task):
            if field.name == "func":
                continue  # Skip callable
            if not field.init:
                continue  # Skip computed fields
            value = getattr(task, field.name)
            data[field.name] = convert(value)

        return json.dumps(data, default=convert).encode(self.encoding)

    def deserialize(self, data: bytes) -> Task:
        """Deserialize JSON bytes to a Task object.

        Args:
            data: JSON bytes to deserialize.

        Returns:
            Reconstructed Task object with func resolved from registry.
        """
        def parse_value(value: Any) -> Any:
            if isinstance(value, dict) and "__datetime__" in value:
                return datetime.fromisoformat(value["__datetime__"])
            return value

        raw = json.loads(data.decode(self.encoding))

        # Parse datetime fields
        parsed = {k: parse_value(v) for k, v in raw.items()}

        # Resolve the function from registry
        func_name = parsed.get("func_name")
        if func_name:
            try:
                parsed["func"] = get_task(func_name)
            except ValueError:
                # Function not registered - will be resolved later
                parsed["func"] = None

        return Task(**parsed)


__all__ = ["JSONSerializer"]
