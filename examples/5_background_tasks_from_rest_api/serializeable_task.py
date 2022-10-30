
from dataclasses import asdict
import json
from typing import Protocol


class SerializeableTask(Protocol):
    def serialize(self):
        """Serialize the task parameters to a JSON string."""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def deserialize(cls, serialized_task: str):
        """Deserialize the task parameters from a JSON string."""
        return cls(**json.loads(serialized_task))

    def run():
        """Execute the task."""
        pass
