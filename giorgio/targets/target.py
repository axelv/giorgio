
import abc
from contextlib import AbstractContextManager
from typing import ContextManager, Protocol


class Target(Protocol):
    """Represents the data parition that will be created when executing a task."""

    def exists(self) -> bool:
        """Checks wether the target data exists."""

    def remove(self) -> bool:
        """Removes the artifacts that are represented by this task."""
