"""Module that provides essential logic to create data tasks."""
from __future__ import annotations
from dataclasses import KW_ONLY, dataclass, field
from typing import Generator, Optional, Protocol
from giorgio.targets import Target


@dataclass
class Task(Protocol):
    _: KW_ONLY
    batcheable: bool = field(default=False)
    output: Optional[Target] = field(default=None, repr=False)
    dependencies: tuple[Task, ...] = field(
        default=tuple(), repr=False)
    input: tuple[Target, ...] = field(default=tuple(), repr=False)

    def reduce(self, other: Task) -> Task:
        raise RuntimeError("%s doesn't support batching.", self.__class__)

    def complete(self) -> bool:
        """Indicate wether a task has already been successfully completed and it's desired artificats are in place."""
        if self.output:
            return self.output.exists()
        raise RuntimeError(
            "Can't determine if task %s completed because output is undefined." % repr(self))

    def run(self) -> Generator[tuple[Task, ...], tuple[Optional[Target], ...], None]:
        """Execute the task and create the expected data artifacts. This assues that the required upstream tasks have been completed"""
