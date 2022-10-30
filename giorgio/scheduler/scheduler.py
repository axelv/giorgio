import logging
from typing import Protocol, TypeVar
from giorgio.tasks.task import Task

TypedTask = TypeVar("TypedTask", bound=Task, contravariant=True)


class Scheduler(Protocol[TypedTask]):
    """The Scheduler takes the responsibility that a task gets completed.
    Task can be dispatched to other machines to be executed or put in a wait queue to be executed later."""

    def schedule_task(self, task: TypedTask):
        """Schedule a task."""
        pass
