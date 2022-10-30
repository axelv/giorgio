import logging
from typing import Protocol
from giorgio.tasks import Task

LOGGER = logging.getLogger(__name__)


class Worker(Protocol):
    """Base Worker class defining interface of a worker and containing some common implementation logic.
    """

    def execute_task(self, task: Task):
        """Check if is complete and execute if necessary."""
        if task.complete():
            LOGGER.debug("Task %s is already complete.", repr(task))
        task.run()

    def start(self):
        """Start a (possibly blocking) listener for tasks to be executed"""
        pass
