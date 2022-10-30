from collections import deque
from enum import Enum
from functools import reduce
from graphlib import TopologicalSorter
from itertools import groupby
import logging
from time import sleep
from typing import Any, Deque, Dict, Iterable, Protocol, Sequence, Set
from giorgio.dag import create_dependency_dict
from giorgio.tasks import Task

LOGGER = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    SUCCESS = 0
    FAILED = 1
    DEPENDENCIES_UPDATED = 2


class TaskFailedException(RuntimeError):
    pass


class UpdatedDependenciesException(Exception):
    pass


class RetryClass(Protocol):
    RETRY: int
    pass


def retry(func):
    def f(self: RetryClass, *args, **kwargs):
        for retry in range(1, self.RETRY):
            try:
                func(self, *args, **kwargs)
                break
            except Exception as e:
                if retry < self.RETRY:
                    continue

        return f


class SimpleScheduler():
    RETRY = 3

    def __init__(self, task) -> None:
        self.task_queue: Deque[Task] = deque()
        self.dependency_dict: Dict[Task, Set[Task]] = {}
        self.task = task
        self.dependency_dict = create_dependency_dict(task)
        self.task_sorter = self.build_task_sorter()

    def has_not_completed(self):
        return self.task_sorter.is_active()

    def report_completed_task(self, task: Task):
        self.task_sorter.done(task)

    def add_dependencies(self, task: Task, dependencies: tuple[Task, ...]):

        self.dependency_dict[task].update(dependencies)
        for dep_task in dependencies:
            if dep_task.dependencies:
                self.dependency_dict.update(create_dependency_dict(dep_task))

    def get_tasks(self):
        return self.task_sorter.get_ready()

    def build_task_sorter(self):
        task_sorter = TopologicalSorter(self.dependency_dict)
        task_sorter.prepare()
        return task_sorter

    @retry
    def execute_task(self, task: Task):

        dependency_generator = task.run()
        self.report_completed_task(task)
        if dependency_generator is not None:
            dyn_dep = next(dependency_generator)
            if all(t.complete() for t in dyn_dep):
                dependency_generator.send(tuple(t.output for t in dyn_dep))
            else:
                self.add_dependencies(task, dyn_dep)
                self.task_sorter = self.build_task_sorter()
                self.task_queue.clear()

    def execute_task_queue(self):
        while len(self.task_queue) > 0:
            task = self.task_queue.pop()
            # only run the task if it hasn't completed
            if task.complete():
                LOGGER.info(
                    f"{repr(task)} is already complete")
                self.report_completed_task(task)
            else:
                try:
                    self.execute_task(task)
                except Exception as exc:
                    raise TaskFailedException(
                        "Can't proceed executing the pipeline because upstream dependency failed: %s" % repr(
                            task)
                    ) from exc

    def reduce_tasks(self, tasks: Iterable[Task]) -> Sequence[Task]:
        reduced_tasks = []
        for cls, grouped_tasks in groupby(tasks, key=lambda t: t):
            if cls.batcheable:
                batch_task = reduce(
                    lambda t1, t2: t1.reduce(t2), grouped_tasks)
                reduced_tasks.append(batch_task)
            else:
                reduced_tasks.extend(reduced_tasks)
        return reduced_tasks

    def queue_released_tasks(self):
        while len(self.task_queue) == 0:
            remaining_tasks = self.reduce_tasks(self.get_tasks())
            if len(remaining_tasks) == 0:
                sleep(1)
                LOGGER.info("Waiting for new tasks to become active.")
            self.task_queue.extend(remaining_tasks)

    def schedule(self):
        if self.task.complete():
            return ExecutionStatus.SUCCESS
        else:
            while self.has_not_completed():
                # loop that queues tasks
                self.queue_released_tasks()

                # loop that execute tasks
                self.execute_task_queue()


def schedule_execute(task: Task):
    s = SimpleScheduler(task)
    s.schedule()
