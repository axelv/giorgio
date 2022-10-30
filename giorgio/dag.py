import itertools
from giorgio import Task


def iter_dependency_dict(*tasks: Task):
    yield {task: set(task.dependencies) for task in tasks}
    for task in tasks:
        yield from iter_dependency_dict(*task.dependencies)


def create_dependency_dict(task: Task):
    iter_of_dicts = iter_dependency_dict(task)
    return dict(itertools.chain(*(d.items() for d in iter_of_dicts)))
