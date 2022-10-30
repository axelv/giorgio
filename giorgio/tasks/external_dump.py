from dataclasses import dataclass
from giorgio.tasks import Task
from giorgio.targets import Target


@dataclass(frozen=True)
class ExternalDump(Task):
    output: Target
    input: tuple = ()
    dependencies: tuple = ()
