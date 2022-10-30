from dataclasses import KW_ONLY, dataclass
from typing import Tuple
from giorgio.serialisation import deserialize_task_from_json, serialize_task_to_json
from giorgio.targets import LocalFileTarget
from giorgio.tasks import SerializeableTask


@dataclass
class TestTask(SerializeableTask):
    _: KW_ONLY
    a: int
    b: float

    def run(self):
        c = self.a*self.b
        with self.output().open("w") as f:
            f.write(c)

    def output(self):
        return LocalFileTarget("test.txt")


@dataclass
class DependentTask(SerializeableTask):

    dependencies: Tuple[TestTask]
    _: KW_ONLY
    d: int
    e: float

    @property
    def input(self):
        return self.dependencies[0].output

    def run(self):
        with self.input.open() as f:

            c = float(f.read())
        with self.output().open("w") as f:
            f.write(c)

    def output(self):
        return LocalFileTarget("test.txt")


def test_serialisation():

    task_original = TestTask(a=2, b=3.0)
    json_task = serialize_task_to_json(task_original)
    task_reconstructed = deserialize_task_from_json(json_task)
    assert task_original == task_reconstructed


def test_serialisation_with_dependencies():

    task_original = DependentTask((TestTask(a=2, b=3.0),), d=2, e=5.0,)
    json_task = serialize_task_to_json(task_original)
    task_reconstructed = deserialize_task_from_json(json_task)
    assert task_original == task_reconstructed
