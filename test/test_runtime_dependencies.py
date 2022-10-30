"""Test wether invalid runtime dependencies are detected"""
import abc
from dataclasses import KW_ONLY, dataclass, field
from typing import Protocol, Tuple
from giorgio import Task
from giorgio.targets import LocalFileTarget


class NumberTarget(LocalFileTarget):

    def read(self) -> float:
        with self.open("r") as f:
            return float(f.read())

    def write(self, number: float):
        with self.open("w") as f:
            return f.write(number)


@dataclass
class Task1(Task, Protocol):
    output: NumberTarget = field(init=False, repr=False)


@dataclass
class Task1A(Task1):
    a: int
    b: float

    def __post_init__(self):
        self.output = NumberTarget("number.txt")

    def run(self):
        c = self.a*self.b
        with self.output.open("w") as f:
            f.write(c)


@dataclass
class Task1B(Task1):
    c: float
    fname: str

    def __post_init__(self):
        self.output = NumberTarget(f"{self.fname}.txt")

    def run(self):
        with self.output.open("w") as f:
            f.write(self.c)


@dataclass
class Task1C(Task):
    c: float
    fname: str
    output: LocalFileTarget = field(init=False, repr=False)

    def __post_init__(self):
        self.output = LocalFileTarget(f"{self.fname}.txt")

    def run(self):
        with self.output.open("w") as f:
            f.write(self.c)


@dataclass
class Task2(Task):

    d: int
    e: float
    _: KW_ONLY
    dependencies: Tuple[Task1, ]
    output: NumberTarget = field(init=False, repr=False)

    def __post_init__(self):
        self.output = NumberTarget("test.txt")

    @property
    def input(self):
        return self.dependencies[0].output

    def run(self):
        c = self.input.read()
        result = c + self.d*self.e
        self.output.write(result)


def create_valid_pipeline_1():
    t1 = Task1A(a=1, b=2.0)
    t2 = Task2(d=2, e=4, dependencies=(t1,),)
    return t2


def create_valid_pipeline_2():

    t1 = Task1B(c=2, fname="task-1")
    t2 = Task2(d=2, e=4, dependencies=(t1,),)
    return t2


def create_invalid_pipeline():
    t1 = Task1C(c=2, fname="invalid-task")
    t2 = Task2(d=2, e=4, dependencies=(t1,),)
    return t2


def main():
    pipeline_1 = create_valid_pipeline_1()
    pipeline_2 = create_valid_pipeline_2()
    pipeline_3 = create_invalid_pipeline()
