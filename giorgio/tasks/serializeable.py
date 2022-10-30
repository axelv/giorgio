
from __future__ import annotations
from dataclasses import KW_ONLY, Field, asdict, dataclass
from typing import Any, Dict, Protocol, Tuple, Type, runtime_checkable
import abc

from giorgio.targets.target import Target
from .task import Task


@dataclass
class SerializeableTask(Task, abc.ABC):
    """Abstract base class for idempotent serializeable tasks with other tasks as dependencies."""
    _: KW_ONLY
    dependencies: tuple[SerializeableTask, ...] = ()

    def __init_subclass__(cls) -> None:
        """Hook to register every Task implementation to make serialization work."""
        name = cls.__name__
        TASK_REGISTER[name] = cls
        return super().__init_subclass__()

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize a task into a tuple containing the name and parameters"""
        parameters = asdict(self)
        for k, v in self.__dataclass_fields__.items():
            assert isinstance(
                v, Field), "Task has a non-Field properety in __dataclass_fields__"
            # only serialize parameters that are passed to the constructor
            # other params are class attributes or are set __post_init__
            if k in parameters and not v.init:
                del parameters[k]
        parameters["dependencies"] = tuple(
            t.serialize() for t in self.dependencies)
        return (self.__class__.__name__, parameters)

    @classmethod
    def deserialize(cls, name, **kwargs) -> SerializeableTask:
        """Deserialize a task given it's name and arguments"""
        if "dependencies" in kwargs:
            kwargs["dependencies"] = tuple(cls.deserialize(dep_name, **params)
                                           for (dep_name, params) in kwargs["dependencies"])
        return TASK_REGISTER[name](**kwargs)


TASK_REGISTER: Dict[str, Type[SerializeableTask]] = {}
