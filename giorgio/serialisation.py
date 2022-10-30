"""Serialization and deserialization of tasks to JSON"""
import json
from typing import Any, Dict, TypedDict
from giorgio.tasks.serializeable import SerializeableTask


class SerializedTask(TypedDict):
    name: str
    parameters: Dict[str, Any]


def serialize_task_to_dict(task: SerializeableTask) -> SerializedTask:
    name, parameters = task.serialize()
    return {"name": name, "parameters": parameters}


def serialize_task_to_json(task: SerializeableTask):
    return json.dumps(serialize_task_to_dict(task))


def deserialize_task_from_dict(config: SerializedTask):
    return SerializeableTask.deserialize(config["name"], **config["parameters"])


def deserialize_task_from_json(json_config: str):
    return deserialize_task_from_dict(json.loads(json_config))
