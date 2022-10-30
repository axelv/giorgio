from enum import Enum


class TaskStatus(str, Enum):
    COMPLETED = "COMPLETE"
    QUEUED = "QUEUED"
