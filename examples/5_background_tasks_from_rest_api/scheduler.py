import os
from serializeable_task import SerializeableTask
from google.cloud.pubsub_v1 import PublisherClient

TOPIC_PATH = os.getenv("TOPIC_PATH")


class Scheduler:
    def __init__(self):
        self.publisher = PublisherClient()

    def schedule_task(self, task: SerializeableTask):
        """Schedules the task. If the task is read, put it on a queue so it can be processesd by other workers."""

        # check if the task is already complete
        # check if the task is already queued

        serialized_task = task.serialize().encode("utf-8")
        future = self.publisher.publish(TOPIC_PATH, serialized_task)
        return future.result()
