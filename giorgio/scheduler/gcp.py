import logging
from giorgio.serialisation import serialize_task_to_json
from giorgio.tasks import SerializeableTask
from .scheduler import Scheduler
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.futures import Future
LOGGER = logging.getLogger(__name__)


class GoogleCloudScheduler(Scheduler[SerializeableTask]):

    def __init__(self, topic_name: str) -> None:
        super().__init__()
        self.topic_name = topic_name
        self.publisher = PublisherClient()

    def schedule_task(self, task: SerializeableTask) -> Future:
        """Schedule a task by serializing it on a queue so that workers can execute it

        Args:
            task (SerializeableTask): Task that should be executed.

        Returns:
            Future: Future returned by Pub/Sub.
        """
        data = serialize_task_to_json(task).encode("utf-8")
        LOGGER.debug("Publishing %s to Pub/Sub", repr(task))
        return self.publisher.publish(self.topic_name, data)
