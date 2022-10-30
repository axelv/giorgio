import logging
from json import JSONDecodeError
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from giorgio.tasks import Task
from giorgio.workers import Worker
from giorgio.serialisation import deserialize_task_from_json

LOGGER = logging.getLogger(__name__)


class PubSubWorker(Worker):
    def __init__(self, subscription: str):
        """Create a worker that processes tasks published on a Pub/Sub queue

        Args:
            subscription (str): Subscription name in the form of "/projects/{project-id}/subscriptions/{subscription-id}" 
        """
        self.subscription: str = subscription

    def __callback(self, message: Message):
        """Callback that should be past to the subscriber. This is executed for each message received on the queue."""
        try:
            task = deserialize_task_from_json(message.data.decode("utf-8"))
        except JSONDecodeError as exc:
            LOGGER.warning("Received invalid message: %s",
                           message.data,
                           exc_info=exc)
            message.ack()
        else:
            LOGGER.info("Received task for execute: %s", repr(task))
            message.ack()
            self.execute_task(task)

    def start(self):
        with SubscriberClient() as subscriber:
            LOGGER.info("Subscribing to the task queue. " + self.subscription)
            future = subscriber.subscribe(self.subscription, self.__callback)
            try:
                future.result()
            except KeyboardInterrupt:
                LOGGER.info("Exiting the core work loop.")
                future.cancel()
