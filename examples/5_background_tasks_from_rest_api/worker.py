import os
from count_words import CountWordsTask
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.message import Message

SUBSCRIPTION = os.getenv("SUBSCRIPTION_PATH")


class Worker:
    def __init__(self, subscription_name):
        self.subscription_name = subscription_name

    def __callback(self, message: Message):
        task = CountWordsTask.deserialize(message.data.decode("utf-8"))
        message.ack()
        print(repr(task))
        task.run()

    def start(self):
        with pubsub_v1.SubscriberClient() as subscriber:
            # subscriber.create_subscription(
            # name=self.subscription_name, topic=self.topic_path)
            print("Subscribing to the task queue. " + self.subscription_name)
            future = subscriber.subscribe(
                self.subscription_name, self.__callback)
            try:
                future.result()
            except KeyboardInterrupt:
                future.cancel()


if __name__ == "__main__":
    worker = Worker(SUBSCRIPTION)
    worker.start()
