from importlib import import_module
import os
from sys import argv
from .worker import PubSubWorker


def main():
    import_module(argv[1])
    subscription = os.getenv("SUBSCRIPTION")
    if subscription is None:
        raise ValueError(
            "Environment variable SUBSCRIPTION should contain the Google Pub/Sub subscription path.")
    worker = PubSubWorker(subscription)

    worker.start()


if __name__ == "__main__":
    main()
