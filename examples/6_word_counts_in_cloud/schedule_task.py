import os
from sys import argv
from count_words import CountWordsTask
from giorgio.scheduler.gcp import GoogleCloudScheduler

TOPIC_PATH = os.getenv("TOPIC_PATH")
assert TOPIC_PATH is not None, "Environment variable TOPIC_PATH should be set."
scheduler = GoogleCloudScheduler(TOPIC_PATH)

if __name__ == "__main__":
    # Setting parameters from CLI arguments and put default values otherwise
    if len(argv) == 3:
        url = argv[1]
        fname = argv[2]
    else:
        url = "https://cloud.google.com/pubsub/docs/reference/libraries"
        fname = "word_on_gcp.json"

    task = CountWordsTask(url, fname)
    print(scheduler.schedule_task(task).result())
