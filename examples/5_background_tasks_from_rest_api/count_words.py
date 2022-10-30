
from dataclasses import asdict, dataclass
from pathlib import Path
import json
from time import sleep
import requests
from html2text import html2text
from tqdm import tqdm

from serializeable_task import SerializeableTask


@dataclass
class CountWordsTask(SerializeableTask):
    """Fetches a page html, counts the words in the page and save the counts as a JSON to disk."""

    url: str
    output_file: Path

    def run(self):
        resp = requests.get(self.url)
        text = html2text(resp.text)

        # count dict containning the number of occurences for each word
        word_count: dict[str, int] = {}

        # split the text on whitespace chars and count
        for word in tqdm(text.split()):
            sleep(0.0001)  # make the task artificially slow for demo purposes
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1

        with open(self.output_file, "w") as f:
            json.dump(word_count, f)


def count_words_at_url(url: str, output_file: Path):
    """Fetches a page html, counts the words in the page and save the counts as a JSON to disk."""
    resp = requests.get(url)
    text = html2text(resp.text)

    # count dict containning the number of occurences for each word
    word_count: dict[str, int] = {}

    # split the text on whitespace chars and count
    for word in tqdm(text.split()):
        sleep(0.0001)  # make the task artificially slow for demo purposes
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1

    with open(output_file, "w") as f:
        json.dump(word_count, f)


if __name__ == "__main__":

    task = CountWordsTask(
        "https://en.wikipedia.org/wiki/Uniform_Resource_Name", "urn_count.json")

    serialized_task = task.serialize()
    # on a queue??
    reconstructed_task = CountWordsTask.deserialize(serialized_task)

    print("Let's now execute the task.")
    reconstructed_task.run()
