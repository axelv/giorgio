
from dataclasses import dataclass, field
import os
from pathlib import Path
import requests
import json
from time import sleep
from giorgio.tasks import SerializeableTask
from html2text import html2text
from tqdm import tqdm


@dataclass
class CountWordsTask(SerializeableTask):
    """Fetches a page html, counts the words in the page and save the counts as a JSON to disk."""

    url: str
    output_file: str
    output_path: Path = field(init=False, repr=False)

    def __post_init__(self):
        # Set derived variables
        base_path = Path(os.getenv("OUTPUT_PATH") or os.getcwd())
        self.output_path = base_path / self.output_file

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

        with open(self.output_path, "w") as f:
            json.dump(word_count, f)
