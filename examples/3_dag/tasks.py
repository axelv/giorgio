from __future__ import annotations
from dataclasses import KW_ONLY, dataclass, field
from datetime import date, datetime
import json
from pathlib import Path
from sys import argv
from giorgio.targets import LocalFileTarget
from giorgio.tasks import Task
from giorgio.scheduler import schedule_execute
from typing import Any, Dict, List


@dataclass(unsafe_hash=True)
class PostsDump(Task):
    filename: Path
    _: KW_ONLY
    output: LocalFileTarget = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        self.output = LocalFileTarget(self.filename)


class InputIsPreviousOutput(Task):
    @property
    def input(self):
        return [t.output for t in self.dependencies]


@dataclass(unsafe_hash=True)
class CountLikesLastMonth(InputIsPreviousOutput):
    user: int
    _: KW_ONLY
    dependencies: tuple[Task, ...]
    output: LocalFileTarget = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        path = Path(f"./last_month_posts_user_{self.user}_with_count.json")
        self.output = LocalFileTarget(path)

    def run(self):
        with self.input[0].open("r") as f:
            posts: List[Dict[str, Any]] = json.load(f)
        current_year = datetime.now().year
        with self.output.open("w") as f:
            json.dump(
                [{**post, "total_likes": len(post["likes"])} for post in posts if date.fromisoformat(
                    post["date"]).year == current_year-1 and post["author"] == self.user], f
            )


@dataclass(unsafe_hash=True)
class Topk(InputIsPreviousOutput):
    user: int
    k: int = 10
    _: KW_ONLY
    dependencies: tuple[CountLikesLastMonth]
    output: LocalFileTarget = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        path = Path(f"./last_month_top_{self.k}_{self.user}.json")
        self.output = LocalFileTarget(path)

    def run(self):
        with self.input[0].open("r") as f:
            posts: List[Dict[str, Any]] = json.load(f)
        with self.output.open("w") as f:
            sorted_posts = sorted(posts,
                                  key=lambda p: p["total_likes"],
                                  reverse=True)
            top_k_posts = sorted_posts[:self.k]
            json.dump(top_k_posts, f)


if __name__ == "__main__":

    # read the cli arguments
    filename = Path(argv[1])
    user = int(argv[2])

    # build the task dag
    posts_dump = PostsDump(filename)
    count_task = CountLikesLastMonth(
        user=user,
        dependencies=(posts_dump,)
    )
    top10_task = Topk(
        user=user,
        dependencies=(count_task,)
    )

    # repr will show that our tasks have a clear signature
    print(repr(top10_task))

    schedule_execute(top10_task)
