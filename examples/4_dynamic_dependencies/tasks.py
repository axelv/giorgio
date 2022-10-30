from __future__ import annotations
from dataclasses import KW_ONLY, dataclass, field
from datetime import date, datetime
from itertools import groupby
import json
import logging
from pathlib import Path
from sys import argv
from giorgio.targets import LocalFileTarget, Target
from giorgio.targets.local_file import JSONTarget
from giorgio.tasks import Task
from giorgio.scheduler import schedule_execute
from typing import Any, Dict, Generator, List, Optional

logging.basicConfig(level=logging.INFO)


@dataclass(unsafe_hash=True)
class PostsDump(Task):
    filename: Path
    _: KW_ONLY
    output: JSONTarget = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        self.output = JSONTarget(self.filename)

    def run(self) -> Generator[tuple[Task, ...], tuple[Optional[Target], ...], None]:
        raise RuntimeError(
            "Can't execute task %s because it represents an external dump." % repr(self))


class InputIsPreviousOutput(Task):
    @property
    def input(self):
        return [t.output for t in self.dependencies]


@dataclass(unsafe_hash=True)
class CountLikesLastMonth(InputIsPreviousOutput):
    user: int
    _: KW_ONLY
    dependencies: tuple[PostsDump]
    output: LocalFileTarget = field(init=False, repr=False, compare=False)

    @property
    def input(self):
        return self.dependencies[0].output

    def __post_init__(self):
        path = Path(f"./last_month_posts_user_{self.user}_with_count.json")
        self.output = LocalFileTarget(path)

    def run(self):
        posts: List[Dict[str, Any]] = self.input.read()
        current_year = datetime.now().year
        post_with_likes_count = [{**post, "total_likes": len(post["likes"])} for post in posts if date.fromisoformat(
            post["date"]).year == current_year-1 and post["author"] == self.user]
        with self.output.open("w") as f:
            json.dump(
                post_with_likes_count,
                f
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
            try:
                posts: List[Dict[str, Any]] = json.load(f)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"{self.input[0]._path} has invalid format.") from exc
        with self.output.open("w") as f:
            sorted_posts = sorted(posts,
                                  key=lambda p: p["total_likes"],
                                  reverse=True)
            top_k_posts = sorted_posts[:self.k]
            json.dump(top_k_posts, f)


@dataclass(unsafe_hash=True)
class TopKForAllUsers(InputIsPreviousOutput):
    filename: Path
    _: KW_ONLY
    dependencies: tuple[PostsDump]
    output: LocalFileTarget = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        self.output = LocalFileTarget("./top_k_for_all_users.json")

    def clean_up(self):
        self.output.remove()

    def complete(self) -> bool:
        return self.output.exists()

    def run(self):
        with self.input[0].open("r") as f:
            posts = json.load(f)

        dependencies = []
        for user, _ in groupby(posts, key=lambda p: p["author"]):
            posts_dump = PostsDump(self.filename)
            count_task = CountLikesLastMonth(
                user=user,
                dependencies=(posts_dump,)
            )
            top10_task = Topk(
                user=user,
                dependencies=(count_task,)
            )
            dependencies.append(top10_task)
        results = yield tuple(dependencies)

        with self.output.open("w") as f:
            f.write(str(len(results)))


if __name__ == "__main__":

    # read the cli arguments
    filename = Path(argv[1])

    # build the task dag
    posts_dump = PostsDump(filename)
    top10_task = TopKForAllUsers(
        filename=filename,
        dependencies=(posts_dump,)
    )

    top10_task.clean_up()
    schedule_execute(top10_task)
