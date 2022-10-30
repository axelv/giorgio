from __future__ import annotations
from dataclasses import KW_ONLY, dataclass, field
from datetime import date, datetime
import json
from pathlib import Path
from sys import argv
from giorgio.targets import Target, LocalFileTarget
from typing import Any, Dict, List, Optional


@dataclass
class Task:
    _: KW_ONLY
    output: Optional[Target] = field(default=None, repr=False)
    input: Optional[Target] = field(default=None, repr=False)


@dataclass()
class CountLikesLastMonth(Task):
    user: int
    _: KW_ONLY
    input: LocalFileTarget = field(repr=False)
    output: LocalFileTarget = field(init=False, repr=False)

    def __post_init__(self):
        path = Path(f"./last_month_posts_user_{self.user}_with_count.json")
        self.output = LocalFileTarget(path)

    def run(self):
        with self.input.open("r") as f:
            posts: List[Dict[str, Any]] = json.load(f)
        current_year = datetime.now().year
        with self.output.open("w") as f:
            json.dump(
                [{**post, "total_likes": len(post["likes"])} for post in posts if date.fromisoformat(
                    post["date"]).year == current_year-1 and post["author"] == self.user], f
            )


@dataclass()
class Topk(Task):
    user: int
    k: int = 10
    _: KW_ONLY
    input: LocalFileTarget = field(repr=False)
    output: LocalFileTarget = field(init=False, repr=False)

    def __post_init__(self):
        path = Path(f"./last_month_top_{self.k}_{self.user}.json")
        self.output = LocalFileTarget(path)

    def run(self):
        with self.input.open("r") as f:
            posts: List[Dict[str, Any]] = json.load(f)
        with self.output.open("w") as f:
            sorted_posts = sorted(posts,
                                  key=lambda p: p["total_likes"],
                                  reverse=True)
            top_k_posts = sorted_posts[:self.k]
            json.dump(top_k_posts, f)


if __name__ == "__main__":
    filename = Path(argv[1])
    user = int(argv[2])
    count_task = CountLikesLastMonth(
        user=user,
        input=LocalFileTarget(filename)
    )
    top10_task = Topk(
        user=user,
        input=count_task.output
    )
    # repr will show that our tasks have a clear signature
    print(repr(top10_task))

    count_task.run()
    top10_task.run()
