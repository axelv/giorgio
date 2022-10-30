from dataclasses import dataclass
from datetime import date, datetime
import json
from sys import argv
from typing import Any, Dict, List


@dataclass()
class CountLikesLastMonth:
    user: int

    def __call__(self, posts: List[Dict[str, Any]]):
        current_year = datetime.now().year
        return [{**post, "total_likes": len(post["likes"])} for post in posts if date.fromisoformat(post["date"]).year == current_year-1 and post["author"] == self.user]


@dataclass()
class Topk:
    k: int = 10

    def __call__(self, posts: List[Dict[str, Any]]):
        return sorted(posts, key=lambda p: p["total_likes"], reverse=True)[:self.k]


if __name__ == "__main__":
    filename = argv[1]
    user = int(argv[2])
    count_task = CountLikesLastMonth(user=user)
    top10_task = Topk()
    with open(filename, "r") as f:
        posts = json.load(f)

    top10_count = top10_task(count_task(posts))

    with open(f"{user}.json", "w") as f:
        json.dump(top10_count, f)
