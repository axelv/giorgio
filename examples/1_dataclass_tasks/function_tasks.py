from datetime import date, datetime
import json
from sys import argv
from typing import Any, Dict, List


def count_likes_last_month(posts: List[Dict[str, Any]], user: int):
    current_year = datetime.now().year
    return [{**post, "total_likes": len(post["likes"])} for post in posts if date.fromisoformat(post["date"]).year == current_year-1 and post["author"] == user]


def top_k(posts: List[Dict[str, Any]], k: int = 10):
    return sorted(posts, key=lambda p: p["total_likes"], reverse=True)[:k]


if __name__ == "__main__":
    filename = argv[1]
    user = int(argv[2])
    with open(filename, "r") as f:
        posts = json.load(f)

    top10_count = top_k(count_likes_last_month(posts, user=user))

    with open(f"{user}.json", "w") as f:
        json.dump(top10_count, f)
