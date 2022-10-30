

from datetime import date, datetime
from itertools import groupby
import json
from sys import argv
from typing import Any, Dict, List


if __name__ == "__main__":
    filename = argv[1]

    with open(filename) as f:
        posts = json.load(f)

    for user, _ in groupby(posts, key=lambda p: p["author"]):
        print("Calculating posts for user "+str(user))
        current_month, current_year = datetime.now().month, datetime.now().year

        posts_last_month: List[Dict[str, Any]] = []
        for post in posts:
            post_dt = date.fromisoformat(post["date"])
            if post["author"] == user and post_dt.year == current_year - 1:
                posts_last_month.append(
                    {"post_id": post["post_id"], "total_likes": len(post["likes"])})

        top_10 = sorted(posts_last_month,
                        key=lambda p: p["total_likes"],
                        reverse=True)[:10]

        with open(f"SCRIPT_last_month_posts_{user}_with_count.json", "w") as f:
            json.dump(top_10, f)
