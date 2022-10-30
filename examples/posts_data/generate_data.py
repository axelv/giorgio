from datetime import date, datetime
import json
from random import choice, randint
from typing import Dict, List, TypedDict


class Post(TypedDict):
    post_id: int
    author: int
    date: str
    likes: List[int]


USERS: List[int] = list(range(2000))
POSTS: List[Post] = []


def generate_random_date_str():
    start_ord = date(2010, 1, 1).toordinal()
    stop_end = datetime.today().toordinal()
    return date.fromordinal(randint(start_ord, stop_end)).isoformat()


if __name__ == "__main__":

    # generate posts
    for id in range(100000):

        # pick an author
        author = choice(USERS)
        post: Post = {
            "post_id": id,
            "author": author,
            "likes": [],
            "date": generate_random_date_str()
        }
        for _ in range(randint(0, 100)):
            liker = choice(USERS)
            post["likes"].append(liker)

        POSTS.append(post)

    with open("posts.json", "w") as f:
        json.dump(POSTS, f, indent=4)
