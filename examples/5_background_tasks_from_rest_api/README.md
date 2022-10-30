# Serializing background tasks using Python dataclasses

Executing long running jobs inside a REST API is not desirable because it blocks the HTTP response to your client
and it consumes CPU and memory which you want to allocate to handling HTTP requests. The solution is to run your time and resource consuming function as _background task_.
But how does that work in practice?

Let's say you find yourself in the following situation:

```python
from pathlib import Path
from pydantic import HttpUrl
from fastapi import FastAPI

app = FastAPI()

# our API route handling a GET http://<domain>/task/execute request
@app.get("/task/execute")
def execute_task(url: HttpUrl, fname: Path):
    # this is a long running task
    count_words_at_url(url, fname)
    return {"msg": "Task executed."}
```

The problem here is that (1) `count_words` needs to finnish before we send back a response and (2) it also consumes CPU-time that we need to process other requests.
A partial solution would be to run this function as a [Background Task](https://fastapi.tiangolo.com/tutorial/background-tasks/) but this doesn't solve our resource problem.

# Making our function serializeable

In order to be able to run this function on another machinek, we need to be able to 'send' our function to another process or machine.
Python has a well known technique for Object-serialization which is _Pickling_, which unfortunatly has [several issues](<[https://docs.python.org/3/library/pickle.html](https://docs.python.org/3/library/pickle.html#comparison-with-json)>) which are unacceptable for us.
The serialization must be save against _arbitrary code execution_ and must be human-readable so we can easily inspect the serialized tasks.

Another solution that can help to serialized our function is by making use of Python data classes.

We can rewrite our `count_words` a little:

```python
from dataclasses import dataclass
@dataclass
class CountWords:
  url: str
  fname: str

  def run(self):
    # body of our function
    pass
```

Making use of some utilities of Python-dataclasses, we can easily serialize the parameters of our functoin:

```python
from dataclassess import asdict
import json

task = CountWords("https://google.com", "word_count_google.json")

serialized_task = json.dumps(asdict(task))
# --> {"url": "https://google.com", "fname": "word_count_google.json"}

deserialized_task = CountWords(**json.loads(serialized_task))

assert task == serialized_task # Python dataclasses ensure this is true
```

# Putting serialized tasks on a queue
