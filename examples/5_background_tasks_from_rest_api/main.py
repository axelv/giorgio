from pathlib import Path
from typing import Union
from pydantic import HttpUrl
from fastapi import FastAPI
from count_words import CountWordsTask
from scheduler import Scheduler

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


scheduler = Scheduler()


@app.get("/task/execute")
def read_item(url: HttpUrl, fname: Path):
    task = CountWordsTask(url, fname)
    scheduler.schedule_task(task)
    return {"msg": "Task scheduled."}
