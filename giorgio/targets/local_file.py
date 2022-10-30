from functools import cache, cached_property
import json
import os
from pathlib import Path
from contextlib import contextmanager
import tempfile
from typing import Any


class LocalFileTarget:
    """Target class that enables atomic writes to a file on local disk."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).absolute()

    def exists(self):
        return self._path.exists()

    def remove(self, assume_exists=False):
        try:
            os.remove(self._path)

        except FileNotFoundError as exc:
            if assume_exists:
                raise

    @contextmanager
    def open(self, mode="w"):
        """Open a temporary file and rename it to avoid file corruption.
        Attribution: @therightstuff, @deichrenner, @hrudham
        :param mode: the file mode defaults to "w", only "w" and "a" are supported
        """

        if self.exists():
            f = open(self._path, mode)
        elif mode == "w":
            # Use the same directory as the destination file so that moving it across
            # file systems does not pose a problem.
            temp_file = tempfile.NamedTemporaryFile(
                delete=False,
                dir=os.path.dirname(self._path))
            # preserve file metadata if it already exists
            f = open(temp_file.name, mode)
        else:
            raise FileNotFoundError(f"No file found with path {self._path}")
        try:
            yield f
            f.flush()
            os.fsync(f.fileno())
            f.close()
        finally:
            if mode == "w" and not self.exists():
                os.replace(temp_file.name, self._path)
                if os.path.exists(temp_file.name):
                    try:
                        os.unlink(temp_file.name)
                    except:
                        pass


class JSONTarget(LocalFileTarget):

    @cache
    def read(self):
        with self.open("r") as f:
            return json.load(f)

    def write(self, obj: Any):
        with self.open("w") as f:
            return json.dump(obj, f)
