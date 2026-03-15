import json
import os

from recsys_tfb.io.base import AbstractDataset


class JSONDataset(AbstractDataset):
    """Dataset for reading and writing structured data as JSON files."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def load(self):
        with open(self._filepath) as f:
            return json.load(f)

    def save(self, data) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        with open(self._filepath, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
