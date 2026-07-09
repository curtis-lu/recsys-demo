import json
import os

from recsys_tfb.io.base import AbstractDataset


class JSONDataset(AbstractDataset):
    """Dataset for reading and writing structured data as JSON files."""

    def __init__(self, filepath: str, optional: bool = False):
        self._filepath = filepath
        self._optional = optional

    def load(self):
        if self._optional and not os.path.exists(self._filepath):
            return None
        with open(self._filepath) as f:
            return json.load(f)

    def save(self, data) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        with open(self._filepath, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
