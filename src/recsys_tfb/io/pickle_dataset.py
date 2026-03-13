import os
import pickle

from recsys_tfb.io.base import AbstractDataset


class PickleDataset(AbstractDataset):
    """Dataset for reading and writing arbitrary Python objects via pickle."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def load(self):
        with open(self._filepath, "rb") as f:
            return pickle.load(f)

    def save(self, data) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        with open(self._filepath, "wb") as f:
            pickle.dump(data, f)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
