import os

from recsys_tfb.io.base import AbstractDataset


class TextDataset(AbstractDataset):
    """Dataset for reading and writing plain text files (HTML, Markdown, etc.)."""

    def __init__(self, filepath: str, encoding: str = "utf-8"):
        self._filepath = filepath
        self._encoding = encoding

    def load(self) -> str:
        with open(self._filepath, encoding=self._encoding) as f:
            return f.read()

    def save(self, data: str) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        with open(self._filepath, "w", encoding=self._encoding) as f:
            f.write(data)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
