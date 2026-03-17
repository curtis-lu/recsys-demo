import os

import lightgbm as lgb

from recsys_tfb.io.base import AbstractDataset


class LightGBMDataset(AbstractDataset):
    """Dataset for reading and writing LightGBM Booster models in native text format."""

    def __init__(self, filepath: str):
        self._filepath = filepath

    def load(self):
        return lgb.Booster(model_file=self._filepath)

    def save(self, data) -> None:
        os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
        data.save_model(self._filepath)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
