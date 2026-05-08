"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class ParquetHandle:
    """Reference to a local parquet directory written by a cache node."""

    path: str

    def to_pandas(self) -> "pd.DataFrame":  # type: ignore[name-defined]
        import pandas as pd

        return pd.read_parquet(self.path, engine="pyarrow")


@dataclass(frozen=True)
class LgbDatasetHandle:
    """Reference to a saved ``lgb.Dataset`` binary on disk.

    ``role`` distinguishes "train" from "train_dev" so callers can build the
    correct reference linkage when reloading.
    """

    bin_path: str
    role: str  # "train" | "train_dev"

    def load(self, reference=None) -> "lgb.Dataset":  # type: ignore[name-defined]
        import lightgbm as lgb

        return lgb.Dataset(self.bin_path, reference=reference)
