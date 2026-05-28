"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.
"""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ParquetHandle:
    """Reference to a local parquet directory written by a cache node."""

    path: str

    def to_pandas(self) -> "pd.DataFrame":  # type: ignore[name-defined]
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        return table.to_pandas(split_blocks=True, self_destruct=True)


@dataclass(frozen=True)
class LgbDatasetHandle:
    """Reference to a saved ``lgb.Dataset`` binary on disk.

    ``role`` distinguishes "train" from "train_dev" so callers can build the
    correct reference linkage when reloading.
    """

    bin_path: str
    role: Literal["train", "train_dev"]

    def load(
        self,
        reference: "lgb.Dataset | None" = None,  # type: ignore[name-defined]
        params: dict | None = None,
    ) -> "lgb.Dataset":  # type: ignore[name-defined]
        import lightgbm as lgb

        return lgb.Dataset(self.bin_path, reference=reference, params=params)
