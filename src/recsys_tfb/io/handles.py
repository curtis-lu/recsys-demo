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
