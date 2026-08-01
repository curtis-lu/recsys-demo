"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.
"""

from dataclasses import dataclass
from typing import Literal, Mapping, Union


@dataclass(frozen=True)
class ParquetHandle:
    """Reference to a local parquet directory written by a cache node."""

    path: str

    def to_pandas(self) -> "pd.DataFrame":  # type: ignore[name-defined]
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        return table.to_pandas(split_blocks=True, self_destruct=True)


def handle_paths(
    handles: Union[ParquetHandle, Mapping[str, ParquetHandle]]
) -> list[str]:
    """Parquet roots behind one handle, or a mapping of them, in sorted-key order.

    Cache nodes that shard a split by key — ``test_model_input``, one directory
    per test month — return a mapping; single-artifact ones return a bare
    handle. Consumers that read across the whole split pass the result straight
    to ``pyarrow.dataset``, which accepts a list of dataset roots and treats
    them as one dataset.

    Order is deterministic (sorted by key) because diagnostics index rows by
    position across separate reads.
    """
    if isinstance(handles, ParquetHandle):
        return [handles.path]
    return [handles[key].path for key in sorted(handles)]


def parquet_dataset(paths: Union[str, list[str]]):
    """One ``pyarrow.dataset`` over one or many hive-partitioned parquet roots.

    A list of *directories* cannot be handed to ``pads.dataset`` directly — it
    reads list elements as file paths and raises IsADirectoryError. The
    supported form for several roots is a list of dataset objects, which this
    builds. A single root returns that dataset unwrapped, so the common case
    stays byte-identical to a plain ``pads.dataset(path)``.

    Fragment order follows the given path order, then path-sort within each
    root — deterministic as long as the caller's list is (``handle_paths``
    sorts by key).
    """
    import pyarrow.dataset as pads

    roots = [paths] if isinstance(paths, str) else list(paths)
    if not roots:
        raise ValueError("parquet_dataset needs at least one parquet root")
    children = [
        pads.dataset(root, format="parquet", partitioning="hive") for root in roots
    ]
    return children[0] if len(children) == 1 else pads.dataset(children)


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
