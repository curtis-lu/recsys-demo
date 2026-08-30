"""Lightweight typed handles for cached training inputs.

These dataclasses flow through the pipeline DAG as references to on-disk
artifacts. Consumers call ``.to_pandas()`` / ``.load()`` to materialize the
underlying data lazily inside their own scope, allowing GC to release memory
between pipeline nodes.

``handle_paths`` / ``open_parquet_dataset`` are the read side of that contract:
a cache node may shard a split by key (``test_model_input`` is one directory per
month), and these turn either shape into something pyarrow can open as one
dataset.
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


def require_complete_cache(
    handles: Union[ParquetHandle, Mapping[str, ParquetHandle]]
) -> None:
    """Raise unless every parquet root behind ``handles`` carries ``_SUCCESS``.

    Pre-check on an input, run before the first read.

    A cache node writes the marker last, so a directory without one is a copy
    that died partway — ``_populate_cache_from_hive`` says as much in its own
    docstring. Reading such a directory does not fail: pyarrow opens whatever
    fragments landed, and the caller computes over a silent subset of the split.

    Deliberately not the cache node's own behaviour. Handed a marker-less
    directory *whose source is still available*, the right move is to clear it
    and rebuild from Hive, and that recovery must stay. This is for the other
    side of the same marker: a consumer handed a handle it cannot rebuild, where
    the only choices left are failing and publishing a number computed over an
    unknown fraction of the data.

    Only directories are checked. A handle pointing at a single parquet file is
    not a cache root — the marker is a property of the directories the cache
    nodes write — so requiring one there would reject a shape the contract never
    covered.
    """
    from pathlib import Path

    if isinstance(handles, ParquetHandle):
        labelled = [("", handles.path)]
    else:
        labelled = [(key, handles[key].path) for key in sorted(handles)]

    incomplete = [
        (key, root) for key, root in labelled
        if Path(root).is_dir() and not (Path(root) / "_SUCCESS").exists()
    ]
    if incomplete:
        detail = ", ".join(
            f"{key}: {root}" if key else root for key, root in incomplete
        )
        raise ValueError(
            "parquet cache is incomplete — no _SUCCESS marker, so the copy that "
            f"wrote it did not finish and an unknown fraction of the split is "
            f"missing: {detail}. Re-run the cache node for this split (its "
            "source is what can rebuild it); do not compute over it."
        )


def open_parquet_dataset(paths: Union[str, list[str]]):
    """One ``pyarrow.dataset`` over one or many hive-partitioned parquet roots.

    A list of *directories* cannot be handed to ``pads.dataset`` directly — it
    reads list elements as file paths and raises IsADirectoryError. The
    supported form for several roots is a list of dataset objects, which this
    builds. A single root returns that dataset unwrapped, so the common case
    stays byte-identical to a plain ``pads.dataset(path)``.

    Fragment order follows the given path order, then path-sort within each
    root — deterministic as long as the caller's list is (``handle_paths``
    sorts by key).

    Caveats of the multi-root form, both absent when everything lived under one
    root: hive partition types are inferred per root, so two months whose
    partition values infer differently (all-numeric item codes in one, strings
    in the other) fail to merge with an ArrowTypeError rather than being
    coerced to a common string type; and the return is a UnionDataset, which
    lacks ``.files``. Nothing downstream uses ``.files`` today.
    """
    import pyarrow.dataset as pads

    roots = [paths] if isinstance(paths, str) else list(paths)
    if not roots:
        raise ValueError("open_parquet_dataset needs at least one parquet root")
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
