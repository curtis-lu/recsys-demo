"""Bounded, memory-frugal parquet reads for training diagnostics.

I/O layer: the only place in diagnostics that touches ``pyarrow.dataset``.
Reads operate on the hive-partitioned ``*_model_input`` caches
(``…/snap_date=…/prod_name=…/``) written by the training cache nodes.

Row order is the deterministic path-sorted fragment order (``use_threads=False``),
identical to ``pyarrow.parquet.read_table``. So positional indices computed
against one read map 1:1 onto another — the byte-for-byte invariant the
diagnostics memory refactor relies on.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _dataset(path: str):
    import pyarrow.dataset as pads

    return pads.dataset(path, format="parquet", partitioning="hive")


def count_rows(path: str) -> int:
    """Total row count from parquet metadata (no data scan)."""
    return int(_dataset(path).count_rows())


def schema_names(path: str) -> list:
    """All column names (including hive partition columns)."""
    return list(_dataset(path).schema.names)


def read_column(path: str, col: str) -> np.ndarray:
    """Read a single column (incl. a hive partition column) as a 1-D numpy array.

    Returns all N values in deterministic fragment order. Used to stratify the
    SHAP sample by item without materializing the full feature table.
    """
    table = _dataset(path).to_table(columns=[col])
    return table.column(col).to_numpy(zero_copy_only=False)


def take_rows(path: str, indices, columns: list) -> pd.DataFrame:
    """Read only the rows at positional ``indices``, projected to ``columns``.

    Memory is bounded to the output plus one row-group batch — the full table is
    never materialized. ``indices`` must be sorted ascending; positions index
    into the deterministic fragment order, so the result equals
    ``read_table(path, columns).to_pandas().iloc[indices]`` byte-for-byte.
    """
    import pyarrow as pa

    ds = _dataset(path)
    idx = np.asarray(indices, dtype=np.int64)
    if idx.size == 0:
        return ds.head(0, columns=list(columns)).to_pandas()

    scanner = ds.scanner(columns=list(columns), use_threads=False)
    out_batches = []
    offset = 0
    pos = 0
    n_idx = idx.size
    for batch in scanner.to_batches():
        n = batch.num_rows
        if n == 0:
            continue
        local = []
        while pos < n_idx and idx[pos] < offset + n:
            local.append(int(idx[pos] - offset))
            pos += 1
        if local:
            out_batches.append(batch.take(pa.array(local, type=pa.int64())))
        offset += n
        if pos >= n_idx:
            break
    if not out_batches:
        return ds.head(0, columns=list(columns)).to_pandas()
    return pa.Table.from_batches(out_batches).to_pandas()
