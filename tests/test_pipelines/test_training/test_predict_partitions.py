"""Which (month, item) partitions the test cache holds — answered without reading a row.

Predict used to project the two partition columns and de-duplicate them, which
materialises one row per data row: about 220 million at production scale to
learn a few hundred pairs (ADR-0030 decision 12.2). The listing now comes from
the directory names alone. The garbage-file tests are what prove it: a reader
that touched a data file would fail on them.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from recsys_tfb.io.handles import open_parquet_dataset
from recsys_tfb.pipelines.training.steps.predict_partitions import (
    partitions_from_directory_names,
)

TIME, ITEM = "snap_date", "prod_name"


def _write(root: Path, rows) -> str:
    df = pd.DataFrame(rows, columns=["cust_id", TIME, ITEM, "feat_a"])
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(root), partition_cols=[TIME, ITEM],
    )
    return str(root)


def _garble(root: str) -> int:
    """Overwrite every data file under ``root`` with bytes no reader accepts."""
    files = list(Path(root).rglob("*.parquet"))
    for f in files:
        f.write_bytes(b"not a parquet file")
    return len(files)


def test_every_pair_once_in_sorted_order(tmp_path):
    root = _write(tmp_path / "m", [
        ("c1", "2025-02-28", "b", 1.0),
        ("c2", "2025-01-31", "b", 2.0),
        ("c3", "2025-01-31", "a", 3.0),
        ("c4", "2025-01-31", "a", 4.0),
    ])
    # A second file in an existing partition must not list it twice.
    _write(tmp_path / "m", [("c5", "2025-01-31", "a", 5.0)])

    assert partitions_from_directory_names(
        open_parquet_dataset(root), TIME, ITEM
    ) == [("2025-01-31", "a"), ("2025-01-31", "b"), ("2025-02-28", "b")]


def test_several_month_roots(tmp_path):
    """The cache hands predict one root per month, opened as one dataset."""
    jan = _write(tmp_path / "jan", [("c1", "2025-01-31", "a", 1.0)])
    feb = _write(tmp_path / "feb", [("c1", "2025-02-28", "a", 1.0),
                                    ("c2", "2025-02-28", "b", 1.0)])

    assert partitions_from_directory_names(
        open_parquet_dataset([jan, feb]), TIME, ITEM
    ) == [("2025-01-31", "a"), ("2025-02-28", "a"), ("2025-02-28", "b")]


def test_no_data_file_is_read(tmp_path):
    root = _write(tmp_path / "m", [("c1", "2025-01-31", "a", 1.0),
                                   ("c2", "2025-01-31", "b", 1.0)])
    ds = open_parquet_dataset(root)
    assert _garble(root) == 2
    # Honest only if reading the rows really fails now.
    with pytest.raises(Exception):
        ds.to_table(columns=[TIME, ITEM])

    assert partitions_from_directory_names(ds, TIME, ITEM) == [
        ("2025-01-31", "a"), ("2025-01-31", "b"),
    ]


def test_a_layout_not_partitioned_by_both_columns_is_refused(tmp_path):
    """A flat file carries the two columns as data; listing it by directory
    would silently find nothing to predict."""
    flat = tmp_path / "flat"
    flat.mkdir()
    pq.write_table(
        pa.Table.from_pandas(pd.DataFrame({
            "cust_id": ["c1"], TIME: ["2025-01-31"], ITEM: ["a"], "feat_a": [1.0],
        }), preserve_index=False),
        str(flat / "part.parquet"),
    )
    with pytest.raises(ValueError, match="not partitioned by"):
        partitions_from_directory_names(open_parquet_dataset(str(flat)), TIME, ITEM)
