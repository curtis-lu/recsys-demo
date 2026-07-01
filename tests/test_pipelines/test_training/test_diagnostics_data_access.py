"""Tests for diagnostics.data_access — bounded parquet reads."""
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pads
import pytest

from recsys_tfb.pipelines.training.diagnostics import data_access as da


def _frame(n=50):
    rng = np.random.RandomState(0)
    return pd.DataFrame({
        "f0": rng.randn(n),
        "f1": rng.randn(n),
        "prod_name": np.where(np.arange(n) % 2 == 0, "A", "B"),
        "snap_date": np.where(np.arange(n) % 3 == 0, "2024-01-31", "2024-02-29"),
        "label": (rng.rand(n) > 0.7).astype(int),
    })


@pytest.fixture
def flat_path(tmp_path):
    pdf = _frame()
    path = str(tmp_path / "flat.parquet")
    # multiple row groups to exercise the batch-gather path
    pq.write_table(pa.Table.from_pandas(pdf), path, row_group_size=7)
    return path, pdf


@pytest.fixture
def part_path(tmp_path):
    pdf = _frame()
    base = str(tmp_path / "parted")
    pads.write_dataset(
        pa.Table.from_pandas(pdf), base, format="parquet",
        partitioning=["snap_date", "prod_name"], partitioning_flavor="hive",
    )
    return base, pdf


def test_count_rows_flat(flat_path):
    path, pdf = flat_path
    assert da.count_rows(path) == len(pdf)


def test_count_rows_partitioned(part_path):
    path, pdf = part_path
    assert da.count_rows(path) == len(pdf)


def test_schema_names_includes_partition_cols(part_path):
    path, _ = part_path
    names = set(da.schema_names(path))
    assert {"f0", "f1", "label", "prod_name", "snap_date"} <= names


def test_read_column_partition_col_reconstructed(part_path):
    path, pdf = part_path
    got = da.read_column(path, "prod_name")
    # order == dataset order == pq.read_table order
    ref = pq.read_table(path).to_pandas()["prod_name"].to_numpy()
    assert list(got) == list(ref)


def test_take_rows_matches_iloc_flat(flat_path):
    path, pdf = flat_path
    ref = pq.read_table(path, columns=["f0", "f1", "label"]).to_pandas()
    idx = np.sort(np.random.RandomState(1).choice(len(pdf), size=10, replace=False))
    got = da.take_rows(path, idx, columns=["f0", "f1", "label"]).reset_index(drop=True)
    exp = ref.iloc[idx].reset_index(drop=True)
    pd.testing.assert_frame_equal(got, exp)


def test_take_rows_matches_iloc_partitioned(part_path):
    path, pdf = part_path
    ref = pq.read_table(path, columns=["f0", "f1", "label", "prod_name"]).to_pandas()
    idx = np.sort(np.random.RandomState(2).choice(len(pdf), size=12, replace=False))
    got = da.take_rows(path, idx, columns=["f0", "f1", "label", "prod_name"])
    got = got.reset_index(drop=True)
    exp = ref.iloc[idx].reset_index(drop=True)
    pd.testing.assert_frame_equal(got[["f0", "f1", "label"]], exp[["f0", "f1", "label"]])
    assert list(got["prod_name"]) == list(exp["prod_name"])


def test_take_rows_empty_returns_typed_empty(flat_path):
    path, _ = flat_path
    got = da.take_rows(path, np.array([], dtype=np.int64), columns=["f0", "label"])
    assert list(got.columns) == ["f0", "label"]
    assert len(got) == 0
