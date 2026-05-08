"""Tests for ParquetHandle and LgbDatasetHandle."""

import dataclasses
from pathlib import Path

import pandas as pd
import pytest


def test_parquet_handle_to_pandas_roundtrip(tmp_path: Path) -> None:
    from recsys_tfb.io.handles import ParquetHandle

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    parquet_dir = tmp_path / "test.parquet"
    df.to_parquet(parquet_dir, engine="pyarrow")

    handle = ParquetHandle(path=str(parquet_dir))
    loaded = handle.to_pandas()

    pd.testing.assert_frame_equal(loaded, df)


def test_parquet_handle_is_frozen(tmp_path: Path) -> None:
    from recsys_tfb.io.handles import ParquetHandle

    handle = ParquetHandle(path=str(tmp_path / "x.parquet"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        handle.path = "/other"  # type: ignore[misc]
