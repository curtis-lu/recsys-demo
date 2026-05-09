"""Tests for io.extract.extract_Xy."""

from pathlib import Path

import numpy as np
import pandas as pd


def _make_handle(tmp_path: Path, df: pd.DataFrame):
    from recsys_tfb.io.handles import ParquetHandle

    parquet_dir = tmp_path / "input.parquet"
    df.to_parquet(parquet_dir, engine="pyarrow")
    return ParquetHandle(path=str(parquet_dir))


def test_extract_xy_returns_numpy_arrays(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy

    df = pd.DataFrame(
        {
            "cust_id": ["c1", "c2", "c3"],
            "snap_date": pd.to_datetime(["2025-01-31"] * 3),
            "prod_name": ["fund", "ccard", "fund"],
            "feat_a": [1.0, 2.0, 3.0],
            "feat_b": [0.1, 0.2, 0.3],
            "label": [0, 1, 0],
        }
    )
    handle = _make_handle(tmp_path, df)
    prep_meta = {
        "feature_columns": ["feat_a", "feat_b", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard", "savings"]},
    }
    parameters = {
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        }
    }

    X, y = extract_Xy(handle, prep_meta, parameters)

    assert X.shape == (3, 3)
    assert list(y) == [0, 1, 0]
    # prod_name is int-coded: fund=0, ccard=1, fund=0
    assert list(X[:, 2]) == [0, 1, 0]
