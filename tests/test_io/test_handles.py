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


def test_lgb_dataset_handle_load_roundtrip(tmp_path: Path) -> None:
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.io.handles import LgbDatasetHandle

    X = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
    y = np.array([0, 1, 0])
    bin_path = tmp_path / "train.bin"
    ds = lgb.Dataset(X, label=y, free_raw_data=False).construct()
    ds.save_binary(str(bin_path))

    handle = LgbDatasetHandle(bin_path=str(bin_path), role="train")
    loaded = handle.load()
    loaded.construct()
    assert loaded.num_data() == 3


def test_lgb_dataset_handle_load_with_reference(tmp_path: Path) -> None:
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.io.handles import LgbDatasetHandle

    X_tr = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
    y_tr = np.array([0, 1, 0])
    X_dev = np.array([[1.5, 2.5], [3.5, 4.5]])
    y_dev = np.array([1, 0])

    train_bin = tmp_path / "train.bin"
    dev_bin = tmp_path / "dev.bin"

    ds_tr = lgb.Dataset(X_tr, label=y_tr, free_raw_data=False).construct()
    ds_tr.save_binary(str(train_bin))

    ds_dev = lgb.Dataset(
        X_dev, label=y_dev, reference=ds_tr, free_raw_data=False
    ).construct()
    ds_dev.save_binary(str(dev_bin))

    train_handle = LgbDatasetHandle(bin_path=str(train_bin), role="train")
    dev_handle = LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev")

    loaded_tr = train_handle.load()
    loaded_tr.construct()  # train must be constructed for dev's reference to be usable
    loaded_dev = dev_handle.load(reference=loaded_tr)
    loaded_dev.construct()
    assert loaded_dev.num_data() == 2
