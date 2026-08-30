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


def _write_month(root: Path, snap_date: str, prods: list[str]) -> str:
    """Write one month's hive-partitioned parquet root, as a cache node would."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [(f"c{i}", snap_date, prod, float(i)) for i, prod in enumerate(prods)]
    df = pd.DataFrame(rows, columns=["cust_id", "snap_date", "prod_name", "feat"])
    out = root / snap_date.replace("-", "") / "test_model_input.parquet"
    pq.write_to_dataset(
        pa.Table.from_pandas(df, preserve_index=False),
        root_path=str(out),
        partition_cols=["snap_date", "prod_name"],
    )
    return str(out)


class TestHandlePaths:
    def test_bare_handle_yields_its_own_path(self) -> None:
        from recsys_tfb.io.handles import ParquetHandle, handle_paths

        assert handle_paths(ParquetHandle("/x/y.parquet")) == ["/x/y.parquet"]

    def test_mapping_is_ordered_by_key_not_insertion(self) -> None:
        """Diagnostics index rows by position across separate reads, so the
        root order must not depend on how the mapping happened to be built."""
        from recsys_tfb.io.handles import ParquetHandle, handle_paths

        out_of_order = {
            "2026-02-28": ParquetHandle("/feb"),
            "2026-01-31": ParquetHandle("/jan"),
        }

        assert handle_paths(out_of_order) == ["/jan", "/feb"]


class TestOpenParquetDatasetOverManyRoots:
    def test_reads_every_root_and_keeps_hive_partition_columns(
        self, tmp_path: Path
    ) -> None:
        """The load-bearing property for per-month test caches: predict
        enumerates partitions from the reconstructed snap_date / prod_name
        columns. If a union dropped them it would see no partitions at all and
        silently predict nothing."""
        from recsys_tfb.io.handles import open_parquet_dataset

        jan = _write_month(tmp_path, "2026-01-31", ["prod_a", "prod_b"])
        feb = _write_month(tmp_path, "2026-02-28", ["prod_a"])

        pdf = open_parquet_dataset([jan, feb]).to_table().to_pandas()

        assert sorted(set(pdf["snap_date"].astype(str))) == ["2026-01-31", "2026-02-28"]
        assert sorted(set(pdf["prod_name"].astype(str))) == ["prod_a", "prod_b"]
        assert len(pdf) == 3

    def test_single_root_matches_a_plain_read(self, tmp_path: Path) -> None:
        from recsys_tfb.io.handles import open_parquet_dataset

        jan = _write_month(tmp_path, "2026-01-31", ["prod_a", "prod_b"])

        one = open_parquet_dataset([jan]).to_table().to_pandas()
        bare = open_parquet_dataset(jan).to_table().to_pandas()

        pd.testing.assert_frame_equal(one, bare)

    def test_empty_root_list_is_rejected(self) -> None:
        from recsys_tfb.io.handles import open_parquet_dataset

        with pytest.raises(ValueError, match="at least one parquet root"):
            open_parquet_dataset([])


class TestRequireCompleteCache:
    """The `_SUCCESS` contract, checked from the consumer's side.

    A cache node writes the marker last, so a directory without one is a copy
    that died partway. The cache node's own recovery (rmtree + rebuild from
    Hive) is tested in test_cache_nodes.py and must stay; this is the other
    half — what a consumer handed such a directory should do when it cannot
    rebuild it (ADR-0014 decision 7, 驗收條件 row 2).
    """

    def _cache_root(self, tmp_path: Path, name: str, *, complete: bool) -> Path:
        root = tmp_path / name
        (root / "snap_date=2026-01-31").mkdir(parents=True)
        pd.DataFrame({"f": [1.0, 2.0]}).to_parquet(
            root / "snap_date=2026-01-31" / "part.parquet", engine="pyarrow"
        )
        if complete:
            (root / "_SUCCESS").touch()
        return root

    def test_a_complete_cache_passes(self, tmp_path: Path) -> None:
        from recsys_tfb.io.handles import ParquetHandle, require_complete_cache

        root = self._cache_root(tmp_path, "train", complete=True)
        require_complete_cache(ParquetHandle(path=str(root)))

    def test_a_marker_less_directory_raises(self, tmp_path: Path) -> None:
        from recsys_tfb.io.handles import ParquetHandle, require_complete_cache

        root = self._cache_root(tmp_path, "train", complete=False)
        with pytest.raises(ValueError, match="_SUCCESS"):
            require_complete_cache(ParquetHandle(path=str(root)))

    def test_every_root_of_a_sharded_handle_is_checked(self, tmp_path: Path) -> None:
        """test_model_input is one directory per month; one bad month is enough."""
        from recsys_tfb.io.handles import ParquetHandle, require_complete_cache

        good = self._cache_root(tmp_path, "2026-01-31", complete=True)
        bad = self._cache_root(tmp_path, "2026-02-28", complete=False)
        handles = {
            "2026-01-31": ParquetHandle(path=str(good)),
            "2026-02-28": ParquetHandle(path=str(bad)),
        }
        with pytest.raises(ValueError, match="2026-02-28"):
            require_complete_cache(handles)

    def test_a_single_parquet_file_is_not_a_cache_root(self, tmp_path: Path) -> None:
        """The marker is a property of the directories cache nodes write.

        A handle pointing at one parquet file carries no marker and never did,
        so requiring one would reject a shape the contract never covered.
        """
        from recsys_tfb.io.handles import ParquetHandle, require_complete_cache

        path = tmp_path / "one.parquet"
        pd.DataFrame({"f": [1.0]}).to_parquet(path, engine="pyarrow")
        require_complete_cache(ParquetHandle(path=str(path)))
