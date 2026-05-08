"""Tests for training cache nodes (post-refactor).

Cache nodes now write parquet to driver-local fs and return a ParquetHandle.
The ``cache.enabled=false`` passthrough mode has been removed; tests must
provide a writable cache_root via tmp_path.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _params_with_cache_root(cache_root: Path) -> dict:
    return {
        "hive": {"db": "ml_recsys"},
        "cache": {"root": str(cache_root)},
        "base_dataset_version": "deadbeef",
        "train_variant_id": "v1",
        "calibration_variant_id": "c1",
    }


def _stub_hdfs(monkeypatch, location: str = "hdfs:/some/path") -> None:
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
        lambda spark, db, table: location,
    )
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local",
        lambda spark, src_glob, dst, glob: Path(dst).mkdir(parents=True, exist_ok=True),
    )


class TestCacheNodeReturnHandle:
    def test_cache_train_returns_parquet_handle(self, tmp_path, monkeypatch):
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_train_model_input(df, params)

        assert isinstance(handle, ParquetHandle)
        assert "train_model_input" in handle.path

    def test_cache_creates_success_marker(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_val_model_input

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()

        params = _params_with_cache_root(tmp_path)
        handle = cache_val_model_input(df, params)

        success = Path(handle.path) / "_SUCCESS"
        assert success.exists()


class TestCacheHit:
    def test_skip_copy_when_success_marker_present(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import (
            _resolve_cache_path,
            cache_train_model_input,
        )

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(_resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "_SUCCESS").touch()

        copy_calls = []
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local",
            lambda *a, **kw: copy_calls.append(1),
        )
        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            lambda *a, **kw: "hdfs:/some/path",
        )

        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert copy_calls == []


class TestPartialCacheRecovery:
    def test_rmtree_when_dir_exists_without_success(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import (
            _resolve_cache_path,
            cache_train_model_input,
        )

        params = _params_with_cache_root(tmp_path)
        cache_path = Path(_resolve_cache_path("train_model_input", params))
        cache_path.mkdir(parents=True, exist_ok=True)
        (cache_path / "stale_partial.parquet").touch()

        _stub_hdfs(monkeypatch)
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        cache_train_model_input(df, params)

        assert not (cache_path / "stale_partial.parquet").exists()
        assert (cache_path / "_SUCCESS").exists()


class TestRejectsNonSparkInput:
    def test_passthrough_mode_removed(self, tmp_path):
        """cache.enabled=false has been removed; pandas inputs must be rejected."""
        import pandas as pd
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input

        params = _params_with_cache_root(tmp_path)
        df = pd.DataFrame({"a": [1]})  # not a Spark DataFrame

        with pytest.raises(TypeError, match="Spark DataFrame"):
            cache_train_model_input(df, params)


class TestPrepareLgbTrainInputs:
    def test_prepare_node_returns_two_lgb_handles(self, tmp_path):
        import pandas as pd
        from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
        from recsys_tfb.pipelines.training.nodes import prepare_lgb_train_inputs

        df = pd.DataFrame(
            {
                "cust_id": ["c1", "c2", "c3", "c4"],
                "snap_date": pd.to_datetime(["2025-01-31"] * 4),
                "prod_name": ["fund", "ccard", "fund", "ccard"],
                "feat_a": [1.0, 2.0, 3.0, 4.0],
                "label": [0, 1, 0, 1],
            }
        )
        train_dir = tmp_path / "tr.parquet"
        dev_dir = tmp_path / "dev.parquet"
        df.to_parquet(train_dir)
        df.to_parquet(dev_dir)

        prep_meta = {
            "feature_columns": ["feat_a", "prod_name"],
            "categorical_columns": ["prod_name"],
            "category_mappings": {"prod_name": ["fund", "ccard"]},
        }
        parameters = {
            "cache": {"root": str(tmp_path / "cache")},
            "base_dataset_version": "v1",
            "train_variant_id": "tv1",
            "schema": {
                "label": "label",
                "identity_columns": ["cust_id", "snap_date", "prod_name"],
            },
            "training": {"algorithm": "lightgbm"},
        }

        train_h, dev_h = prepare_lgb_train_inputs(
            ParquetHandle(str(train_dir)),
            ParquetHandle(str(dev_dir)),
            prep_meta,
            parameters,
        )

        assert isinstance(train_h, LgbDatasetHandle)
        assert isinstance(dev_h, LgbDatasetHandle)
        assert train_h.role == "train"
        assert dev_h.role == "train_dev"
