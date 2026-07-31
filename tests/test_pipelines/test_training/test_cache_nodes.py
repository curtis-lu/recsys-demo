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


def _params_with_test_dates(cache_root: Path, test_snap_dates: list[str]) -> dict:
    """Cache params carrying a ``dataset.test_snap_dates`` list.

    Mirrors the merged parameters dict the CLI hands to nodes (all
    ``parameters_*.yaml`` deep-merged + runtime version params).
    """
    params = _params_with_cache_root(cache_root)
    params["dataset"] = {"test_snap_dates": list(test_snap_dates)}
    return params


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


class TestTestWindowCacheKey:
    """``test_model_input``'s cache is keyed on the literal, sorted test months.

    Adding a month to ``dataset.test_snap_dates`` must force a fresh copy;
    re-running the same config must still hit. The months appear verbatim in the
    path (deliberately not hashed) so ``ls`` answers "is this the cache I want?"
    without reverse-engineering a digest — that readability is the external
    contract, which is why a path component is asserted here at all.
    """

    @staticmethod
    def _run_with_dates(monkeypatch, tmp_path, dates, copy_calls):
        """Run cache_test_model_input under `dates`, recording copy destinations."""
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            lambda *a, **kw: "hdfs:/some/path",
        )

        def _record(spark, src_glob, dst, glob):
            copy_calls.append(dst)
            Path(dst).mkdir(parents=True, exist_ok=True)

        monkeypatch.setattr(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local", _record
        )
        df = MagicMock()
        df.sql_ctx.sparkSession = MagicMock()
        return cache_test_model_input(df, _params_with_test_dates(tmp_path, dates))

    def test_same_test_dates_rerun_hits_cache(self, tmp_path, monkeypatch):
        copy_calls: list[str] = []
        dates = ["2026-01-31", "2026-02-28"]

        first = self._run_with_dates(monkeypatch, tmp_path, dates, copy_calls)
        second = self._run_with_dates(monkeypatch, tmp_path, dates, copy_calls)

        assert second.path == first.path
        assert copy_calls == [first.path]

    def test_changed_test_dates_rerun_misses_cache(self, tmp_path, monkeypatch):
        copy_calls: list[str] = []

        first = self._run_with_dates(monkeypatch, tmp_path, ["2026-01-31"], copy_calls)
        second = self._run_with_dates(
            monkeypatch, tmp_path, ["2026-01-31", "2026-02-28"], copy_calls
        )

        assert second.path != first.path
        assert copy_calls == [first.path, second.path]

    def test_cache_path_carries_literal_sorted_months(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path

        params = _params_with_test_dates(tmp_path, ["2026-02-28", "2026-01-31"])

        parts = Path(_resolve_cache_path("test_model_input", params)).parts

        assert "20260131_20260228" in parts

    def test_other_splits_layout_untouched_by_test_dates(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path

        one = _params_with_test_dates(tmp_path, ["2026-01-31"])
        two = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        for name in (
            "train_model_input",
            "train_dev_model_input",
            "val_model_input",
            "calibration_model_input",
        ):
            assert _resolve_cache_path(name, one) == _resolve_cache_path(name, two)

        # val is test's structural twin (same single-layer layout before this
        # change); pin its literal path so "the window landed on the wrong
        # split" cannot pass as "both sides moved together".
        assert _resolve_cache_path("val_model_input", two) == str(
            tmp_path / "deadbeef" / "val_model_input.parquet"
        )

    def test_absent_test_dates_resolve_to_explicit_sentinel(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path

        empty = _params_with_test_dates(tmp_path, [])
        missing = _params_with_cache_root(tmp_path)  # no `dataset` block at all

        empty_path = _resolve_cache_path("test_model_input", empty)

        assert Path(empty_path).parts[-2] == "no_test_dates"
        assert _resolve_cache_path("test_model_input", missing) == empty_path


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
