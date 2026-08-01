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
    """Cache params carrying ``dataset.test_snap_dates``.

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


def _recording_hdfs(monkeypatch, copy_calls: list, available: set | None = None):
    """Stub the HDFS layer, recording every copy destination.

    ``available``: the ``snap_date=`` values the source table holds. A glob that
    matches nothing raises FileNotFoundError — the real ``copy_hdfs_to_local``
    does exactly this (utils/hdfs.py), and per-month copying is what turns it
    into the "you forgot to run dataset" guard.
    """
    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
        lambda *a, **kw: "hdfs:/some/path",
    )

    def _copy(spark, src_glob, dst, glob):
        if available is not None:
            month = src_glob.rsplit("snap_date=", 1)[-1]
            if month not in available:
                raise FileNotFoundError(f"No HDFS paths matched: {src_glob}")
        copy_calls.append(dst)
        Path(dst).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local", _copy
    )


def _spark_df() -> MagicMock:
    df = MagicMock()
    df.sql_ctx.sparkSession = MagicMock()
    return df


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


class TestPerMonthTestCache:
    """``test_model_input`` caches one directory per configured test month.

    Each month is its own cache entry with its own ``_SUCCESS``, so adding a
    month adds a directory and leaves every existing month untouched. The
    directory name states exactly one month, so name and contents cannot
    disagree.
    """

    def test_returns_one_handle_per_configured_month(self, tmp_path, monkeypatch):
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        _stub_hdfs(monkeypatch)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        handles = cache_test_model_input(_spark_df(), params)

        # keys are verbatim config values — no format conversion
        assert sorted(handles) == ["2026-01-31", "2026-02-28"]
        assert all(isinstance(h, ParquetHandle) for h in handles.values())

    def test_month_directory_is_literal_yyyymmdd(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        _stub_hdfs(monkeypatch)
        params = _params_with_test_dates(tmp_path, ["2026-01-31"])

        handle = cache_test_model_input(_spark_df(), params)["2026-01-31"]

        # Assert the adjacent pair: asserting the month alone would leave the
        # `test_months` grouping layer with no contract at all.
        assert Path(handle.path).parts[-3:-1] == ("test_months", "20260131")

    def test_adding_a_month_does_not_recopy_existing_months(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)

        before = cache_test_model_input(
            _spark_df(), _params_with_test_dates(tmp_path, ["2026-01-31"])
        )
        copy_calls.clear()
        after = cache_test_model_input(
            _spark_df(), _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])
        )

        # only the new month was copied; January's directory is reused as-is
        assert copy_calls == [after["2026-02-28"].path]
        assert after["2026-01-31"].path == before["2026-01-31"].path

    def test_same_config_rerun_hits_every_month(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        cache_test_model_input(_spark_df(), params)
        copy_calls.clear()
        cache_test_model_input(_spark_df(), params)

        assert copy_calls == []

    def test_month_absent_from_source_fails_loud(self, tmp_path, monkeypatch):
        """Configured a month but never ran dataset → the copy glob matches
        nothing and FileNotFoundError propagates. This guard is inherent to
        per-month copying; no extra coverage check is written for it."""
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls, available={"2026-01-31"})
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])

        with pytest.raises(FileNotFoundError, match="2026-02-28"):
            cache_test_model_input(_spark_df(), params)

    def test_partial_month_rebuilds_without_touching_siblings(self, tmp_path, monkeypatch):
        from recsys_tfb.pipelines.training.nodes import cache_test_model_input

        copy_calls: list = []
        _recording_hdfs(monkeypatch, copy_calls)
        params = _params_with_test_dates(tmp_path, ["2026-01-31", "2026-02-28"])
        handles = cache_test_model_input(_spark_df(), params)

        # corrupt only February: drop its _SUCCESS and leave debris behind
        feb = Path(handles["2026-02-28"].path)
        (feb / "_SUCCESS").unlink()
        (feb / "stale_partial.parquet").touch()
        copy_calls.clear()

        cache_test_model_input(_spark_df(), params)

        assert copy_calls == [str(feb)]
        assert not (feb / "stale_partial.parquet").exists()
        assert (feb / "_SUCCESS").exists()

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

        # val is test's structural twin (single-layer before this change); pin
        # its literal path so "the month layer landed on the wrong split"
        # cannot pass as "both sides moved together".
        assert _resolve_cache_path("val_model_input", two) == str(
            tmp_path / "deadbeef" / "val_model_input.parquet"
        )


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
