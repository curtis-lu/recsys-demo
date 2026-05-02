"""Tests for training pipeline cache nodes."""
from pathlib import Path

import pandas as pd
import pytest


def _params_with_versions(cache_root: str, enabled: bool = True) -> dict:
    return {
        "base_dataset_version": "base_v1",
        "train_variant_id": "train_v1",
        "calibration_variant_id": "calib_v1",
        "cache": {"enabled": enabled, "root": cache_root},
    }


# ---- _resolve_cache_path ----

class TestResolveCachePath:
    def test_val_uses_base_only(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        path = _resolve_cache_path("val_model_input", params)
        assert path == str(tmp_path / "base_v1" / "val_model_input.parquet")

    def test_test_uses_base_only(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        path = _resolve_cache_path("test_model_input", params)
        assert path == str(tmp_path / "base_v1" / "test_model_input.parquet")

    def test_train_uses_base_and_train_variant(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        path = _resolve_cache_path("train_model_input", params)
        assert path == str(
            tmp_path / "base_v1" / "train_variants" / "train_v1" / "train_model_input.parquet"
        )

    def test_train_dev_uses_base_and_train_variant(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        path = _resolve_cache_path("train_dev_model_input", params)
        assert path == str(
            tmp_path / "base_v1" / "train_variants" / "train_v1" / "train_dev_model_input.parquet"
        )

    def test_calibration_uses_base_and_calibration_variant(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        path = _resolve_cache_path("calibration_model_input", params)
        assert path == str(
            tmp_path / "base_v1" / "calibration_variants" / "calib_v1" / "calibration_model_input.parquet"
        )

    def test_unknown_dataset_raises(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params = _params_with_versions(str(tmp_path))
        with pytest.raises(ValueError, match="unknown dataset"):
            _resolve_cache_path("not_a_real_dataset", params)

    def test_changing_train_variant_changes_train_path_only(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _resolve_cache_path
        params_a = _params_with_versions(str(tmp_path))
        params_b = {**params_a, "train_variant_id": "train_v2"}
        assert _resolve_cache_path("train_model_input", params_a) != _resolve_cache_path(
            "train_model_input", params_b
        )
        assert _resolve_cache_path("val_model_input", params_a) == _resolve_cache_path(
            "val_model_input", params_b
        )


# ---- _is_spark_df ----

class TestIsSparkDataframe:
    def test_pandas_dataframe_is_not_spark(self):
        from recsys_tfb.pipelines.training.nodes import _is_spark_df
        assert _is_spark_df(pd.DataFrame({"a": [1]})) is False

    def test_object_with_sql_ctx_is_spark(self):
        from recsys_tfb.pipelines.training.nodes import _is_spark_df

        class Fake:
            sql_ctx = object()

        assert _is_spark_df(Fake()) is True

    def test_none_is_not_spark(self):
        from recsys_tfb.pipelines.training.nodes import _is_spark_df
        assert _is_spark_df(None) is False


class _FakeReader:
    def __init__(self, marker: object):
        self.marker = marker
        self.read_paths: list[str] = []

    def parquet(self, path: str):
        self.read_paths.append(path)
        return f"reread_from::{path}"


class _FakeSparkSession:
    def __init__(self, marker: object):
        self.read = _FakeReader(marker)


class _FakeSparkDF:
    def __init__(self):
        self.sql_ctx = type("SqlCtx", (), {})()
        self.sql_ctx.sparkSession = _FakeSparkSession(marker=self)


# ---- _cache_or_passthrough ----

class TestCacheOrPassthroughDev:
    def test_cache_disabled_returns_input_unchanged(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough
        params = _params_with_versions(str(tmp_path), enabled=False)
        df = pd.DataFrame({"a": [1]})
        out = _cache_or_passthrough(df, "train_model_input", params)
        assert out is df

    def test_pandas_input_with_cache_enabled_passthrough_with_warning(self, tmp_path, caplog):
        import logging
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough
        params = _params_with_versions(str(tmp_path), enabled=True)
        df = pd.DataFrame({"a": [1]})
        with caplog.at_level(logging.WARNING, logger="recsys_tfb.pipelines.training.nodes"):
            out = _cache_or_passthrough(df, "train_model_input", params)
        assert out is df
        assert any("not a Spark DataFrame" in rec.message for rec in caplog.records)


class TestCacheOrPassthroughProd:
    def test_cache_miss_returns_input_unchanged(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough
        params = _params_with_versions(str(tmp_path), enabled=True)
        df = _FakeSparkDF()
        out = _cache_or_passthrough(df, "train_model_input", params)
        assert out is df

    def test_cache_hit_returns_local_reread(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            _cache_or_passthrough,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("train_model_input", params))
        target.mkdir(parents=True)
        (target / "_SUCCESS").touch()

        df = _FakeSparkDF()
        out = _cache_or_passthrough(df, "train_model_input", params)
        assert out == f"reread_from::{target.as_uri()}"

    def test_partial_cache_is_cleared_and_treated_as_miss(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            _cache_or_passthrough,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("train_model_input", params))
        target.mkdir(parents=True)
        # NOTE: no _SUCCESS file -> partial
        (target / "garbage.parquet").write_text("partial")

        df = _FakeSparkDF()
        out = _cache_or_passthrough(df, "train_model_input", params)
        assert out is df
        assert not target.exists(), "partial cache must be removed"


# ---- Cache nodes ----

class TestCacheNodes:
    def test_cache_train_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_train_model_input,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("train_model_input", params))
        target.mkdir(parents=True)
        (target / "_SUCCESS").touch()
        out = cache_train_model_input(_FakeSparkDF(), params)
        assert out == f"reread_from::{target.as_uri()}"

    def test_cache_train_dev_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_train_dev_model_input,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("train_dev_model_input", params))
        target.mkdir(parents=True)
        (target / "_SUCCESS").touch()
        out = cache_train_dev_model_input(_FakeSparkDF(), params)
        assert out == f"reread_from::{target.as_uri()}"

    def test_cache_val_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_val_model_input,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("val_model_input", params))
        target.mkdir(parents=True)
        (target / "_SUCCESS").touch()
        out = cache_val_model_input(_FakeSparkDF(), params)
        assert out == f"reread_from::{target.as_uri()}"

    def test_cache_calibration_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_calibration_model_input,
            _resolve_cache_path,
        )
        params = _params_with_versions(str(tmp_path), enabled=True)
        target = Path(_resolve_cache_path("calibration_model_input", params))
        target.mkdir(parents=True)
        (target / "_SUCCESS").touch()
        out = cache_calibration_model_input(_FakeSparkDF(), params)
        assert out == f"reread_from::{target.as_uri()}"

    def test_cache_node_dev_passthrough(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input
        params = _params_with_versions(str(tmp_path), enabled=False)
        df = pd.DataFrame({"a": [1]})
        assert cache_train_model_input(df, params) is df


class TestCacheRunnerIntegration:
    """Integration: cache node short-circuits Hive read on second Runner run."""

    def test_second_run_uses_local_cache(self, tmp_path):
        from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
        from recsys_tfb.core.node import Node
        from recsys_tfb.core.pipeline import Pipeline
        from recsys_tfb.core.runner import Runner
        from recsys_tfb.io.base import AbstractDataset
        from recsys_tfb.pipelines.training.nodes import (
            _resolve_cache_path,
            cache_train_model_input,
        )

        params = _params_with_versions(str(tmp_path), enabled=True)
        cache_path = _resolve_cache_path("train_model_input", params)

        load_calls: list[int] = []

        class FakeHiveDataset(AbstractDataset):
            def __init__(self):
                self._df = _FakeSparkDF()

            def load(self):
                load_calls.append(1)
                return self._df

            def save(self, data):
                raise AssertionError("FakeHiveDataset.save should never be called by Runner")

            def exists(self):
                return True

        catalog = DataCatalog()
        catalog.add("train_model_input", FakeHiveDataset())
        catalog.add("parameters", MemoryDataset(data=params))

        pipeline = Pipeline([
            Node(
                cache_train_model_input,
                inputs=["train_model_input", "parameters"],
                outputs="cached_train_model_input",
            ),
        ])

        # First run: cache miss; cache node returns the input df unchanged.
        Runner().run(pipeline, catalog)
        assert len(load_calls) == 1
        fake_source = catalog.get_dataset("train_model_input")
        first_run_cached = catalog.load("cached_train_model_input")
        # Cache miss: the cache node returned the original df unchanged.
        assert first_run_cached is fake_source._df

        # Simulate framework's parquet write by populating _SUCCESS marker.
        # In production, ParquetDataset(write_mode=ignore) does this via Spark.
        Path(cache_path).mkdir(parents=True, exist_ok=True)
        (Path(cache_path) / "_SUCCESS").touch()

        # Second run: cache hit; cache node short-circuits and returns
        # spark.read.parquet(<uri>) (a string sentinel from _FakeReader).
        Runner().run(pipeline, catalog)
        # Runner.load is per-run; HiveTableDataset.load() returns a lazy plan
        # so this is cheap. The real cache-hit proof is the next assertion.
        assert len(load_calls) == 2
        second_run_cached = catalog.load("cached_train_model_input")
        assert second_run_cached == f"reread_from::{Path(cache_path).as_uri()}"


class TestParametersWiringRegression:
    """Regression: cache nodes need version IDs in parameters dict, not just runtime_params."""

    def test_cache_node_raises_clear_error_when_versions_missing(self, tmp_path):
        """Without the __main__.py fix, this test catches the KeyError early."""
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough

        # Simulate a params dict that has cache enabled but lacks version IDs
        # (i.e., what `__main__.py:119` would inject if the fix were missing).
        params_no_versions = {
            "cache": {"enabled": True, "root": str(tmp_path)},
        }
        df = _FakeSparkDF()

        # The current behavior: KeyError naming the missing key.
        # This test documents the expected failure mode for ops debugging.
        import pytest as _pytest
        with _pytest.raises(KeyError, match="base_dataset_version"):
            _cache_or_passthrough(df, "train_model_input", params_no_versions)

    def test_cache_node_runs_when_versions_present(self, tmp_path):
        """Mirrors the post-fix __main__.py behavior: substitution_params injected
        into parameters MemoryDataset, so cache helpers find the version IDs."""
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough

        # Simulate substitution_params = {**yaml_params, **runtime_params}
        params_with_versions = {
            "cache": {"enabled": True, "root": str(tmp_path)},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
        }
        df = _FakeSparkDF()

        # Cache miss path; should return df unchanged without raising.
        out = _cache_or_passthrough(df, "train_model_input", params_with_versions)
        assert out is df
