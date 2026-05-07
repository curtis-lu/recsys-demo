"""Tests for training pipeline cache nodes."""
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

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


class _FakeSparkDF:
    """Minimal spark-df stand-in: only carries sql_ctx.sparkSession."""

    def __init__(self):
        self.sql_ctx = type("SqlCtx", (), {})()
        self.sql_ctx.sparkSession = MagicMock(name="spark")


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
    def _params(self, tmp_path):
        return {
            "hive": {"db": "ml_recsys"},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
            "cache": {"enabled": True, "root": str(tmp_path)},
        }

    def _seed_local_cache(self, local_path: Path):
        """Write a minimal valid hive-partitioned parquet so pd.read_parquet works."""
        part = local_path / "snap_date=2025-10-31" / "prod_name=fund"
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"a": [1, 2], "label": [0, 1]}).to_parquet(
            part / "data.parquet"
        )

    def test_cache_miss_triggers_populate_and_returns_pandas(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            _cache_or_passthrough,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("train_model_input", params))

        # Mock _populate_cache_from_hive to materialize a fake parquet locally
        def fake_populate(spark, name, params_arg, dst):
            self._seed_local_cache(Path(dst))

        with patch(
            "recsys_tfb.pipelines.training.nodes._populate_cache_from_hive",
            side_effect=fake_populate,
        ) as mock_populate:
            out = _cache_or_passthrough(
                _FakeSparkDF(), "train_model_input", params
            )

        # Assertions
        assert isinstance(out, pd.DataFrame)
        assert "a" in out.columns
        assert "snap_date" in out.columns  # partition col restored by pyarrow
        assert "prod_name" in out.columns
        mock_populate.assert_called_once()
        assert (local / "_SUCCESS").exists()

    def test_cache_hit_skips_populate_and_returns_pandas(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            _cache_or_passthrough,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("train_model_input", params))
        local.mkdir(parents=True)
        self._seed_local_cache(local)
        (local / "_SUCCESS").touch()

        with patch(
            "recsys_tfb.pipelines.training.nodes._populate_cache_from_hive"
        ) as mock_populate:
            out = _cache_or_passthrough(
                _FakeSparkDF(), "train_model_input", params
            )

        assert isinstance(out, pd.DataFrame)
        assert "a" in out.columns
        mock_populate.assert_not_called()

    def test_partial_cache_clears_and_repopulates(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            _cache_or_passthrough,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("train_model_input", params))
        local.mkdir(parents=True)
        # No _SUCCESS — partial state
        (local / "garbage").write_text("partial")

        def fake_populate(spark, name, params_arg, dst):
            self._seed_local_cache(Path(dst))

        with patch(
            "recsys_tfb.pipelines.training.nodes._populate_cache_from_hive",
            side_effect=fake_populate,
        ) as mock_populate:
            out = _cache_or_passthrough(
                _FakeSparkDF(), "train_model_input", params
            )

        assert isinstance(out, pd.DataFrame)
        mock_populate.assert_called_once()
        assert not (local / "garbage").exists()  # partial cleared
        assert (local / "_SUCCESS").exists()


# ---- Cache nodes ----

class TestCacheNodes:
    def _params(self, tmp_path):
        return {
            "hive": {"db": "ml_recsys"},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
            "cache": {"enabled": True, "root": str(tmp_path)},
        }

    def _seed_cache(self, local_path: Path):
        part = local_path / "snap_date=2025-10-31" / "prod_name=fund"
        part.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"a": [1]}).to_parquet(part / "data.parquet")
        (local_path / "_SUCCESS").touch()

    def test_cache_train_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_train_model_input,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("train_model_input", params))
        local.mkdir(parents=True)
        self._seed_cache(local)

        out = cache_train_model_input(_FakeSparkDF(), params)
        assert isinstance(out, pd.DataFrame)

    def test_cache_train_dev_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_train_dev_model_input,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("train_dev_model_input", params))
        local.mkdir(parents=True)
        self._seed_cache(local)

        out = cache_train_dev_model_input(_FakeSparkDF(), params)
        assert isinstance(out, pd.DataFrame)

    def test_cache_val_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_val_model_input,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("val_model_input", params))
        local.mkdir(parents=True)
        self._seed_cache(local)

        out = cache_val_model_input(_FakeSparkDF(), params)
        assert isinstance(out, pd.DataFrame)

    def test_cache_calibration_model_input_passes_dataset_name(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import (
            cache_calibration_model_input,
            _resolve_cache_path,
        )

        params = self._params(tmp_path)
        local = Path(_resolve_cache_path("calibration_model_input", params))
        local.mkdir(parents=True)
        self._seed_cache(local)

        out = cache_calibration_model_input(_FakeSparkDF(), params)
        assert isinstance(out, pd.DataFrame)

    def test_cache_node_dev_passthrough(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input
        params = self._params(tmp_path)
        params["cache"]["enabled"] = False
        df = pd.DataFrame({"a": [1]})
        assert cache_train_model_input(df, params) is df


class TestCacheRunnerIntegration:
    """Integration: cache node short-circuits HDFS copy on second Runner run."""

    def test_second_run_uses_local_cache(self, tmp_path):
        from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
        from recsys_tfb.core.node import Node
        from recsys_tfb.core.pipeline import Pipeline
        from recsys_tfb.core.runner import Runner
        from recsys_tfb.io.base import AbstractDataset
        from recsys_tfb.pipelines.training.nodes import (
            cache_train_model_input,
            _resolve_cache_path,
        )

        params = {
            "hive": {"db": "ml_recsys"},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
            "cache": {"enabled": True, "root": str(tmp_path)},
        }
        cache_path = Path(_resolve_cache_path("train_model_input", params))

        populate_calls: list[str] = []

        def fake_populate(spark, name, params_arg, dst):
            populate_calls.append(name)
            part = Path(dst) / "snap_date=2025-10-31" / "prod_name=fund"
            part.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"a": [1]}).to_parquet(part / "data.parquet")

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

        with patch(
            "recsys_tfb.pipelines.training.nodes._populate_cache_from_hive",
            side_effect=fake_populate,
        ):
            # First run: cache miss; populate called once
            Runner().run(pipeline, catalog)
            assert populate_calls == ["train_model_input"]
            assert (cache_path / "_SUCCESS").exists()
            first_cached = catalog.load("cached_train_model_input")
            assert isinstance(first_cached, pd.DataFrame)

            # Second run: cache hit; populate NOT called again
            Runner().run(pipeline, catalog)
            assert populate_calls == ["train_model_input"]  # still 1, not 2
            second_cached = catalog.load("cached_train_model_input")
            assert isinstance(second_cached, pd.DataFrame)


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

        params_with_versions = {
            "hive": {"db": "ml_recsys"},
            "cache": {"enabled": True, "root": str(tmp_path)},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
        }

        # Mock populate so we don't actually try HDFS
        def fake_populate(spark, name, params_arg, dst):
            part = Path(dst) / "snap_date=A" / "prod_name=B"
            part.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"a": [1]}).to_parquet(part / "data.parquet")

        with patch(
            "recsys_tfb.pipelines.training.nodes._populate_cache_from_hive",
            side_effect=fake_populate,
        ):
            df = _FakeSparkDF()
            out = _cache_or_passthrough(
                df, "train_model_input", params_with_versions
            )

        assert isinstance(out, pd.DataFrame)


class TestPopulateCacheFromHive:
    def _params(self, tmp_path):
        return {
            "hive": {"db": "ml_recsys"},
            "base_dataset_version": "base_v1",
            "train_variant_id": "train_v1",
            "calibration_variant_id": "calib_v1",
            "cache": {"enabled": True, "root": str(tmp_path)},
        }

    def test_train_model_input_constructs_correct_src_glob(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _populate_cache_from_hive

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            return_value="hdfs://nn/warehouse/ml_recsys.db/train_model_input",
        ), patch(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local"
        ) as mock_copy:
            _populate_cache_from_hive(
                MagicMock(), "train_model_input", self._params(tmp_path), "/tmp/dst"
            )

        mock_copy.assert_called_once_with(
            ANY,
            "hdfs://nn/warehouse/ml_recsys.db/train_model_input"
            "/base_dataset_version=base_v1/train_variant_id=train_v1/snap_date=*",
            "/tmp/dst",
            glob=True,
        )

    def test_val_model_input_does_not_include_train_variant(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _populate_cache_from_hive

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            return_value="hdfs://nn/warehouse/ml_recsys.db/val_model_input",
        ), patch(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local"
        ) as mock_copy:
            _populate_cache_from_hive(
                MagicMock(), "val_model_input", self._params(tmp_path), "/tmp/dst"
            )

        mock_copy.assert_called_once_with(
            ANY,
            "hdfs://nn/warehouse/ml_recsys.db/val_model_input"
            "/base_dataset_version=base_v1/snap_date=*",
            "/tmp/dst",
            glob=True,
        )

    def test_calibration_model_input_uses_calibration_variant(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import _populate_cache_from_hive

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            return_value="hdfs://nn/warehouse/ml_recsys.db/calibration_model_input",
        ), patch(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local"
        ) as mock_copy:
            _populate_cache_from_hive(
                MagicMock(),
                "calibration_model_input",
                self._params(tmp_path),
                "/tmp/dst",
            )

        mock_copy.assert_called_once_with(
            ANY,
            "hdfs://nn/warehouse/ml_recsys.db/calibration_model_input"
            "/base_dataset_version=base_v1/calibration_variant_id=calib_v1/snap_date=*",
            "/tmp/dst",
            glob=True,
        )

    def test_source_table_override_from_parameters(self, tmp_path):
        """parameters['cache']['source_tables'] overrides _CACHE_SOURCE_TABLE.

        Real-world use: company prod env that prefixes Hive table names
        (e.g. recsys_prod_train_model_input). Override aligns the cache
        lookup with catalog.yaml's HiveTableDataset.table field.
        """
        from recsys_tfb.pipelines.training.nodes import _populate_cache_from_hive

        params = self._params(tmp_path)
        params["cache"]["source_tables"] = {
            "train_model_input": "recsys_prod_train_model_input"
        }

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_hive_table_location",
            return_value="hdfs://nn/warehouse/ml_recsys.db/recsys_prod_train_model_input",
        ) as mock_loc, patch(
            "recsys_tfb.pipelines.training.nodes.copy_hdfs_to_local"
        ) as mock_copy:
            _populate_cache_from_hive(
                MagicMock(), "train_model_input", params, "/tmp/dst"
            )

        # Override flows into get_hive_table_location's `table` arg
        mock_loc.assert_called_once_with(ANY, "ml_recsys", "recsys_prod_train_model_input")
        # And the resolved location is used for the glob pattern
        mock_copy.assert_called_once_with(
            ANY,
            "hdfs://nn/warehouse/ml_recsys.db/recsys_prod_train_model_input"
            "/base_dataset_version=base_v1/train_variant_id=train_v1/snap_date=*",
            "/tmp/dst",
            glob=True,
        )
