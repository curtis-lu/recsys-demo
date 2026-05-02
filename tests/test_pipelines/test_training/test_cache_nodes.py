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
