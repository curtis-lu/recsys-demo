"""Tests for recsys_tfb.utils.spark.get_or_create_spark_session."""

import pytest

from recsys_tfb.utils.spark import get_or_create_spark_session


@pytest.fixture(autouse=True)
def _stop_session_between_tests():
    """Ensure each test starts without an active SparkSession."""
    from pyspark.sql import SparkSession

    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    yield
    after = SparkSession.getActiveSession()
    if after is not None:
        after.stop()


def _minimal_configs(extra: dict | None = None) -> dict:
    base = {
        "app_name": "recsys_tfb_test",
        "spark.master": "local[1]",
        "spark.sql.shuffle.partitions": "1",
        "spark.default.parallelism": "1",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "512m",
    }
    if extra:
        base.update(extra)
    return base


class TestWithConfigs:
    def test_creates_session_with_passed_configs(self):
        configs = _minimal_configs(
            {"spark.sql.session.timeZone": "Asia/Taipei"}
        )
        spark = get_or_create_spark_session(configs)
        try:
            assert (
                spark.conf.get("spark.sql.session.timeZone") == "Asia/Taipei"
            )
        finally:
            spark.stop()

    def test_app_name_from_configs(self):
        configs = _minimal_configs({"app_name": "my-custom-app"})
        spark = get_or_create_spark_session(configs)
        try:
            assert spark.sparkContext.appName == "my-custom-app"
        finally:
            spark.stop()

    def test_app_name_default_when_missing(self):
        configs = _minimal_configs()
        del configs["app_name"]
        spark = get_or_create_spark_session(configs)
        try:
            assert spark.sparkContext.appName == "recsys_tfb"
        finally:
            spark.stop()


class TestValidation:
    def test_non_dict_raises_typeerror(self):
        with pytest.raises(TypeError, match="must be a dict"):
            get_or_create_spark_session("not a dict")  # type: ignore[arg-type]

    def test_invalid_value_type_raises_valueerror(self):
        with pytest.raises(ValueError, match="bad_key"):
            get_or_create_spark_session(
                {"app_name": "x", "bad_key": [1, 2, 3]}
            )


class TestFallback:
    def test_no_configs_returns_active_session(self):
        first = get_or_create_spark_session(_minimal_configs())
        try:
            second = get_or_create_spark_session(None)
            assert second is first
        finally:
            first.stop()

    def test_no_configs_no_active_falls_back_to_loader(
        self, monkeypatch, tmp_path
    ):
        # Build a fake conf/ dir with parameters.yaml that has spark: block
        conf = tmp_path / "conf"
        (conf / "base").mkdir(parents=True)
        (conf / "base" / "parameters.yaml").write_text(
            "spark:\n"
            "  app_name: from-fallback\n"
            "  spark.master: local[1]\n"
            "  spark.sql.shuffle.partitions: '1'\n"
            "  spark.default.parallelism: '1'\n"
            "  spark.ui.enabled: 'false'\n"
            "  spark.driver.memory: 512m\n"
        )
        monkeypatch.chdir(tmp_path)

        spark = get_or_create_spark_session(None)
        try:
            assert spark.sparkContext.appName == "from-fallback"
        finally:
            spark.stop()
