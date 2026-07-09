"""Tests for recsys_tfb.utils.spark.get_or_create_spark_session."""

import pytest
from pyspark.sql import SparkSession

from recsys_tfb.utils.spark import (
    _is_session_alive,
    get_or_create_spark_session,
    stop_spark_session,
)

pytestmark = pytest.mark.spark


@pytest.fixture(autouse=True)
def _stop_session_between_tests():
    """Ensure each test starts without an active SparkSession or remembered configs."""
    from pyspark.sql import SparkSession

    from recsys_tfb.utils.spark import reset_spark_session_state

    existing = SparkSession.getActiveSession()
    if existing is not None:
        existing.stop()
    reset_spark_session_state()
    yield
    after = SparkSession.getActiveSession()
    if after is not None:
        after.stop()
    reset_spark_session_state()


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

    def test_fallback_validates_yaml_values(self, monkeypatch, tmp_path):
        """yaml 路徑仍套 _validate_values:非 str/int/bool 的值要 raise。

        _build_from_yaml 改走 _build(不遞迴回 mode-1,避免污染 canonical
        記憶),因此必須自己補回驗證。這裡放一個 list 值證明驗證生效。
        """
        conf = tmp_path / "conf"
        (conf / "base").mkdir(parents=True)
        (conf / "base" / "parameters.yaml").write_text(
            "spark:\n"
            "  app_name: bad-yaml\n"
            "  spark.master: local[1]\n"
            "  bad_key:\n"
            "    - 1\n"
            "    - 2\n"
        )
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ValueError, match="bad_key"):
            get_or_create_spark_session(None)

    def test_fallback_resolves_env_placeholders(self, monkeypatch, tmp_path):
        """Fallback path serves ${env.*}-resolved spark config to the builder.

        Regression: previously, when tune_hyperparameters stopped the session
        and a downstream node triggered fallback rebuild, yaml values like
        ``${vdclient.cdp.driver_port}`` reached SparkConf as literal strings →
        ``spark.driver.port should be int`` at runtime. ${env.*} resolution
        now happens inside ConfigLoader (not a direct resolver call in
        _fallback_create); this env-based case is the unit-testable surrogate.
        """
        monkeypatch.setenv("RECSYS_TFB_TEST_APP_NAME", "from-env-placeholder")
        conf = tmp_path / "conf"
        (conf / "base").mkdir(parents=True)
        (conf / "base" / "parameters.yaml").write_text(
            "spark:\n"
            "  app_name: ${env.RECSYS_TFB_TEST_APP_NAME}\n"
            "  spark.master: local[1]\n"
            "  spark.sql.shuffle.partitions: '1'\n"
            "  spark.default.parallelism: '1'\n"
            "  spark.ui.enabled: 'false'\n"
            "  spark.driver.memory: 512m\n"
        )
        monkeypatch.chdir(tmp_path)

        spark = get_or_create_spark_session(None)
        try:
            assert spark.sparkContext.appName == "from-env-placeholder"
        finally:
            spark.stop()


class TestCanonicalConfigs:
    def test_mode2_rebuild_uses_remembered_configs(self, monkeypatch, tmp_path):
        """mode-2 重建用 mode-1 記住的 configs,不重讀 yaml。

        chdir 到一個沒有 conf/ 的空目錄:若實作退回 yaml,會 raise
        RuntimeError('conf/ not found'),測試就抓得到。
        """
        first = get_or_create_spark_session(
            _minimal_configs({"app_name": "canonical-app"})
        )
        first.stop()
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(None)
        try:
            assert second.sparkContext.appName == "canonical-app"
        finally:
            second.stop()

    def test_mode2_rebuild_remembers_enable_hive(self, monkeypatch, tmp_path):
        first = get_or_create_spark_session(
            _minimal_configs(), enable_hive=True
        )
        first.stop()
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(None)
        try:
            assert (
                second.conf.get("spark.sql.catalogImplementation") == "hive"
            )
        finally:
            second.stop()


class TestDeadContextRecovery:
    """JVM 端停掉 SparkContext(等同 cluster 端殺掉 app)後的復原。

    根因同一個:JVM 端自行死亡不動 Python 端狀態。PySpark 的 getOrCreate 只在
    Python 端 _jsc is None 時才重建(pyspark/sql/session.py:264),而 _jsc 只有
    Python 端 SparkContext.stop() 才會設成 None(pyspark/context.py:568)。同一個
    根因在死亡前 session 有沒有被用過而有兩種表現:

    - 用過的 session(SharedState / sessionState 已 lazy init):getOrCreate 走
      else 分支呼叫 applyModifiableSettings,不重建 SharedState,於是**安靜地把
      同一個死 session 原封不動回傳**;要到之後 createDataFrame 摸到
      defaultParallelism 才炸 stopped SparkContext。這是**生產那條**——HPO 前
      cache_* 節點早把 session 用熱了。
    - 從未用過的 session:applyModifiableSettings 會觸發 SharedState.<init>,
      當場撞已停的 LiveListenerBus 而拋 Py4JJavaError(LiveListenerBus is
      stopped)。這是另一條表現,不是生產走的那條。

    兩條都被 _stop_and_clear(先 Python 端 stop 清掉單例)修好。前兩個測試暖機以
    釘住生產那條,第三個測試守未暖機那條。
    """

    @staticmethod
    def _kill_jvm_context(session):
        """Stop the JVM-side SparkContext, leaving Python-side state intact."""
        session.sparkContext._jsc.sc().stop()

    def test_mode2_rebuilds_after_jvm_side_stop(self, monkeypatch, tmp_path):
        first = get_or_create_spark_session(_minimal_configs())
        # 暖機:讓 SharedState / sessionState 完成 lazy init。生產情境裡 session
        # 早就被 cache_* 節點用過了;沒暖機的話 getOrCreate 會在
        # applyModifiableSettings 觸發 SharedState.<init> 而當場炸
        # (LiveListenerBus is stopped),那是另一條失敗路徑,不是我們要守的那條。
        assert first.createDataFrame([(0,)], ["a"]).count() == 1
        self._kill_jvm_context(first)
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(None)
        try:
            assert second is not first
            assert second.createDataFrame([(1,), (2,)], ["a"]).count() == 2
        finally:
            second.stop()

    def test_mode1_rebuilds_after_jvm_side_stop(self, monkeypatch, tmp_path):
        first = get_or_create_spark_session(_minimal_configs())
        # 暖機:見 test_mode2_rebuilds_after_jvm_side_stop 的說明。
        assert first.createDataFrame([(0,)], ["a"]).count() == 1
        self._kill_jvm_context(first)
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(_minimal_configs())
        try:
            assert second is not first
            assert second.createDataFrame([(1,)], ["a"]).count() == 1
        finally:
            second.stop()

    def test_rebuilds_after_jvm_side_stop_on_never_used_session(
        self, monkeypatch, tmp_path
    ):
        """未暖機的 session 死掉時,getOrCreate 會在 applyModifiableSettings
        觸發 SharedState.<init> 而拋 LiveListenerBus is stopped——與「用過的
        session 被安靜回傳」是同一個根因(Python 端單例沒被清)的兩種表現。
        """
        first = get_or_create_spark_session(_minimal_configs())
        self._kill_jvm_context(first)
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(None)
        try:
            assert second is not first
            assert second.createDataFrame([(1,)], ["a"]).count() == 1
        finally:
            second.stop()


class TestStopSparkSession:
    """明確釋放 session,讓 driver-local 的長區段不必抱著閒置的 executors。"""

    def test_stops_alive_session_and_is_idempotent(self):
        session = get_or_create_spark_session(_minimal_configs())
        assert _is_session_alive(session)

        assert stop_spark_session() is True
        assert SparkSession.getActiveSession() is None

        # 第二次沒有東西可停
        assert stop_spark_session() is False

    def test_stops_jvm_dead_session_without_raising(self):
        """對 JVM 端已死的 session 呼叫也要安全——這是叢集回收 app 後的狀態。"""
        session = get_or_create_spark_session(_minimal_configs())
        session.sparkContext._jsc.sc().stop()

        assert stop_spark_session() is True
        assert SparkSession._instantiatedSession is None
