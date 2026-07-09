# Spark Session Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 training pipeline 在長時間 HPO 之後仍能可靠完成 test prediction 寫入——修好從未生效的 SparkSession 重建路徑，並主動釋放 HPO 期間閒置的 session。

**Architecture:** `src/recsys_tfb/utils/spark.py` 從無狀態的 `getOrCreate` 包裝改為薄的 module-level manager：mode-1（帶 configs，五個 CLI entry 呼叫）記住 canonical configs；mode-2（無參數，所有 IO 呼叫）在偵測到 session 已死時，先用 Python 端 `.stop()` 清掉 PySpark 的單例，再用記住的 configs 真的重建。`tune_hyperparameters` 第一行呼叫 `release_spark_session(parameters)` 主動釋放 session，消除數小時的閒置窗口。不新增 DAG 節點。

**Tech Stack:** Python 3.10.9、PySpark 3.3.2、pytest。設計依據見 `docs/superpowers/specs/2026-07-09-spark-session-lifecycle-design.md`。

---

## 環境鐵則（每個 task 都適用，不要省）

所有指令從 worktree root 執行：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
```

跑測試一律用絕對路徑的 venv python + 指向本 worktree 的 `PYTHONPATH`（裸跑會抓到 main 的 `src`，靜默測錯 code）：

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

**不要跑 `tests/test_evaluation` 全量**（約 33 分鐘）。本計劃只需要 `tests/test_utils/test_spark.py` 與 `tests/test_pipelines/test_training/`。

**Task 0 之前先建 baseline**：main 上有既知 failing / 互擾測試（清單見 `docs/operations/known-pitfalls.md §5`），不先記錄 baseline 就無法區分「我弄壞的」與「本來就壞的」。

---

## File Structure

| 檔案 | 責任 | 動作 |
|---|---|---|
| `src/recsys_tfb/utils/spark.py` | SparkSession 生命週期的唯一擁有者：建立、canonical config 記憶、死亡偵測、清除、重建、釋放、instrumentation | 改寫（146 行 → 約 230 行） |
| `src/recsys_tfb/pipelines/training/nodes.py` | 在 `tune_hyperparameters` 第一行釋放 session | 加 1 個 import + 3 行 |
| `conf/base/parameters_training.yaml` | `spark_lifecycle.release_during_hpo` 開關（頂層 ops block，不影響 `model_version`） | 加 3 行 |
| `tests/test_utils/test_spark.py` | manager 的單元測試 | 加 autouse 重置 + 7 個測試 |
| `tests/test_pipelines/test_training/test_release_spark_session.py` | 驗證 `tune_hyperparameters` 第一行就釋放 session | 新建 |

`utils/spark.py` 最終的公開介面：

- `get_or_create_spark_session(spark_configs=None, enable_hive=False) -> SparkSession`（簽名不變）
- `stop_spark_session() -> bool`
- `release_spark_session(parameters: dict) -> bool`
- `reset_spark_session_state() -> None`（test-only）
- `SparkSessionUnavailableError(RuntimeError)`

私有：`_is_session_alive`、`_stop_and_clear`、`_safe_app_id`、`_mark_alive`、`_build`、`_rebuild_or_active`、`_build_from_yaml`、`_validate_values`。

---

## Task 0: 建立 baseline

**Files:** 無（只讀）

- [ ] **Step 1: 記錄改動前的測試結果**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py tests/test_pipelines/test_training/ -q \
  2>&1 | tail -20 | tee /tmp/baseline_spark.txt
```

預期：全綠，或出現 `known-pitfalls.md §5` 已記載的既有 failure。把輸出留著，Task 8 要對照。

- [ ] **Step 2: 確認 pre-flight**

```bash
readlink /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/.venv
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
```

預期：`/Users/curtislu/projects/recsys_tfb/.venv` 與 `Python 3.10.9`。任一不符先修再繼續。

---

## Task 1: canonical config 記憶（mode-1 記住，mode-2 重建時使用）

**為什麼先做這個**：後續每個測試都要在「沒有 conf/ 目錄」的情況下快速重建 `local[1]` session。先有記憶機制，後面的測試才不會依賴 repo 的 `conf/`（慢且不確定）。

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_utils/test_spark.py`，先把 autouse fixture 換成同時重置模組狀態的版本（否則 canonical configs 會跨測試洩漏，`test_no_configs_no_active_falls_back_to_loader` 會拿到上一個測試記住的 configs 而非 yaml）：

```python
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
```

在檔案結尾新增：

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestCanonicalConfigs -q
```

預期：兩個測試都 FAIL。第一個先炸 `ImportError: cannot import name 'reset_spark_session_state'`（fixture 匯入失敗）。

- [ ] **Step 3: 實作**

把 `src/recsys_tfb/utils/spark.py` 整個換成下列內容（本 task 只加 canonical 記憶；死亡復原在 Task 2）：

```python
"""SparkSession entrypoint: config-driven creation with canonical-config memory."""

import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_VALID_VALUE_TYPES = (str, int, bool)

# Canonical configs, remembered from the first mode-1 call (a CLI entry).
# mode-2 rebuilds from these instead of re-reading yaml: the yaml path guesses
# the env from CONF_ENV (never set anywhere) and misses the per-pipeline
# `spark:` block that _load_spark_config merges in.
_canonical_configs: dict[str, Any] | None = None
_canonical_enable_hive: bool = False


def reset_spark_session_state() -> None:
    """Forget the canonical configs. Test-only: module state leaks across tests."""
    global _canonical_configs, _canonical_enable_hive
    _canonical_configs = None
    _canonical_enable_hive = False


def get_or_create_spark_session(
    spark_configs: dict[str, Any] | None = None,
    enable_hive: bool = False,
) -> SparkSession:
    """Create or return the SparkSession.

    Two call modes:

    1. Pipeline entrypoint passes ``spark_configs`` (already deep-merged
       ``params["spark"]``). The configs are remembered as canonical and a
       session is created. If an active session already exists, runtime
       configs are applied and the existing session is returned
       (cluster-level configs would be ignored by PySpark — a warning is
       logged).
    2. IO / SQLRunner / scripts call with ``None``. An active session is
       returned directly. Otherwise a session is rebuilt from the remembered
       canonical configs, or — if no mode-1 call ever happened (scripts,
       tests) — from the base ``parameters.yaml`` ``spark:`` block.

    enable_hive (default False): when True, the builder calls
        ``.enableHiveSupport()`` before ``getOrCreate()``. Required for
        ``HiveTableDataset`` write paths in tests (``STORED AS PARQUET``
        DDL needs Hive parser support). Production code paths leave this
        False; the cluster session inherits Hive support from
        ``SPARK_CONF_DIR``'s ``hive-site.xml`` rather than this flag.
        Remembered alongside the configs so a rebuild keeps Hive support.

    Raises:
        TypeError: ``spark_configs`` is not a dict.
        ValueError: any value is not str / int / bool.
    """
    global _canonical_configs, _canonical_enable_hive

    if spark_configs is None:
        return _rebuild_or_active()

    if not isinstance(spark_configs, dict):
        raise TypeError(
            f"spark_configs must be a dict, got {type(spark_configs).__name__}"
        )
    _validate_values(spark_configs)

    _canonical_configs = dict(spark_configs)
    _canonical_enable_hive = enable_hive

    if SparkSession.getActiveSession() is not None:
        logger.warning(
            "Active SparkSession already exists; cluster-level configs "
            "in spark_configs will be ignored by PySpark."
        )

    return _build(spark_configs, enable_hive)


def _rebuild_or_active() -> SparkSession:
    """Return the active session, or rebuild one (canonical configs, else yaml)."""
    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        return active

    if _canonical_configs is not None:
        return _build(_canonical_configs, _canonical_enable_hive)

    return _build_from_yaml()


def _build(spark_configs: dict[str, Any], enable_hive: bool) -> SparkSession:
    app_name = spark_configs.get("app_name", "recsys_tfb")
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs.items():
        if key == "app_name":
            continue
        builder = builder.config(key, value)
    if enable_hive:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()


def _is_session_alive(session: SparkSession) -> bool:
    """True only if the session's SparkContext is still running.

    A non-None ``_jsc`` means the Python wrapper still holds a context
    object, not that the JVM context is live: a stopped SparkContext can
    leave ``_jsc`` non-None, and any subsequent ``parallelize`` /
    ``createDataFrame`` then raises ``IllegalStateException: Cannot call
    methods on a stopped SparkContext``. Probe ``isStopped()`` so a dead
    session reads as not-alive and the caller rebuilds a fresh one.
    """
    try:
        jsc = session.sparkContext._jsc
        return jsc is not None and not jsc.sc().isStopped()
    except Exception:
        return False


def _validate_values(spark_configs: dict[str, Any]) -> None:
    bad = [
        k
        for k, v in spark_configs.items()
        if not isinstance(v, _VALID_VALUE_TYPES)
    ]
    if bad:
        raise ValueError(
            "spark_configs values must be str / int / bool. "
            f"Invalid keys: {bad}"
        )


def _build_from_yaml() -> SparkSession:
    """Build a session from base parameters.yaml. Only for never-configured callers.

    Reached by scripts and tests that call mode-2 without any prior mode-1
    call. Pipeline runs always go through the canonical configs instead.
    """
    import os
    from pathlib import Path

    from recsys_tfb.core.config import ConfigLoader

    env = os.environ.get("CONF_ENV", "local")
    conf_dir = Path.cwd() / "conf"
    if not conf_dir.is_dir():
        raise RuntimeError(
            f"No active SparkSession and conf/ not found at {conf_dir}. "
            "Cannot build fallback session."
        )
    loader = ConfigLoader(str(conf_dir), env=env)
    try:
        base_params = loader.get_parameters_by_name("parameters")
    except KeyError as exc:
        raise RuntimeError(
            "No active SparkSession and parameters.yaml not found in conf/."
        ) from exc
    spark_configs = base_params.get("spark", {})

    # Match the entrypoint path (__main__._load_spark_config): resolve
    # ${vdclient.*.*} placeholders before handing dict to the builder.
    # Otherwise yaml values like ${vdclient.cdp.driver_port} reach SparkConf
    # as literal strings → "spark.driver.port should be int".
    # ${env.*} placeholders are already resolved by ConfigLoader.
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders
    spark_configs = resolve_vdclient_placeholders(spark_configs)

    logger.info(
        "Fallback: building SparkSession (yaml=conf/%s/parameters.yaml, "
        "connection settings from SPARK_CONF_DIR)",
        env,
    )
    return _build(spark_configs or {"app_name": "recsys_tfb"}, False)
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS（含既有的 7 個測試——特別確認 `test_no_configs_no_active_falls_back_to_loader` 仍綠，那證明 autouse 重置有效）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "feat(spark): mode-1 記住 canonical configs,mode-2 重建時沿用

修掉重建時 CONF_ENV 永遠是 local、且漏掉 parameters_<pipeline>.yaml
的 spark: 區塊的問題。yaml fallback 只留給從未 configure 過的呼叫者
(scripts/tests)。"
```

---

## Task 2: 死亡 context 的復原（核心修復）

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_utils/test_spark.py` 結尾新增：

```python
class TestDeadContextRecovery:
    """JVM 端停掉 SparkContext(等同 cluster 端殺掉 app)後的復原。

    PySpark 的 getOrCreate 只在 Python 端 _jsc is None 時才重建
    (pyspark/sql/session.py:264),而 _jsc 只有 Python 端 SparkContext.stop()
    才會設成 None(pyspark/context.py:568)。JVM 端自行停止不動 Python 狀態,
    因此未修復前 getOrCreate 會把同一個死 session 原封不動回傳。
    """

    @staticmethod
    def _kill_jvm_context(session):
        """Stop the JVM-side SparkContext, leaving Python-side state intact."""
        session.sparkContext._jsc.sc().stop()

    def test_mode2_rebuilds_after_jvm_side_stop(self, monkeypatch, tmp_path):
        first = get_or_create_spark_session(_minimal_configs())
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
        self._kill_jvm_context(first)
        monkeypatch.chdir(tmp_path)

        second = get_or_create_spark_session(_minimal_configs())
        try:
            assert second is not first
            assert second.createDataFrame([(1,)], ["a"]).count() == 1
        finally:
            second.stop()
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestDeadContextRecovery -q
```

預期：兩個都 FAIL，訊息是 `assert second is not first`（`second is first`）。這正是公司環境那個 bug。

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/utils/spark.py` 頂端的 import 加上 `SparkContext`：

```python
from pyspark import SparkContext
from pyspark.sql import SparkSession
```

新增 `_stop_and_clear`（放在 `_is_session_alive` 之後）：

```python
def _stop_and_clear(session: SparkSession) -> None:
    """Stop a session Python-side and clear PySpark's singletons.

    ``SparkSession.builder.getOrCreate()`` treats a session as reusable
    whenever ``_instantiatedSession._sc._jsc`` is not None. Only a Python-side
    ``SparkContext.stop()`` sets ``_jsc`` to None, so a JVM-side death leaves
    the singletons pointing at a corpse and every "rebuild" hands it back.
    Clearing them is what makes the next getOrCreate actually build.

    ``SparkSession.stop()`` clears the singletons only if its JVM calls
    succeed; when the py4j gateway itself is gone it raises partway through.
    The explicit assignments below are the belt-and-braces for that case.
    """
    try:
        session.stop()
    except Exception as exc:  # noqa: BLE001 — JVM/gateway may already be gone
        logger.warning("SparkSession.stop() raised while clearing: %s", exc)

    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    SparkContext._active_spark_context = None
```

把 `_rebuild_or_active` 換成：

```python
def _rebuild_or_active() -> SparkSession:
    """Return the active session, or rebuild one (canonical configs, else yaml)."""
    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        return active

    if active is not None:
        _stop_and_clear(active)
    else:
        stale = SparkSession._instantiatedSession
        if stale is not None and not _is_session_alive(stale):
            _stop_and_clear(stale)

    if _canonical_configs is not None:
        return _build(_canonical_configs, _canonical_enable_hive)

    return _build_from_yaml()
```

在 `get_or_create_spark_session` 的 mode-1 分支，把那段 warning 換成先檢查存活：

```python
    active = SparkSession.getActiveSession()
    if active is not None and not _is_session_alive(active):
        _stop_and_clear(active)
    elif active is not None:
        logger.warning(
            "Active SparkSession already exists; cluster-level configs "
            "in spark_configs will be ignored by PySpark."
        )

    return _build(spark_configs, enable_hive)
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS。

- [ ] **Step 5: 驗證測試真的覆蓋新路徑（mutation check）**

**不要**去註解 `_stop_and_clear` 裡的三行單例清除——那三行只在 py4j gateway 也死掉時才有作用。本測試的 JVM 是活的，`session.stop()` 內部就已經把單例清乾淨了，註解掉那三行測試仍會全綠（**假綠**）。

正確的 mutation 是拿掉**呼叫**。在 `_rebuild_or_active` 裡把這一行註解掉：

```python
    if active is not None:
        pass  # _stop_and_clear(active)
```

跑：

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestDeadContextRecovery -q
```

預期：`test_mode2_rebuilds_after_jvm_side_stop` 轉紅，訊息是 `assert second is not first`。

若它仍然綠：代表測試沒走到新路徑（例如 `getActiveSession()` 在你的 pyspark 版本回了 None），先修測試再繼續，不要宣稱完成。

確認轉紅後把呼叫改回來，重跑確認轉綠。同樣的 mutation 也對 mode-1 做一次（註解 `get_or_create_spark_session` 裡的 `_stop_and_clear(active)`），確認 `test_mode1_rebuilds_after_jvm_side_stop` 轉紅。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "fix(spark): JVM 端停掉 context 後真的重建 session

_is_session_alive(PR#75)偵測得到死亡,但重建委派給 builder.getOrCreate(),
而 PySpark 只在 Python 端 _jsc is None 時才建新的。先 Python 端 stop 清掉
單例,重建才會發生。這是公司環境 HPO 後 predict 撞 stopped SparkContext 的
直接原因。"
```

---

## Task 3: `stop_spark_session()`

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

```python
class TestStopSparkSession:
    def test_stops_alive_session_and_is_idempotent(self):
        from recsys_tfb.utils.spark import stop_spark_session

        session = get_or_create_spark_session(_minimal_configs())
        assert _is_session_alive(session)

        assert stop_spark_session() is True
        assert SparkSession.getActiveSession() is None

        # 第二次沒有東西可停
        assert stop_spark_session() is False

    def test_stops_jvm_dead_session_without_raising(self):
        from recsys_tfb.utils.spark import stop_spark_session

        session = get_or_create_spark_session(_minimal_configs())
        session.sparkContext._jsc.sc().stop()

        assert stop_spark_session() is True
        assert SparkSession._instantiatedSession is None
```

這些測試用到 `SparkSession` 與 `_is_session_alive`。把 `tests/test_utils/test_spark.py` 第 3-5 行：

```python
import pytest

from recsys_tfb.utils.spark import get_or_create_spark_session
```

換成：

```python
import pytest
from pyspark.sql import SparkSession

from recsys_tfb.utils.spark import _is_session_alive, get_or_create_spark_session
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestStopSparkSession -q
```

預期：FAIL，`ImportError: cannot import name 'stop_spark_session'`。

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/utils/spark.py` 新增（放在 `get_or_create_spark_session` 之後）：

```python
def stop_spark_session() -> bool:
    """Stop the current SparkSession, if any. Returns True when one was stopped.

    Idempotent: safe on an already-dead session and on no session at all.
    The next mode-2 call rebuilds from the canonical configs.
    """
    session = SparkSession.getActiveSession() or SparkSession._instantiatedSession
    if session is None:
        return False

    app_id = _safe_app_id(session)
    _stop_and_clear(session)
    logger.info(
        "SparkSession released (application_id=%s)", app_id,
        extra={"event": "spark_session_released", "application_id": app_id},
    )
    return True


def _safe_app_id(session: SparkSession) -> str | None:
    try:
        return session.sparkContext.applicationId
    except Exception:  # noqa: BLE001 — context may already be gone
        return None
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "feat(spark): stop_spark_session() 冪等釋放 session"
```

---

## Task 4: `SparkSessionUnavailableError`

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

```python
class TestBuildFailure:
    def test_build_failure_wrapped_in_spark_session_unavailable(
        self, monkeypatch
    ):
        """重建失敗要丟看得懂的例外,不要讓 py4j 的原始例外冒出來。"""
        import recsys_tfb.utils.spark as spark_mod
        from recsys_tfb.utils.spark import SparkSessionUnavailableError

        class _ExplodingBuilder:
            def appName(self, *_args):
                return self

            def config(self, *_args):
                return self

            def enableHiveSupport(self):
                return self

            def getOrCreate(self):
                raise RuntimeError("gateway is gone")

        class _FakeSparkSession:
            builder = _ExplodingBuilder()
            _instantiatedSession = None
            _activeSession = None

            @staticmethod
            def getActiveSession():
                return None

        monkeypatch.setattr(spark_mod, "SparkSession", _FakeSparkSession)

        with pytest.raises(SparkSessionUnavailableError) as excinfo:
            get_or_create_spark_session(_minimal_configs())

        assert isinstance(excinfo.value.__cause__, RuntimeError)
        assert "gateway is gone" in str(excinfo.value.__cause__)
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestBuildFailure -q
```

預期：FAIL，`ImportError: cannot import name 'SparkSessionUnavailableError'`。

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/utils/spark.py` 的 `logger = ...` 之後新增：

```python
class SparkSessionUnavailableError(RuntimeError):
    """A SparkSession could not be created or rebuilt.

    Raised instead of letting py4j's ``Py4JNetworkError`` (dead JVM gateway —
    unrecoverable in-process, the run must be restarted) or Spark's
    ``IllegalStateException`` surface at an unrelated call site.
    """
```

把 `_build` 的 `getOrCreate()` 包起來，並記錄 `applicationId`：

```python
def _build(spark_configs: dict[str, Any], enable_hive: bool) -> SparkSession:
    app_name = spark_configs.get("app_name", "recsys_tfb")
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs.items():
        if key == "app_name":
            continue
        builder = builder.config(key, value)
    if enable_hive:
        builder = builder.enableHiveSupport()

    try:
        session = builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001 — surface one readable error
        raise SparkSessionUnavailableError(
            f"Failed to build SparkSession (app_name={app_name!r}, "
            f"last_application_id={_last_app_id!r}). If the py4j gateway is "
            "dead the driver JVM is gone and the run must be restarted."
        ) from exc

    _mark_alive(session)
    return session
```

`_last_app_id` 與 `_mark_alive` 在 Task 5 定義。為了讓本 task 可獨立通過，先加最小版本（Task 5 再擴充）：

```python
_last_app_id: str | None = None


def _mark_alive(session: SparkSession) -> None:
    global _last_app_id
    _last_app_id = _safe_app_id(session)
```

並在 `reset_spark_session_state` 補上重置：

```python
def reset_spark_session_state() -> None:
    """Forget the canonical configs and instrumentation state. Test-only."""
    global _canonical_configs, _canonical_enable_hive, _last_app_id
    _canonical_configs = None
    _canonical_enable_hive = False
    _last_app_id = None
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "feat(spark): 重建失敗包成 SparkSessionUnavailableError"
```

---

## Task 5: Layer 1 instrumentation

公司環境只有應用層 log。這三個事件讓下次失敗時能分辨「閒置回收型」與「固定時長 token 過期型」。

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

```python
class TestInstrumentation:
    def test_emits_created_event(self, caplog):
        import logging as _logging

        with caplog.at_level(_logging.INFO, logger="recsys_tfb.utils.spark"):
            session = get_or_create_spark_session(_minimal_configs())
        try:
            events = [
                r.event for r in caplog.records if hasattr(r, "event")
            ]
            assert "spark_session_created" in events
        finally:
            session.stop()

    def test_emits_context_dead_event_with_idle_seconds(
        self, caplog, monkeypatch, tmp_path
    ):
        import logging as _logging

        first = get_or_create_spark_session(_minimal_configs())
        first.sparkContext._jsc.sc().stop()
        monkeypatch.chdir(tmp_path)

        with caplog.at_level(_logging.WARNING, logger="recsys_tfb.utils.spark"):
            second = get_or_create_spark_session(None)
        try:
            dead = [
                r
                for r in caplog.records
                if getattr(r, "event", None) == "spark_context_dead"
            ]
            assert len(dead) == 1
            assert isinstance(dead[0].seconds_since_last_use, int)
        finally:
            second.stop()
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestInstrumentation -q
```

預期：兩個都 FAIL（`spark_session_created` 不在 events；`spark_context_dead` 找不到）。

- [ ] **Step 3: 實作**

頂端加 `import time`，並把 `_last_alive_ts` 加進模組狀態與 `reset_spark_session_state`：

```python
import logging
import time
from typing import Any

...

_last_app_id: str | None = None
_last_alive_ts: float | None = None


def reset_spark_session_state() -> None:
    """Forget the canonical configs and instrumentation state. Test-only."""
    global _canonical_configs, _canonical_enable_hive, _last_app_id, _last_alive_ts
    _canonical_configs = None
    _canonical_enable_hive = False
    _last_app_id = None
    _last_alive_ts = None
```

`_mark_alive` 改為同時記時間戳，並在建立後發出 created 事件：

```python
def _mark_alive(session: SparkSession) -> None:
    global _last_app_id, _last_alive_ts
    _last_app_id = _safe_app_id(session)
    _last_alive_ts = time.time()
```

在 `_build` 的 `_mark_alive(session)` 之後加：

```python
    logger.info(
        "SparkSession ready (application_id=%s, app_name=%s)",
        _last_app_id, app_name,
        extra={
            "event": "spark_session_created",
            "application_id": _last_app_id,
            "app_name": app_name,
        },
    )
    return session
```

`_rebuild_or_active` 在偵測到死亡時發出 dead 事件；活著時更新時間戳：

```python
def _rebuild_or_active() -> SparkSession:
    """Return the active session, or rebuild one (canonical configs, else yaml)."""
    global _last_alive_ts

    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        _last_alive_ts = time.time()
        return active

    dead = active
    if dead is None:
        stale = SparkSession._instantiatedSession
        if stale is not None and not _is_session_alive(stale):
            dead = stale

    if dead is not None:
        idle = int(time.time() - (_last_alive_ts or time.time()))
        logger.warning(
            "Detected stopped SparkContext; rebuilding "
            "(last_application_id=%s, seconds_since_last_use=%d)",
            _last_app_id, idle,
            extra={
                "event": "spark_context_dead",
                "last_application_id": _last_app_id,
                "seconds_since_last_use": idle,
            },
        )
        _stop_and_clear(dead)

    if _canonical_configs is not None:
        return _build(_canonical_configs, _canonical_enable_hive)

    return _build_from_yaml()
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "feat(spark): 三個結構化事件供 Layer 1 歸因

spark_session_created / spark_session_released / spark_context_dead。
公司環境只有應用層 log,靠 seconds_since_last_use 分辨閒置回收型與
固定時長 token 過期型。"
```

---

## Task 6: `release_spark_session(parameters)` 與 config 開關

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Modify: `conf/base/parameters_training.yaml`
- Test: `tests/test_utils/test_spark.py`

- [ ] **Step 1: 寫失敗測試**

```python
class TestReleaseSparkSession:
    def test_releases_by_default(self):
        from recsys_tfb.utils.spark import release_spark_session

        get_or_create_spark_session(_minimal_configs())
        assert release_spark_session({}) is True
        assert SparkSession.getActiveSession() is None

    def test_toggle_off_keeps_session_alive(self):
        from recsys_tfb.utils.spark import release_spark_session

        session = get_or_create_spark_session(_minimal_configs())
        params = {"spark_lifecycle": {"release_during_hpo": False}}
        try:
            assert release_spark_session(params) is False
            assert _is_session_alive(session)
        finally:
            session.stop()
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py::TestReleaseSparkSession -q
```

預期：FAIL，`ImportError: cannot import name 'release_spark_session'`。

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/utils/spark.py` 新增（放在 `stop_spark_session` 之後）：

```python
def release_spark_session(parameters: dict) -> bool:
    """Release the SparkSession before a long driver-local stretch (HPO).

    Holding an idle Spark application for hours invites the cluster to reclaim
    it; the context then dies JVM-side and every later Spark call fails. Give
    the executors back instead, and let the next mode-2 caller rebuild from the
    canonical configs.

    Returns True when a session was actually stopped.
    """
    lifecycle = parameters.get("spark_lifecycle") or {}
    if not lifecycle.get("release_during_hpo", True):
        logger.info(
            "spark_lifecycle.release_during_hpo=false; keeping SparkSession alive"
        )
        return False
    return stop_spark_session()
```

在 `conf/base/parameters_training.yaml` 的 `spark:` 區塊之後、`training:` 之前插入：

```yaml
# ops block（與 spark:/mlflow:/cache: 同層）——不影響 model_version。
# HPO 是 driver-local 的,期間 Spark 完全閒置;閒置的 application 會被叢集端
# 回收,之後 predict 寫 Hive 就撞 stopped SparkContext。改成主動釋放,由下一個
# 用到 Spark 的節點依 canonical configs 重建。設為 false 可停用（session 全程存活）。
spark_lifecycle:
  release_during_hpo: true
```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py -q
```

預期：全部 PASS。

- [ ] **Step 5: 確認沒有動到 model_version**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.core.versioning import _model_version_payload
p = {'training': {'a': 1}, 'spark_lifecycle': {'release_during_hpo': True}}
assert _model_version_payload(p) == {'training': {'a': 1}}, _model_version_payload(p)
print('spark_lifecycle 不進 model_version payload: OK')
"
```

預期輸出：`spark_lifecycle 不進 model_version payload: OK`

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/utils/spark.py conf/base/parameters_training.yaml tests/test_utils/test_spark.py
git commit -m "feat(spark): release_spark_session() + spark_lifecycle 開關

開關放頂層 ops block,不進 _model_version_payload,不會 bump model_version。"
```

---

## Task 7: 接進 `tune_hyperparameters`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py:388-409`
- Create: `tests/test_pipelines/test_training/test_release_spark_session.py`

- [ ] **Step 1: 寫失敗測試**

建立 `tests/test_pipelines/test_training/test_release_spark_session.py`：

```python
"""tune_hyperparameters 必須在做任何事之前先釋放 SparkSession。

Runner 是嚴格循序的(core/runner.py:65),所以「函式體第一行」在構造上就等於
「所有排在它前面的節點都跑完之後」。這比新增一個 DAG 節點可靠:零入度節點的
排序取決於宣告位置(core/pipeline.py:76-79),而 tune_hyperparameters 並沒有
消費 test_parquet_handle,DAG 不會強制 cache_test_model_input 排在它之前。
"""

import pytest

import recsys_tfb.pipelines.training.nodes as nodes


class _ReleasedFirst(Exception):
    """Sentinel: raised from the patched release to prove it ran first."""


def test_tune_hyperparameters_releases_spark_before_anything_else(monkeypatch):
    calls = []

    def _fake_release(parameters):
        calls.append(parameters)
        raise _ReleasedFirst

    monkeypatch.setattr(nodes, "release_spark_session", _fake_release)

    # 全部傳 None:若 release 不是第一個語句,函式會先在別處炸(TypeError /
    # KeyError),而不是丟出我們的 sentinel。
    with pytest.raises(_ReleasedFirst):
        nodes.tune_hyperparameters(None, None, None, None, {"training": {}})

    assert len(calls) == 1
    assert calls[0] == {"training": {}}
```

- [ ] **Step 2: 跑測試確認失敗**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_release_spark_session.py -q
```

預期：FAIL，`AttributeError: <module 'recsys_tfb.pipelines.training.nodes'> has no attribute 'release_spark_session'`。

- [ ] **Step 3: 實作**

在 `src/recsys_tfb/pipelines/training/nodes.py` 的 import 區（第 24 行 `from recsys_tfb.utils.hdfs import ...` 之後）加上：

```python
from recsys_tfb.utils.spark import release_spark_session
```

在 `tune_hyperparameters` 的 docstring 之後、`from recsys_tfb.io.extract import extract_Xy_with_groups` 之前插入：

```python
    # HPO 與其後的 finalize/calibrate 全是 driver-local:Spark 從這裡到
    # predict_and_write_test_predictions 完全閒置,可能數小時。閒置的
    # application 會被叢集端回收,context 在 JVM 端死掉,之後寫 Hive 就撞
    # IllegalStateException。主動釋放,由 predict 節點依 canonical configs 重建。
    #
    # 注意:fb0d4c4 也曾在此 stop 過 session,並在 85b28699 被移除——那次是誤診
    # 效能問題(真因是 OMP thread oversubscription),且當時的重建路徑無法處理
    # JVM 端死亡。這次不同:釋放是目的,且重建對兩種死法都有效。
    release_spark_session(parameters)

```

- [ ] **Step 4: 跑測試確認通過**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/test_release_spark_session.py -q
```

預期：1 passed。

- [ ] **Step 5: 確認沒弄壞既有 training 測試**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_training/ -q 2>&1 | tail -15
```

預期：與 Task 0 的 baseline 一致。若 `test_nodes.py` 或 `test_hpo_resume.py` 因為 `tune_hyperparameters` 多了 side effect 而失敗，在該測試裡 monkeypatch `nodes.release_spark_session` 成 no-op（`lambda _params: False`），不要改實作。

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_release_spark_session.py
git commit -m "feat(training): tune_hyperparameters 第一行釋放 SparkSession

消除 HPO 期間數小時的閒置窗口。放在函式體而非新增 DAG 節點:Runner 循序執行,
「第一行」在構造上就等於「前面節點都跑完」;零入度節點的排序則取決於宣告位置。"
```

---

## Task 8: 端到端驗證與交付前檢查

**Files:** 無（驗證）

- [ ] **Step 1: 跑本 plan 涵蓋的全部測試**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_utils/test_spark.py tests/test_pipelines/test_training/ -q 2>&1 | tail -10
```

預期：全綠，且與 `/tmp/baseline_spark.txt` 對照沒有新增 failure。把最後 10 行貼進交付訊息。

- [ ] **Step 2: 本機 local[*] 實跑 dataset → training**

repo 既有的 e2e 腳本就是做這件事（`scripts/local_e2e.sh`：`local_spark_setup.py --reset` → `dataset --env local` → `training --env local`）。它用 `ROOT="$(cd "$(dirname "$0")/.." && pwd)"` 解析根目錄，所以在 worktree 裡跑就是 worktree 的 `data/` 與 `conf/`，不會踩到 main。

先確認 data/ 隔離，再跑（**>2 分鐘，必須 background 執行**，不要 foreground 阻塞）：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
PYTHONPATH=$PWD/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
  scripts/local_spark_setup.py --check-isolation

bash scripts/local_e2e.sh 2>&1 | tee /tmp/training_run.log
```

預期最後一行是 `✅ local e2e（環境證明）完成`。

- [ ] **Step 3: 驗證 log 出現預期的三個事件**

```bash
grep -nE "spark_session_created|spark_session_released|Detected stopped SparkContext|SparkSession ready" /tmp/training_run.log
```

預期順序：`SparkSession ready`（entry 建立）→ `SparkSession released`（`tune_hyperparameters` 釋放）→ `SparkSession ready`（predict 節點 lazy 重建）。

**不應該**出現 `Detected stopped SparkContext`——本機沒有叢集回收 app，release 走的是 Python 端 stop，`getActiveSession()` 會回 None。若出現，代表 `_stop_and_clear` 沒把單例清乾淨，回頭查。

- [ ] **Step 4: 驗證 predictions 真的寫進去**

`training_eval_predictions` 的 database 是 `conf/base/catalog.yaml:287` 的 `${hive.db}`，解析後為 `conf/base/parameters.yaml:5` 的 `ml_recsys`。

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=$PWD/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from recsys_tfb.utils.spark import get_or_create_spark_session
spark = get_or_create_spark_session(
    {'app_name': 'verify', 'spark.master': 'local[1]'}, enable_hive=True
)
df = spark.table('ml_recsys.training_eval_predictions')
print('rows:', df.count())
df.show(3)
spark.stop()
"
```

預期：`rows:` 大於 0。

附帶保險：`compute_test_mAP_spark`（`pipeline.py:144-148`）就在 predict 之後讀這張表，所以 Step 2 的 training 跑完沒炸，本身已經是「重建後的 session 能讀寫 Hive」的證據。

- [ ] **Step 5: graphify rebuild**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-session-lifecycle
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 6: 確認改動範圍**

```bash
git diff --stat main..HEAD
```

預期只有這五個檔案（加上 spec 與 plan 文件）：

```
conf/base/parameters_training.yaml
src/recsys_tfb/pipelines/training/nodes.py
src/recsys_tfb/utils/spark.py
tests/test_pipelines/test_training/test_release_spark_session.py
tests/test_utils/test_spark.py
```

有其他檔案就是動到邊界外，回頭查。

- [ ] **Step 7: 交付訊息必須明說的事**

- Layer 1（誰在公司叢集停掉 context）**沒有解**，本輪只埋了 instrumentation。下次失敗時去 grep `spark_context_dead` 事件的 `seconds_since_last_use`。
- **重建 = 在 YARN 上重新提交一個新的 application**（新 app id、新 executors、需要有效的 delegation token）。若公司環境對此有稽核限制，此設計需調整。這在本機驗不了。
- `--from-node finalize_model` 這類切片會跳過 `tune_hyperparameters`，因此不會 release；那段 session 保持存活，由 Task 2 的重建修復兜底。

---

## 附錄：這個 bug 的一句話版本

`_is_session_alive()` 正確地說出「這個 session 死了」（PR#75），然後呼叫了一個不會重建的重建——因為 PySpark 的 `getOrCreate()` 只認 Python 端的 `_jsc is None`，而 JVM 端自行死亡從不清 Python 端狀態。
