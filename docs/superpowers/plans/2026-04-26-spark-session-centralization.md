# Spark Session Centralization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace all scattered `SparkSession.builder.getOrCreate()` callsites with a single configurable entrypoint `get_or_create_spark_session()`, sourcing config from `conf/<env>/parameters.yaml` (base common) deep-merged with `conf/<env>/parameters_<pipeline>.yaml` (pipeline-specific override).

**Architecture:** Pipeline CLI commands explicitly load `spark:` config from base + their pipeline file, deep-merge the two, and pass the result to `get_or_create_spark_session()` before running the pipeline. IO layer / SQLRunner / scripts call `get_or_create_spark_session()` with no args; if no active session exists they fall back to loading base `parameters.yaml` via ConfigLoader.

**Tech Stack:** PySpark 3.3.2, Typer, Ploomber, ConfigLoader (existing in `src/recsys_tfb/core/config.py`), pytest.

**Spec:** `docs/superpowers/specs/2026-04-26-spark-session-centralization-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `src/recsys_tfb/utils/spark.py` | Rewrite | Single entrypoint `get_or_create_spark_session()` with fallback + validation |
| `src/recsys_tfb/utils/__init__.py` | Modify | Re-export new function |
| `src/recsys_tfb/__main__.py` | Modify | Add `_load_spark_config(config, pipeline)` helper; call in each pipeline command before `_execute_pipeline` |
| `src/recsys_tfb/io/hive_table_dataset.py:153-156` | Modify | Replace `SparkSession.builder.getOrCreate()` with new function |
| `src/recsys_tfb/io/parquet_dataset.py:31-33,55-57` | Modify | Same |
| `src/recsys_tfb/pipelines/source_etl/sql_runner.py:146-148` | Modify | Same |
| `scripts/suggest_categorical_cols.py:251-258` | Modify | Same |
| `tests/conftest.py:1-26` | Modify | Use new function with test minimal config |
| `tests/test_io/test_hive_table_dataset.py:20-23` | Modify | Update mock target path |
| `tests/test_utils/__init__.py` | Create | Empty package marker |
| `tests/test_utils/test_spark.py` | Create | Unit tests for new function |
| `conf/base/parameters.yaml` | Modify | Add `spark:` block with common config |
| `conf/base/parameters_training.yaml` | Modify | Add `spark:` override |
| `conf/base/parameters_inference.yaml` | Modify | Add `spark:` override |
| `conf/local/parameters.yaml` | Create or modify | Add `spark.master: local[*]` for local env |

---

### Task 1: Rewrite `get_or_create_spark_session` with new signature and validation

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py`
- Test: `tests/test_utils/test_spark.py` (new)
- Modify: `tests/test_utils/__init__.py` (new, empty)

This task introduces the new function with validation and the simplest path (configs passed in). Fallback path is added in Task 2.

- [ ] **Step 1: Create empty test package marker**

```bash
mkdir -p tests/test_utils
touch tests/test_utils/__init__.py
```

- [ ] **Step 2: Write the failing tests for the configs-passed-in path**

Create `tests/test_utils/test_spark.py`:

```python
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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_utils/test_spark.py -v`
Expected: tests fail because either the function still has the old signature, or validation does not exist.

- [ ] **Step 4: Rewrite the function**

Replace the entire contents of `src/recsys_tfb/utils/spark.py`:

```python
"""SparkSession entrypoint with config-driven creation and fallback."""

import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_VALID_VALUE_TYPES = (str, int, bool)


def get_or_create_spark_session(
    spark_configs: dict[str, Any] | None = None,
) -> SparkSession:
    """Create or return the SparkSession.

    Two call modes:

    1. Pipeline entrypoint passes ``spark_configs`` (already deep-merged
       ``params["spark"]``). Builder is configured and a session is
       created. If an active session already exists, runtime configs are
       applied and the existing session is returned (cluster-level
       configs would be ignored by PySpark — a warning is logged).
    2. IO / SQLRunner / scripts call with ``None``. If an active session
       exists, return it directly. Otherwise fall back to loading the
       base ``parameters.yaml`` ``spark:`` block via ConfigLoader and
       create a session from that.

    Raises:
        TypeError: ``spark_configs`` is not a dict.
        ValueError: any value is not str / int / bool.
    """
    if spark_configs is None:
        return _fallback_create()

    if not isinstance(spark_configs, dict):
        raise TypeError(
            f"spark_configs must be a dict, got {type(spark_configs).__name__}"
        )
    _validate_values(spark_configs)

    if SparkSession.getActiveSession() is not None:
        logger.warning(
            "Active SparkSession already exists; cluster-level configs "
            "in spark_configs will be ignored by PySpark."
        )

    app_name = spark_configs.get("app_name", "recsys_tfb")
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs.items():
        if key == "app_name":
            continue
        builder = builder.config(key, value)
    return builder.getOrCreate()


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


def _fallback_create() -> SparkSession:
    """Stub for Task 2 — returns active session or raises."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    raise RuntimeError(
        "No active SparkSession and no spark_configs provided. "
        "Fallback to ConfigLoader is implemented in Task 2."
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_utils/test_spark.py -v`
Expected: PASS for `TestWithConfigs` (3 tests) and `TestValidation` (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/__init__.py tests/test_utils/test_spark.py
git commit -m "refactor: rewrite get_or_create_spark_session with dict signature and validation"
```

---

### Task 2: Implement fallback path (active session check + ConfigLoader)

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py:_fallback_create`
- Test: `tests/test_utils/test_spark.py` (append cases)

- [ ] **Step 1: Write the failing tests for the fallback path**

Append to `tests/test_utils/test_spark.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify failures**

Run: `pytest tests/test_utils/test_spark.py::TestFallback -v`
Expected:
- `test_no_configs_returns_active_session` PASSES (already works in stub).
- `test_no_configs_no_active_falls_back_to_loader` FAILS (raises `RuntimeError`).

- [ ] **Step 3: Implement the real fallback**

Replace `_fallback_create` in `src/recsys_tfb/utils/spark.py`:

```python
def _fallback_create() -> SparkSession:
    """Return active session, or build one from base parameters.yaml."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from pathlib import Path

    from recsys_tfb.core.config import ConfigLoader

    import os

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
    if not spark_configs:
        raise RuntimeError(
            "No active SparkSession and parameters.yaml has no 'spark:' "
            "block. Add one or pass spark_configs explicitly."
        )
    logger.info(
        "Fallback: building SparkSession from conf/%s/parameters.yaml", env
    )
    return get_or_create_spark_session(spark_configs)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_utils/test_spark.py -v`
Expected: PASS for all 7 tests.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/spark.py tests/test_utils/test_spark.py
git commit -m "feat: spark session fallback reads conf/<env>/parameters.yaml"
```

---

### Task 3: Add `spark:` block to `conf/base/parameters.yaml`

**Files:**
- Modify: `conf/base/parameters.yaml`

- [ ] **Step 1: Read the current file**

Run: `cat conf/base/parameters.yaml | head -40`

Identify a sensible location to insert the new top-level `spark:` block (typically near other infrastructure-level settings).

- [ ] **Step 2: Add the spark block**

Append (or insert near top) to `conf/base/parameters.yaml`:

```yaml
spark:
  app_name: recsys_tfb
  spark.serializer: org.apache.spark.serializer.KryoSerializer
  spark.sql.session.timeZone: Asia/Taipei
  spark.sql.catalogImplementation: hive
  spark.executor.memory: 4g
  spark.executor.cores: 2
```

Notes:
- Do NOT add `spark.hadoop.hive.metastore.uris` here — production sets it via `conf/production/parameters.yaml`. Local dev uses default in-memory metastore by omitting the key.
- Do NOT add `spark.master` here — local sets it via `conf/local/parameters.yaml`; production cluster supplies via `spark-submit`.

- [ ] **Step 3: Add or update `conf/local/parameters.yaml` for local-dev defaults**

If `conf/local/parameters.yaml` exists, append `spark:` block (deep-merge handles it). Otherwise create the file:

```yaml
spark:
  spark.master: local[*]
```

- [ ] **Step 4: Verify YAML parses cleanly via ConfigLoader**

Run:

```bash
python -c "from recsys_tfb.core.config import ConfigLoader; \
  c = ConfigLoader('conf', env='local'); \
  p = c.get_parameters_by_name('parameters'); \
  print(p.get('spark'))"
```

Expected: prints the merged dict including both `app_name` and `spark.master: local[*]`.

- [ ] **Step 5: Commit**

```bash
git add conf/base/parameters.yaml conf/local/parameters.yaml
git commit -m "feat: add base spark config to parameters.yaml"
```

---

### Task 4: Add helper `_load_spark_config` and call it from each pipeline command

**Files:**
- Modify: `src/recsys_tfb/__main__.py`

The CLI's existing `_load_config_and_setup` returns merged `params` via `config.get_parameters()`, which deep-merges **all** `parameters_*.yaml` files. That clobbers `spark:` across pipelines (alphabetical merge order). We need a helper that explicitly merges only `parameters.yaml` (base) + `parameters_<pipeline>.yaml`.

- [ ] **Step 1: Write the helper at the top of `__main__.py` (after imports, before commands)**

Insert near `_find_conf_dir`:

```python
from recsys_tfb.core.config import _deep_merge  # noqa: E402  module-level import below imports

def _load_spark_config(config: ConfigLoader, pipeline: str) -> dict:
    """Return base + pipeline-specific spark config, deep-merged.

    Reads ``parameters.yaml`` (base common) and ``parameters_<pipeline>.yaml``
    (pipeline override). Keys in pipeline file override base. Returns ``{}``
    when neither file has a ``spark:`` block.
    """
    try:
        base_params = config.get_parameters_by_name("parameters")
    except KeyError:
        base_params = {}
    base_spark = base_params.get("spark", {})

    try:
        pipe_params = config.get_parameters_by_name(f"parameters_{pipeline}")
    except KeyError:
        pipe_params = {}
    pipe_spark = pipe_params.get("spark", {})

    return _deep_merge(base_spark, pipe_spark)
```

Note on the import: `_deep_merge` is currently a module-private function in `core/config.py`. Either (a) add the import as shown (acceptable but uses a private name), or (b) reuse the simpler shallow override since spark configs are flat — both layers are dict-of-strings. Pick **(b)** to avoid private-name coupling:

Replace the function body with:

```python
def _load_spark_config(config: ConfigLoader, pipeline: str) -> dict:
    """Return base + pipeline-specific spark config, merged (pipeline wins)."""
    try:
        base_params = config.get_parameters_by_name("parameters")
    except KeyError:
        base_params = {}
    try:
        pipe_params = config.get_parameters_by_name(f"parameters_{pipeline}")
    except KeyError:
        pipe_params = {}
    base_spark = dict(base_params.get("spark", {}))
    pipe_spark = pipe_params.get("spark", {})
    base_spark.update(pipe_spark)
    return base_spark
```

(Spark configs are a flat dict of strings, so `dict.update` is sufficient and avoids the private import.)

- [ ] **Step 2: Add a call to the helper inside each pipeline command**

In `__main__.py`, modify each pipeline command to call `get_or_create_spark_session()` after `_load_config_and_setup` and before `_execute_pipeline` (or before `SQLRunner` for the ETL commands).

For **non-ETL commands** (`dataset`, `training`, `inference`, `evaluation`, `baselines`), insert immediately after `_load_config_and_setup(...)`:

```python
    from recsys_tfb.utils.spark import get_or_create_spark_session

    spark_configs = _load_spark_config(config, "<pipeline_name>")
    get_or_create_spark_session(spark_configs)
```

Specifically:

- `dataset()` (around line 247): pipeline name `"dataset"`.
- `training()` (around line 353): pipeline name `"training"`.
- `inference()` (around line 460): pipeline name `"inference"`.
- `evaluation()` (around line 530): pipeline name `"evaluation"`.
- `baselines()` (around line 587): pipeline name `"baselines"`.

For **ETL commands**, modify `_run_etl()` (around line 136) instead — it's the shared executor:

```python
def _run_etl(stage, env, target_dates, restart_from):
    from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner
    from recsys_tfb.utils.spark import get_or_create_spark_session

    config, params, backend, run_context = _load_config_and_setup(stage, env)

    spark_configs = _load_spark_config(config, stage)
    get_or_create_spark_session(spark_configs)

    # ... rest unchanged ...
```

Move the existing `from recsys_tfb.pipelines.source_etl.sql_runner import SQLRunner` line so it sits next to the new import (cosmetic). Do NOT remove or move any other lines.

- [ ] **Step 3: Smoke-test that imports load**

Run: `python -c "from recsys_tfb.__main__ import app; print('ok')"`
Expected: `ok` (no ImportError).

- [ ] **Step 4: Run existing tests to ensure no regressions**

Run: `pytest tests/ -x -q`
Expected: PASS (unchanged behavior because IO layer still uses `SparkSession.builder.getOrCreate()`; CLI changes only add a session-creation step before pipeline runs, which is idempotent if a session already exists).

If `tests/test_utils/test_spark.py::TestFallback::test_no_configs_no_active_falls_back_to_loader` fails because of CWD pollution from another test, you may need to add a `monkeypatch.chdir(tmp_path)` already-present check. Re-run only the failing test for diagnostics.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/__main__.py
git commit -m "feat: pipeline CLI commands build SparkSession from merged spark config"
```

---

### Task 5: Migrate IO layer to call `get_or_create_spark_session()`

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py:153-156`
- Modify: `src/recsys_tfb/io/parquet_dataset.py:31-33,55-57`
- Modify: `src/recsys_tfb/pipelines/source_etl/sql_runner.py:146-148`
- Modify: `tests/test_io/test_hive_table_dataset.py:20-23`

This task changes IO consumers but does not change behavior — they still get a session, just through the new entrypoint.

- [ ] **Step 1: Update `tests/test_io/test_hive_table_dataset.py` mock target**

Find the existing `_patch_spark` helper (around line 20-23). Replace:

```python
def _patch_spark(spark: MagicMock):
    return patch(
        "pyspark.sql.SparkSession.builder.getOrCreate", return_value=spark
    )
```

With:

```python
def _patch_spark(spark: MagicMock):
    return patch(
        "recsys_tfb.io.hive_table_dataset.get_or_create_spark_session",
        return_value=spark,
    )
```

- [ ] **Step 2: Update `hive_table_dataset.py`**

Find the `_get_spark` method (around line 153-156). Replace:

```python
    def _get_spark(self):
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
```

With:

```python
    def _get_spark(self):
        from recsys_tfb.utils.spark import get_or_create_spark_session

        return get_or_create_spark_session()
```

Also update the **module-level** import block at the top of the file: leave existing imports as-is (don't add a top-level import; keep the lazy import to avoid circular import at IO module load time).

- [ ] **Step 3: Update `parquet_dataset.py`**

Find both occurrences (around line 31-33 and 55-57). Replace each:

```python
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
```

With:

```python
            from recsys_tfb.utils.spark import get_or_create_spark_session

            spark = get_or_create_spark_session()
```

(Two replacements in this file.)

- [ ] **Step 4: Update `sql_runner.py`**

Find the `_initialize_context` method (around line 146-148). Replace:

```python
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
```

With:

```python
        from recsys_tfb.utils.spark import get_or_create_spark_session

        spark = get_or_create_spark_session()
```

- [ ] **Step 5: Run IO and SQL runner tests**

Run: `pytest tests/test_io/ tests/test_pipelines/test_source_etl/ -v`
Expected: PASS. The mock-target update in step 1 is the only behaviorally-relevant change; tests should be green.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py \
        src/recsys_tfb/io/parquet_dataset.py \
        src/recsys_tfb/pipelines/source_etl/sql_runner.py \
        tests/test_io/test_hive_table_dataset.py
git commit -m "refactor: IO layer uses get_or_create_spark_session"
```

---

### Task 6: Migrate `tests/conftest.py` and `scripts/suggest_categorical_cols.py`

**Files:**
- Modify: `tests/conftest.py`
- Modify: `scripts/suggest_categorical_cols.py:251-258`

- [ ] **Step 1: Update `tests/conftest.py`**

Replace the entire `spark` fixture body (around line 11-25):

```python
@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    test_configs = {
        "app_name": "recsys_tfb_test",
        "spark.master": "local[1]",
        "spark.sql.shuffle.partitions": "1",
        "spark.default.parallelism": "1",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "1g",
    }
    session = get_or_create_spark_session(test_configs)
    yield session
    session.stop()
```

Leave the `os.environ["PYSPARK_PYTHON"] = sys.executable` lines and the `from pyspark.sql import SparkSession` import — wait, the import is now unused. Remove `from pyspark.sql import SparkSession` from line 5.

- [ ] **Step 2: Update `scripts/suggest_categorical_cols.py`**

Replace the spark-backend block (around line 250-266):

```python
    elif backend == "spark":
        from recsys_tfb.utils.spark import get_or_create_spark_session

        spark = get_or_create_spark_session()
        try:
            sdf, stem = _load_spark(source, spark)
            categorical, implicit, n_rows = suggest_categorical_columns_spark(
                sdf, max_cardinality
            )
            n_cols = len(sdf.schema.fields)
        finally:
            spark.stop()
```

This drops the explicit `local[*] / appName / ui.enabled=false` config — they now come from `conf/<env>/parameters.yaml` via fallback. The script keeps `spark.stop()` because it is a one-shot CLI tool.

- [ ] **Step 3: Run all tests**

Run: `pytest tests/ -x -q`
Expected: PASS.

- [ ] **Step 4: Smoke-test the script**

Run:

```bash
python scripts/suggest_categorical_cols.py --help
```

Expected: Typer help output, no import errors.

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py scripts/suggest_categorical_cols.py
git commit -m "refactor: tests and scripts use get_or_create_spark_session"
```

---

### Task 7: Add pipeline-specific `spark:` overrides for training and inference

**Files:**
- Modify: `conf/base/parameters_training.yaml`
- Modify: `conf/base/parameters_inference.yaml`

- [ ] **Step 1: Add training override**

Append (or insert near top) to `conf/base/parameters_training.yaml`:

```yaml
spark:
  app_name: recsys_tfb-training
  spark.executor.memory: 16g
  spark.executor.cores: 4
  spark.driver.memory: 8g
```

- [ ] **Step 2: Add inference override**

Append (or insert near top) to `conf/base/parameters_inference.yaml`:

```yaml
spark:
  app_name: recsys_tfb-inference
  spark.executor.memory: 24g
  spark.executor.cores: 4
  spark.sql.shuffle.partitions: 400
```

- [ ] **Step 3: Verify merge via ConfigLoader**

Run:

```bash
python -c "
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.__main__ import _load_spark_config
c = ConfigLoader('conf', env='local')
print('training:', _load_spark_config(c, 'training'))
print('inference:', _load_spark_config(c, 'inference'))
print('dataset:', _load_spark_config(c, 'dataset'))
"
```

Expected:
- `training:` prints `app_name: recsys_tfb-training`, `spark.executor.memory: 16g`, plus base common (timeZone, serializer, etc.).
- `inference:` prints `app_name: recsys_tfb-inference`, `spark.executor.memory: 24g`, etc.
- `dataset:` prints base only (`app_name: recsys_tfb`, base common).

- [ ] **Step 4: Commit**

```bash
git add conf/base/parameters_training.yaml conf/base/parameters_inference.yaml
git commit -m "feat: pipeline-specific spark overrides for training and inference"
```

---

### Task 8: Local smoke test

**Files:** None (manual test)

- [ ] **Step 1: Pick the lightest pipeline that touches Spark**

Use `source_etl`'s `feature_etl` sub-command (it builds a SparkSession via `SQLRunner`, but with `dry_run=True` in local env it only renders SQL — fast). Check the env-resolved `dry_run` default in `_run_etl`: when `env == "local"` it defaults to `True`, so no real Spark work, but the session is still created.

Actually, with `dry_run=True`, `_initialize_context` returns `(None, None)` early and doesn't build a session. So smoke-test the session construction another way:

```bash
python -c "
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.__main__ import _load_spark_config
from recsys_tfb.utils.spark import get_or_create_spark_session

c = ConfigLoader('conf', env='local')
configs = _load_spark_config(c, 'training')
spark = get_or_create_spark_session(configs)
print('appName:', spark.sparkContext.appName)
print('master:', spark.sparkContext.master)
for k in sorted(configs):
    print(f'  conf {k!r} -> {spark.conf.get(k) if k.startswith(\"spark.\") else configs[k]}')
spark.stop()
"
```

Expected:
- `appName: recsys_tfb-training`
- `master: local[*]` (from `conf/local/parameters.yaml`)
- conf entries match expected merged values (training overrides + base common).

- [ ] **Step 2: Verify behavior with no spark block**

Run:

```bash
python -c "
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.__main__ import _load_spark_config
configs = _load_spark_config(ConfigLoader('conf', env='local'), 'sample_pool_etl')
print(configs)
"
```

Expected: prints just the base-common spark dict (no pipeline override since `parameters_sample_pool_etl.yaml` has no `spark:` block).

- [ ] **Step 3: Final full test run**

Run: `pytest tests/ -q`
Expected: All green.

- [ ] **Step 4: Commit (if anything changed)**

If smoke-testing exposed any issue and you fixed it, commit:

```bash
git add <fixed files>
git commit -m "fix: <what was wrong>"
```

Otherwise no commit needed.

---

## Self-Review

Spec coverage:

- ✓ Single entrypoint `get_or_create_spark_session()` — Task 1, 2.
- ✓ `spark_configs` dict signature — Task 1 step 4.
- ✓ Pipeline traffic path (configs passed in) — Task 1.
- ✓ Active-session warning — Task 1.
- ✓ Validation (TypeError, ValueError) — Task 1.
- ✓ Fallback path (active session → ConfigLoader) — Task 2.
- ✓ Base `parameters.yaml` `spark:` block — Task 3.
- ✓ Pipeline `spark:` overrides — Task 7.
- ✓ Env override (`conf/local/parameters.yaml`) — Task 3 step 3.
- ✓ Pipeline command bootstraps session — Task 4.
- ✓ ETL command bootstraps session via `_run_etl` — Task 4 step 2.
- ✓ IO layer migration — Task 5.
- ✓ Tests + scripts migration — Task 5, 6.
- ✓ Mock target update for `test_hive_table_dataset.py` — Task 5 step 1.
- ✓ Test fixture uses new function — Task 6.
- ✓ Smoke test — Task 8.

Placeholder scan: No "TBD" / "TODO" / "implement later" / "similar to Task N" / "add appropriate error handling" patterns.

Type / signature consistency:

- `get_or_create_spark_session(spark_configs: dict | None = None) -> SparkSession` — used in Tasks 1, 2, 5, 6.
- `_load_spark_config(config: ConfigLoader, pipeline: str) -> dict` — used in Task 4 (defined) and Tasks 7, 8 (called).
- `app_name` is a special key extracted before iterating builder configs — applied consistently in Task 1.

Special note on the `_deep_merge` import discussion in Task 4: the plan picks the no-private-import variant (`dict.update`) and uses it in step 1. The discussion of option (a) is informational only — only option (b) is implemented.

Note for the executing engineer: `_load_spark_config` lives in `__main__.py` for pragmatism. If a future pipeline needs the same helper from another entrypoint, lift it into `src/recsys_tfb/core/config.py` as a method on `ConfigLoader`. Out of scope for this plan.
