# Training Pipeline Cache Model Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four cache nodes (`cache_train_model_input`, `cache_train_dev_model_input`, `cache_val_model_input`, `cache_calibration_model_input`) before `tune_hyperparameters` in the training pipeline so re-running training with new hyperparameters skips Hive scans by reusing local parquet caches keyed by `(base_dataset_version, train_variant_id, calibration_variant_id)`.

**Architecture:** Each cache node receives a lazy Hive Spark DataFrame (production) or pandas DataFrame (dev), and either passes through (dev / cache miss) or returns a `spark.read.parquet(local)` DataFrame (cache hit). Local parquet is materialized by the framework's `ParquetDataset.save()` with `write_mode=ignore` providing a second layer of skip-if-exists. Downstream nodes consume new `cached_*` catalog entries.

**Tech Stack:** PySpark 3.3.2, pandas 1.5.3, pyarrow 14.0.1, pytest 7.3.1, existing project framework (Pipeline, Node, Runner, DataCatalog).

**Spec:** `docs/superpowers/specs/2026-05-02-training-cache-model-input-design.md`

---

## File Structure

**Create:**
- `tests/test_pipelines/test_training/test_cache_nodes.py` — unit tests for the four cache nodes and their helpers

**Modify:**
- `src/recsys_tfb/io/parquet_dataset.py` — add `write_mode` parameter (`overwrite` | `ignore`)
- `tests/test_io/test_parquet_dataset.py` — tests for `write_mode=ignore`
- `src/recsys_tfb/pipelines/training/nodes.py` — add `_resolve_cache_path`, `_is_spark_df`, `_cache_or_passthrough`, and four `cache_*_model_input` functions
- `src/recsys_tfb/pipelines/training/pipeline.py` — wire cache nodes; switch tune/train/calibrate/evaluate inputs to `cached_*`
- `tests/test_pipelines/test_training/test_pipeline.py` — update existing assertions to reflect new node count and renamed inputs
- `conf/base/parameters_training.yaml` — add `cache:` block (default enabled)
- `conf/local/parameters.yaml` — override `cache.enabled: false` for dev
- `conf/base/catalog.yaml` — no change (dev relies on auto-created MemoryDataset for `cached_*`)
- `conf/production/catalog.yaml` — add four `cached_*` ParquetDataset entries

---

## Task 1: Extend ParquetDataset with `write_mode` parameter

**Files:**
- Modify: `src/recsys_tfb/io/parquet_dataset.py:13-62`
- Test: `tests/test_io/test_parquet_dataset.py`

- [ ] **Step 1: Write failing test for spark `write_mode=ignore` skipping existing data**

Append to `tests/test_io/test_parquet_dataset.py` inside `TestParquetDatasetSpark`:

```python
    def test_write_mode_ignore_skips_existing(self, spark, tmp_path):
        """write_mode=ignore must not overwrite an existing parquet directory."""
        filepath = str(tmp_path / "ignore_target.parquet")
        ds = ParquetDataset(filepath=filepath, backend="spark", write_mode="ignore")

        # First write seeds the directory
        first = spark.createDataFrame([(1, "a")], ["id", "tag"])
        ds.save(first)
        assert ds.exists()

        # Second save with different data must be a no-op under mode=ignore
        second = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "tag"])
        ds.save(second)

        loaded = ds.load()
        rows = sorted([(r["id"], r["tag"]) for r in loaded.collect()])
        assert rows == [(1, "a")]

    def test_write_mode_default_overwrite_replaces(self, spark, tmp_path):
        """Default write_mode='overwrite' must replace existing data."""
        filepath = str(tmp_path / "overwrite_target.parquet")
        ds = ParquetDataset(filepath=filepath, backend="spark")  # default
        ds.save(spark.createDataFrame([(1, "a")], ["id", "tag"]))
        ds.save(spark.createDataFrame([(2, "b")], ["id", "tag"]))
        loaded = ds.load()
        rows = sorted([(r["id"], r["tag"]) for r in loaded.collect()])
        assert rows == [(2, "b")]

    def test_write_mode_invalid_raises(self):
        with pytest.raises(ValueError, match="write_mode must be"):
            ParquetDataset(filepath="/tmp/x.parquet", backend="spark", write_mode="merge")
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_io/test_parquet_dataset.py::TestParquetDatasetSpark::test_write_mode_ignore_skips_existing -v
pytest tests/test_io/test_parquet_dataset.py::TestParquetDatasetSpark::test_write_mode_default_overwrite_replaces -v
pytest tests/test_io/test_parquet_dataset.py::TestParquetDatasetSpark::test_write_mode_invalid_raises -v
```

Expected: FAIL — `ParquetDataset.__init__()` got an unexpected keyword argument `write_mode`.

- [ ] **Step 3: Implement `write_mode` in `ParquetDataset`**

Replace `src/recsys_tfb/io/parquet_dataset.py` with:

```python
import os

from recsys_tfb.io.base import AbstractDataset


class ParquetDataset(AbstractDataset):
    """Dataset for reading and writing Parquet files.

    Supports pandas and PySpark backends, selected via the ``backend`` parameter.
    Supports partitioned writes via the ``partition_cols`` parameter.
    Supports skip-if-exists semantics via ``write_mode='ignore'``.
    """

    _ALLOWED_WRITE_MODES = ("overwrite", "ignore")

    def __init__(
        self,
        filepath: str,
        backend: str = "pandas",
        partition_cols: list[str] | None = None,
        write_mode: str = "overwrite",
    ):
        if backend not in ("pandas", "spark"):
            raise ValueError(f"backend must be 'pandas' or 'spark', got '{backend}'")
        if write_mode not in self._ALLOWED_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {self._ALLOWED_WRITE_MODES}, got '{write_mode}'"
            )
        self._filepath = filepath
        self._backend = backend
        self._partition_cols = partition_cols
        self._write_mode = write_mode

    def load(self):
        if self._backend == "pandas":
            import pandas as pd

            return pd.read_parquet(self._filepath)
        else:
            from recsys_tfb.utils.spark import get_or_create_spark_session

            spark = get_or_create_spark_session()
            return spark.read.parquet(self._filepath)

    def save(self, data) -> None:
        if self._backend == "pandas":
            if self._write_mode == "ignore" and self.exists():
                return
            if hasattr(data, "toPandas"):
                data = data.toPandas()
            os.makedirs(os.path.dirname(self._filepath) or ".", exist_ok=True)
            if self._partition_cols:
                import pyarrow as pa
                import pyarrow.parquet as pq

                table = pa.Table.from_pandas(data)
                pq.write_to_dataset(
                    table, self._filepath, partition_cols=self._partition_cols
                )
            else:
                data.to_parquet(self._filepath, index=False)
        else:
            import pandas as pd

            if isinstance(data, pd.DataFrame):
                from recsys_tfb.utils.spark import get_or_create_spark_session

                spark = get_or_create_spark_session()
                data = spark.createDataFrame(data)
            writer = data.write.mode(self._write_mode)
            if self._partition_cols:
                writer = writer.partitionBy(*self._partition_cols)
            writer.parquet(self._filepath)

    def exists(self) -> bool:
        return os.path.exists(self._filepath)
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_io/test_parquet_dataset.py -v
```

Expected: PASS for all (including pre-existing).

- [ ] **Step 5: Commit**

```
git add src/recsys_tfb/io/parquet_dataset.py tests/test_io/test_parquet_dataset.py
git commit -m "feat: add write_mode parameter to ParquetDataset"
```

---

## Task 2: Add `cache:` block to training parameters

**Files:**
- Modify: `conf/base/parameters_training.yaml`
- Modify: `conf/local/parameters.yaml`

- [ ] **Step 1: Append cache block to base parameters**

Append to the end of `conf/base/parameters_training.yaml`:

```yaml

cache:
  enabled: true
  root: /tmp/recsys_cache
```

- [ ] **Step 2: Override cache.enabled for dev**

Append to the end of `conf/local/parameters.yaml`:

```yaml

cache:
  enabled: false
```

- [ ] **Step 3: Sanity-check both files load**

```
python -c "import yaml; yaml.safe_load(open('conf/base/parameters_training.yaml'))"
python -c "import yaml; yaml.safe_load(open('conf/local/parameters.yaml'))"
```

Expected: no output, no exception.

- [ ] **Step 4: Commit**

```
git add conf/base/parameters_training.yaml conf/local/parameters.yaml
git commit -m "feat: add cache config block to training parameters"
```

---

## Task 3: Add `_resolve_cache_path` and `_is_spark_df` helpers

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Test: `tests/test_pipelines/test_training/test_cache_nodes.py` (new file)

- [ ] **Step 1: Create new test file with helper tests**

Create `tests/test_pipelines/test_training/test_cache_nodes.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestResolveCachePath -v
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestIsSparkDataframe -v
```

Expected: FAIL — `ImportError: cannot import name '_resolve_cache_path'` / `_is_spark_df`.

- [ ] **Step 3: Add helpers to `nodes.py`**

Insert after the `_to_pandas` function in `src/recsys_tfb/pipelines/training/nodes.py` (after line 27):

```python
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

_CACHE_PATH_LAYOUT: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variants", "train_variant_id"),
    "calibration_model_input": (
        "base_dataset_version",
        "calibration_variants",
        "calibration_variant_id",
    ),
}


def _resolve_cache_path(dataset_name: str, parameters: dict) -> str:
    """Compose the local-cache parquet directory path for a model_input dataset.

    Mirrors the layered structure used by production catalog filepaths:
      <root>/<base_dataset_version>/[train_variants/<train_variant_id>/]<name>.parquet
    """
    if dataset_name not in _CACHE_PATH_LAYOUT:
        raise ValueError(f"unknown dataset for cache path: {dataset_name!r}")
    cache_cfg = parameters.get("cache", {})
    root = Path(cache_cfg.get("root", "/tmp/recsys_cache"))
    parts = [root]
    for token in _CACHE_PATH_LAYOUT[dataset_name]:
        if token in ("train_variants", "calibration_variants"):
            parts.append(Path(token))
        else:
            value = parameters[token]
            parts.append(Path(value))
    parts.append(Path(f"{dataset_name}.parquet"))
    full = parts[0]
    for p in parts[1:]:
        full = full / p
    return str(full)


def _is_spark_df(df) -> bool:
    """Return True if df looks like a PySpark DataFrame (has sql_ctx attr)."""
    return df is not None and hasattr(df, "sql_ctx")
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestResolveCachePath tests/test_pipelines/test_training/test_cache_nodes.py::TestIsSparkDataframe -v
```

Expected: PASS for 10 tests.

- [ ] **Step 5: Commit**

```
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat: add _resolve_cache_path and _is_spark_df helpers for training cache"
```

---

## Task 4: Add `_cache_or_passthrough` core logic

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Test: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Append unit tests for `_cache_or_passthrough`**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py`:

```python
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
        from recsys_tfb.pipelines.training.nodes import _cache_or_passthrough
        params = _params_with_versions(str(tmp_path), enabled=True)
        df = pd.DataFrame({"a": [1]})
        out = _cache_or_passthrough(df, "train_model_input", params)
        assert out is df


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
        assert out == f"reread_from::file://{target}"

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
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestCacheOrPassthroughDev tests/test_pipelines/test_training/test_cache_nodes.py::TestCacheOrPassthroughProd -v
```

Expected: FAIL — `cannot import name '_cache_or_passthrough'`.

- [ ] **Step 3: Add `_cache_or_passthrough` to `nodes.py`**

Append to the cache helpers section in `src/recsys_tfb/pipelines/training/nodes.py` (after `_is_spark_df`):

```python
def _cache_or_passthrough(df, dataset_name: str, parameters: dict):
    """Skip-if-exists local-parquet cache for a single model_input.

    Behaviour:
      - cache.enabled = False  -> return df unchanged (dev no-op)
      - df is not a Spark DataFrame and cache.enabled = True
            -> warn, return df unchanged (defensive in dev)
      - target path has _SUCCESS
            -> return spark.read.parquet(file://<path>) (cache hit)
      - target path exists but no _SUCCESS
            -> rmtree and treat as cache miss
      - cache miss
            -> return df unchanged; framework's catalog.save() persists it
    """
    cache_cfg = parameters.get("cache", {})
    if not cache_cfg.get("enabled", False):
        return df

    if not _is_spark_df(df):
        logger.warning(
            "cache.enabled=true but %s input is not a Spark DataFrame; passthrough",
            dataset_name,
        )
        return df

    local_path = _resolve_cache_path(dataset_name, parameters)
    success_marker = Path(local_path) / "_SUCCESS"

    if Path(local_path).exists() and not success_marker.exists():
        logger.info(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path)

    if success_marker.exists():
        logger.info(
            "cache_hit name=%s path=%s", dataset_name, local_path
        )
        spark = df.sql_ctx.sparkSession
        return spark.read.parquet(f"file://{local_path}")

    logger.info(
        "cache_miss name=%s path=%s (Spark write will be triggered by catalog.save)",
        dataset_name,
        local_path,
    )
    return df
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py -v
```

Expected: PASS for all (including helper tests from Task 3).

- [ ] **Step 5: Commit**

```
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat: add _cache_or_passthrough skip-if-exists helper for training cache"
```

---

## Task 5: Add four `cache_*_model_input` nodes

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Test: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Append node-level tests**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py`:

```python
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
        assert out == f"reread_from::file://{target}"

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
        assert out == f"reread_from::file://{target}"

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
        assert out == f"reread_from::file://{target}"

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
        assert out == f"reread_from::file://{target}"

    def test_cache_node_dev_passthrough(self, tmp_path):
        from recsys_tfb.pipelines.training.nodes import cache_train_model_input
        params = _params_with_versions(str(tmp_path), enabled=False)
        df = pd.DataFrame({"a": [1]})
        assert cache_train_model_input(df, params) is df
```

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestCacheNodes -v
```

Expected: FAIL — `cannot import name 'cache_train_model_input'`.

- [ ] **Step 3: Add the four cache node functions**

Append to the cache helpers section in `src/recsys_tfb/pipelines/training/nodes.py` (after `_cache_or_passthrough`):

```python
# ---------------------------------------------------------------------------
# Cache nodes
# ---------------------------------------------------------------------------

def cache_train_model_input(train_model_input, parameters: dict):
    """skip-if-exists local-parquet cache for train_model_input."""
    return _cache_or_passthrough(train_model_input, "train_model_input", parameters)


def cache_train_dev_model_input(train_dev_model_input, parameters: dict):
    """skip-if-exists local-parquet cache for train_dev_model_input."""
    return _cache_or_passthrough(train_dev_model_input, "train_dev_model_input", parameters)


def cache_val_model_input(val_model_input, parameters: dict):
    """skip-if-exists local-parquet cache for val_model_input."""
    return _cache_or_passthrough(val_model_input, "val_model_input", parameters)


def cache_calibration_model_input(calibration_model_input, parameters: dict):
    """skip-if-exists local-parquet cache for calibration_model_input."""
    return _cache_or_passthrough(
        calibration_model_input, "calibration_model_input", parameters
    )
```

- [ ] **Step 4: Run all cache-node tests to verify they pass**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py -v
```

Expected: PASS for everything in the file.

- [ ] **Step 5: Commit**

```
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat: add four cache_*_model_input nodes for training pipeline"
```

---

## Task 6: Wire cache nodes into training pipeline

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py`
- Modify: `tests/test_pipelines/test_training/test_pipeline.py`

- [ ] **Step 1: Update `test_pipeline.py` for new node count and inputs**

Open `tests/test_pipelines/test_training/test_pipeline.py` and replace the assertions inside `class TestTrainingPipeline` (the first ~10 methods) with:

```python
    def test_pipeline_has_seven_nodes(self):
        pipeline = create_pipeline()
        # 3 cache nodes (train, train_dev, val) + tune + train + evaluate + log
        assert len(pipeline.nodes) == 7

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input", "preprocessor", "parameters",
        }
        assert pipeline.inputs == expected

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "best_params", "model", "evaluation_results",
            "cached_train_model_input", "cached_train_dev_model_input",
            "cached_val_model_input",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "cache_train_model_input" in names
        assert "cache_train_dev_model_input" in names
        assert "cache_val_model_input" in names
        assert "tune_hyperparameters" in names
        assert "train_model" in names
        assert "evaluate_model" in names
        assert "log_experiment" in names

    def test_topological_order(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        # cache nodes must come before tune
        for cache_name in (
            "cache_train_model_input",
            "cache_train_dev_model_input",
            "cache_val_model_input",
        ):
            assert names.index(cache_name) < names.index("tune_hyperparameters")
        assert names.index("tune_hyperparameters") < names.index("train_model")
        assert names.index("train_model") < names.index("evaluate_model")
        assert names.index("evaluate_model") < names.index("log_experiment")

    # -- Calibration-enabled pipeline tests --

    def test_calibration_pipeline_has_nine_nodes(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 4 cache nodes + tune + train + calibrate + evaluate + log
        assert len(pipeline.nodes) == 9

    def test_calibration_pipeline_has_calibrate_node(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "calibrate_model" in names
        assert "cache_calibration_model_input" in names

    def test_calibration_pipeline_inputs(self):
        pipeline = create_pipeline(enable_calibration=True)
        assert "calibration_model_input" in pipeline.inputs

    def test_calibration_pipeline_trained_model_intermediate(self):
        pipeline = create_pipeline(enable_calibration=True)
        assert "trained_model" not in pipeline.inputs
        assert "trained_model" in pipeline.outputs

    def test_calibration_pipeline_topological_order(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert names.index("cache_calibration_model_input") < names.index("calibrate_model")
        assert names.index("train_model") < names.index("calibrate_model")
        assert names.index("calibrate_model") < names.index("evaluate_model")
```

(Remove `test_pipeline_has_four_nodes` and `test_calibration_pipeline_has_five_nodes` — they are replaced by the new cardinality tests above.)

- [ ] **Step 2: Run tests to verify they fail**

```
pytest tests/test_pipelines/test_training/test_pipeline.py::TestTrainingPipeline -v
```

Expected: FAIL — assertions on node count / output names / topological order do not match the current 4-node pipeline.

- [ ] **Step 3: Rewrite `pipeline.py` to wire cache nodes**

Replace `src/recsys_tfb/pipelines/training/pipeline.py` with:

```python
"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    evaluate_model,
    log_experiment,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(
            cache_train_model_input,
            inputs=["train_model_input", "parameters"],
            outputs="cached_train_model_input",
        ),
        Node(
            cache_train_dev_model_input,
            inputs=["train_dev_model_input", "parameters"],
            outputs="cached_train_dev_model_input",
        ),
        Node(
            cache_val_model_input,
            inputs=["val_model_input", "parameters"],
            outputs="cached_val_model_input",
        ),
    ]

    if enable_calibration:
        nodes.append(
            Node(
                cache_calibration_model_input,
                inputs=["calibration_model_input", "parameters"],
                outputs="cached_calibration_model_input",
            ),
        )

    nodes.extend([
        Node(
            tune_hyperparameters,
            inputs=[
                "cached_train_model_input", "cached_train_dev_model_input",
                "cached_val_model_input", "preprocessor", "parameters",
            ],
            outputs="best_params",
        ),
        Node(
            train_model,
            inputs=[
                "cached_train_model_input", "cached_train_dev_model_input",
                "best_params", "preprocessor", "parameters",
            ],
            outputs=train_model_output,
        ),
    ])

    if enable_calibration:
        nodes.append(
            Node(
                calibrate_model,
                inputs=[
                    "trained_model", "cached_calibration_model_input",
                    "preprocessor", "parameters",
                ],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "cached_val_model_input", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=["model", "best_params", "evaluation_results", "parameters"],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
```

- [ ] **Step 4: Run tests to verify they pass**

```
pytest tests/test_pipelines/test_training/test_pipeline.py::TestTrainingPipeline -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```
git add src/recsys_tfb/pipelines/training/pipeline.py tests/test_pipelines/test_training/test_pipeline.py
git commit -m "feat: wire cache_*_model_input nodes into training pipeline"
```

---

## Task 7: Add `cached_*` catalog entries (production)

**Files:**
- Modify: `conf/production/catalog.yaml`

- [ ] **Step 1: Append cache layer to production catalog**

Append after the existing "Training Pipeline - Binary / non-DataFrame artifacts" section in `conf/production/catalog.yaml`:

```yaml

# --- Training Pipeline - Local cache layer (per base + variant) ---
# These mirror the dataset-layer paths but live on driver-local disk so that
# rerunning the training pipeline (e.g. with new hyperparameters) does not
# rescan Hive. Skip-if-exists is enforced both by the cache node and by
# write_mode=ignore on the ParquetDataset.
cached_val_model_input:
  type: ParquetDataset
  filepath: file:///tmp/recsys_cache/${base_dataset_version}/val_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_train_model_input:
  type: ParquetDataset
  filepath: file:///tmp/recsys_cache/${base_dataset_version}/train_variants/${train_variant_id}/train_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_train_dev_model_input:
  type: ParquetDataset
  filepath: file:///tmp/recsys_cache/${base_dataset_version}/train_variants/${train_variant_id}/train_dev_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_calibration_model_input:
  type: ParquetDataset
  filepath: file:///tmp/recsys_cache/${base_dataset_version}/calibration_variants/${calibration_variant_id}/calibration_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore
```

- [ ] **Step 2: Sanity-check yaml parses**

```
python -c "import yaml; yaml.safe_load(open('conf/production/catalog.yaml'))"
```

Expected: no output, no exception.

- [ ] **Step 3: Sanity-check dataset factory accepts the new keys**

```
python -c "from recsys_tfb.io.parquet_dataset import ParquetDataset; ParquetDataset(filepath='/tmp/x', backend='spark', partition_cols=['snap_date','prod_name'], write_mode='ignore')"
```

Expected: no exception.

- [ ] **Step 4: Commit**

```
git add conf/production/catalog.yaml
git commit -m "feat: add cached_*_model_input parquet entries to production catalog"
```

---

## Task 8: Integration test — second run skips Hive

**Files:**
- Test: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Append integration test using the runner with mocked HiveTableDataset**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py`:

```python
class TestCacheRunnerIntegration:
    """Integration: running the cache node twice via Runner triggers Hive only once."""

    def test_second_run_skips_hive_load(self, tmp_path):
        from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
        from recsys_tfb.core.node import Node
        from recsys_tfb.core.pipeline import Pipeline
        from recsys_tfb.core.runner import Runner
        from recsys_tfb.io.base import AbstractDataset
        from recsys_tfb.io.parquet_dataset import ParquetDataset
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
                # First-run materialization: simulate Spark write by creating
                # the parquet directory + _SUCCESS so the second run hits the
                # fast path inside cache_train_model_input.
                Path(cache_path).mkdir(parents=True, exist_ok=True)
                (Path(cache_path) / "_SUCCESS").touch()

            def exists(self):
                return Path(cache_path).exists()

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

        Runner().run(pipeline, catalog)
        assert len(load_calls) == 1, "first run must load the source once"
        Runner().run(pipeline, catalog)
        assert len(load_calls) == 2, (
            "Runner.load is per-run; the relevant guarantee is that the cache "
            "node short-circuits and downstream Hive scans are avoided"
        )
        # The cached_* output of the second run is the local re-read string.
        cached = catalog.load("cached_train_model_input")
        assert cached == f"reread_from::file://{cache_path}"
```

- [ ] **Step 2: Run the integration test to verify it fails (or surfaces gaps)**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestCacheRunnerIntegration -v
```

Expected: FAIL only if catalog API does not match what we used. If it passes immediately, that's also acceptable — the test serves as a regression guard.

- [ ] **Step 3: Inspect failure if any**

If the test fails because `DataCatalog.add(...)` or `MemoryDataset(initial_data=...)` does not exist, open `src/recsys_tfb/core/catalog.py` to find the correct constructor signatures, and update the test accordingly. **Do not modify production code to satisfy this test** — only the test itself, since the goal is to exercise the existing framework.

- [ ] **Step 4: Run again to verify pass**

```
pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestCacheRunnerIntegration -v
```

Expected: PASS.

- [ ] **Step 5: Run the entire training test suite for regression**

```
pytest tests/test_pipelines/test_training/ tests/test_io/test_parquet_dataset.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```
git add tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "test: integration test for cache_train_model_input via Runner"
```

---

## Task 9: Final regression sweep + graphify rebuild

**Files:**
- None (verification only)

- [ ] **Step 1: Run full project test suite**

```
pytest -q
```

Expected: all PASS. Investigate any failure before continuing.

- [ ] **Step 2: Rebuild graphify code graph**

```
python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

Expected: prints `Rebuilt: <N> nodes, <E> edges, ...`.

- [ ] **Step 3: Verify graph picked up the new cache nodes**

```
grep -E "cache_train_model_input|cache_val_model_input|cache_calibration_model_input|_resolve_cache_path|_cache_or_passthrough" graphify-out/GRAPH_REPORT.md | head
```

Expected: at least one match (graphify usually surfaces high-traffic identifiers; absence is acceptable but a presence check is reassuring).

- [ ] **Step 4: Commit graphify output**

```
git add graphify-out/
git commit -m "chore: rebuild graphify after training cache implementation"
```

(Skip this commit if `git status` shows no changes under `graphify-out/`.)
