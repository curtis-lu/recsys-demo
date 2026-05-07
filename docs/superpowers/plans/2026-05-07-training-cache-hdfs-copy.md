# Training Cache HDFS Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace cache layer's distributed Spark `file://` IO with cache-node-managed HDFS copyToLocal, fixing the YARN Mkdirs failure.

**Architecture:** Cache node owns disk persistence (was: framework's ParquetDataset). Catalog drops `cached_*_model_input` entries → catalog auto-creates MemoryDataset for in-memory passthrough. New `utils/hdfs.py` module isolates HDFS↔driver-local mechanics from cache protocol logic.

**Tech Stack:** PySpark 3.3.2 (JVM bridge via `spark._jvm` / `spark._jsc`), pyarrow for partition-aware parquet read, pytest with `unittest.mock` for JVM bridge mocking.

**Spec:** `docs/superpowers/specs/2026-05-07-training-cache-hdfs-copy-design.md`

---

## File Structure

**New files:**
- `src/recsys_tfb/utils/hdfs.py` — `get_hive_table_location`, `copy_hdfs_to_local`
- `tests/test_utils/test_hdfs.py` — unit tests for the two helpers

**Modified files:**
- `src/recsys_tfb/pipelines/training/nodes.py` — add `_CACHE_SOURCE_TABLE`, `_CACHE_OUTER_PARTITIONS`, `_populate_cache_from_hive`; rewrite `_cache_or_passthrough` miss/hit paths
- `tests/test_pipelines/test_training/test_cache_nodes.py` — replace `_FakeReader`/`_FakeSparkSession`/`_FakeSparkDF` set; rewrite prod-path tests; add `TestPopulateCacheFromHive`
- `conf/base/catalog.yaml` — delete 4 `cached_*_model_input` entries
- `conf/base/parameters_training.yaml` — update `cache:` section comment

---

## Tasks

### Task 1: utils/hdfs.py — `get_hive_table_location`

**Files:**
- Create: `src/recsys_tfb/utils/hdfs.py`
- Create: `tests/test_utils/test_hdfs.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_utils/test_hdfs.py` with:

```python
"""Tests for recsys_tfb.utils.hdfs."""

from collections import namedtuple
from unittest.mock import MagicMock

import pytest


# DESCRIBE FORMATTED rows expose .col_name and .data_type
FakeRow = namedtuple("FakeRow", ["col_name", "data_type"])


class TestGetHiveTableLocation:
    def test_parses_location_from_describe_formatted(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("col1", "STRING"),
            FakeRow("col2", "INT"),
            FakeRow("Location", "hdfs://nn:9000/warehouse/db.foo"),
            FakeRow("Table Type", "MANAGED_TABLE"),
        ]

        result = get_hive_table_location(spark, "db", "foo")

        assert result == "hdfs://nn:9000/warehouse/db.foo"
        spark.sql.assert_called_once_with("DESCRIBE FORMATTED db.foo")

    def test_strips_whitespace_in_col_name_and_data_type(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("  Location  ", "  hdfs://nn/path  "),
        ]

        result = get_hive_table_location(spark, "db", "foo")
        assert result == "hdfs://nn/path"

    def test_raises_when_location_row_missing(self):
        from recsys_tfb.utils.hdfs import get_hive_table_location

        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            FakeRow("col1", "STRING"),
        ]

        with pytest.raises(RuntimeError, match="Location not found"):
            get_hive_table_location(spark, "db", "foo")
```

- [ ] **Step 2: Run test — verify it fails**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py::TestGetHiveTableLocation -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'recsys_tfb.utils.hdfs'`

- [ ] **Step 3: Create utils/hdfs.py with `get_hive_table_location`**

Create `src/recsys_tfb/utils/hdfs.py`:

```python
"""HDFS↔driver-local file-copy utilities.

Pure mechanics, agnostic to caller. No knowledge of Hive business semantics
or cache protocol — those live in the calling module (e.g. training/nodes.py).
"""

from __future__ import annotations


def get_hive_table_location(spark, database: str, table: str) -> str:
    """Return the HDFS Location URI of a Hive table via DESCRIBE FORMATTED.

    Args:
        spark: active SparkSession.
        database: Hive database name.
        table: Hive table name.

    Returns:
        Raw URI from the Location row (e.g. 'hdfs://nn:9000/warehouse/db.tbl').

    Raises:
        RuntimeError: if no row with col_name == 'Location' is present.
    """
    rows = spark.sql(f"DESCRIBE FORMATTED {database}.{table}").collect()
    for row in rows:
        col_name = row.col_name.strip() if row.col_name else ""
        if col_name == "Location":
            return row.data_type.strip()
    raise RuntimeError(
        f"Location not found in DESCRIBE FORMATTED for {database}.{table}"
    )
```

- [ ] **Step 4: Run test — verify it passes**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py::TestGetHiveTableLocation -v`
Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/hdfs.py tests/test_utils/test_hdfs.py
git commit -m "feat(utils): add hdfs.get_hive_table_location"
```

---

### Task 2: utils/hdfs.py — `copy_hdfs_to_local` (non-glob)

**Files:**
- Modify: `src/recsys_tfb/utils/hdfs.py`
- Modify: `tests/test_utils/test_hdfs.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_utils/test_hdfs.py`:

```python
def _make_fake_spark():
    """Build a MagicMock spark simulating the JVM bridge surface we use."""
    spark = MagicMock()
    spark._jsc.hadoopConfiguration.return_value = MagicMock(name="hadoop_conf")

    fs = MagicMock(name="FileSystem")
    spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    def make_path(s):
        p = MagicMock(name=f"Path({s})")
        p.__str__ = lambda self: s
        # getName() returns the basename — needed for glob path computation
        p.getName.return_value = s.rstrip("/").split("/")[-1] or "/"
        return p

    spark._jvm.org.apache.hadoop.fs.Path.side_effect = make_path
    return spark, fs


class TestCopyHdfsToLocal:
    def test_non_glob_calls_copyToLocalFile_once(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        dst = str(tmp_path / "out")

        copy_hdfs_to_local(spark, "hdfs://nn/foo/bar", dst)

        assert fs.copyToLocalFile.call_count == 1
        assert (tmp_path / "out").exists()  # mkdir done

    def test_non_glob_uses_filesystem_from_src_path(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        copy_hdfs_to_local(spark, "hdfs://nn/x", str(tmp_path / "y"))

        # FileSystem.get is called with the hadoop config we built above
        spark._jvm.org.apache.hadoop.fs.FileSystem.get.assert_called_once()
```

- [ ] **Step 2: Run test — verify it fails**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py::TestCopyHdfsToLocal -v`
Expected: FAIL with `ImportError: cannot import name 'copy_hdfs_to_local'`

- [ ] **Step 3: Add `copy_hdfs_to_local` (non-glob path only)**

Append to `src/recsys_tfb/utils/hdfs.py`:

```python
import os


def copy_hdfs_to_local(
    spark, src: str, dst: str, *, glob: bool = False
) -> None:
    """Copy an HDFS path (file or directory) to a driver-local path.

    Uses Spark's Hadoop FileSystem via JVM bridge — does not depend on a
    `hadoop` CLI on PATH.

    Args:
        spark: active SparkSession (used for JVM bridge + Hadoop config).
        src: HDFS source URI (or glob pattern when glob=True).
        dst: driver-local destination directory.
        glob: if True, treat src as a glob pattern and copy every match
            into dst/, preserving each match's basename.

    Raises:
        FileNotFoundError: glob=True and no paths matched.
    """
    os.makedirs(dst, exist_ok=True)

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

    src_path = jvm.org.apache.hadoop.fs.Path(src)

    if glob:
        # Implemented in Task 3
        raise NotImplementedError("glob mode added in Task 3")
    else:
        dst_path = jvm.org.apache.hadoop.fs.Path(dst)
        # copyToLocalFile(deleteSource, src, dst, useRawLocalFileSystem)
        fs.copyToLocalFile(False, src_path, dst_path, False)
```

- [ ] **Step 4: Run test — verify it passes**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py::TestCopyHdfsToLocal -v`
Expected: 2 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/hdfs.py tests/test_utils/test_hdfs.py
git commit -m "feat(utils): add hdfs.copy_hdfs_to_local non-glob path"
```

---

### Task 3: utils/hdfs.py — `copy_hdfs_to_local` (glob mode)

**Files:**
- Modify: `src/recsys_tfb/utils/hdfs.py`
- Modify: `tests/test_utils/test_hdfs.py`

- [ ] **Step 1: Write the failing tests**

Append to `TestCopyHdfsToLocal` in `tests/test_utils/test_hdfs.py`:

```python
    def test_glob_iterates_over_globStatus_results(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()

        # Build two FileStatus mocks with different basenames
        def make_status(basename):
            status = MagicMock(name=f"FileStatus({basename})")
            inner_path = MagicMock(name=f"Path({basename})")
            inner_path.getName.return_value = basename
            status.getPath.return_value = inner_path
            return status

        fs.globStatus.return_value = [
            make_status("snap_date=2025-10-31"),
            make_status("snap_date=2025-09-30"),
        ]

        dst = str(tmp_path / "cache")
        copy_hdfs_to_local(
            spark, "hdfs://nn/foo/snap_date=*", dst, glob=True
        )

        # globStatus called once with src pattern
        fs.globStatus.assert_called_once()
        # copyToLocalFile called twice, one per match
        assert fs.copyToLocalFile.call_count == 2

    def test_glob_raises_when_no_matches(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        fs.globStatus.return_value = None  # Hadoop returns null on no match

        with pytest.raises(FileNotFoundError, match="No HDFS paths matched"):
            copy_hdfs_to_local(
                spark, "hdfs://nn/empty/*", str(tmp_path), glob=True
            )

    def test_glob_raises_when_empty_match_array(self, tmp_path):
        from recsys_tfb.utils.hdfs import copy_hdfs_to_local

        spark, fs = _make_fake_spark()
        fs.globStatus.return_value = []

        with pytest.raises(FileNotFoundError, match="No HDFS paths matched"):
            copy_hdfs_to_local(
                spark, "hdfs://nn/empty/*", str(tmp_path), glob=True
            )
```

- [ ] **Step 2: Run tests — verify they fail**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py::TestCopyHdfsToLocal -v`
Expected: 3 NEW tests FAIL with `NotImplementedError: glob mode added in Task 3`

- [ ] **Step 3: Implement glob mode**

Replace the `if glob: raise NotImplementedError(...)` block in `src/recsys_tfb/utils/hdfs.py` with:

```python
    if glob:
        statuses = fs.globStatus(src_path)
        if statuses is None or len(statuses) == 0:
            raise FileNotFoundError(f"No HDFS paths matched: {src}")
        for status in statuses:
            sub_src = status.getPath()
            basename = sub_src.getName()
            sub_dst_path = jvm.org.apache.hadoop.fs.Path(
                os.path.join(dst, basename)
            )
            fs.copyToLocalFile(False, sub_src, sub_dst_path, False)
    else:
        dst_path = jvm.org.apache.hadoop.fs.Path(dst)
        fs.copyToLocalFile(False, src_path, dst_path, False)
```

- [ ] **Step 4: Run tests — verify all pass**

Run: `.venv/bin/pytest tests/test_utils/test_hdfs.py -v`
Expected: 8 PASSED (3 location + 2 non-glob + 3 glob)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/utils/hdfs.py tests/test_utils/test_hdfs.py
git commit -m "feat(utils): add hdfs.copy_hdfs_to_local glob mode"
```

---

### Task 4: nodes.py — `_populate_cache_from_hive`

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Modify: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pipelines/test_training/test_cache_nodes.py` (after existing imports add `from unittest.mock import ANY, MagicMock, patch` if absent — project uses `unittest.mock` directly, not `pytest-mock`):

```python
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
```

- [ ] **Step 2: Run tests — verify they fail**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestPopulateCacheFromHive -v`
Expected: FAIL with `ImportError: cannot import name '_populate_cache_from_hive'`

- [ ] **Step 3: Add lookup tables and helper to nodes.py**

In `src/recsys_tfb/pipelines/training/nodes.py`, add the import at the top (next to other recsys_tfb imports):

```python
from recsys_tfb.utils.hdfs import copy_hdfs_to_local, get_hive_table_location
```

Then, immediately after the existing `_CACHE_PATH_LAYOUT` dict (around line 46), add:

```python
# cache name → source Hive table (under parameters["hive"]["db"])
_CACHE_SOURCE_TABLE: dict[str, str] = {
    "val_model_input": "val_model_input",
    "test_model_input": "test_model_input",
    "train_model_input": "train_model_input",
    "train_dev_model_input": "train_dev_model_input",
    "calibration_model_input": "calibration_model_input",
}

# Outer (string) Hive partitions encoding the variant boundaries.
# Mirrors catalog.yaml's `partition_filter` keys; copy these as the
# subtree root, then `snap_date=*` is the inner glob pattern.
_CACHE_OUTER_PARTITIONS: dict[str, tuple[str, ...]] = {
    "val_model_input": ("base_dataset_version",),
    "test_model_input": ("base_dataset_version",),
    "train_model_input": ("base_dataset_version", "train_variant_id"),
    "train_dev_model_input": ("base_dataset_version", "train_variant_id"),
    "calibration_model_input": ("base_dataset_version", "calibration_variant_id"),
}


def _populate_cache_from_hive(
    spark, dataset_name: str, parameters: dict, local_dst: str
) -> None:
    """Copy the relevant Hive partition subtree to driver-local fs.

    Local layout after copy:
        <local_dst>/snap_date=.../prod_name=.../*.parquet
    """
    db = parameters["hive"]["db"]
    table = _CACHE_SOURCE_TABLE[dataset_name]
    location = get_hive_table_location(spark, db, table)
    outer = "/".join(
        f"{tok}={parameters[tok]}"
        for tok in _CACHE_OUTER_PARTITIONS[dataset_name]
    )
    src_glob = f"{location.rstrip('/')}/{outer}/snap_date=*"
    copy_hdfs_to_local(spark, src_glob, local_dst, glob=True)
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py::TestPopulateCacheFromHive -v`
Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "feat(training): add _populate_cache_from_hive helper"
```

---

### Task 5: nodes.py — rewrite `_cache_or_passthrough` (test + impl together)

This task changes `_cache_or_passthrough`'s contract: it now returns a pandas DataFrame in both miss and hit paths, and performs HDFS copy on miss. All existing prod-path tests must be updated in lockstep with the implementation to keep the suite green at commit boundary.

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py`
- Modify: `tests/test_pipelines/test_training/test_cache_nodes.py`

- [ ] **Step 1: Replace prod-path tests with new contract**

In `tests/test_pipelines/test_training/test_cache_nodes.py`, **delete** the following classes/fixtures (they are tied to the old `spark.read.parquet` contract):

- `_FakeReader` (lines 94-101)
- `_FakeSparkSession` (lines 104-106)

**Keep** `_FakeSparkDF` but simplify it to just supply `sql_ctx.sparkSession`:

```python
class _FakeSparkDF:
    """Minimal spark-df stand-in: only carries sql_ctx.sparkSession."""

    def __init__(self):
        self.sql_ctx = type("SqlCtx", (), {})()
        self.sql_ctx.sparkSession = MagicMock(name="spark")
```

Replace `class TestCacheOrPassthroughProd` (lines 136-172) with:

```python
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
```

- [ ] **Step 2: Update wrapper-node tests (`TestCacheNodes`) for new contract**

Replace `TestCacheNodes` (lines 177-230) with:

```python
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
```

- [ ] **Step 3: Update `TestCacheRunnerIntegration` for new contract**

Replace `TestCacheRunnerIntegration` (lines 233-298) with:

```python
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
```

- [ ] **Step 4: Update `TestParametersWiringRegression::test_cache_node_runs_when_versions_present`**

Replace the body of `test_cache_node_runs_when_versions_present` (lines 321-337) with:

```python
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
```

- [ ] **Step 5: Run tests — verify they fail**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py -v`
Expected: NEW prod-path tests FAIL because `_cache_or_passthrough` still has old behavior.

- [ ] **Step 6: Rewrite `_cache_or_passthrough` implementation**

Replace `_cache_or_passthrough` in `src/recsys_tfb/pipelines/training/nodes.py` (lines 78-124) with:

```python
def _cache_or_passthrough(df, dataset_name: str, parameters: dict):
    """Skip-if-exists local-parquet cache for a single model_input.

    Behaviour:
      - cache.enabled = False  -> return df unchanged (dev no-op)
      - df is not a Spark DataFrame and cache.enabled = True
            -> warn, return df unchanged (defensive in dev)
      - target path has _SUCCESS
            -> read locally via pd.read_parquet (cache hit)
      - target path exists but no _SUCCESS
            -> rmtree and treat as cache miss
      - cache miss
            -> hadoop fs copyToLocal HDFS subtree to driver-local;
               touch _SUCCESS; read locally via pd.read_parquet
    """
    import pandas as pd  # local import: keep nodes.py top-level light

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
        logger.warning(
            "Partial cache detected at %s, clearing before retry", local_path
        )
        shutil.rmtree(local_path, ignore_errors=True)

    if not success_marker.exists():
        spark = df.sql_ctx.sparkSession
        logger.info("cache_miss name=%s path=%s", dataset_name, local_path)
        _populate_cache_from_hive(spark, dataset_name, parameters, local_path)
        success_marker.touch()
    else:
        logger.info("cache_hit name=%s path=%s", dataset_name, local_path)

    return pd.read_parquet(local_path, engine="pyarrow")
```

- [ ] **Step 7: Run all cache-node tests — verify all pass**

Run: `.venv/bin/pytest tests/test_pipelines/test_training/test_cache_nodes.py -v`
Expected: ALL passing (`TestResolveCachePath`, `TestIsSparkDataframe`, `TestCacheOrPassthroughDev`, `TestCacheOrPassthroughProd`, `TestCacheNodes`, `TestCacheRunnerIntegration`, `TestParametersWiringRegression`, `TestPopulateCacheFromHive`).

- [ ] **Step 8: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_cache_nodes.py
git commit -m "refactor(training): cache_or_passthrough owns HDFS copy + returns pandas"
```

---

### Task 6: Delete cached_*_model_input from catalog.yaml

**Files:**
- Modify: `conf/base/catalog.yaml`

- [ ] **Step 1: Run full test suite to capture pre-change baseline**

Run: `.venv/bin/pytest tests/ -q`
Expected: ALL passing (baseline before catalog edit).

- [ ] **Step 2: Delete the four `cached_*_model_input` entries from catalog.yaml**

In `conf/base/catalog.yaml`, delete lines 200-226 (the four entries plus their preceding comment block from line 194):

Lines to delete:
```yaml
# --- Training Pipeline - Driver-local parquet cache (per base + variant) ---
# These mirror the dataset-layer paths but live on driver-local disk so that
# rerunning the training pipeline (e.g. with new hyperparameters) does not
# rescan Hive. Requires spark.master=local[*] (driver=executor 同 JVM)，否則
# distributed worker container 看不到 host fs。Skip-if-exists 由 cache node
# 與 ParquetDataset 的 write_mode=ignore 雙重保證。
cached_val_model_input:
  type: ParquetDataset
  filepath: file:///Users/curtislu/projects/recsys_tfb/data/recsys_cache/${base_dataset_version}/val_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_train_model_input:
  type: ParquetDataset
  filepath: file:///Users/curtislu/projects/recsys_tfb/data/recsys_cache/${base_dataset_version}/train_variants/${train_variant_id}/train_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_train_dev_model_input:
  type: ParquetDataset
  filepath: file:///Users/curtislu/projects/recsys_tfb/data/recsys_cache/${base_dataset_version}/train_variants/${train_variant_id}/train_dev_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore

cached_calibration_model_input:
  type: ParquetDataset
  filepath: file:///Users/curtislu/projects/recsys_tfb/data/recsys_cache/${base_dataset_version}/calibration_variants/${calibration_variant_id}/calibration_model_input.parquet
  backend: spark
  partition_cols: [snap_date, prod_name]
  write_mode: ignore
```

Replace with a single short comment so future readers know why the entries are absent:

```yaml
# Note: cached_*_model_input are intentionally not registered here.
# Persistence is owned by training/nodes.py::_cache_or_passthrough
# (driver-local parquet via hadoop fs copyToLocal); catalog auto-creates
# MemoryDataset for these names for in-memory passthrough between nodes.
# See docs/superpowers/specs/2026-05-07-training-cache-hdfs-copy-design.md
```

- [ ] **Step 3: Run full test suite — verify still all pass**

Run: `.venv/bin/pytest tests/ -q`
Expected: ALL passing. If any test relied on the catalog entry existing, fix or delete that test.

- [ ] **Step 4: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "chore(catalog): drop cached_*_model_input entries (cache node owns disk IO)"
```

---

### Task 7: Update parameters_training.yaml comment

**Files:**
- Modify: `conf/base/parameters_training.yaml`

- [ ] **Step 1: Update the cache section comment**

In `conf/base/parameters_training.yaml`, replace lines 43-49:

```yaml
# Driver-local parquet cache for materialized model inputs (skips Hive scans on
# re-runs). Keyed by base_dataset_version / train_variant_id / calibration_variant_id.
# 路徑必須與 catalog.yaml 中 cached_*_model_input.filepath（去掉 file:// scheme）
# 一致 —— cache node 用 root 做 existence check、catalog 用 filepath 做 Spark write。
cache:
  enabled: true
  root: /Users/curtislu/projects/recsys_tfb/data/recsys_cache
```

with:

```yaml
# Driver-local parquet cache for materialized model inputs (skips Hive scans on
# re-runs). Keyed by base_dataset_version / train_variant_id / calibration_variant_id.
# `cache.root` is the single source of truth for the cache path: both
# _resolve_cache_path() (existence check) and _populate_cache_from_hive()
# (hadoop fs copyToLocal destination) read from here. catalog.yaml does NOT
# register cached_*_model_input — see catalog.yaml inline note.
cache:
  enabled: true
  root: /Users/curtislu/projects/recsys_tfb/data/recsys_cache
```

- [ ] **Step 2: Run full test suite — verify nothing breaks**

Run: `.venv/bin/pytest tests/ -q`
Expected: ALL passing.

- [ ] **Step 3: Commit**

```bash
git add conf/base/parameters_training.yaml
git commit -m "docs(params): clarify cache.root ownership after catalog cleanup"
```

---

### Task 8: Manual smoke test on dev cluster

This task verifies the implementation against a real Spark + HDFS setup before any prod attempt. It does not produce code; the engineer runs commands and confirms behavior.

**Prerequisites:**
- `~/dev-cluster/` Docker stack running (see `~/dev-cluster/README.md`)
- `scripts/setup_hive_dev.py` already executed → `ml_recsys.train_model_input` etc. exist on HDFS
- `/etc/hosts` has `127.0.0.1 namenode datanode hive-metastore spark-master`

- [ ] **Step 1: Clear any existing local cache**

```bash
rm -rf data/recsys_cache/
```

- [ ] **Step 2: Run training pipeline end-to-end (cache miss path)**

```bash
source ~/dev-cluster/scripts/client-env.sh
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

Expected:
- Logs show `cache_miss name=train_model_input ...`
- Logs show `hdfs_copy src=hdfs://namenode:9000/...train_model_input/base_dataset_version=.../train_variant_id=.../snap_date=* dst=...`
- After completion, `data/recsys_cache/<bdv>/train_variants/<tvi>/train_model_input.parquet/_SUCCESS` exists
- Pipeline completes without errors

- [ ] **Step 3: Re-run training (cache hit path)**

```bash
.venv/bin/python -m recsys_tfb training --env production
```

Expected:
- Logs show `cache_hit name=train_model_input ...` for each cache node
- No `hdfs_copy` log lines (populate is skipped)
- Pipeline completes faster than first run

- [ ] **Step 4: Sanity check schema fidelity**

In a Python REPL:

```bash
.venv/bin/python -c "
import pandas as pd
from pathlib import Path
local = next(Path('data/recsys_cache').rglob('train_model_input.parquet'))
df = pd.read_parquet(local, engine='pyarrow')
print('shape:', df.shape)
print('columns:', list(df.columns))
print('dtypes:', df.dtypes)
print('partition values seen:', sorted(df['snap_date'].unique()), sorted(df['prod_name'].unique()))
"
```

Expected:
- Non-zero rows
- `snap_date` and `prod_name` present as columns (restored from hive partition dir names)
- Other feature columns present

- [ ] **Step 5: Note any deviations**

If anything diverges from expected behavior, capture the exact log lines / error messages. Common issues:
- `hadoop fs` not available via JVM bridge → check `HADOOP_CONF_DIR` is set by `client-env.sh`
- partition columns missing as DataFrame columns → pyarrow version too old for hive partitioning auto-detection (need `>= 14.0`)
- empty `out.shape` → check the HDFS source path actually contains `snap_date=...` subdirs for the resolved `base_dataset_version` / `train_variant_id`

If smoke test passes, proceed to push to remote (separate manual step, not part of plan).

---

## Self-Review Notes

Spec coverage check (against `docs/superpowers/specs/2026-05-07-training-cache-hdfs-copy-design.md`):

| Spec section | Plan task |
|---|---|
| `utils/hdfs.py::get_hive_table_location` | Task 1 |
| `utils/hdfs.py::copy_hdfs_to_local` non-glob | Task 2 |
| `utils/hdfs.py::copy_hdfs_to_local` glob | Task 3 |
| `_CACHE_SOURCE_TABLE`, `_CACHE_OUTER_PARTITIONS`, `_populate_cache_from_hive` | Task 4 |
| `_cache_or_passthrough` rewrite + 9 existing tests updated | Task 5 |
| Delete 4 catalog entries | Task 6 |
| Update `parameters_training.yaml` comment | Task 7 |
| Manual dev cluster verification | Task 8 |

Risk coverage:
- Spec risk #1 (JVM bridge mock complexity) — addressed by detailed `_make_fake_spark()` fixture in Task 2
- Spec risk #2 (pyarrow partition dtype) — addressed by Task 8 step 4 schema check
- Spec risk #3 (driver disk capacity) — out of plan scope (operations concern)
- Spec risk #4 (HADOOP_CONF_DIR) — addressed by Task 8 step 5 troubleshooting hint
- Spec risk #5 (concurrent write race) — out of plan scope (single-writer assumption documented in spec)
