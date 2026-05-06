# Dataset Hive Tables: Version as Partition Column — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 dataset pipeline 寫到 Hive 的 11 張 DataFrame artifact 表，從「version 字串塞進 table 名」改成「version 字串作為 Hive partition column」，11 邏輯 artifact = 11 張表，跨版本共存於同表的 partition 之中。

**Architecture:** 在 `HiveTableDataset` 引入 `partition_filter`（dict）欄位作為「static partition column」抽象——load 時自動 inject `WHERE` 子句，save 時自動把 literal value column 補上 DataFrame 並走既有 dynamic partition overwrite 機制（不需要改用 raw `INSERT OVERWRITE PARTITION` SQL）。Catalog 11 張 dataset 表全改寫去掉 `_${version}` suffix，改宣告 `partition_filter` + `partition_cols`。Dev-cluster 用 nuke + 重灌驗證從零跑得起來。

**Tech Stack:** Python 3.10, PySpark 3.3.2, Hive Metastore (managed tables), pytest 7.3.1（mock-based unit tests）。

**Spec:** `docs/superpowers/specs/2026-05-06-dataset-version-as-partition-design.md`

---

## File Structure

**Modify:**
- `src/recsys_tfb/io/hive_table_dataset.py` — 加 `partition_filter` 欄位、`_validate` 擴充、`_build_create_ddl` PARTITIONED BY 合併、`load()` 注 WHERE、`save()` 補/驗 static partition col。
- `tests/test_io/test_hive_table_dataset.py` — 新增 `partition_filter` 相關 unit tests（沿用現有 mock 風格）。
- `conf/base/catalog.yaml` — 11 張 dataset 表改寫（line 28-115）。

**Create:**
- `scripts/nuke_ml_recsys.py` — 一次性清庫腳本（spark-submit in dev-cluster container）。

**Not changed (明確列出，避免 scope creep):**
- `src/recsys_tfb/core/versioning.py`（hash 算法）
- `src/recsys_tfb/core/catalog.py`（dispatch 是泛型 kwargs，自動接到新欄位）
- 其他 pipeline node 程式碼（讀 / 寫透過 catalog 抽象，不直接碰 table 名）
- inference 端 `score_table` / `validated_predictions` catalog 條目
- `data/dataset/${base_dataset_version}/*.json`、`data/recsys_cache/...`、`data/models/...` 路徑

---

## Task 1: `HiveTableDataset.__init__` 接受 `partition_filter` + 基本驗證

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py`
- Test: `tests/test_io/test_hive_table_dataset.py`

`partition_filter` 是 `dict[str, str]`，宣告 static partition columns 與其常數值。Validation 規則：
- 預設 `None` → 維持目前行為。
- 若提供，必須是 `dict[str, str]`，所有 value 為非空字串。
- `partition_filter` keys 不可與 `columns` 或 `partition_cols` 中的 name 重疊。
- `read_only=True` 時允許設 `partition_filter`（讀取時要注 WHERE）。

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_io/test_hive_table_dataset.py` 的 `TestValidation` class（位置：line 27 開始）：

```python
    def test_partition_filter_overlaps_columns(self):
        with pytest.raises(ValueError, match="partition_filter.*overlap"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[
                    {"name": "a", "type": "STRING"},
                    {"name": "ver", "type": "STRING"},
                ],
                partition_filter={"ver": "abc12345"},
                external=False,
            )

    def test_partition_filter_overlaps_partition_cols(self):
        with pytest.raises(ValueError, match="partition_filter.*overlap"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_cols=[{"name": "ver", "type": "STRING"}],
                partition_filter={"ver": "abc12345"},
                external=False,
            )

    def test_partition_filter_value_must_be_non_empty_string(self):
        with pytest.raises(ValueError, match="partition_filter.*value"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_filter={"ver": ""},
                external=False,
            )
        with pytest.raises(ValueError, match="partition_filter.*value"):
            HiveTableDataset(
                database="db",
                table="t",
                columns=[{"name": "a", "type": "STRING"}],
                partition_filter={"ver": 123},
                external=False,
            )

    def test_partition_filter_allowed_on_read_only(self):
        ds = HiveTableDataset(
            database="db",
            table="t",
            partition_filter={"ver": "abc"},
            read_only=True,
        )
        assert ds._partition_filter == {"ver": "abc"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestValidation -v`
Expected: 4 new tests FAIL with `TypeError: __init__() got an unexpected keyword argument 'partition_filter'` 或 AttributeError on `_partition_filter`。

- [ ] **Step 3: Add `partition_filter` to constructor + validation**

Edit `src/recsys_tfb/io/hive_table_dataset.py`:

在 `__init__` 簽章加 `partition_filter: dict | None = None`（放在 `partition_cols` 之後、`external` 之前）：

```python
    def __init__(
        self,
        database: str,
        table: str,
        columns: list[dict] | str | None = None,
        partition_cols: list[dict] | None = None,
        partition_filter: dict | None = None,
        external: bool = True,
        location: str | None = None,
        stored_as: str = "PARQUET",
        write_mode: str = "overwrite",
        table_properties: dict | None = None,
        read_only: bool = False,
    ):
        self._database = database
        self._table = table
        self._infer_columns = columns == "auto"
        self._columns: list[dict] = [] if self._infer_columns else (columns or [])
        self._partition_cols = partition_cols or []
        self._partition_filter = dict(partition_filter or {})
        self._external = external
        self._location = location
        self._stored_as = stored_as
        self._write_mode = write_mode
        self._table_properties = table_properties or {}
        self._read_only = read_only

        self._validate()
```

在 `_validate()` 末尾（line 99 之後）加：

```python
        if self._partition_filter:
            for k, v in self._partition_filter.items():
                if not isinstance(v, str) or not v:
                    raise ValueError(
                        f"partition_filter value for '{k}' must be a non-empty "
                        f"string for Hive table '{self._database}.{self._table}', "
                        f"got {v!r}"
                    )
            filter_names = set(self._partition_filter.keys())
            existing = col_names | part_names
            overlap_filter = filter_names & existing
            if overlap_filter:
                raise ValueError(
                    f"partition_filter keys overlap with columns/partition_cols "
                    f"on {sorted(overlap_filter)} for Hive table "
                    f"'{self._database}.{self._table}'"
                )
```

注意：`col_names` 與 `part_names` 在 `_validate()` 內已有（line 92-93），但只在 non-read-only path 之後出現。為了讓 read-only 也能驗 overlap，把 `col_names` / `part_names` 計算移到 `_validate()` 開頭、`_read_only` early-return 之前：

```python
    def _validate(self) -> None:
        if self._write_mode not in _VALID_WRITE_MODES:
            raise ValueError(
                f"write_mode must be one of {_VALID_WRITE_MODES}, "
                f"got '{self._write_mode}'"
            )

        col_names = {c["name"] for c in self._columns}
        part_names = {c["name"] for c in self._partition_cols}

        if self._partition_filter:
            for k, v in self._partition_filter.items():
                if not isinstance(v, str) or not v:
                    raise ValueError(
                        f"partition_filter value for '{k}' must be a non-empty "
                        f"string for Hive table '{self._database}.{self._table}', "
                        f"got {v!r}"
                    )
            filter_names = set(self._partition_filter.keys())
            overlap_filter = filter_names & (col_names | part_names)
            if overlap_filter:
                raise ValueError(
                    f"partition_filter keys overlap with columns/partition_cols "
                    f"on {sorted(overlap_filter)} for Hive table "
                    f"'{self._database}.{self._table}'"
                )

        if self._read_only:
            return

        if not self._columns and not self._infer_columns:
            raise ValueError(
                f"columns is required for writable Hive table "
                f"'{self._database}.{self._table}' (use 'auto' to infer from DataFrame)"
            )

        if self._external and not self._location:
            raise ValueError(
                f"external=True requires 'location' for Hive table "
                f"'{self._database}.{self._table}'"
            )

        if not self._external and self._location:
            logger.warning(
                "Managed Hive table '%s.%s' has explicit location '%s'; "
                "managed tables normally use the Hive warehouse directory.",
                self._database,
                self._table,
                self._location,
            )

        overlap = col_names & part_names
        if overlap:
            raise ValueError(
                f"columns and partition_cols overlap on {sorted(overlap)} "
                f"for Hive table '{self._database}.{self._table}'"
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestValidation -v`
Expected: 全部 PASS（含原有的 6 個 + 新增 4 個 = 10 個）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git commit -m "feat(hive): accept partition_filter on HiveTableDataset (validation only)"
```

---

## Task 2: `_build_create_ddl` 把 `partition_filter` 欄位放進 PARTITIONED BY (outer)

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py` (`_build_create_ddl`)
- Test: `tests/test_io/test_hive_table_dataset.py`

CREATE TABLE 的 PARTITIONED BY 子句把 `partition_filter` 的 keys 放在最外層、`partition_cols` 跟在後面。`partition_filter` columns 預設 STRING type。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_io/test_hive_table_dataset.py` 加新的 class（接在 `TestDDLColumnComment` 之後，約 line 181）：

```python
class TestDDLPartitionFilter:
    def test_filter_only_no_dynamic_partition(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_keys",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "snap_date", "type": "STRING"},
            ],
            partition_filter={"base_dataset_version": "abc12345"},
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert "PARTITIONED BY (base_dataset_version STRING)" in ddl

    def test_filter_outer_dynamic_inner(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_model_input",
            columns=[{"name": "cust_id", "type": "STRING"}],
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert (
            "PARTITIONED BY (base_dataset_version STRING, snap_date STRING)"
            in ddl
        )

    def test_filter_multiple_keys_preserve_order(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            columns=[{"name": "cust_id", "type": "STRING"}],
            partition_filter={
                "base_dataset_version": "abc12345",
                "train_variant_id": "def67890",
            },
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        ddl = ds._build_create_ddl()
        assert (
            "PARTITIONED BY (base_dataset_version STRING, "
            "train_variant_id STRING, snap_date STRING)"
        ) in ddl
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestDDLPartitionFilter -v`
Expected: 3 個 test FAIL（DDL 沒有包含 `base_dataset_version`）。

- [ ] **Step 3: Update `_build_create_ddl`**

在 `_build_create_ddl()` 修改 PARTITIONED BY 區塊（原 line 185-189）：

```python
        all_part_cols = [
            {"name": k, "type": "STRING"} for k in self._partition_filter.keys()
        ] + list(self._partition_cols)
        if all_part_cols:
            part_defs = ", ".join(
                _format_col(c) for c in all_part_cols
            )
            parts.append(f"PARTITIONED BY ({part_defs})")
```

`if self._partition_cols:` 改為新邏輯（用 `all_part_cols`），確保 `partition_filter` 單獨存在時也會產出 PARTITIONED BY。

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestDDLPartitionFilter tests/test_io/test_hive_table_dataset.py::TestDDLExternalPartitioned tests/test_io/test_hive_table_dataset.py::TestDDLManagedNonPartitioned -v`
Expected: 全部 PASS（新 3 + 既有 5）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git commit -m "feat(hive): include partition_filter cols in CREATE TABLE PARTITIONED BY"
```

---

## Task 3: `load()` 注入 `WHERE` 子句

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py` (`load`)
- Test: `tests/test_io/test_hive_table_dataset.py`

當 `partition_filter` 存在時，`load()` 走 `spark.sql("SELECT * FROM db.t WHERE k1='v1' AND k2='v2'")`，partition column 自然保留在 result DataFrame；否則維持原本 `spark.table()`。Filter value 的單引號做 escape (`'` → `''`)。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_io/test_hive_table_dataset.py` 加新的 class（接在 `TestExists` 之後）：

```python
class TestLoadWithPartitionFilter:
    def test_load_without_filter_uses_spark_table(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="feature_table",
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.table.assert_called_once_with("ml_recsys.feature_table")
        spark.sql.assert_not_called()

    def test_load_single_filter_injects_where(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="val_model_input",
            partition_filter={"base_dataset_version": "abc12345"},
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.val_model_input "
            "WHERE base_dataset_version = 'abc12345'"
        )
        spark.table.assert_not_called()

    def test_load_multi_filter_joins_with_and(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="train_model_input",
            partition_filter={
                "base_dataset_version": "abc12345",
                "train_variant_id": "def67890",
            },
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.train_model_input "
            "WHERE base_dataset_version = 'abc12345' "
            "AND train_variant_id = 'def67890'"
        )

    def test_load_escapes_single_quote_in_value(self):
        ds = HiveTableDataset(
            database="ml_recsys",
            table="t",
            partition_filter={"k": "ab'cd"},
            read_only=True,
        )
        spark = _make_spark_mock()
        with _patch_spark(spark):
            ds.load()
        spark.sql.assert_called_once_with(
            "SELECT * FROM ml_recsys.t WHERE k = 'ab''cd'"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestLoadWithPartitionFilter -v`
Expected: 3 個帶 filter 的 test FAIL（load 還在用 spark.table）。第 1 個應 PASS（保留 spark.table 行為）。

- [ ] **Step 3: Update `load()`**

替換 `load()` 方法（原 line 103-105）：

```python
    def load(self):
        spark = self._get_spark()
        if not self._partition_filter:
            return spark.table(self._qualified_name)
        where = " AND ".join(
            f"{k} = '{self._escape_sql_value(v)}'"
            for k, v in self._partition_filter.items()
        )
        return spark.sql(
            f"SELECT * FROM {self._qualified_name} WHERE {where}"
        )
```

在 class 末尾（`_build_create_ddl` 之後）加 helper：

```python
    @staticmethod
    def _escape_sql_value(v: str) -> str:
        return v.replace("'", "''")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestLoadWithPartitionFilter tests/test_io/test_hive_table_dataset.py::TestReadOnly -v`
Expected: 全部 PASS（新 4 + 既有 2）。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git commit -m "feat(hive): inject WHERE clause on load when partition_filter set"
```

---

## Task 4: `save()` 自動補 / 驗證 static partition column 並寫入

**Files:**
- Modify: `src/recsys_tfb/io/hive_table_dataset.py` (`save`, `_insert_column_order`, `_to_spark`-adjacent helpers)
- Test: `tests/test_io/test_hive_table_dataset.py`

`save()` 行為：
1. Caller 預期不帶 static partition column。若 DataFrame 沒帶，自動 `withColumn(name, lit(value))` 補上；若已帶且值與 `partition_filter` 一致，沿用；若已帶但值不一致，**raise**（防誤寫）。
2. `partition_filter` 視為「extra dynamic partition cols with constant per-row value」，併入既有 `partitionOverwriteMode=dynamic` + `insertInto` 路徑——columns 順序：regular → filter → dynamic。
3. 寫完後的 partition log 也把 filter cols 一併計入。

Implementation notes：
- 偵測 DataFrame 是否含某 column：`name in df.columns`。
- 補 column：`from pyspark.sql.functions import lit; df = df.withColumn(name, lit(value))`。
- 驗證 mismatch：`df.filter(col(name) != value).limit(1).count() > 0`（不能信賴 `df.select(name).distinct().collect()` 在大資料上跑——但這檢查只在「caller 預先帶了 static col」這個邊角才觸發，正常 catalog 用法不會走到，可接受）。

實際上：caller 不應預先帶 static col（pipeline node 不會自己塞 `base_dataset_version`），所以「mismatch raise」這條 path 主要是防呆。為了避免 distinct collect 對大表造成 perf risk，**只在 caller 預先帶了該 column 時才做檢查**——並用 `df.select(name).distinct().limit(2).collect()` 限制掃描成本（看到 >1 distinct value 就 raise）。

- [ ] **Step 1: Write the failing tests**

在 `tests/test_io/test_hive_table_dataset.py` 加新的 class（接在 `TestSaveAppendMode` 之後）：

```python
class TestSaveWithPartitionFilter:
    def _make_ds(self, **kw):
        defaults = dict(
            database="ml_recsys",
            table="val_model_input",
            columns=[
                {"name": "cust_id", "type": "STRING"},
                {"name": "score", "type": "DOUBLE"},
            ],
            partition_filter={"base_dataset_version": "abc12345"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
        defaults.update(kw)
        return HiveTableDataset(**defaults)

    def test_save_adds_static_col_when_missing(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "snap_date"]
        df.withColumn.return_value = df
        df.select.return_value = df
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark), \
             patch("pyspark.sql.functions.lit") as mock_lit:
            mock_lit.return_value = "LIT_abc12345"
            ds.save(df)

        # withColumn called for missing static partition col
        df.withColumn.assert_any_call("base_dataset_version", "LIT_abc12345")

        # Column reorder: regular cols, then static filter col, then dynamic
        df.select.assert_any_call(
            "cust_id", "score", "base_dataset_version", "snap_date"
        )

        # Dynamic mode set
        spark.conf.set.assert_any_call(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )

        # Insert into target table
        writer.insertInto.assert_called_once_with("ml_recsys.val_model_input")

    def test_save_keeps_static_col_when_value_matches(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        # distinct() check returns matching value
        distinct_row = MagicMock()
        distinct_row.__getitem__.return_value = "abc12345"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            distinct_row
        ]
        df.select.return_value.distinct.return_value.collect.return_value = []
        writer = MagicMock()
        df.write.mode.return_value = writer

        with _patch_spark(spark):
            ds.save(df)

        # withColumn NOT called for the static col
        for call in df.withColumn.call_args_list:
            assert call[0][0] != "base_dataset_version"

        writer.insertInto.assert_called_once_with("ml_recsys.val_model_input")

    def test_save_raises_on_static_col_value_mismatch(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        # distinct() returns a different value
        bad_row = MagicMock()
        bad_row.__getitem__.return_value = "XXBADXX"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            bad_row
        ]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="partition_filter.*mismatch"):
            ds.save(df)

    def test_save_raises_on_multiple_static_values(self):
        ds = self._make_ds()
        spark = _make_spark_mock()
        df = MagicMock(name="DataFrame")
        df.columns = ["cust_id", "score", "base_dataset_version", "snap_date"]
        df.select.return_value = df
        # distinct() returns 2 distinct values
        r1, r2 = MagicMock(), MagicMock()
        r1.__getitem__.return_value = "abc12345"
        r2.__getitem__.return_value = "OTHERVER"
        df.select.return_value.distinct.return_value.limit.return_value.collect.return_value = [
            r1, r2
        ]

        with _patch_spark(spark), \
             pytest.raises(ValueError, match="partition_filter.*mismatch"):
            ds.save(df)
```

注意：這四個 test 用 mock 重現 spark DataFrame chain，比較囉唆但符合既有 file 風格。

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py::TestSaveWithPartitionFilter -v`
Expected: 4 個 test FAIL（save 還沒處理 static partition）。

- [ ] **Step 3: Update `save()` and `_insert_column_order`**

在 `_insert_column_order()` 改成：

```python
    def _insert_column_order(self) -> list[str]:
        return (
            [c["name"] for c in self._columns]
            + list(self._partition_filter.keys())
            + [c["name"] for c in self._partition_cols]
        )
```

在 `save()` 內，於 `self._ensure_table_exists(spark)` 之後、`if self._partition_cols:` 之前，插入「補 / 驗 static partition col」邏輯：

```python
        if self._partition_filter:
            df = self._apply_partition_filter_cols(df)
```

並在 class 內加 helper（放在 `_to_spark` 之後）：

```python
    def _apply_partition_filter_cols(self, df):
        """Ensure DataFrame has static partition columns with the filter values.

        - Missing column: add via withColumn(lit(value)).
        - Present with matching value: keep as-is.
        - Present with non-matching or multiple distinct values: raise.
        """
        from pyspark.sql.functions import lit

        for k, v in self._partition_filter.items():
            if k not in df.columns:
                df = df.withColumn(k, lit(v))
                continue
            distinct = (
                df.select(k).distinct().limit(2).collect()
            )
            distinct_vals = {row[k] for row in distinct}
            if distinct_vals != {v}:
                raise ValueError(
                    f"partition_filter mismatch for column '{k}' on "
                    f"'{self._qualified_name}': expected {{'{v}'}}, "
                    f"DataFrame has {distinct_vals}"
                )
        return df
```

並在 `save()` 內把 `if self._partition_cols:` 改為涵蓋 `partition_filter` 也算「有 partition」（要設 dynamic mode）：

```python
        if self._partition_cols or self._partition_filter:
            spark.conf.set(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

        df = df.select(*self._insert_column_order())
        df.write.mode(self._write_mode).insertInto(self._qualified_name)

        if (self._partition_cols or self._partition_filter) and self._write_mode == "overwrite":
            part_cols = list(self._partition_filter.keys()) + [
                c["name"] for c in self._partition_cols
            ]
            written = (
                df.select(*part_cols).distinct().collect()
            )
            logger.info(
                "Wrote %d partitions to %s: %s",
                len(written),
                self._qualified_name,
                [{c: row[c] for c in part_cols} for row in written],
            )
```

`_infer_columns_from_spark` 的 `exclude` 也要把 filter keys 排除（避免 static col 被當成「資料欄」放進 main columns block）：

於 `save()` 中找到：

```python
        if self._infer_columns and not self._columns:
            self._columns = _infer_columns_from_spark(
                df, exclude={c["name"] for c in self._partition_cols}
            )
```

改為：

```python
        if self._infer_columns and not self._columns:
            self._columns = _infer_columns_from_spark(
                df,
                exclude={c["name"] for c in self._partition_cols}
                | set(self._partition_filter.keys()),
            )
```

注意：`_infer_columns_from_spark` 是在 `_apply_partition_filter_cols` 之後跑的——因為若 caller 沒帶 static col、且 `columns="auto"`，inference 必須在 lit column 補完之後才不會漏掉欄位。但同時 exclude 集合裡包含 filter keys，所以即使 lit column 已補進 df，也不會被推進 main columns。檢視 `save()` 流程順序：

```python
    def save(self, data) -> None:
        if self._read_only:
            raise RuntimeError(...)

        spark = self._get_spark()
        df = self._to_spark(spark, data)

        if self._partition_filter:
            df = self._apply_partition_filter_cols(df)   # 補 static col

        if self._infer_columns and not self._columns:
            self._columns = _infer_columns_from_spark(
                df,
                exclude={c["name"] for c in self._partition_cols}
                | set(self._partition_filter.keys()),
            )

        self._ensure_table_exists(spark)

        if self._partition_cols or self._partition_filter:
            spark.conf.set(
                "spark.sql.sources.partitionOverwriteMode", "dynamic"
            )

        df = df.select(*self._insert_column_order())
        df.write.mode(self._write_mode).insertInto(self._qualified_name)

        if (self._partition_cols or self._partition_filter) and self._write_mode == "overwrite":
            part_cols = list(self._partition_filter.keys()) + [
                c["name"] for c in self._partition_cols
            ]
            written = df.select(*part_cols).distinct().collect()
            logger.info(
                "Wrote %d partitions to %s: %s",
                len(written),
                self._qualified_name,
                [{c: row[c] for c in part_cols} for row in written],
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_io/test_hive_table_dataset.py -v`
Expected: 全檔 PASS（含原有 test 與新增 test 共約 30 個）。

特別注意 `TestSaveExternalPartitioned::test_save_runs_ddl_sets_dynamic_mode_and_insertInto` 與其他既有 save 測試是否仍 pass——它們沒設 `partition_filter`，行為應與舊版完全一致。

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/hive_table_dataset.py tests/test_io/test_hive_table_dataset.py
git commit -m "feat(hive): save() auto-adds static partition col, raises on mismatch"
```

---

## Task 5: 改寫 `conf/base/catalog.yaml` 的 11 張 dataset 表

**Files:**
- Modify: `conf/base/catalog.yaml` (lines 28-115)

11 張 dataset 表全部去掉 `_${...}` 表名 suffix；改宣告 `partition_filter` + `partition_cols`（snap_date 補齊到所有表）。Source tables 與 inference tables 不動。

- [ ] **Step 1: Write the new catalog content**

替換 `conf/base/catalog.yaml` 的 line 27-115 段（即 `# --- Dataset base layer` 到 `# --- Training Pipeline - Binary` 之前）為：

```yaml
# --- Dataset base layer (per base_dataset_version) ---
# Single Hive table per logical artifact; base_dataset_version is the outer
# partition column. snap_date is the inner dynamic partition.
val_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: val_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
  partition_cols:
    - {name: snap_date, type: STRING}

test_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: test_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
  partition_cols:
    - {name: snap_date, type: STRING}

val_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: val_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
  partition_cols:
    - {name: snap_date, type: STRING}

test_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: test_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
  partition_cols:
    - {name: snap_date, type: STRING}

preprocessor:
  type: JSONDataset
  filepath: data/dataset/${base_dataset_version}/preprocessor.json

category_mappings:
  type: JSONDataset
  filepath: data/dataset/${base_dataset_version}/category_mappings.json

# --- Dataset train-variant layer (per base + train_variant) ---
sample_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: sample_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

train_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

train_dev_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_dev_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

train_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

train_dev_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: train_dev_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    train_variant_id: ${train_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

# --- Dataset calibration-variant layer (per base + calibration_variant) ---
calibration_keys:
  type: HiveTableDataset
  database: ${hive.db}
  table: calibration_keys
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    calibration_variant_id: ${calibration_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}

calibration_model_input:
  type: HiveTableDataset
  database: ${hive.db}
  table: calibration_model_input
  external: false
  columns: "auto"
  partition_filter:
    base_dataset_version: ${base_dataset_version}
    calibration_variant_id: ${calibration_variant_id}
  partition_cols:
    - {name: snap_date, type: STRING}
```

注意保留：`preprocessor` / `category_mappings` 是 JSONDataset（不動）；inference 的 `score_table` / `validated_predictions` 不動；source tables 不動。

- [ ] **Step 2: 用 catalog loader 試 instantiate 一次（smoke test）**

執行 Python REPL 確認 yaml parse + `HiveTableDataset.__init__` 不報錯：

```bash
.venv/bin/python -c "
from recsys_tfb.core.config import ConfigLoader
from recsys_tfb.core.catalog import DataCatalog
loader = ConfigLoader(base_dir='conf/base', env_dir=None)
params = loader.load_parameters()
params.setdefault('base_dataset_version', 'abc12345')
params.setdefault('train_variant_id', 'def67890')
params.setdefault('calibration_variant_id', 'ghi13579')
params.setdefault('hive', {'db': 'ml_recsys'})
catalog_dict = loader.render_catalog(params)
catalog = DataCatalog.from_dict(catalog_dict)
for name in ['val_model_input', 'train_model_input', 'calibration_model_input']:
    ds = catalog._datasets[name]
    print(name, '->', ds._database + '.' + ds._table, ds._partition_filter)
"
```

Expected：印出三個 dataset 的 qualified name（`ml_recsys.val_model_input` 等）與 `partition_filter` dict。如報錯就是 ConfigLoader/DataCatalog API 跟 plan 假設不符——回去讀 `src/recsys_tfb/core/config.py` / `catalog.py` 對齊 API call。

如果 API 不一樣，調整 smoke 命令即可，**不要改 plan task 結構**。

- [ ] **Step 3: 跑既有 test suite 確認無 regression**

Run: `.venv/bin/pytest tests/ -x --ignore=tests/scenarios -q`
Expected: 全部 PASS。如果 dataset pipeline 的 unit/integration test 沒用真 Spark，應該不受 catalog 改動影響；如果踩到某 test hardcode `_${version}` 表名，就回 plan 加新 task fix。

- [ ] **Step 4: Commit**

```bash
git add conf/base/catalog.yaml
git commit -m "refactor(catalog): dataset Hive tables - version as partition column"
```

---

## Task 6: 新增 `scripts/nuke_ml_recsys.py`

**Files:**
- Create: `scripts/nuke_ml_recsys.py`

一次性清庫腳本，給 dev-cluster 重灌使用。風格對齊既有 `scripts/setup_hive_dev.py`（spark-submit in container）。

- [ ] **Step 1: Write the script**

Create `scripts/nuke_ml_recsys.py`:

```python
"""Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.

Run inside dev-cluster spark container via spark-submit, with /workspace mounted
to the host project root. Pair with setup_hive_dev.py to rebuild source tables.

This is intended for dev-cluster only. DO NOT run against production.
"""

from pyspark.sql import SparkSession

DB = "ml_recsys"


def main() -> None:
    spark = (
        SparkSession.builder.appName("nuke_ml_recsys")
        .enableHiveSupport()
        .getOrCreate()
    )

    existed = spark.catalog.databaseExists(DB)
    if existed:
        spark.sql(f"DROP DATABASE {DB} CASCADE")
        print(f"[ok] dropped database: {DB}")
    else:
        print(f"[skip] database does not exist: {DB}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    print(f"[ok] created empty database: {DB}")

    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify script parses**

Run: `.venv/bin/python -c "import ast; ast.parse(open('scripts/nuke_ml_recsys.py').read()); print('ok')"`
Expected: prints `ok`.

- [ ] **Step 3: Commit**

```bash
git add scripts/nuke_ml_recsys.py
git commit -m "feat(scripts): nuke_ml_recsys - drop ml_recsys CASCADE for clean reset"
```

---

## Task 7: Dev-cluster e2e smoke validation（手動，不寫程式）

**Files:** none（這 task 是執行驗證，產出是 console log + Hive metastore 狀態）。

驗證新 partition layout 從零跑得起來。**這個 task 包含與真實 Spark cluster 互動的指令，不是 unit test**——會產生實際 Hive table、寫實際 parquet 到 HDFS。預計時間 10-20 分鐘。

需要先在 host 啟好 dev-cluster（`cd ~/dev-cluster && ./scripts/up.sh` 之類，依 dev-cluster README）。

- [ ] **Step 1: Nuke + 重灌 source tables**

走 admin wrapper（dev-cluster-spark skill SOP-6 / README admin pattern）：

```bash
scripts/dev_admin.sh scripts/nuke_ml_recsys.py
scripts/dev_admin.sh scripts/setup_hive_dev.py
```

Expected: nuke 印出 `[ok] dropped database` + `[ok] created empty database: ml_recsys at hdfs://namenode:9000/...`；setup 印出 `feature_table / label_table / sample_pool` 三張表 row count。整體幾秒到 1 分鐘內。

- [ ] **Step 2: 跑 dataset pipeline**

Host 端：

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb dataset --env production
```

Expected: 跑完無錯。

- [ ] **Step 3: 驗 Hive 結果——表名與 partition**

新增一支臨時驗證腳本 `scripts/_dev_inspect_partitions.py`（不 commit，跑完即可刪）：

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("inspect_partitions")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sql("SHOW TABLES IN ml_recsys").show(50, truncate=False)
for t in ["val_model_input", "train_model_input", "calibration_model_input"]:
    print(f"\n--- partitions of {t} ---")
    spark.sql(f"SHOW PARTITIONS ml_recsys.{t}").show(100, truncate=False)
print("\n--- describe val_model_input ---")
spark.sql("DESCRIBE ml_recsys.val_model_input").show(100, truncate=False)
spark.stop()
```

然後：

```bash
scripts/dev_admin.sh scripts/_dev_inspect_partitions.py
```

Expected:
- `SHOW TABLES IN ml_recsys` 回 14 張（3 source + 11 dataset），無 `*_<hash>` 命名表。
- `SHOW PARTITIONS ml_recsys.val_model_input` 回 `base_dataset_version=<hash>/snap_date=YYYY-MM-DD` 多列。
- `SHOW PARTITIONS ml_recsys.train_model_input` 回 `base_dataset_version=<hash>/train_variant_id=<hash>/snap_date=YYYY-MM-DD`。
- `DESCRIBE` 列出含 partition columns 的完整 schema。

- [ ] **Step 4: 跑下游 pipeline 確認讀得到**

```bash
# training（注意切 SPARK_CONF_DIR 到 local，CLAUDE.md 規定）
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production

# inference / evaluation 切回 distributed
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb inference --env production
.venv/bin/python -m recsys_tfb evaluation --env production
```

Expected: 全部跑完無錯。

- [ ] **Step 5: 跨版本共存驗證**

改一個無關緊要的 dataset parameter（例如 `parameters_dataset.yaml` 內的某個 sampling ratio）使 `base_dataset_version` 變動，再跑一次 dataset pipeline：

```bash
.venv/bin/python -m recsys_tfb dataset --env production
```

然後改 `scripts/_dev_inspect_partitions.py` 為：

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("xver").enableHiveSupport().getOrCreate()
spark.sql("""
    SELECT base_dataset_version, COUNT(*) AS n
    FROM ml_recsys.val_model_input
    GROUP BY base_dataset_version
""").show(truncate=False)
spark.stop()
```

跑：

```bash
scripts/dev_admin.sh scripts/_dev_inspect_partitions.py
```

Expected: 兩列，兩個不同 `base_dataset_version` hash，舊 partition 仍存在。

- [ ] **Step 6: Commit smoke test result（如果有 log/note 想保留）**

如果想紀錄結果到 repo（optional），可寫一份 `docs/superpowers/notes/2026-05-06-dataset-partition-smoke.md` 把 SHOW PARTITIONS 輸出貼進去並 commit。否則略過此 step。

如果 smoke test 中發現任何問題，回到對應 task 修；不要在這個 task 內 hot-fix。

---

## 驗收標準

- 全部 unit tests pass：`.venv/bin/pytest tests/ -q`
- Dev-cluster e2e 跑完 4 條 pipeline（dataset → training → inference → evaluation）無錯。
- `SHOW TABLES IN ml_recsys` 回 14 張，無 `*_<hash>` 命名。
- 跨版本共存驗證：改參數重跑 dataset 後，同表多版本可查。

## 不包含（spec 已列為 Future Work）

- Production (CDP/YARN) cutover plan。
- 跨版本 schema drift 治理。
- inference 端 `score_table` / `validated_predictions` 的 partition column 順序對齊。
