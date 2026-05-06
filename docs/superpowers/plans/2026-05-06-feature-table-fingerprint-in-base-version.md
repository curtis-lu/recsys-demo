# feature_table fingerprint in base_dataset_version Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `feature_table` 的 schema fingerprint（column name + dtype，依 metastore 順序）納入 `base_dataset_version` 的 hash payload，讓 feature_table 擴/縮欄位時自動 bust 版本，避免 cache 路徑被舊輸入污染。

**Architecture:** 在 `core/versioning.py` 新增純函式 `compute_feature_table_fingerprint(columns)`（接受 `[(name, dtype), ...]` 排序保留原順序，回傳 8 字元 hash），擴展 `compute_base_dataset_version` 與 `build_manifest_metadata` 接受 optional fingerprint 參數（default None 維持向後相容）。`__main__.py:dataset()` CLI 在 SparkSession 啟動後從 Hive metastore 讀取 `<hive.db>.feature_table` schema、計算 fingerprint、傳進 hash 計算與 manifest。training/inference resolve 路徑透過 manifest + symlink，**不需改動**。

**Tech Stack:** Python 3.10、PySpark 3.3.2、pytest 7.3.1、Typer 0.20.1。純函式以 unit test 覆蓋；CLI 整合靠既有 `tests/scenarios/test_scenario_3_new_features.py` 把關。

---

## File Structure

| 檔案 | 動作 | 責任 |
|---|---|---|
| `src/recsys_tfb/core/versioning.py` | Modify | 新增 `compute_feature_table_fingerprint`；擴展 `compute_base_dataset_version` 與 `build_manifest_metadata` 接受 optional `feature_table_fingerprint` |
| `src/recsys_tfb/__main__.py` | Modify | `dataset()` CLI：讀 Hive `feature_table` schema、計算 fingerprint、傳給 hash 計算與 manifest、寫 log |
| `tests/test_core/test_versioning.py` | Modify | 新增 `TestComputeFeatureTableFingerprint`；擴充 `TestComputeBaseDatasetVersion` 與 `TestBuildManifestMetadata` 對 fingerprint 的測試 |

---

## Task 1: `compute_feature_table_fingerprint` helper（純函式 + TDD）

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py`（新增函式，緊接在 `_hash8` 之後）
- Test: `tests/test_core/test_versioning.py`（新增 `TestComputeFeatureTableFingerprint` class）

設計選擇：**保留輸入順序**。理由：`preprocessing/_spark.py:_compute_feature_columns` 會依 `feature_table.columns` 原順序產出 `feature_columns`，此順序 LightGBM 訓練/推論時會被使用，欄位順序變動需要視為 schema 變動。

- [ ] **Step 1.1: 寫失敗測試**

在 `tests/test_core/test_versioning.py` 中加 `TestComputeFeatureTableFingerprint` class（建議放在 `TestComputeBaseDatasetVersion` 之前），先 import 新函式：

```python
# 在 from recsys_tfb.core.versioning import (...) 加入：
    compute_feature_table_fingerprint,
```

新增測試 class：

```python
class TestComputeFeatureTableFingerprint:
    def test_returns_8_char_hex(self):
        cols = [("snap_date", "date"), ("cust_id", "string"), ("aum_total", "double")]
        fp = compute_feature_table_fingerprint(cols)
        assert _HEX8_RE.match(fp)

    def test_deterministic(self):
        cols = [("snap_date", "date"), ("cust_id", "string")]
        assert compute_feature_table_fingerprint(cols) == \
            compute_feature_table_fingerprint(cols)

    def test_order_sensitive(self):
        a = [("snap_date", "date"), ("cust_id", "string")]
        b = [("cust_id", "string"), ("snap_date", "date")]
        assert compute_feature_table_fingerprint(a) != \
            compute_feature_table_fingerprint(b)

    def test_dtype_sensitive(self):
        a = [("aum_total", "double")]
        b = [("aum_total", "float")]
        assert compute_feature_table_fingerprint(a) != \
            compute_feature_table_fingerprint(b)

    def test_added_column_changes_fingerprint(self):
        base = [("snap_date", "date"), ("cust_id", "string")]
        extended = base + [("new_feat", "double")]
        assert compute_feature_table_fingerprint(base) != \
            compute_feature_table_fingerprint(extended)

    def test_empty_columns_returns_hex(self):
        assert _HEX8_RE.match(compute_feature_table_fingerprint([]))

    def test_accepts_iterable(self):
        # tuple of tuples 應該與 list of tuples 等價
        cols_list = [("snap_date", "date"), ("cust_id", "string")]
        cols_tuple = (("snap_date", "date"), ("cust_id", "string"))
        assert compute_feature_table_fingerprint(cols_list) == \
            compute_feature_table_fingerprint(cols_tuple)
```

- [ ] **Step 1.2: 跑測試確認 fail**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestComputeFeatureTableFingerprint -v
```
Expected: ImportError 或 FAILED — `cannot import name 'compute_feature_table_fingerprint'`。

- [ ] **Step 1.3: 實作函式**

編輯 `src/recsys_tfb/core/versioning.py`，在 `_hash8` 函式之後加：

```python
def compute_feature_table_fingerprint(
    columns: "Iterable[tuple[str, str]]",
) -> str:
    """Hash an ordered (name, dtype) sequence describing feature_table schema.

    Order matters: feature_table column order propagates into ``feature_columns``
    in :mod:`recsys_tfb.preprocessing`, which determines the LightGBM feature
    ordering. Reordering columns changes downstream model inputs, so it must
    bust the version.
    """
    payload = {"feature_table_columns": [list(item) for item in columns]}
    return _hash8(payload)
```

並在檔案頂端 import 區塊加：
```python
from typing import Iterable
```
（若已存在則略過。本檔案目前沒有用 typing.Iterable。）

- [ ] **Step 1.4: 跑測試確認通過**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestComputeFeatureTableFingerprint -v
```
Expected: 7 passed。

- [ ] **Step 1.5: Commit**

```bash
git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "$(cat <<'EOF'
feat(versioning): add compute_feature_table_fingerprint helper

Pure function that hashes an ordered (column_name, dtype) sequence into
an 8-char hex digest. Ordering is preserved because feature_table column
order propagates into the LightGBM feature ordering downstream.
EOF
)"
```

---

## Task 2: 擴展 `compute_base_dataset_version` 接受 optional fingerprint

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py:compute_base_dataset_version`
- Test: `tests/test_core/test_versioning.py:TestComputeBaseDatasetVersion`

設計選擇：**參數 default `None`，向後相容**。`fingerprint=None` 時的 hash 與舊版完全一致（payload 不含該 key），既有 caller 與測試不需改動。

- [ ] **Step 2.1: 寫失敗測試**

在 `tests/test_core/test_versioning.py` 的 `TestComputeBaseDatasetVersion` class 內，於最後一個 method 之後加入 4 個新測試：

```python
    def test_fingerprint_default_none_matches_legacy(self):
        # fingerprint=None 必須與不傳該參數時 hash 完全一致（向後相容）
        legacy = compute_base_dataset_version(_base_params(), _sample_schema())
        with_none = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint=None
        )
        assert legacy == with_none

    def test_different_fingerprints_yield_different_hashes(self):
        a = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="aaaaaaaa"
        )
        b = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="bbbbbbbb"
        )
        assert a != b

    def test_same_fingerprint_yields_same_hash(self):
        a = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        b = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        assert a == b

    def test_fingerprint_set_differs_from_unset(self):
        # 一旦 caller 開始傳 fingerprint，hash 應該與「沒傳」分流
        legacy = compute_base_dataset_version(_base_params(), _sample_schema())
        with_fp = compute_base_dataset_version(
            _base_params(), _sample_schema(), feature_table_fingerprint="cafeb0ba"
        )
        assert legacy != with_fp
```

- [ ] **Step 2.2: 跑測試確認 fail**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestComputeBaseDatasetVersion -v
```
Expected: 4 個新測試 FAIL — `compute_base_dataset_version() got an unexpected keyword argument 'feature_table_fingerprint'`。

- [ ] **Step 2.3: 實作擴展**

編輯 `src/recsys_tfb/core/versioning.py:compute_base_dataset_version`，把整個函式換成：

```python
def compute_base_dataset_version(
    params: dict,
    schema: dict,
    feature_table_fingerprint: str | None = None,
) -> str:
    """Hash non-sampling dataset params, canonical schema, and feature_table
    fingerprint.

    The resulting ID keys pipeline outputs that are invariant under sampling
    changes. ``params`` is the ``parameters_dataset`` dict; any keys in
    ``ALL_SAMPLING_KEYS`` under ``params["dataset"]`` are stripped before
    hashing so train/calibration sampling experiments do not invalidate
    val/test/preprocessor artifacts.

    ``feature_table_fingerprint`` (optional) reflects the actual
    ``feature_table`` schema (column name + dtype, ordered). When provided it
    busts the version on schema changes so the dataset cache cannot collide
    with a different physical input. ``None`` preserves legacy hashing for
    backward compatibility.
    """
    stripped = copy.deepcopy(params)
    ds = stripped.get("dataset")
    if isinstance(ds, dict):
        for key in ALL_SAMPLING_KEYS:
            ds.pop(key, None)
    payload: dict = {"dataset": stripped, "schema": schema}
    if feature_table_fingerprint is not None:
        payload["feature_table_fingerprint"] = feature_table_fingerprint
    return _hash8(payload)
```

- [ ] **Step 2.4: 跑測試確認通過**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestComputeBaseDatasetVersion -v
```
Expected: 全部 PASS（含 4 個新測試 + 8 個舊測試）。

- [ ] **Step 2.5: 跑全 versioning 測試確認沒打到別的東西**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py -v
```
Expected: 全部 PASS。

- [ ] **Step 2.6: Commit**

```bash
git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "$(cat <<'EOF'
feat(versioning): accept optional feature_table_fingerprint in base hash

compute_base_dataset_version now optionally hashes a fingerprint of the
physical feature_table schema. fingerprint=None keeps legacy behavior so
existing callers and tests stay valid.
EOF
)"
```

---

## Task 3: 擴展 `build_manifest_metadata` 寫入 fingerprint

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py:build_manifest_metadata`
- Test: `tests/test_core/test_versioning.py:TestBuildManifestMetadata`

設計選擇：optional 欄位，**只有 caller 傳值時才寫入 manifest**。training/inference 不需要傳，dataset CLI 會傳。

- [ ] **Step 3.1: 寫失敗測試**

在 `tests/test_core/test_versioning.py:TestBuildManifestMetadata` class 內最後加：

```python
    def test_dataset_manifest_records_feature_table_fingerprint(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            base_dataset_version="abc12345",
            feature_table_fingerprint="cafeb0ba",
        )
        assert meta["feature_table_fingerprint"] == "cafeb0ba"

    def test_manifest_omits_fingerprint_when_not_provided(self):
        meta = build_manifest_metadata(
            version="abc12345",
            pipeline="dataset",
            parameters={"sample_ratio": 0.1},
            base_dataset_version="abc12345",
        )
        assert "feature_table_fingerprint" not in meta
```

- [ ] **Step 3.2: 跑測試確認 fail**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestBuildManifestMetadata -v
```
Expected: `test_dataset_manifest_records_feature_table_fingerprint` FAIL —
`build_manifest_metadata() got an unexpected keyword argument`。

- [ ] **Step 3.3: 實作擴展**

編輯 `src/recsys_tfb/core/versioning.py:build_manifest_metadata`，把函式 signature 與 body 分別擴展：

簽章新增一個 keyword-only 參數（按字母順序穿插在 `calibration_variant_id` 之後比較好讀，但為了 minimize diff 直接擺在 `artifacts` 之前）：

```python
def build_manifest_metadata(
    *,
    version: str,
    pipeline: str,
    parameters: dict,
    base_dataset_version: str | None = None,
    train_variant_id: str | None = None,
    calibration_variant_id: str | None = None,
    model_version: str | None = None,
    parent_version: str | None = None,
    variant_kind: str | None = None,
    feature_table_fingerprint: str | None = None,
    artifacts: list[str] | None = None,
) -> dict:
```

並在 body 對應位置（`if variant_kind is not None:` 之後、`if artifacts is not None:` 之前）加：

```python
    if feature_table_fingerprint is not None:
        metadata["feature_table_fingerprint"] = feature_table_fingerprint
```

同時更新 docstring：

```python
    """Build a manifest metadata dict with standard fields.

    ``parent_version`` and ``variant_kind`` are written on variant sub-directory
    manifests to link them back to their base dataset manifest.
    ``feature_table_fingerprint`` is written on dataset base manifests so the
    physical feature_table schema at run time is recoverable from manifest.
    """
```

- [ ] **Step 3.4: 跑測試確認通過**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py::TestBuildManifestMetadata -v
```
Expected: 全部 PASS（含 2 個新測試）。

- [ ] **Step 3.5: Commit**

```bash
git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "$(cat <<'EOF'
feat(versioning): record feature_table_fingerprint in dataset manifest

Optional manifest field; written only when caller provides it. Lets us
recover the physical feature_table schema for any base_dataset_version
without re-querying Hive.
EOF
)"
```

---

## Task 4: `__main__.py:dataset()` 計算並傳遞 fingerprint

**Files:**
- Modify: `src/recsys_tfb/__main__.py:dataset()`（行 271-355 範圍）

設計選擇：
- 從已 init 好的 SparkSession 直接 `spark.table("<db>.feature_table").schema` 拿欄位 → metastore 查詢，不掃資料，cost 微乎其微。
- 用 `params.get("hive", {}).get("db", "ml_recsys")` 取得 Hive db，與其他 catalog 模板一致。
- fingerprint 同時：(a) 餵進 `compute_base_dataset_version`、(b) 寫進 base 層 manifest。
- 不寫進 train_variant / calibration_variant manifest（那兩層的 hash 不依賴 feature_table）。

無 unit test：本任務是 Spark 整合層，靠 Task 5 的 scenario 整合測試把關。

- [ ] **Step 4.1: 修改 import**

編輯 `src/recsys_tfb/__main__.py` 頂端 import：

```python
from recsys_tfb.core.versioning import (
    build_manifest_metadata,
    compute_base_dataset_version,
    compute_calibration_variant_id,
    compute_feature_table_fingerprint,
    compute_model_version,
    compute_train_variant_id,
    read_manifest,
    resolve_base_dataset_version,
    resolve_model_version,
    resolve_variant_id,
    update_symlink,
    write_manifest,
)
```

- [ ] **Step 4.2: 在 `dataset()` 內計算 fingerprint 並傳遞**

找到 `__main__.py:dataset()` 函式（約行 271 起）。把目前這段：

```python
    schema_hash = get_schema_for_hash(params)
    base_v = compute_base_dataset_version(params_dataset, schema_hash)
    train_v = compute_train_variant_id(params_dataset)
    cal_v = (
        compute_calibration_variant_id(params_dataset) if enable_calibration else None
    )

    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)
```

改為：

```python
    from recsys_tfb.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session()
    hive_db = params.get("hive", {}).get("db", "ml_recsys")
    feature_table_fqn = f"{hive_db}.feature_table"
    feature_table_columns = [
        (f.name, f.dataType.simpleString())
        for f in spark.table(feature_table_fqn).schema.fields
    ]
    feature_table_fp = compute_feature_table_fingerprint(feature_table_columns)

    schema_hash = get_schema_for_hash(params)
    base_v = compute_base_dataset_version(
        params_dataset, schema_hash, feature_table_fingerprint=feature_table_fp,
    )
    train_v = compute_train_variant_id(params_dataset)
    cal_v = (
        compute_calibration_variant_id(params_dataset) if enable_calibration else None
    )

    logger.info("feature_table_fingerprint: %s (%d cols)",
                feature_table_fp, len(feature_table_columns))
    logger.info("base_dataset_version: %s", base_v)
    logger.info("train_variant_id:     %s", train_v)
    if cal_v is not None:
        logger.info("calibration_variant_id: %s", cal_v)
```

注意：`get_or_create_spark_session(...)` 已在 `dataset()` 函式更上方被呼叫過（行 279），這裡無參數的 `get_or_create_spark_session()` 會直接拿到既存的 SparkSession，不會重 init。本檔案頂端已存在類似 pattern（line 276）。

- [ ] **Step 4.3: 把 fingerprint 寫進 base manifest**

往下找 `_write_pipeline_manifest(version_dir=base_dir, ...)` 那段（約行 318）。把 `metadata_kwargs` dict 從：

```python
        metadata_kwargs={
            "version": base_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "base_dataset_version": base_v,
            "artifacts": _dir_artifacts(base_dir),
        },
```

改為：

```python
        metadata_kwargs={
            "version": base_v,
            "pipeline": "dataset",
            "parameters": params_dataset,
            "base_dataset_version": base_v,
            "feature_table_fingerprint": feature_table_fp,
            "artifacts": _dir_artifacts(base_dir),
        },
```

train_variant / calibration_variant 的 `metadata_kwargs` **不要動**（fingerprint 屬於 base 層概念）。

- [ ] **Step 4.4: 確認 import 與 syntax 正確**

Run:
```bash
.venv/bin/python -c "from recsys_tfb.__main__ import app; print('ok')"
```
Expected: `ok`，無 ImportError、無 SyntaxError。

- [ ] **Step 4.5: Commit**

```bash
git add src/recsys_tfb/__main__.py
git commit -m "$(cat <<'EOF'
feat(cli): hash feature_table schema into base_dataset_version

dataset CLI reads feature_table column list from Hive metastore at run
time, computes an 8-char fingerprint, and feeds it into both the base
version hash and the dataset base manifest. Adding/removing/reordering
feature_table columns now busts base_dataset_version automatically,
preventing cache directories from being shared across mismatched inputs.
EOF
)"
```

---

## Task 5: 端到端驗證（scenario 3 + dev-cluster smoke）

**Files:** 無修改，純驗證。

- [ ] **Step 5.1: 跑全部 versioning 與 schema 單元測試**

Run:
```bash
.venv/bin/pytest tests/test_core/test_versioning.py tests/test_core/test_schema.py tests/test_core/test_schema_validation.py -v
```
Expected: 全部 PASS。

- [ ] **Step 5.2: 跑 scenario 3（新增 feature 欄位的整合驗證）**

Run:
```bash
.venv/bin/pytest tests/scenarios/test_scenario_3_new_features.py -v
```
Expected: 5 個測試全部 PASS。這驗證 Task 4 改動沒打壞既有 dataset → training → inference 流程，且新欄位仍會流到 model_input 與 scoring_dataset。

- [ ] **Step 5.3: 跑全部 scenarios（向後相容檢查）**

Run:
```bash
.venv/bin/pytest tests/scenarios/ -v
```
Expected: 全部 PASS（4 個 scenario）。

- [ ] **Step 5.4: dev-cluster reset + 第一次 dataset run + 讀回 Hive 驗證**

需要 dev-cluster 已起（`docker compose ps` 看 `devcluster-*` 都 `running`）。先 reset 確保乾淨初始狀態：

```bash
scripts/dev_admin.sh scripts/nuke_ml_recsys.py
scripts/dev_admin.sh scripts/setup_hive_dev.py
```

寫一個一次性 Hive 讀回驗證腳本（驗證後會刪除）：

```bash
cat > scripts/_verify_dataset_partition.py <<'EOF'
"""一次性驗證 script：讀回 Hive train_model_input 的特定 base_dataset_version partition。

Usage: scripts/dev_admin.sh scripts/_verify_dataset_partition.py <base_v>
"""
import sys
from pyspark.sql import SparkSession

base_v = sys.argv[1]
spark = SparkSession.builder.getOrCreate()
df = spark.sql(
    f"SELECT * FROM ml_recsys.train_model_input "
    f"WHERE base_dataset_version='{base_v}'"
)
n_rows = df.count()
n_cols = len(df.columns)
print(f"[verify] base_v={base_v} row_count={n_rows} col_count={n_cols}")
print(f"[verify] first 5 cols: {df.columns[:5]}")
print(f"[verify] last 5 cols: {df.columns[-5:]}")
assert n_rows > 0, f"FAIL: partition base_v={base_v} has 0 rows"
print(f"[verify] OK")
EOF
```

跑第一次 dataset 並擷取 `base_v`：

```bash
source ~/dev-cluster/scripts/client-env.sh
.venv/bin/python -m recsys_tfb dataset --env production 2>&1 | tee /tmp/run1.log
BASE_V_1=$(grep -E "^[^|]*base_dataset_version: [0-9a-f]{8}$" /tmp/run1.log | head -1 | awk '{print $NF}')
FP_1=$(grep -E "feature_table_fingerprint: [0-9a-f]{8}" /tmp/run1.log | head -1 | awk '{print $(NF-1)}')
echo "Run 1: base_v=$BASE_V_1 fingerprint=$FP_1"
```

驗證 manifest 寫入：

```bash
ls "data/dataset/$BASE_V_1/manifest.json"
.venv/bin/python -c "
import json
m = json.load(open('data/dataset/$BASE_V_1/manifest.json'))
assert m['feature_table_fingerprint'] == '$FP_1', f'manifest fp mismatch: {m[\"feature_table_fingerprint\"]} vs $FP_1'
print('[verify] manifest fingerprint =', m['feature_table_fingerprint'])
"
```

讀回 Hive partition：

```bash
scripts/dev_admin.sh scripts/_verify_dataset_partition.py "$BASE_V_1"
```

Expected:
- log 印出 `feature_table_fingerprint: <8 hex>` 與 `base_dataset_version: <8 hex>`
- `data/dataset/$BASE_V_1/manifest.json` 存在，`feature_table_fingerprint` 欄位值等於 log 印的 fp
- `_verify_dataset_partition.py` 印 `[verify] base_v=$BASE_V_1 row_count=N col_count=M` 且 `n_rows > 0`、`[verify] OK`

- [ ] **Step 5.5: ALTER + 第二次 run，驗證 fingerprint 變動真的產生新 partition**

模擬欄位擴增。寫一個 ALTER + 灌新欄位資料的腳本：

```bash
cat > scripts/_alter_add_test_col.py <<'EOF'
"""一次性測試 script：對 ml_recsys.feature_table 加一個 test_new_feat DOUBLE 欄位，
並對所有 partition 重寫一個固定值，避免新欄位整欄 NULL 影響 fit_preprocessor。
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.sql("ALTER TABLE ml_recsys.feature_table ADD COLUMNS (test_new_feat DOUBLE)")
spark.sql("DESCRIBE ml_recsys.feature_table").show(50, False)

# 用 INSERT OVERWRITE 重寫資料把 test_new_feat 灌成 random double
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df = spark.table("ml_recsys.feature_table").withColumn(
    "test_new_feat", F.rand(seed=42)
)
df.write.mode("overwrite").insertInto("ml_recsys.feature_table")
print("[alter] ALTER + backfill done")
EOF
scripts/dev_admin.sh scripts/_alter_add_test_col.py
```

跑第二次 dataset：

```bash
.venv/bin/python -m recsys_tfb dataset --env production 2>&1 | tee /tmp/run2.log
BASE_V_2=$(grep -E "^[^|]*base_dataset_version: [0-9a-f]{8}$" /tmp/run2.log | head -1 | awk '{print $NF}')
FP_2=$(grep -E "feature_table_fingerprint: [0-9a-f]{8}" /tmp/run2.log | head -1 | awk '{print $(NF-1)}')
echo "Run 2: base_v=$BASE_V_2 fingerprint=$FP_2"
```

關鍵斷言：兩次 run 的 fingerprint 與 base_v 都必須變動：

```bash
test "$FP_1" != "$FP_2" || { echo "FAIL: fingerprint unchanged"; exit 1; }
test "$BASE_V_1" != "$BASE_V_2" || { echo "FAIL: base_v unchanged"; exit 1; }
echo "[verify] base_v changed: $BASE_V_1 -> $BASE_V_2"
```

兩個 partition 必須並存且 schema 寬度差 1：

```bash
ls "data/dataset/$BASE_V_1/manifest.json" "data/dataset/$BASE_V_2/manifest.json"

scripts/dev_admin.sh scripts/_verify_dataset_partition.py "$BASE_V_1"
scripts/dev_admin.sh scripts/_verify_dataset_partition.py "$BASE_V_2"

cat > scripts/_verify_two_partitions.py <<'EOF'
"""驗證兩個 partition 並存、column count 差 1（新欄位 test_new_feat）。"""
import sys
from pyspark.sql import SparkSession

base_v_1, base_v_2 = sys.argv[1], sys.argv[2]
spark = SparkSession.builder.getOrCreate()
df1 = spark.sql(
    f"SELECT * FROM ml_recsys.train_model_input WHERE base_dataset_version='{base_v_1}'"
)
df2 = spark.sql(
    f"SELECT * FROM ml_recsys.train_model_input WHERE base_dataset_version='{base_v_2}'"
)
c1, c2 = len(df1.columns), len(df2.columns)
print(f"[verify] partition {base_v_1}: {c1} cols, {df1.count()} rows")
print(f"[verify] partition {base_v_2}: {c2} cols, {df2.count()} rows")
assert c2 == c1 + 1, f"FAIL: expected col diff=1, got {c2}-{c1}={c2-c1}"
new_cols = set(df2.columns) - set(df1.columns)
assert new_cols == {"test_new_feat"}, f"FAIL: new cols={new_cols}"
print("[verify] OK: two partitions coexist, schema differs by exactly +test_new_feat")
EOF
scripts/dev_admin.sh scripts/_verify_two_partitions.py "$BASE_V_1" "$BASE_V_2"
```

Expected:
- `BASE_V_1 != BASE_V_2`、`FP_1 != FP_2`
- 兩個 manifest 檔都存在，互不覆蓋
- `_verify_dataset_partition.py` 對兩個 base_v 分別都成功讀回（`[verify] OK`）
- `_verify_two_partitions.py` 印 `[verify] OK: two partitions coexist, schema differs by exactly +test_new_feat`

- [ ] **Step 5.6: 清理驗證 artifact**

刪除一次性 script，並 reset Hive 還原（Hive 不直接支援 `DROP COLUMN`，所以走 nuke + setup）：

```bash
rm scripts/_verify_dataset_partition.py scripts/_alter_add_test_col.py scripts/_verify_two_partitions.py
rm /tmp/run1.log /tmp/run2.log
scripts/dev_admin.sh scripts/nuke_ml_recsys.py
scripts/dev_admin.sh scripts/setup_hive_dev.py
```

Expected: 三個 script 已刪、兩個 log 已刪、`ml_recsys` 重建成 setup_hive_dev 的乾淨狀態（不留 `test_new_feat` 欄位）。

- [ ] **Step 5.7: 更新 CLAUDE.md / 相關 docs（若有）**

檢查 `CLAUDE.md`、`README.md`、`docs/` 有沒有提到 `base_dataset_version` 計算邏輯：

```bash
grep -rn "base_dataset_version" CLAUDE.md README.md docs/ 2>/dev/null | grep -v "plans/"
```

若有提到「`base_dataset_version` 從 yaml 計算」之類說法，補充 fingerprint 改動。沒有就跳過此 step。

- [ ] **Step 5.8: 重建 graphify 索引**

Run:
```bash
.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

Expected: 重建完成，無 error。

- [ ] **Step 5.9: 最終 commit（如有 docs 改動）+ summary**

如果 Step 5.7 有改 docs：

```bash
git add -A docs/ CLAUDE.md README.md  # 視實際改了哪些
git commit -m "docs: note feature_table_fingerprint in base_dataset_version"
```

否則直接：

```bash
git log --oneline -10
```

確認 4 個 commit（Task 1-4）按順序在 main 之上。

---

## Self-Review

**Spec coverage:**
- ✅ `compute_feature_table_fingerprint` 純函式 → Task 1
- ✅ `compute_base_dataset_version` 接受 fingerprint → Task 2
- ✅ Manifest 寫入 fingerprint → Task 3
- ✅ CLI 計算與傳遞 → Task 4
- ✅ 向後相容（fingerprint=None 行為等同舊版）→ Task 2 step 2.1 第一個測試
- ✅ 端到端驗證 → Task 5
- ✅ training / inference resolve 不需改 → Task 4 設計選擇明示
- ✅ **Hive 寫入正確性**（兩次 run 寫到兩個 partition、都讀得回來、schema 差 1 欄）→ Task 5 step 5.4-5.5 用一次性 verify script 透過 `dev_admin.sh` 從 Hive `SELECT` 回來斷言（手動但有腳本斷言把關）
- ✅ **驗證 artifact 清理**（一次性 script 與 log 不留在 repo / tmp）→ Task 5 step 5.6

**Placeholder scan:** 無 TODO / TBD / "implement later" / "similar to" — 所有 code block 完整可貼。

**Type consistency:**
- `compute_feature_table_fingerprint(columns: Iterable[tuple[str, str]]) -> str` — Task 1 定義，Task 4 用 `[(f.name, f.dataType.simpleString()) for f in ...]`（list of tuples，符合 Iterable[tuple[str, str]]）。
- `compute_base_dataset_version` 第三個參數 keyword-only `feature_table_fingerprint: str | None = None` — Task 2 定義，Task 4 用 keyword 形式 `feature_table_fingerprint=feature_table_fp` 傳遞。
- `build_manifest_metadata` keyword-only `feature_table_fingerprint: str | None = None` — Task 3 定義，Task 4 用 dict key `"feature_table_fingerprint"` 傳給 `metadata_kwargs`（`_write_pipeline_manifest` 把該 dict 解開傳給 `build_manifest_metadata`）。

無命名不一致。
