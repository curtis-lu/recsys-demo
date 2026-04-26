# 集中管理 SparkSession 建立流程

**日期**：2026-04-26
**狀態**：Design — pending implementation

## 背景與動機

目前 `SparkSession.builder.getOrCreate()` 散落於六處：
- `src/recsys_tfb/io/hive_table_dataset.py:156`
- `src/recsys_tfb/io/parquet_dataset.py:33,57`
- `src/recsys_tfb/pipelines/source_etl/sql_runner.py:148`
- `tests/conftest.py:15`
- `tests/test_io/test_hive_table_dataset.py:22`（mock 點）
- `scripts/suggest_categorical_cols.py:254`

`src/recsys_tfb/utils/spark.py` 已提供 `get_or_create_spark_session()`，但只是 thin wrapper，未被任何 production code 使用，也不支援 config 來自於 YAML。

帶來的問題：
1. **無法依 pipeline 設定不同 Spark config**（例如 training 需要 16g executor.memory，inference 需要 24g + 較高的 shuffle.partitions）。
2. **共通 config（serializer、timezone、Hive metastore URI）重複散落**。
3. **環境差異（local vs production）難以集中管理**。

## 目標

- 建立 SparkSession 的**唯一進入點**：`get_or_create_spark_session()`。
- **共通 config** 集中於 `conf/<env>/parameters.yaml` 的 `spark:` 區塊。
- **Pipeline-specific override** 放在 `conf/<env>/parameters_<pipeline>.yaml` 的 `spark:` 區塊，透過既有 ConfigLoader 的 `_deep_merge` 自動合併。
- IO / SQLRunner / scripts 在無 active session 時自動 fallback 讀預設 config。
- 不破壞既有 Ploomber DAG / Typer CLI 結構。

## 非目標

- 不引入新的 Spark config 抽象層（profile / preset 等）。
- 不重構 ConfigLoader 既有 deep merge 行為。
- 不管理 SparkSession lifecycle（stop / restart）；沿用 PySpark 既有「process 結束自動釋放」。

## 架構與職責邊界

### 唯一進入點

`src/recsys_tfb/utils/spark.py` 提供：

```python
def get_or_create_spark_session(
    spark_configs: dict | None = None,
) -> SparkSession:
    """
    建立或取得 SparkSession。

    呼叫情境：
    1) Pipeline 入口傳入 spark_configs（已 deep-merge 後的 params["spark"]）
       - 若已有 active session：套用 runtime config 並回傳（cluster-level config 會被
         PySpark 忽略，是 framework 既有限制）；發出 warning。
       - 若無 active session：用 spark_configs 建立。
    2) IO / SQLRunner / scripts 不傳 spark_configs
       - 若已有 active session：直接回傳。
       - 若無：自動 fallback 讀 conf/<env>/parameters.yaml 的 spark: 區塊建立。
    """
```

### 職責分工

| 層 | 行為 |
|---|---|
| Pipeline command（CLI sub-command 函式）| Load params → 呼叫 `get_or_create_spark_session(params.get("spark", {}))` → 進 `create_pipeline()` |
| IO 層（`HiveTableDataset`、`ParquetDataset`）| 改為 `get_or_create_spark_session()`（無參數） |
| `SQLRunner` | 同上 |
| `tests/conftest.py` fixture | `get_or_create_spark_session(test_minimal_config)` |
| `scripts/suggest_categorical_cols.py` | `get_or_create_spark_session()`（吃 conf/local fallback）|

### 關鍵不變式

- Pipeline-specific spark config **只可能透過 pipeline command 進入系統**。IO 層拿不到 pipeline context，也不該拿到。
- 同一個 process 內只會有一個 SparkSession（PySpark 既有保證）。
- Fallback 路徑只會在獨立執行 IO / scripts 的情境下觸發。

## Config Schema

### Base 共通項

`conf/base/parameters.yaml` 新增：

```yaml
spark:
  app_name: recsys_tfb                                          # pipeline 可覆寫
  spark.serializer: org.apache.spark.serializer.KryoSerializer
  spark.sql.session.timeZone: Asia/Taipei
  spark.sql.catalogImplementation: hive
  spark.hadoop.hive.metastore.uris: thrift://<host>:<port>
  # 保守預設值（IO 獨立執行 / 未覆寫的 pipeline 適用）
  spark.executor.memory: 4g
  spark.executor.cores: 2
```

### Pipeline 覆寫範例

```yaml
# conf/base/parameters_training.yaml
spark:
  app_name: recsys_tfb-training
  spark.executor.memory: 16g
  spark.executor.cores: 4
  spark.driver.memory: 8g
```

```yaml
# conf/base/parameters_inference.yaml
spark:
  app_name: recsys_tfb-inference
  spark.executor.memory: 24g
  spark.executor.cores: 4
  spark.sql.shuffle.partitions: 400
```

### 環境覆寫

`conf/local/parameters.yaml`、`conf/production/parameters.yaml` 也可放 `spark:` 來覆寫 base：

```yaml
# conf/local/parameters.yaml（範例）
spark:
  spark.master: local[*]
  spark.hadoop.hive.metastore.uris: ""
```

優先序（沿用既有 ConfigLoader）：`base < env < pipeline`。

### Deep-merge 行為

- ConfigLoader 既有 `_deep_merge` 處理：dict-vs-dict 採 deep merge，相同 key 後者覆寫，base 獨有 key 保留。
- `spark:` 內容是平面 dict（key 為 `app_name` 或 `spark.<dotted-key>`，value 為 str / int / bool）。
- `app_name` 是特殊 key，呼叫 builder 的 `.appName()`；其餘走 `.config(k, v)`。

### 驗證

`get_or_create_spark_session()` 內部執行：

- `spark_configs` 必須是 dict，否則 raise `TypeError`。
- 所有 value 必須是 `str | int | bool`，否則 raise `ValueError` 並列出有問題的 key。

## 實作流程

```
進入 get_or_create_spark_session(spark_configs)
        │
        ├─ spark_configs is None？
        │       └─ 是 → Fallback 路徑：
        │               1. SparkSession.getActiveSession() 有 → 直接回傳
        │               2. 無 → ConfigLoader 載入 conf/<env>/，
        │                       取 params["spark"]，繼續走「建立」分支
        │
        └─ 否 → Pipeline 入口路徑：用傳入 spark_configs 走「建立」分支

建立分支：
        ├─ 驗證 spark_configs 為 dict、value 型別合法
        ├─ 抽出 app_name（預設 "recsys_tfb"）
        ├─ builder = SparkSession.builder.appName(app_name)
        ├─ for k, v in spark_configs.items()（排除 "app_name"）:
        │       builder = builder.config(k, v)
        ├─ 若已有 active session → logger.warning（cluster-level configs 會被忽略）
        └─ return builder.getOrCreate()
```

### Signature 變動（破壞性）

舊：
```python
def get_or_create_spark_session(app_name: str = "recsys_tfb", **spark_configs) -> SparkSession
```

新：
```python
def get_or_create_spark_session(spark_configs: dict | None = None) -> SparkSession
```

`app_name` 改成 `spark_configs["app_name"]`（在 YAML 的 `spark:` 區塊裡）。已確認 repo 內無外部呼叫者使用舊 signature，可直接破壞改寫。

## 呼叫點修改清單

| 檔案 | 現況 | 改成 |
|---|---|---|
| `src/recsys_tfb/utils/spark.py` | 11 行 thin wrapper | 重寫成上述流程（含 fallback、驗證、warning） |
| `src/recsys_tfb/io/hive_table_dataset.py:156` | `return SparkSession.builder.getOrCreate()` | `return get_or_create_spark_session()` |
| `src/recsys_tfb/io/parquet_dataset.py:33,57` | `spark = SparkSession.builder.getOrCreate()` | `spark = get_or_create_spark_session()` |
| `src/recsys_tfb/pipelines/source_etl/sql_runner.py:148` | `spark = SparkSession.builder.getOrCreate()` | `spark = get_or_create_spark_session()` |
| `tests/conftest.py:15-22` | 自建 local session | `get_or_create_spark_session(test_minimal_config)` |
| `tests/test_io/test_hive_table_dataset.py:22` | mock `pyspark.sql.SparkSession.builder.getOrCreate` | mock `recsys_tfb.io.hive_table_dataset.get_or_create_spark_session` |
| `scripts/suggest_categorical_cols.py:254-257` | 自建 local session | `get_or_create_spark_session()` |
| Pipeline command 入口（每個 sub-command） | 無 session 建立 | Load params → `get_or_create_spark_session(params.get("spark", {}))` → `create_pipeline()` |

## Migration 步驟

執行順序（避免中間狀態壞掉）：

1. 重寫 `utils/spark.py`（新 signature + 新行為）。
2. 同步更新 `tests/conftest.py` fixture 與 `test_hive_table_dataset.py` mock 路徑。
3. 改 IO 三檔（`hive_table_dataset.py`、`parquet_dataset.py`、`sql_runner.py`）的 import 與呼叫。
4. 改 `scripts/suggest_categorical_cols.py`。
5. 在 `conf/base/parameters.yaml` 新增 `spark:` 區塊。
6. 在每個 pipeline command 入口插入 `get_or_create_spark_session(params.get("spark", {}))`。
7. 為 `parameters_training.yaml`、`parameters_inference.yaml` 加 `spark:` 覆寫；其他 pipeline 暫不加（吃 base 預設）。
8. 跑全測 + 跑 `python -m recsys_tfb source_etl` smoke test，印出 `spark.sparkContext.getConf().getAll()` 確認 config 正確套用。

## 測試策略

### 單元測試（新增 `tests/test_utils/test_spark.py`）

| 案例 | 驗證 |
|---|---|
| `test_with_configs_creates_session` | 傳入 dict，回傳 SparkSession，`spark.conf.get(key)` 能取出傳入值 |
| `test_app_name_from_configs` | `{"app_name": "foo"}` → `sparkContext.appName == "foo"` |
| `test_app_name_default_when_missing` | `{}` → appName == "recsys_tfb" |
| `test_no_configs_returns_active_session` | 已有 active session、不傳參數 → 回傳同一個 session（`is` 比較） |
| `test_no_configs_no_active_falls_back_to_loader` | 沒 active session、不傳參數 → mock ConfigLoader 回傳 fake spark dict → 用該 dict 建 session |
| `test_invalid_value_type_raises` | `{"foo": [1, 2]}` → raise `ValueError`，錯誤訊息列出 `foo` |
| `test_non_dict_raises` | `"not a dict"` → raise `TypeError` |
| `test_active_session_logs_warning_on_new_configs` | 已有 active session 時傳新 configs → caplog 抓到 warning |

### 整合測試

- `tests/conftest.py` fixture 改用新函式後，沿用既有所有 IO / pipeline 測試；若全 pass 即證明無回歸。
- `test_hive_table_dataset.py` 的 mock 點修改是必要修改而非新測試。

### 不重新驗證的部分

- ConfigLoader 的 `_deep_merge` 已被 `tests/test_core/test_config_loader.py::TestDeepMerge` 涵蓋。`spark:` 是 dict-of-strings，不需另行測試。

### Smoke test（手動，非 CI）

在 local 環境跑 `python -m recsys_tfb source_etl ...`，啟動後印出 `spark.sparkContext.getConf().getAll()` 確認：
1. base 共通項已套用（serializer、timeZone）。
2. pipeline 覆寫項已套用。
3. `app_name` 為預期值。

### Test fixture 的 minimal config

```python
test_minimal_config = {
    "app_name": "recsys_tfb_test",
    "spark.sql.shuffle.partitions": "2",
    "spark.default.parallelism": "2",
    "spark.master": "local[2]",
}
```

## 向後相容性

- `get_or_create_spark_session()` 的 signature 是破壞性變更。已掃過 repo 確認無其他呼叫點使用舊 signature。
- `parameters.yaml` 新增 `spark:` 區塊不影響現有 YAML key。
- 若 `parameters_<pipeline>.yaml` 沒有 `spark:` 區塊，pipeline 入口傳入空 dict，session 完全使用 base 預設——已驗證行為等同於目前的 zero-config session 加上 base 共通項。

## 風險與緩解

| 風險 | 緩解 |
|---|---|
| Cluster-level config 在 active session 已存在時會被 PySpark 忽略 | Pipeline 入口是第一個建 session 的人；fallback 路徑（IO 先建、pipeline 後建）會發出 warning |
| 環境差異未涵蓋（例如 production Hive metastore URI 寫死在 base） | `conf/production/parameters.yaml` 用 env override 處理；migration 步驟 5 確認此分層 |
| Test 中 fixture session 與 production session 行為不一致 | Test fixture 走同一個 `get_or_create_spark_session()`，僅參數不同；test_minimal_config 故意以 `local[2]` 加速但不改變 builder 路徑 |
