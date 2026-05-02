# Training Pipeline：cache_model_input 節點設計

- 日期：2026-05-02
- 影響範圍：`pipelines/training`、`conf/base/catalog.yaml`、`conf/production/catalog.yaml`、`conf/base/parameters_training.yaml`、`io/parquet_dataset.py`
- 狀態：Draft（待 user review）

## 動機

目前 production 環境下，`train_model_input`、`train_dev_model_input`、`val_model_input`、`calibration_model_input` 透過 catalog 對應到 Hive 外部表。每次重跑 training pipeline（例如僅調整 hyperparameter）都會重新從 Hive 掃描資料，造成不必要的 IO 與等待時間。

需求：在 `tune_hyperparameters` 之前新增節點，把上述四個 model_input 落地為 driver local 端的 parquet，後續執行只要 cache 命中就完全不再觸碰 Hive。

## 範圍

### 本次納入

- 新增四個 cache 節點（`cache_train_model_input`、`cache_train_dev_model_input`、`cache_val_model_input`、`cache_calibration_model_input`）
- 新增四個對應 catalog 條目 `cached_*`
- 修改 `training/pipeline.py`，下游節點改吃 `cached_*`
- 擴充 `ParquetDataset`：支援 `partition_cols: list[str]` 與 `write_mode: ignore`
- skip-if-exists：以本地路徑（含 `base_dataset_version` / `train_variant_id` / `calibration_variant_id` 三層版本）作為 cache key，搭配 `_SUCCESS` 偵測

### 本次不納入（但保留擴充性）

- 把 cache 後的 parquet 進一步轉為 `lgb.Dataset` / `xgb.DMatrix` 等 algorithm-native 格式
- 保留未來在 `cached_*` 與 `tune_hyperparameters` 之間插入 `materialize_model_input` 節點的位置（依 `parameters.training.algorithm` 分派）
- cache 節點介面只承諾「DataFrame in / DataFrame out」，不放 algorithm-specific 條件，避免之後 materialize 層介入時要回頭改 cache 節點

## 設計

### 架構

```
training pipeline
  ├── cache_train_model_input            ← 新增
  ├── cache_train_dev_model_input        ← 新增
  ├── cache_val_model_input              ← 新增
  ├── cache_calibration_model_input      ← 新增（僅在 enable_calibration=True 時加入）
  ├── tune_hyperparameters               (改吃 cached_*)
  ├── train_model                        (改吃 cached_*)
  ├── calibrate_model                    (改吃 cached_*；enable_calibration)
  ├── evaluate_model                     (改吃 cached_*)
  └── log_experiment
```

未來可在 `cached_*` 與 `tune_hyperparameters` 之間插入演算法分派的 materialize 節點，但本次 spec 不實作。

### 為何拆成 4 個獨立節點

- 單一職責：每個節點處理一個 model_input、一個 output，與框架 `Node(outputs="...")` 一對一
- 可選擇性：`enable_calibration=False` 時不加入 calibration cache 節點，避免不必要的觸發
- 可獨立測試：每個節點 mock 一個輸入一個輸出
- 失敗隔離：某個 dataset 寫入失敗不會影響其他三個 cache 是否成立
- DAG 可視性：log/timing 可分別觀察 train vs val vs calibration 的 cache 行為
- 共用核心：四節點主體相同，抽到 helper（`_cache_or_passthrough`）避免重複

### 落地格式：Parquet

- Spark 端 `df.write.partitionBy(...).parquet(...)` 是分散式寫入路徑，不需 collect 到 driver
- 多 partition key（snap_date, prod_name）以原生 partitionBy 表達
- dev (pandas) / prod (Spark) 都可使用同一檔案（pyarrow 後端）
- schema 與 dtype 完整保留，schema evolution 容忍度高
- 跨演算法相容（pandas / Spark / Arrow / lightgbm reader / xgboost reader 都可吃）

### 路徑模板

鏡像 production catalog 中現有 dataset 的路徑層級結構：

```
/tmp/recsys_cache/
  {base_dataset_version}/
    val_model_input.parquet/                                          ← _SUCCESS 在這層
    train_variants/{train_variant_id}/
      train_model_input.parquet/
      train_dev_model_input.parquet/
    calibration_variants/{calibration_variant_id}/
      calibration_model_input.parquet/
```

切換 `base_dataset_version`、`train_variant_id`、`calibration_variant_id` 任一者，路徑改變、cache 自然 miss。

### Catalog 條目

#### `conf/production/catalog.yaml` 新增

```yaml
# --- Training pipeline - Local cache layer ---
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

`partition_cols` 順序對應 `partitionBy` 層級（最上層先列）。各 dataset 可設不同的 partition_cols；本 spec 預設 `[snap_date, prod_name]`，使用者可在 catalog 各條目自行調整。

#### `conf/base/catalog.yaml`

dev catalog **不新增** `cached_*` 條目。Runner 找不到時自動建 MemoryDataset；cache 節點在 dev 走 no-op 分支，回傳的 pandas DataFrame 經 MemoryDataset 直接傳給下游。

### Parameters

#### `conf/base/parameters_training.yaml` 新增

```yaml
cache:
  enabled: true
  root: /tmp/recsys_cache
```

#### `conf/local/parameters.yaml`（dev override）

```yaml
cache:
  enabled: false
```

`cache.root` 用於日後若需移到其他 driver local 路徑時的覆寫；本次預設 `/tmp/recsys_cache`，與 catalog filepath 對齊。

### Cache 節點實作

```python
# pipelines/training/nodes.py
from pathlib import Path

def _cache_or_passthrough(df, dataset_name: str, parameters: dict):
    """skip-if-exists 共用邏輯。

    - dev (cache.enabled=false 或 backend=pandas)：直接 passthrough
    - prod 且 _SUCCESS 存在：回傳從 local parquet 重讀的 lazy Spark DF
    - prod 且 _SUCCESS 不存在：回傳原 lazy DF，交給框架的 ParquetDataset.save() 觸發 Spark write
    - prod 且目錄存在但 _SUCCESS 缺：視為 partial 寫入，清空該路徑後視為 miss
    """
    cache_cfg = parameters.get("cache", {})
    if not cache_cfg.get("enabled", False):
        return df

    if not _is_spark_df(df):
        logger.warning("cache.enabled=true but input is not a Spark DataFrame; passthrough")
        return df

    local_path = _resolve_cache_path(dataset_name, cache_cfg, parameters)
    success_marker = Path(local_path) / "_SUCCESS"

    if Path(local_path).exists() and not success_marker.exists():
        logger.info("Partial cache detected at %s, clearing", local_path)
        _rmtree(local_path)

    if success_marker.exists():
        logger.info("cache_hit name=%s path=%s", dataset_name, local_path)
        spark = df.sql_ctx.sparkSession
        return spark.read.parquet(f"file://{local_path}")

    logger.info("cache_miss name=%s path=%s (will trigger Spark write)", dataset_name, local_path)
    return df


def cache_train_model_input(train_model_input, parameters):
    return _cache_or_passthrough(train_model_input, "train_model_input", parameters)

def cache_train_dev_model_input(train_dev_model_input, parameters):
    return _cache_or_passthrough(train_dev_model_input, "train_dev_model_input", parameters)

def cache_val_model_input(val_model_input, parameters):
    return _cache_or_passthrough(val_model_input, "val_model_input", parameters)

def cache_calibration_model_input(calibration_model_input, parameters):
    return _cache_or_passthrough(calibration_model_input, "calibration_model_input", parameters)
```

`_resolve_cache_path` 從 `parameters` 取出 `base_dataset_version` / `train_variant_id` / `calibration_variant_id`，依 dataset_name 決定子路徑層級，組出絕對路徑。`_is_spark_df` 以 `hasattr(df, "sql_ctx")` 判定。

### Pipeline 接線

```python
# pipelines/training/pipeline.py
def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(cache_train_model_input,
             inputs=["train_model_input", "parameters"],
             outputs="cached_train_model_input"),
        Node(cache_train_dev_model_input,
             inputs=["train_dev_model_input", "parameters"],
             outputs="cached_train_dev_model_input"),
        Node(cache_val_model_input,
             inputs=["val_model_input", "parameters"],
             outputs="cached_val_model_input"),
    ]

    if enable_calibration:
        nodes.append(
            Node(cache_calibration_model_input,
                 inputs=["calibration_model_input", "parameters"],
                 outputs="cached_calibration_model_input"),
        )

    nodes.extend([
        Node(tune_hyperparameters,
             inputs=["cached_train_model_input", "cached_train_dev_model_input",
                     "cached_val_model_input", "preprocessor", "parameters"],
             outputs="best_params"),
        Node(train_model,
             inputs=["cached_train_model_input", "cached_train_dev_model_input",
                     "best_params", "preprocessor", "parameters"],
             outputs=train_model_output),
    ])

    if enable_calibration:
        nodes.append(
            Node(calibrate_model,
                 inputs=["trained_model", "cached_calibration_model_input",
                         "preprocessor", "parameters"],
                 outputs="model"),
        )

    nodes.extend([
        Node(evaluate_model,
             inputs=["model", "cached_val_model_input", "preprocessor", "parameters"],
             outputs="evaluation_results"),
        Node(log_experiment,
             inputs=["model", "best_params", "evaluation_results", "parameters"],
             outputs=None),
    ])

    return Pipeline(nodes)
```

### 資料流

#### 首次執行（cache miss）

```
catalog.load("train_model_input")
  └─ HiveTableDataset.load() → spark.table(...)        // lazy plan，無實際掃描

cache_train_model_input(lazy_df, parameters)
  └─ _SUCCESS 不存在 → return lazy_df （原樣回傳）

runner: catalog.save("cached_train_model_input", lazy_df)
  └─ ParquetDataset(spark, partition_cols, mode=ignore).save(lazy_df)
  └─ df.write.mode("ignore").partitionBy("snap_date", "prod_name").parquet(file:///tmp/...)
       ↑ Spark 此時實際掃 Hive、寫 local parquet、生成 _SUCCESS

下游節點 catalog.load("cached_train_model_input")
  └─ spark.read.parquet(file:///tmp/...) lazy

tune_hyperparameters
  └─ _to_pandas(df) 才實體化   // 與現況一致
```

#### 重跑（cache hit）

```
catalog.load("train_model_input")
  └─ lazy plan（仍未掃 Hive）

cache_train_model_input(lazy_df, parameters)
  └─ _SUCCESS 存在
  └─ return spark.read.parquet(file:///tmp/...)        // 改回傳 local lazy DF

runner: catalog.save("cached_train_model_input", local_lazy_df)
  └─ ParquetDataset.save() with mode("ignore")
  └─ Spark 偵測目標目錄已存在 → write job no-op
       ↑ Hive 完全沒被掃

tune_hyperparameters
  └─ 從 local parquet 走，後續流程不變
```

#### 三層版本切換時的行為

| 變動                               | 路徑變化                                              | 結果                                          |
|------------------------------------|-------------------------------------------------------|-----------------------------------------------|
| 改 hyperparam 重跑（同版本）       | 路徑不變                                              | 全部 cache hit                                |
| 改 sample_ratio（train_variant）   | `train_variants/{train_variant_id}/` 變動             | train / train_dev miss；val / calibration hit |
| 改 calibration_variant_id          | `calibration_variants/{calibration_variant_id}/` 變動 | calibration miss；其他 hit                    |
| 改 base_dataset_version            | 全部變動                                              | 全部 miss                                     |

### Skip 判定的雙保險

1. cache 節點先偵測 `_SUCCESS` → 命中時直接回傳 local lazy DF，連 Hive lazy plan 都換成 local plan
2. ParquetDataset 用 `mode("ignore")` → 即便 cache 節點未命中（如 partial 寫入殘留被先清掉），Spark write 也不會覆寫已有目錄
3. 兩道防線確保「非預期狀態下也不會誤打 Hive」

## ParquetDataset 擴充

### 新增參數

- `partition_cols: list[str]`：spark backend `save()` 時 `df.write.partitionBy(*partition_cols)`；pandas backend 透過 `pq.write_to_dataset(..., partition_cols=...)` 對齊
- `write_mode: str`，預設 `"overwrite"`；新增 `"ignore"` 對應 spark `df.write.mode("ignore")`、pandas backend 在目標路徑已存在 `_SUCCESS` 時 no-op

### 行為

- `partition_cols` 為空時行為與現況一致
- `write_mode="ignore"` 與 `partition_cols` 可同時使用
- `partition_cols` 設定但 DataFrame 缺對應欄位時，由 Spark / pyarrow 端 raise；ParquetDataset 不重複驗證

## 錯誤處理

| 情境                                              | 處理                                                                 |
|---------------------------------------------------|----------------------------------------------------------------------|
| `cache.root` 不存在                               | cache 節點 / ParquetDataset 自動 `mkdir -p`；無寫入權限直接 raise    |
| 寫入中途失敗（partial parquet，無 `_SUCCESS`）    | cache 節點偵測「目錄存在但無 `_SUCCESS`」→ `rmtree` 後視為 miss      |
| `_SUCCESS` 存在但 schema 不符                     | 不主動驗證；信任路徑即版本 key。使用者需手動 `rm -rf <path>` 清除    |
| 磁碟空間不足                                       | Spark write 自然 raise IOError；不吞錯，由 runner 標記節點失敗       |
| Hive 端無資料（空 partition）                     | 寫出空 parquet 與 `_SUCCESS`；下游照常 load 空 DF（與現況一致）      |
| dev 誤設 `cache.enabled=true` 但 backend=pandas   | cache 節點偵測 `df` 非 Spark DF（無 `sql_ctx`）→ 警告 log 後 passthrough |
| `partition_cols` 欄位不存在                       | Spark write raise；訊息明確指出 dataset；不重複驗證                  |

### Logging

沿用既有 `log_step` 與 runner 自動 timing：

```
INFO  cache_train_model_input: cache_hit=true path=/tmp/recsys_cache/v1/train_variants/x/train_model_input.parquet
INFO  cache_train_model_input: cache_miss path=... (will trigger Spark write)
INFO  cache_train_model_input completed in 142.3s
```

## 測試

### 新增

`tests/test_pipelines/test_training/test_cache_nodes.py`：

1. **dev backend → no-op**：`cache.enabled=false` 時直接回傳輸入，不建任何路徑
2. **首次執行（cache miss）**：`_SUCCESS` 不存在 → 回傳原始 lazy df
3. **重跑（cache hit）**：先建 `_SUCCESS` → 回傳值是 `spark.read.parquet(local_path)`，原始 df 的 action 沒被呼叫
4. **partial cache 修復**：路徑存在但無 `_SUCCESS` → 自動清空後視為 miss
5. **路徑解析三元組**：base / train_variant / calibration_variant 任一變動 → 路徑變動
6. **enable_calibration=False**：pipeline 不含 `cache_calibration_model_input`
7. **整合測試**：用合成資料跑完 training pipeline 兩次，第二次驗證 Hive mock 的 `spark.table` 沒被呼叫

### 擴充

`tests/test_io/test_parquet_dataset.py`：

- `partition_cols=[snap_date]` 與 `[snap_date, prod_name]` 寫入後讀回，partition discovery 正常
- `write_mode="ignore"`：目標目錄已存在時 save 為 no-op，內容不被覆寫

### 回歸

`tests/test_pipelines/test_training/test_pipeline.py`：tune / train / evaluate / calibrate 的 inputs 名稱從 `train_model_input` 等改為 `cached_*`。

## 未來擴充（不在本次範圍）

預留 `materialize_model_input` 節點位置，介於 `cached_*` 與 `tune_hyperparameters` 之間，依 `parameters.training.algorithm` 將 cached parquet 轉為 algorithm-native 格式（lgb.Dataset binary、xgb.DMatrix buffer 等），落地於獨立 cache 路徑。設計契約：

- 節點 signature：`materialize_model_input(cached_*, preprocessor, parameters) -> AlgorithmDataset`
- catalog 對應 `train_dataset` / `val_dataset` 等新增條目，用 `LgbDatasetDataset` / `XgbDMatrixDataset` 等新 Dataset 型別
- `tune_hyperparameters` / `train_model` 內部 `_extract_Xy` 可改為直接接受 algorithm-native 格式
- 本層 cache 節點不需任何修改，介面已就緒

## 開放問題

- `cache.root` 是否需要在 production parameters 提供環境變數覆寫機制（例如 `${RECSYS_CACHE_ROOT}`）？目前先 hardcode `/tmp/recsys_cache`，待運維反饋
- 是否需要 CLI flag `--refresh-cache` 強制重建？目前不加，使用者改 version 或手動 `rm -rf` 處理
- production 端 `/tmp` 容量是否足以承載 train_model_input 的全量資料？需與基礎建設確認
