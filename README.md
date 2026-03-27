# 批次排序推薦框架

批次排序推薦框架——為每位用戶對多個候選項目打分並排序。預設範例為商業銀行金融產品推薦（22 類產品 × ~1000 萬客戶 × ~500 特徵），但適用於任何「用戶 × 候選項目 × 二分類標籤 → 排序」的場景。

---

## 導覽

| 你想做什麼 | 看這裡 |
|-----------|--------|
| 快速跑起來看看效果 | [快速開始](#快速開始) |
| 從原始 Hive 表產出特徵/標籤表 | [Source ETL 參數](#source-etl-參數-parameters_source_etlyaml) / [執行方式](#pipeline-執行) |
| 用自己的資料跑 pipeline | [使用自己的資料](#使用自己的資料) |
| 調整參數或設定 | [設定檔說明](#設定檔說明) / [修改需求對照表](#修改需求對照表) |
| 修改特徵工程、模型、排序邏輯 | [修改需求對照表](#修改需求對照表) / [客製化邊界](#客製化邊界) |
| 接手維運或後續開發 | [維運重點](#維運重點) / [常見錯誤與排查](#常見錯誤與排查) |
| 了解整體架構與設計 | [專案全貌](#專案全貌) |

---

## 適用情境

**解決的問題**：你有一群用戶、一組候選項目，以及歷史上「用戶是否對項目感興趣」的二分類標籤（0/1）。你想為每位用戶預測各候選項目的興趣分數，排序後輸出推薦清單。

**適用場景**：

- 金融產品推薦（信用卡、貸款、基金、保險）
- 溝通通路偏好
- 廣告文案類型排序
- 任何「多個候選項 × 用戶 × 二分類標籤 → 排序」的場景

**框架假設與限制**：

| 假設 | 說明 |
|------|------|
| 批次推論 | 週期性（如每週）產出全量排序結果，非即時服務 |
| 表格特徵 | 用戶特徵為結構化數值/類別欄位，非序列、圖、文字 |
| 二分類轉排序 | 以二分類預測機率作為排序分數（Strategy 1） |
| LightGBM | 預設模型為 LightGBM；可透過 ModelAdapter 抽象替換，但需實作新 adapter |
| CPU-only | 目標環境為 4 core / 128GB RAM，無 GPU |
| 離線環境 | 無網路存取、不可安裝額外套件 |

---

## 快速開始

### 環境需求

- Python 3.10+

### 安裝

```bash
pip install -e ".[dev]"
```

### 產生合成假資料

```bash
python scripts/generate_synthetic_data.py
```

執行後 `data/` 目錄下會產生：

```
data/
├── feature_table.parquet   # 客戶特徵表（合成）
├── label_table.parquet     # 客戶標籤表（合成）
└── sample_pool.parquet     # 抽樣池（合成）
```

### 依序執行 pipeline

```bash
# Step 0（Production only）: Source ETL — 從原始 Hive 表產出 feature/label/sample_pool
# Local 環境使用合成假資料，不需執行此步驟
python -m recsys_tfb --pipeline source_etl --env production --snap-dates 2024-01-31,2024-02-29

# Step 1: Dataset Building — 抽樣、5-way 切分、特徵工程
python -m recsys_tfb --pipeline dataset --env local

# Step 2: Training — Optuna 超參搜尋 + LightGBM 訓練 + 機率校準
python -m recsys_tfb --pipeline training --env local

# Step 3: Inference — 批次打分 + 排序 + 驗證
python -m recsys_tfb --pipeline inference --env local
```

每步完成後 `data/` 下會新增對應產出：

```
data/
├── feature_table.parquet
├── label_table.parquet
├── sample_pool.parquet
├── dataset/<dataset_version>/              # Step 1 產出
│   ├── train_model_input.parquet
│   ├── train_dev_model_input.parquet
│   ├── calibration_model_input.parquet     # enable_calibration=true 時
│   ├── val_model_input.parquet
│   ├── test_model_input.parquet
│   ├── preprocessor.pkl
│   ├── category_mappings.json
│   └── manifest.json
├── models/<model_version>/                 # Step 2 產出
│   ├── model.txt
│   ├── model_meta.json
│   ├── best_params.json
│   ├── evaluation_results.json
│   └── manifest.json
└── inference/<model_version>/<snap_date>/  # Step 3 產出
    ├── scoring_dataset.parquet
    ├── score_table.parquet
    ├── ranked_predictions.parquet
    └── validated_predictions.parquet
```

其中 `<dataset_version>` 和 `<model_version>` 是根據參數自動計算的 8 碼 SHA-256 hash。

---

## 專案全貌

### 架構概覽

採用 Kedro 風格的輕量框架（無 Kedro 依賴）：

```
Node → Pipeline → Runner → Catalog
```

- **Node**：封裝純函數的計算單元，宣告輸入/輸出
- **Pipeline**：節點的有向無環圖（DAG），Kahn's algorithm 拓撲排序
- **Runner**：依序執行節點，附帶結構化日誌
- **Catalog**：資料 I/O 抽象層，支援 Parquet、Pickle、JSON、ModelAdapter 等格式
- **ConfigLoader**：YAML base + env 覆蓋，deep merge 語義

### 流水線與相依關係

| 流水線 | 狀態 | 說明 |
|--------|------|------|
| Source ETL | ✅ | SQL 驅動的特徵/標籤/抽樣池表建構（PySpark）。獨立 SQLRunner，YAML 定義執行順序，支援 dry-run / backfill / restart-from |
| Dataset Building | ✅ | 分層抽樣、5-way 切分（train/train-dev/calibration/val/test）、特徵工程。雙後端 |
| Training | ✅ | Optuna 超參搜尋、LightGBM 訓練、機率校準（isotonic/sigmoid）、mAP 評估、MLflow 追蹤 |
| Inference | ✅ | 批量打分、preprocessor 複用、排序、6 項 sanity check 驗證。雙後端 |

四條 pipeline 依序執行，後者依賴前者的產出：

```
原始 Hive 表 ──┐
               ▼
        ┌──────────────┐
        │  Source ETL   │  ← 獨立 SQLRunner（非 DAG 框架）
        └──────────────┘
              │
              ▼
sample_pool ────┐                                              feature_table ──┐
feature_table ──┤                                              preprocessor ───┤
label_table ────┤                                              model ──────────┤
                ▼                                                              ▼
        ┌───────────────┐      ┌──────────────┐               ┌──────────────┐
        │    Dataset     │      │   Training   │               │  Inference   │
        │   Building     │─────▶│              │──────────────▶│              │
        └───────────────┘      └──────────────┘               └──────────────┘
              │                       │                               │
              ▼                       ▼                               ▼
     train/train-dev/cal/      model, best_params              ranked_predictions
     val/test model_input      evaluation_results              validated_predictions
     preprocessor              calibrator (optional)
     category_mappings
```

**跨 pipeline 共享的 catalog dataset：**

| 共享 Dataset | 產出 Pipeline | 消費 Pipeline | 說明 |
|--------------|---------------|---------------|------|
| `feature_table` | （外部輸入） | Dataset, Inference | 客戶特徵表 |
| `label_table` | （外部輸入） | Dataset | 客戶標籤表 |
| `sample_pool` | （外部輸入） | Dataset | 抽樣池（定義候選客戶 × 產品組合） |
| `train_model_input` | Dataset | Training | 訓練集（特徵 + 標籤，已前處理） |
| `train_dev_model_input` | Dataset | Training | 開發集（用於 early stopping） |
| `calibration_model_input` | Dataset | Training | 校準集（用於機率校準，optional） |
| `val_model_input` | Dataset | Training | 驗證集（用於 mAP 評估） |
| `test_model_input` | Dataset | Training | 測試集（用於最終評估） |
| `preprocessor` | Dataset | Inference | 特徵編碼器（categorical mapping、欄位清單等） |
| `category_mappings` | Dataset | （參考用） | 產品類別對照表 |
| `model` | Training | Inference | ModelAdapter（含模型權重 + metadata + 選配 calibrator） |

### 版本管理機制

```
parameters_dataset.yaml
        │
        ▼ SHA-256 前 8 碼
  dataset_version ──────────────────────────────────┐
        │                                           │
        ▼                                           ▼
parameters_training.yaml + dataset_version    Inference 路徑：
        │                                    data/inference/${model_version}/${snap_date}/
        ▼ SHA-256 前 8 碼
  model_version
```

- Dataset pipeline 產出存放在 `data/dataset/${dataset_version}/`
- Training pipeline 產出存放在 `data/models/${model_version}/`
- Inference pipeline 產出存放在 `data/inference/${model_version}/${snap_date}/`
- **Symlink**：每次執行自動更新 `latest`；手動 promote 後建立 `best`
- `model_version` 依賴 `dataset_version`，確保模型可追溯到其訓練資料
- 每次 pipeline 執行產出 JSON **manifest**，記錄版本、時間戳、git commit、參數快照

### 專案結構

```
src/recsys_tfb/
  __main__.py               — CLI 入口（Typer）
  core/
    config.py               — ConfigLoader（YAML base + env 合併）
    catalog.py              — DataCatalog（dataset registry & 路徑解析）
    node.py                 — Node（函數封裝 + 命名 I/O）
    pipeline.py             — Pipeline（Kahn's algorithm 拓撲排序）
    runner.py               — Runner（依序執行 + 結構化日誌）
    versioning.py           — Hash-based 版本管理、manifest、symlink
    schema.py               — Config-driven 欄位名稱解析
    logging.py              — RunContext、JSON/Console formatter
  io/
    base.py                 — AbstractDataset 介面
    parquet_dataset.py      — ParquetDataset（pandas/spark 雙後端）
    pickle_dataset.py       — PickleDataset
    json_dataset.py         — JSONDataset
    model_adapter_dataset.py — ModelAdapterDataset（model.txt + metadata + calibrator）
  models/
    base.py                 — ModelAdapter ABC、ADAPTER_REGISTRY、get_adapter()
    lightgbm_adapter.py     — LightGBMAdapter
    calibrated_adapter.py   — CalibratedModelAdapter（isotonic/sigmoid 包裝）
  pipelines/
    __init__.py             — Pipeline registry（get_pipeline, list_pipelines）
    preprocessing.py        — 統一前處理邏輯（dataset/training/inference 共用）
    source_etl/             — Source ETL（models.py, sql_renderer.py, checks.py, audit.py, sql_runner.py）
    dataset/                — Dataset building（nodes_pandas.py, nodes_spark.py, pipeline.py）
    training/               — Training（nodes.py, pipeline.py）
    inference/              — Inference（nodes_pandas.py, nodes_spark.py, pipeline.py, validation.py）
  evaluation/
    metrics.py              — 排序指標（mAP, nDCG, precision@K, recall@K, MRR）
    distributions.py        — 分數/排名分布圖表
    calibration.py          — 校準曲線
    segments.py             — 客群/持有產品組合分析
    baselines.py            — 全域/客群熱門度 baseline
    report.py               — HTML 報告產生（Plotly 離線內嵌）
    compare.py              — 模型比較邏輯與視覺化
    statistics.py           — 統計摘要
  utils/
    spark.py                — Spark 工具函數

conf/                       — YAML 配置 + SQL
scripts/
  generate_synthetic_data.py  — 產生合成假資料
  promote_model.py            — 模型版本晉升（手動觸發）
  evaluate_model.py           — 模型評估 CLI（analyze/compare）
tests/                        — 測試套件
data/                         — 開發用合成資料 & pipeline 產出
```

---

## 使用自己的資料

### 輸入資料 schema

框架需要 **3 張表**作為輸入：

#### feature_table（用戶特徵表）

| 欄位 | 型別 | 必填 | 說明 |
|------|------|------|------|
| `snap_date` | datetime (`YYYY-MM-DD`) | 是 | 快照日期 |
| `cust_id` | string | 是 | 用戶唯一識別碼 |
| *其餘欄位* | float 或 categorical | 是（至少 1 個） | 數值或類別特徵，名稱不限 |

#### label_table（標籤表）

| 欄位 | 型別 | 必填 | 說明 |
|------|------|------|------|
| `snap_date` | datetime (`YYYY-MM-DD`) | 是 | 快照日期 |
| `cust_id` | string | 是 | 用戶唯一識別碼 |
| `prod_name` | string | 是 | 候選項目名稱 |
| `label` | int (0/1) | 是 | 二分類標籤：1 表示感興趣 |
| `apply_start_date` | datetime | 否 | 輔助欄位（pipeline 會自動 drop） |
| `apply_end_date` | datetime | 否 | 輔助欄位（pipeline 會自動 drop） |
| `cust_segment_typ` | string | 否 | 客戶分群（可用於分層抽樣） |

#### sample_pool（抽樣池）

定義哪些 `(snap_date, cust_id, prod_name)` 組合是候選樣本。Dataset pipeline 從這張表抽樣，而非直接從 label_table 抽樣。

> **彈性**：數值特徵欄位數量和名稱完全自由。框架透過排除法（移除 join key + drop 清單後，剩餘皆為特徵）自動偵測。日期欄位接受 `datetime64[ns]` 或 `"YYYY-MM-DD"` 字串（自動轉換）。

### Step-by-step 操作流程

**Step 1**：準備你的 feature_table、label_table、sample_pool，放入 `data/` 目錄（或任意路徑）。

**Step 2**：修改設定檔：

| 檔案 | 要改的 key | 說明 |
|------|-----------|------|
| `conf/base/catalog.yaml` | `feature_table.filepath`、`label_table.filepath`、`sample_pool.filepath` | 指向你的資料路徑 |
| `conf/base/parameters_dataset.yaml` | `dataset.train_snap_date_start`、`dataset.train_snap_date_end` | 訓練集日期範圍 |
| `conf/base/parameters_dataset.yaml` | `dataset.val_snap_dates`、`dataset.test_snap_dates` | 驗證/測試日期 |
| `conf/base/parameters_dataset.yaml` | `dataset.calibration_snap_dates` | 校準集日期（若啟用） |
| `conf/base/parameters_dataset.yaml` | `prepare_model_input.drop_columns` | 若你的 label_table 沒有 `apply_start_date` 等輔助欄位，從 drop 清單中移除 |
| `conf/base/parameters_dataset.yaml` | `prepare_model_input.categorical_columns` | 改為你資料中的類別欄位名稱 |
| `conf/base/parameters_dataset.yaml` | `dataset.sample_group_keys` | 改為你要分層抽樣的欄位 |
| `conf/base/parameters_inference.yaml` | `inference.products` | 改為你要打分的候選項目清單 |
| `conf/base/parameters_inference.yaml` | `inference.snap_dates` | 改為推論日期 |

**Step 3**：執行 pipeline：

```bash
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local
```

---

## 設定檔說明

### 配置管理架構

```
conf/
├── base/                   # 基礎配置（跨環境共享）
│   ├── catalog.yaml
│   ├── parameters.yaml
│   ├── parameters_dataset.yaml
│   ├── parameters_training.yaml
│   ├── parameters_inference.yaml
│   ├── parameters_evaluation.yaml
│   └── parameters_source_etl.yaml
├── local/                  # 本地開發環境覆蓋
│   ├── catalog.yaml
│   └── parameters_source_etl.yaml   # dry_run: true
├── production/             # 生產環境配置
│   ├── catalog.yaml
│   ├── parameters.yaml
│   └── parameters_source_etl.yaml   # dry_run: false
└── sql/etl/                # Source ETL SQL 檔案
    ├── feature/            # 特徵表 SQL（feature_aum, feature_sav, ...）
    ├── label/              # 標籤表 SQL（label_ccard, label_exchange, ...）
    └── sample_pool/        # 抽樣池 SQL
```

- `base/` 存放跨環境共享的預設值
- `local/` 和 `production/` 可覆蓋同名 key（deep merge：nested dict 遞迴合併，scalar 值直接替換）
- 透過 `--env` 參數切換環境：`python -m recsys_tfb -p dataset -e production`
- `--env` 預設值為 `local`

### 全域參數 (`parameters.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `project_name` | str | `recsys_tfb` | 專案名稱，用於 MLflow 實驗命名 |
| `random_seed` | int | `42` | 全域隨機種子，影響抽樣、Optuna、LightGBM |
| `backend` | str | `pandas` | 預設後端（`pandas` 或 `spark`） |
| `schema.columns.time` | str | `snap_date` | 時間欄位名稱 |
| `schema.columns.entity` | list | `[cust_id]` | 實體欄位名稱 |
| `schema.columns.item` | str | `prod_name` | 項目欄位名稱 |
| `schema.columns.label` | str | `label` | 標籤欄位名稱 |
| `schema.columns.score` | str | `score` | 預測分數欄位名稱 |
| `schema.columns.rank` | str | `rank` | 排名欄位名稱 |
| `logging.level` | str | `INFO` | 日誌等級 |
| `logging.console` | bool | `true` | 是否輸出到 console |
| `logging.file.enabled` | bool | `true` | 是否寫入日誌檔 |
| `logging.file.path` | str | `logs/` | 日誌檔目錄 |
| `logging.file.format` | str | `json` | 日誌檔格式（`json`） |

### Dataset 參數 (`parameters_dataset.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `dataset.train_snap_date_start` | str | `"2025-01-31"` | 訓練集日期範圍起始 |
| `dataset.train_snap_date_end` | str | `"2025-10-31"` | 訓練集日期範圍結束 |
| `dataset.sample_ratio` | float | `1.0` | 分層抽樣率，1.0 表示全量 |
| `dataset.sample_group_keys` | list[str] | `["cust_segment_typ", "prod_name"]` | 分層抽樣的分組欄位 |
| `dataset.sample_ratio_overrides` | dict | `{}` | 特定分組的抽樣率覆蓋，key 以 `\|` 連接（如 `"mass\|fund_mix": 0.5`） |
| `dataset.train_dev_ratio` | float | `0.1` | Train-dev 佔 train 日期資料的比例（按 `cust_id` 切分） |
| `dataset.enable_calibration` | bool | `true` | 是否啟用校準集切分 |
| `dataset.calibration_snap_dates` | list[str] | `["2025-11-30"]` | 校準集使用的快照日期 |
| `dataset.calibration_sample_ratio` | float | `1.0` | 校準集抽樣率 |
| `dataset.val_snap_dates` | list[str] | `["2025-12-31"]` | 驗證集使用的快照日期 |
| `dataset.val_sample_ratio` | float | `1.0` | 驗證集抽樣率 |
| `dataset.test_snap_dates` | list[str] | `["2026-01-31"]` | 測試集使用的快照日期 |
| `prepare_model_input.drop_columns` | list[str] | 見下方 | `prepare_model_input` 時要移除的欄位 |
| `prepare_model_input.categorical_columns` | list[str] | 見下方 | 需做 integer encoding 的類別欄位 |

`drop_columns` 預設值：`["snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"]`

`categorical_columns` 預設值：`["prod_name", "gender", "risk_attr", "education_level", "marital_status", "channel_preference"]`

### Training 參數 (`parameters_training.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `training.algorithm` | str | `lightgbm` | 模型演算法（對應 ADAPTER_REGISTRY） |
| `training.algorithm_params.objective` | str | `binary` | LightGBM 目標函數 |
| `training.algorithm_params.metric` | str | `binary_logloss` | LightGBM 評估指標 |
| `training.calibration.enabled` | bool | `true` | 是否執行機率校準 |
| `training.calibration.method` | str | `isotonic` | 校準方法（`isotonic` 或 `sigmoid`） |
| `training.n_trials` | int | `20` | Optuna 超參搜尋試驗次數 |
| `training.num_iterations` | int | `500` | LightGBM boosting 迭代輪數 |
| `training.early_stopping_rounds` | int | `50` | 早停耐心值 |
| `training.search_space.learning_rate` | {low, high} | `{0.001, 0.1}` | 學習率搜尋範圍（log-scale） |
| `training.search_space.num_leaves` | {low, high} | `{4, 64}` | 葉節點數搜尋範圍 |
| `training.search_space.max_depth` | {low, high} | `{3, 8}` | 最大樹深搜尋範圍 |
| `training.search_space.min_child_samples` | {low, high} | `{5, 100}` | 葉節點最小樣本數搜尋範圍 |
| `training.search_space.subsample` | {low, high} | `{0.6, 1.0}` | 行抽樣比例搜尋範圍 |
| `training.search_space.colsample_bytree` | {low, high} | `{0.6, 1.0}` | 列抽樣比例搜尋範圍 |
| `mlflow.experiment_name` | str | `recsys_tfb` | MLflow 實驗名稱 |
| `mlflow.tracking_uri` | str | `mlruns` | MLflow tracking 儲存路徑 |

### Source ETL 參數 (`parameters_source_etl.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `source_etl.dry_run` | bool | `true`（local）/ `false`（production） | Dry-run 模式只 render SQL 不執行 |
| `source_etl.variables.target_db` | str | `dev_ml_feature` / `ml_feature` | 產出表的 Hive database |
| `source_etl.source_checks.<table>` | dict | — | 來源表新鮮度/schema 檢查設定 |
| `source_etl.source_checks.<table>.partition_key` | str | — | 來源表的 partition 欄位 |
| `source_etl.source_checks.<table>.min_row_count` | int | `0` | 最小 row count 門檻 |
| `source_etl.source_checks.<table>.expected_columns` | dict | — | 預期欄位名稱→型別對照（schema drift 檢查） |
| `source_etl.source_checks.<table>.allow_new_columns` | bool | `true` | 是否允許來源表新增欄位 |
| `source_etl.tables[].name` | str | — | 產出表名稱 |
| `source_etl.tables[].sql_file` | str | — | SQL 檔案路徑（相對於 `conf/sql/etl/`） |
| `source_etl.tables[].partition_by` | list[str] | — | Partition 欄位 |
| `source_etl.tables[].primary_key` | list[str] | `[]` | 主鍵欄位（用於 duplicate check） |
| `source_etl.tables[].depends_on` | list[str] | `[]` | 依賴的上游表（用於順序驗證） |
| `source_etl.tables[].quality_checks` | dict | `{}` | Output 品質檢查（`min_row_count`、`max_duplicate_key_ratio`、`max_null_ratio`） |
| `source_etl.audit.database` | str | `${target_db}` | Audit table 所在 database |
| `source_etl.audit.table` | str | `etl_audit_log` | Audit table 名稱 |

### Inference 參數 (`parameters_inference.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `inference.use_calibration` | bool | `true` | 是否使用校準後的機率。若 `false` 且模型有 calibrator，則使用原始預測 |
| `inference.snap_dates` | list[str] | `["2025-12-31"]` | 推論使用的快照日期 |
| `inference.products` | list[str] | 見下方 | 要打分的產品代碼列表 |

`products` 預設值：`["exchange_usd", "exchange_fx", "fund_stock", "fund_bond", "fund_mix", "ccard_ins", "ccard_bill", "ccard_cash"]`

### Evaluation 參數 (`parameters_evaluation.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `evaluation.k_values` | list | `[5, "all"]` | @K 指標的 K 值，`"all"` 在執行時解析為產品總數 |
| `evaluation.segment_columns` | list[str] | `["cust_segment_typ"]` | 分群分析用的欄位 |
| `evaluation.segment_sources` | dict | 見設定檔 | 外部分群資料來源（Parquet 檔，join 到標籤表） |

### Catalog 配置 (`catalog.yaml`)

每個 catalog entry 包含：

| 欄位 | 必填 | 說明 |
|------|------|------|
| `type` | 是 | 資料集型別：`ParquetDataset`、`PickleDataset`、`JSONDataset`、`ModelAdapterDataset` |
| `filepath` | 是 | 檔案路徑，支援模板變數 `${dataset_version}`、`${model_version}`、`${snap_date}` |
| `backend` | 否 | 僅 `ParquetDataset`：`pandas`（預設）或 `spark` |

環境覆蓋範例：

```yaml
# conf/local/catalog.yaml（本地開發）
feature_table:
  type: ParquetDataset
  filepath: data/feature_table.parquet
  backend: pandas

# conf/production/catalog.yaml（生產環境）
feature_table:
  type: ParquetDataset
  filepath: hdfs:///data/recsys/feature_table.parquet
  backend: spark
```

### 參數相依關係

| 約束條件 | 說明 |
|----------|------|
| train 日期 ∩ calibration/val/test 日期 = ∅ | 各 split 日期不可重疊，否則資料洩漏。程式碼會在執行前檢查並 raise `ValueError` |
| 所有指定日期 ⊆ sample_pool 日期 | 指定日期必須存在於 sample_pool 中 |
| `train_snap_date_start` ≤ `train_snap_date_end` | 否則立即報錯 |
| `inference.products` ⊆ category_mappings 的產品集合 | 推論產品必須是訓練時出現過的產品 |
| `inference.snap_dates` ⊆ feature_table 日期 | 推論日期必須存在於 feature_table 中 |
| `early_stopping_rounds` < `num_iterations` | 否則早停永遠不會觸發 |
| `search_space.*.low` < `search_space.*.high` | 搜尋下界必須小於上界 |
| `random_seed` 影響範圍 | 同時影響抽樣、Optuna sampler、LightGBM 訓練 |
| 修改 `parameters_dataset.yaml` | 產生新的 `dataset_version`，training 和 inference 都需重跑 |
| 修改 `parameters_training.yaml` | 產生新的 `model_version`，inference 需重跑 |

---

## 修改需求對照表

| 想做什麼 | 改哪裡 | 具體位置 |
|----------|--------|----------|
| 新增/修改 ETL SQL 轉換 | SQL + 設定檔 | `conf/sql/etl/` 新增 SQL 檔案 + `parameters_source_etl.yaml` 的 `tables` 清單 |
| 新增 ETL 來源表驗證 | 設定檔 | `parameters_source_etl.yaml` → `source_checks` |
| 改 ETL 產出 database | 設定檔 | `parameters_source_etl.yaml` → `variables.target_db` |
| 換資料來源路徑 | 設定檔 | `conf/base/catalog.yaml` → `feature_table.filepath`、`label_table.filepath`、`sample_pool.filepath` |
| 改訓練/驗證/測試日期 | 設定檔 | `parameters_dataset.yaml` → `train_snap_date_start/end`、`val_snap_dates`、`test_snap_dates` |
| 改推論日期 | 設定檔 | `parameters_inference.yaml` → `inference.snap_dates` |
| 改候選產品清單 | 設定檔 | `parameters_inference.yaml` → `inference.products` |
| 改抽樣率 | 設定檔 | `parameters_dataset.yaml` → `dataset.sample_ratio`、`sample_ratio_overrides` |
| 改超參搜尋範圍/次數 | 設定檔 | `parameters_training.yaml` → `training.search_space.*`、`training.n_trials` |
| 開關機率校準 | 設定檔 | `parameters_dataset.yaml` → `enable_calibration`；`parameters_training.yaml` → `calibration.enabled`；`parameters_inference.yaml` → `use_calibration` |
| 改要 drop 的欄位 | 設定檔 | `parameters_dataset.yaml` → `prepare_model_input.drop_columns` |
| 改類別欄位 | 設定檔 | `parameters_dataset.yaml` → `prepare_model_input.categorical_columns` |
| 改特徵工程邏輯 | 程式碼 | `pipelines/dataset/nodes_pandas.py`（及 `nodes_spark.py`）→ `build_dataset` |
| 改前處理邏輯 | 程式碼 | `pipelines/preprocessing.py` |
| 改抽樣策略 | 程式碼 | `pipelines/dataset/nodes_pandas.py` → `select_keys` |
| 改模型演算法 | 程式碼 | 實作新的 `ModelAdapter`（參考 `models/lightgbm_adapter.py`），註冊到 `ADAPTER_REGISTRY` |
| 改超參搜尋策略 | 程式碼 | `pipelines/training/nodes.py` → `tune_hyperparameters` |
| 改評估指標 | 程式碼 | `pipelines/training/nodes.py` → `evaluate_model` |
| 改排序/過濾邏輯 | 程式碼 | `pipelines/inference/nodes_pandas.py` → `rank_predictions` |
| 改推論驗證規則 | 程式碼 | `pipelines/inference/validation.py` → `validate_predictions` |
| 切換 pandas/spark 後端 | 設定檔 | `catalog.yaml` 的 `backend` 欄位；或 `conf/production/parameters.yaml` → `backend: spark` |

---

## 執行方式與輸出

### Pipeline 執行

```bash
# Source ETL（獨立執行器，僅 production 環境需要）
python -m recsys_tfb --pipeline source_etl --env production --snap-dates 2024-01-31,2024-02-29
python -m recsys_tfb --pipeline source_etl --env production --snap-dates 2024-01-31 --restart-from feature_concat

# Dataset / Training / Inference（走 Node/Pipeline/Runner DAG 框架）
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local
python -m recsys_tfb --pipeline inference --env local --model-version ab12cd34  # 指定模型版本
python -m recsys_tfb -p dataset -e local  # 簡寫
```

**Source ETL 專用參數：**

| 參數 | 說明 |
|------|------|
| `--snap-dates` | 逗號分隔日期清單，指定要處理的快照日期（如 `2024-01-31,2024-02-29`） |
| `--restart-from` | 從指定表名重新開始（跳過之前的表） |

**版本解析邏輯：**

| Pipeline | dataset_version | model_version |
|----------|----------------|---------------|
| source_etl | —（不使用版本管理） | — |
| dataset | 自動計算（hash of parameters_dataset.yaml） | — |
| training | 讀取 `latest` symlink（或 `--dataset-version`） | 自動計算（hash of params + dataset_version） |
| inference | 從 model manifest 讀取（fallback: `latest`） | 讀取 `best` symlink（或 `--model-version`） |

### 模型評估

```bash
# 單一模型分析（產出 Plotly HTML 報告 + metrics.json）
python scripts/evaluate_model.py analyze <model_version> --snap-date 2025-12-31

# 兩個模型版本比較
python scripts/evaluate_model.py compare <model_a> <model_b> --snap-date 2025-12-31

# 模型 vs baseline 比較
python scripts/evaluate_model.py compare <model_version> --baseline global_popularity --snap-date 2025-12-31
```

- `model_version` 可使用版本 hash、`latest` 或 `best`
- `--k-values 3,5,10` 可自訂 K 值（預設 5, all）
- 報告輸出至 `data/evaluation/` 下對應版本目錄

### 模型晉升

```bash
python scripts/promote_model.py              # 自動選擇 mAP 最高版本
python scripts/promote_model.py <version>    # 指定版本
python scripts/promote_model.py --dry-run    # 預覽不執行
```

建立 `data/models/best` symlink，Inference pipeline 預設讀取此版本。

> **注意**：promote 為手動操作，請勿在自動化流程中執行。

### 測試

```bash
pytest                                        # 全部測試
pytest tests/ -v                              # verbose 輸出
pytest tests/test_core/test_config.py -v      # 單一測試檔
pytest tests/scenarios/ -v                    # 情境測試
```

---

## 客製化邊界

### 設定檔調整（不需改程式碼）

修改 `conf/base/parameters_*.yaml` 即可調整行為，改完重跑對應 pipeline 生效：

| 調整項目 | 設定檔 | 範例 |
|---------|--------|------|
| 抽樣率、分組、日期切分 | `parameters_dataset.yaml` | `sample_ratio: 0.1` 做快速驗證 |
| Optuna 試驗次數、迭代數、早停 | `parameters_training.yaml` | `n_trials: 100` 做更充分搜尋 |
| 推論日期、產品清單 | `parameters_inference.yaml` | 新增產品代碼到 `products` |
| 校準開關 | 三個 `parameters_*.yaml` | 各自獨立控制 |
| 隨機種子 | `parameters.yaml` | 換 `random_seed` 測試穩定性 |
| 資料路徑、後端 | `catalog.yaml` | 從 `pandas` 改為 `spark` |
| 環境覆蓋 | `conf/local/` 或 `conf/production/` | 生產環境指定 HDFS 路徑 |

### 節點邏輯修改（改 nodes）

Pipeline 節點都是純函數，可直接修改或替換：

| 要改的邏輯 | 修改檔案 | 函數 |
|-----------|----------|------|
| 特徵工程 | `pipelines/dataset/nodes_pandas.py`（及 `nodes_spark.py`） | `build_dataset` |
| 前處理（編碼、欄位篩選） | `pipelines/preprocessing.py` | `fit_preprocessor_metadata`、`transform_to_model_input` |
| 抽樣策略 | `pipelines/dataset/nodes_pandas.py` | `select_keys` |
| 超參搜尋空間/策略 | `pipelines/training/nodes.py` | `tune_hyperparameters` |
| 模型訓練邏輯 | `pipelines/training/nodes.py` | `train_model` |
| 機率校準 | `pipelines/training/nodes.py` | `calibrate_model` |
| 評估指標 | `pipelines/training/nodes.py` | `evaluate_model` |
| 排序/過濾邏輯 | `pipelines/inference/nodes_pandas.py` | `rank_predictions` |
| 推論驗證規則 | `pipelines/inference/validation.py` | `validate_predictions` |

> 如果同時支援雙後端，`nodes_pandas.py` 和 `nodes_spark.py` 需保持同步修改。

### 框架層（不建議改動）

以下模組構成 pipeline 執行的基礎設施。修改前請確認完全理解其影響：

| 模組 | 路徑 | 設計理由 |
|------|------|----------|
| 核心框架 | `core/node.py`, `pipeline.py`, `runner.py` | 提供聲明式 DAG 定義與執行，讓 pipeline 邏輯與執行順序解耦 |
| 設定載入 | `core/config.py` | 統一 YAML 合併語義，避免各處重複解析設定 |
| Catalog | `core/catalog.py` | 資料 I/O 與 pipeline 邏輯解耦，同一節點不需知道資料來源是本地還是 HDFS |
| 版本管理 | `core/versioning.py` | Hash-based 版本確保可追溯性——改參數就換版本，不會意外覆蓋舊結果 |
| Schema | `core/schema.py` | 集中管理欄位名稱，避免 hard-code 散落各處 |
| I/O 適配器 | `io/` | 隔離序列化格式差異，新增格式只需實作 `AbstractDataset` |
| 模型抽象 | `models/` | `ModelAdapter` ABC 讓訓練/推論邏輯不綁定特定模型框架 |
| Source ETL | `pipelines/source_etl/` | 獨立 SQLRunner，不走 Node/Pipeline/Runner DAG。負責從原始 Hive 表產出 feature/label/sample_pool |
| CLI | `__main__.py` | 統一入口，處理版本解析、symlink 更新、manifest 寫入 |

---

## 維運重點

### 關鍵不變量

這些一致性假設如果被破壞，會導致預測結果無效：

| 不變量 | 說明 | 後果 |
|--------|------|------|
| Preprocessor 一致 | Inference 使用的 preprocessor 必須與 Training 所用的 Dataset pipeline 產出相同 | 特徵編碼不一致 → 預測錯誤 |
| 產品清單一致 | `inference.products` 必須是 `category_mappings` 中出現過的產品子集 | 未知產品編碼為 -1 → 模型行為未定義 |
| Feature schema 一致 | Inference 時的 feature_table 欄位必須與 Dataset Building 時一致 | 缺欄位 → `ValueError`；多欄位 → 被忽略但應排查 |
| 特徵欄位順序 | Preprocessor 記錄了訓練時的欄位順序，推論時必須一致 | 順序不一致 → 特徵值被送到錯誤的模型節點 |

### 資料契約

#### Hard-coded 欄位依賴

| 分類 | 欄位 | 說明 |
|------|------|------|
| **Join key** | `snap_date`, `cust_id` | 所有 pipeline 的合併/分群基礎（hard-coded） |
| **可設定 drop 欄位** | 見 `drop_columns` 預設值 | 在 `prepare_model_input` 中移除，可透過 YAML 覆蓋 |
| **可設定 categorical 欄位** | 見 `categorical_columns` 預設值 | Integer encoding，可透過 YAML 覆蓋 |
| **Label** | `label` | 目標變數，0/1 整數 |

#### 日期格式

- 所有日期欄位須為 `datetime64[ns]`，字串 `"YYYY-MM-DD"` 會自動轉換
- `label` 必須為 0 或 1 的整數
- 數值特徵為 float

#### 隱含行為

- **Inference cross-join**：推論時將 `feature_table` 的客戶與 `inference.products` 做笛卡兒積
- **未知類別處理**：推論時遇到訓練未見過的類別值，編碼為 `-1`（log warning，不報錯）
- **mAP 分群**：評估時依 `(snap_date, cust_id)` 分群計算 AP
- **Train-dev 切分**：按 `cust_id` 切分（非 row-level），同一客戶的所有資料在同一 partition
- **Spark → pandas 轉換**：Training pipeline 內部統一轉 pandas，需要足夠的 driver memory

### 設計決策與理由

| 決策 | 理由 |
|------|------|
| Pipeline 分三段而非一段到底 | 允許獨立重跑——改參數不必重新建 dataset，改推論日期不必重新訓練 |
| 設定放 YAML、邏輯留 code | YAML 適合調參頻率高且無副作用的值；邏輯變更需要 code review 和測試 |
| Hash-based 版本管理 | 改參數 = 新版本 hash → 不會意外覆蓋舊版本；manifest 保留完整 metadata |
| 雙後端（pandas/spark） | 本地開發用 pandas 快速迭代；生產環境用 Spark 處理 ~10M 客戶規模 |
| 統一前處理（`preprocessing.py`） | 確保 dataset/training/inference 三條 pipeline 的前處理邏輯完全一致，避免 train-serve skew |
| ModelAdapter 抽象 | 目前只有 LightGBM，但預留替換模型的擴充點，不需改動 training/inference pipeline 邏輯 |
| Inference 後驗證（6 項 sanity check） | 在寫出結果前攔截異常——缺 row、NaN、分數超出範圍、rank 不連續等 |
| Symlink（latest/best）而非資料庫 | 離線環境無資料庫服務，symlink 是最輕量的「指向目前版本」機制 |

---

## 常見錯誤與排查

| 症狀 | 可能原因 | 排查方向 |
|------|---------|---------|
| `ValueError: Date split overlap detected` | `parameters_dataset.yaml` 中 train/calibration/val/test 日期有重疊 | 檢查 `train_snap_date_start/end`、`calibration_snap_dates`、`val_snap_dates`、`test_snap_dates` 是否互不重疊 |
| `ValueError: train_snap_date_start > train_snap_date_end` | 訓練日期範圍反轉 | 確認 start ≤ end |
| `ValueError: Missing feature columns in scoring dataset: [...]` | Inference 的 feature_table 缺少訓練時存在的欄位 | 比對 `preprocessor.pkl` 中的 `feature_columns` 與目前 feature_table 的欄位。通常是 feature_table schema 演進後未重建 dataset |
| `FileNotFoundError: ...best... Run training and promote a model first` | 未執行 promote 就跑 inference | 先執行 `python scripts/promote_model.py` 建立 `best` symlink |
| `FileNotFoundError: ...latest... Run the dataset pipeline first` | 未執行 dataset pipeline 就跑 training | 確認 `data/dataset/latest` symlink 存在 |
| WARNING: `Unknown categories found` | 推論資料出現訓練未見過的類別值 | 這些值會被編碼為 `-1`。若影響預測品質，考慮重新訓練或擴充訓練資料 |
| `ValidationError` (inference) | 推論結果未通過 sanity check | 檢視錯誤訊息中列出的失敗項目。常見：row count 不符（feature_table 缺客戶）、score 超出 [0,1]、rank 不連續 |
| LightGBM 訓練錯誤 | 訓練集可能為空（日期範圍內無資料）或特徵全為 NaN | 檢查指定日期在 sample_pool 和 feature_table 中是否有資料 |
| Model manifest 讀取 warning | Manifest 損壞或缺少 `dataset_version` key | Inference 會 fallback 到 `latest` dataset symlink（靜默降級）。建議修復 manifest 或重新訓練 |
| `category_mappings` 與 `inference.products` 不匹配 | 推論產品不在訓練時的產品集合中 | 比對 `data/dataset/<version>/category_mappings.json` 與 `parameters_inference.yaml` 的 `products` 清單 |

---

## 目前狀態與路線圖

### 已完成

- ✅ Kedro-inspired 核心框架（Node, Pipeline, Runner, Catalog, ConfigLoader）
- ✅ Config-driven 欄位 schema + 結構化日誌
- ✅ Source ETL Pipeline（獨立 SQLRunner、YAML 設定、dry-run、backfill、restart-from、source/output checks、Hive audit）
- ✅ Dataset Building Pipeline（分層抽樣、5-way 切分、特徵工程、雙後端）
- ✅ Training Pipeline（Optuna 超參搜尋、LightGBM 訓練、mAP 評估、MLflow 追蹤）
- ✅ Inference Pipeline（批次打分、preprocessor 複用、排序、6 項 sanity check 驗證、雙後端）
- ✅ Hash-based 版本管理（manifest、symlink latest/best）
- ✅ ModelAdapter 抽象（LightGBM adapter + adapter registry）
- ✅ 機率校準（CalibratedModelAdapter，isotonic/sigmoid）
- ✅ 5-way dataset split（train / train-dev / calibration / val / test）
- ✅ 統一前處理（dataset/training/inference 共用 `preprocessing.py`）
- ✅ 參數化日期範圍 + per-product 抽樣覆蓋
- ✅ 評估模組（mAP, nDCG, precision@K, recall@K, MRR + baselines + Plotly 報告 + 模型比較 CLI）
- ✅ pandas/PySpark 雙後端支援
- ✅ Strategy 1 MVP（單一二分類器 + mAP）

### 待完成

- ⬚ Source ETL Phase 2（per-column data quality rules、automatic failure resume、通知機制）
- ⬚ Evaluation Pipeline（獨立 pipeline 化）
- ⬚ 觀測性增強（data-quality profiling、artifact lineage）
- ⬚ 版本管理增強（manifest 擴充、版本 CLI、rollback）
- ⬚ Safe rerun checkpointing
- ⬚ Strategy 2-4（OVR、LambdaRank、兩級排序）

### 模型策略路線圖

| 策略 | 狀態 | 說明 |
|------|------|------|
| Strategy 1（MVP） | ✅ | 單一二分類器，產品名稱作為特徵，mAP 評估 |
| Strategy 2 | ⬚ | 每個產品獨立的 One-vs-Rest 分類器 |
| Strategy 3 | ⬚ | Strategy 1/2 + 排序層（如 LambdaRank） |
| Strategy 4 | ⬚ | Strategy 1/2 + 兩級排序（品類 → 子品類） |

---

## 技術棧

| 元件 | 版本 |
|------|------|
| Python | 3.10+ |
| PySpark | 3.3.2 |
| LightGBM | 4.6.0 |
| scikit-learn | 1.5.0 |
| MLflow | 3.1.0 |
| Optuna | 4.5.0 |
| pandas | 1.5.3 |
| numpy | 1.25.0 |
| pyarrow | 14.0.1 |
| Plotly | 5.17.0 |
| SHAP | 0.42.1 |
| Typer | 0.20.1 |
| joblib | 1.2.0 |
| Ploomber | 0.23.3 |
| pytest | 7.3.1 |
