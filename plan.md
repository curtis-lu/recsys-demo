# 專案骨架 + MVP Pipeline 實作計畫

## Context

商業銀行產品推薦排序模型專案（recsys_tfb），採用 Kedro-inspired 自建輕量框架。已完成 MVP（Strategy 1 + mAP）、Inference Pipeline、hash-based 版本管理、pandas/PySpark 雙後端支援、Phase 1 修正（inference 版本修正 + 欄位彈性化）、config-driven column schema、以及 structured logging 框架。

## 已確認的關鍵決策


| 決策項目       | 選擇                                    |
| ---------- | ------------------------------------- |
| 框架         | Kedro-inspired 自建輕量框架 + Ploomber 編排   |
| 資料存取層      | 抽象 I/O 層，開發用本地 Parquet，正式環境用 Hive     |
| SQL ETL    | 正式環境才執行，開發環境用假 Parquet 資料             |
| 訓練資料處理     | PySpark 前處理 + pandas 訓練               |
| MLflow     | 本地檔案系統儲存                              |
| Config     | 環境分層 YAML（base/local/production）      |
| 推論         | PySpark UDF-free 批次推論（mapInPandas）    |
| 測試         | 本地 SparkSession + 小樣本 Parquet         |
| Safe rerun | 檢查點機制，但不在 MVP                         |
| 特徵工程       | SQL ETL 已完成大部分，Dataset Building 最小化轉換 |
| MVP 範圍     | Dataset Building + Training           |
| 開發順序       | 先骨架再 MVP                              |


## 專案結構

```
recsys_tfb/
├─ src/recsys_tfb/
│   ├─ __init__.py
│   ├─ __main__.py              # CLI 進入點 (Typer)
│   ├─ core/
│   │   ├─ __init__.py
│   │   ├─ config.py            # ConfigLoader
│   │   ├─ catalog.py           # DataCatalog
│   │   ├─ node.py              # Node
│   │   ├─ pipeline.py          # Pipeline
│   │   ├─ runner.py            # Runner（含結構化日誌事件）
│   │   ├─ schema.py            # get_schema() — config-driven 欄位名稱
│   │   ├─ logging.py           # RunContext、JsonFormatter、ConsoleFormatter、setup_logging
│   │   └─ versioning.py        # Hash-based 版本管理、manifest、symlink
│   ├─ io/
│   │   ├─ __init__.py
│   │   ├─ base.py              # AbstractDataset
│   │   ├─ parquet_dataset.py   # ParquetDataset（支援 pandas & PySpark）
│   │   ├─ pickle_dataset.py    # PickleDataset（模型檔等）
│   │   └─ json_dataset.py      # JSONDataset（category_mappings 等）
│   ├─ pipelines/
│   │   ├─ __init__.py          # Pipeline registry (get_pipeline, list_pipelines)
│   │   ├─ dataset/
│   │   │   ├─ __init__.py
│   │   │   ├─ nodes_pandas.py  # pandas 後端節點函數
│   │   │   ├─ nodes_spark.py   # PySpark 後端節點函數
│   │   │   └─ pipeline.py      # Pipeline 定義（backend 切換）
│   │   ├─ training/
│   │   │   ├─ __init__.py
│   │   │   ├─ nodes.py         # 純函數：調參、訓練、評估、記錄
│   │   │   └─ pipeline.py      # Pipeline 定義
│   │   └─ inference/
│   │       ├─ __init__.py
│   │       ├─ nodes_pandas.py  # pandas 後端節點函數
│   │       ├─ nodes_spark.py   # PySpark 後端節點函數
│   │       └─ pipeline.py      # Pipeline 定義（backend 切換）
│   ├─ evaluation/
│   │   ├─ __init__.py
│   │   ├─ metrics.py          # 排序指標（mAP, nDCG, precision@K, recall@K, MRR）
│   │   ├─ distributions.py    # 分數/排名分布圖表
│   │   ├─ calibration.py      # 校準曲線
│   │   ├─ segments.py         # 客群/持有產品組合分析
│   │   ├─ baselines.py        # 全域/客群熱門度 baseline
│   │   ├─ report.py           # HTML 報告產生（Plotly 離線內嵌）
│   │   └─ compare.py          # 模型比較邏輯與視覺化
│   └─ utils/
│       ├─ __init__.py
│       └─ spark.py             # SparkSession 建立等工具
├─ conf/
│   ├─ base/
│   │   ├─ catalog.yaml
│   │   ├─ parameters.yaml
│   │   ├─ parameters_dataset.yaml
│   │   ├─ parameters_training.yaml
│   │   └─ parameters_inference.yaml
│   ├─ local/
│   │   └─ catalog.yaml
│   └─ production/
│       └─ catalog.yaml
├─ scripts/
│   ├─ generate_synthetic_data.py   # 合成假資料產生
│   ├─ promote_model.py             # 模型版本晉升（手動觸發）
│   └─ evaluate_model.py            # 模型評估 CLI（analyze/compare）
├─ tests/
│   ├─ conftest.py
│   ├─ test_cli.py
│   ├─ test_core/
│   ├─ test_io/
│   ├─ test_evaluation/
│   ├─ test_pipelines/
│   └─ scripts/
├─ data/                        # 本地假資料
├─ pyproject.toml
└─ CLAUDE.md
```

## 已完成階段

### Phase 1：專案骨架 ✅

- **Step 1.1** ✅ 專案初始化 — `pyproject.toml`、目錄結構
- **Step 1.2** ✅ ConfigLoader — YAML base + env 深度合併
- **Step 1.3** ✅ I/O 抽象層 — AbstractDataset、ParquetDataset、PickleDataset
- **Step 1.4** ✅ DataCatalog — 根據 catalog.yaml 實例化 Dataset 物件
- **Step 1.5** ✅ Node / Pipeline / Runner — 拓撲排序（Kahn's algorithm）+ 依序執行
- **Step 1.6** ✅ CLI — Typer，`python -m recsys_tfb run --pipeline <name> --env <env>`
- **Step 1.7** ✅ Config YAML — catalog.yaml、parameters*.yaml

### Phase 2：Dataset Building Pipeline ✅

- **Step 2.1** ✅ 假資料準備 — `scripts/generate_synthetic_data.py`
- **Step 2.2** ✅ Dataset Building Nodes — select_sample_keys、split_keys、build_dataset（×3）、prepare_model_input
- **Step 2.3** ✅ Pipeline 定義 — 支援 pandas/spark 雙後端切換
- **Step 2.4** ✅ Config — parameters_dataset.yaml
- **Step 2.5** ✅ I/O 擴充 — JSONDataset（category_mappings）
- **Step 2.6** ✅ 測試 — test_pipelines/test_dataset/

### Phase 3：Training Pipeline ✅

- **Step 3.1** ✅ Training Nodes — tune_hyperparameters（Optuna）、train_model、evaluate_model（mAP）、log_experiment（MLflow）、compare_model_versions
- **Step 3.2** ✅ Pipeline 定義
- **Step 3.3** ✅ Config — parameters_training.yaml
- **Step 3.4** ✅ 測試 — test_pipelines/test_training/

### Phase 4：Inference Pipeline + 版本管理 ✅

- **Step 4.1** ✅ Inference Pipeline — build_scoring_dataset、apply_preprocessor、predict_scores、rank_predictions，支援 pandas/spark 雙後端
- **Step 4.2** ✅ Hash-based 版本管理 — SHA-256 hash 產生 dataset_version / model_version，manifest JSON、symlink（latest/best）
- **Step 4.3** ✅ 模型晉升腳本 — `scripts/promote_model.py`
- **Step 4.4** ✅ Catalog 模板變數 — `${dataset_version}`、`${model_version}`、`${snap_date}` 路徑替換
- **Step 4.5** ✅ 測試 — test_pipelines/test_inference/、test_core/test_versioning.py、scripts/test_promote_model.py

### Phase 4.5：修正已知問題 + 欄位彈性化 ✅

- **Step 4.5.1** ✅ README `--env` 文件修正
- **Step 4.5.2** ✅ Inference output 改用實際 model hash（非 `"best"`）
- **Step 4.5.3** ✅ Inference latest symlink 自動更新
- **Step 4.5.4** ✅ `prepare_model_input` 欄位設定彈性化（drop_columns / categorical_columns 移至 YAML）
- **Step 4.5.5** ✅ 測試驗證

### Phase 5：Config-driven Column Schema + Structured Logging ✅

- **Step 5.1** ✅ `core/schema.py` — `get_schema(parameters)` 純函數，從 `parameters.yaml` 的 `schema.columns` 讀取欄位名稱，預設值向後相容
- **Step 5.2** ✅ Dataset Building Pipeline 欄位替換 — `nodes_pandas.py`、`nodes_spark.py`、`pipeline.py` 所有 hard-coded 欄位改用 `get_schema()`
- **Step 5.3** ✅ Training Pipeline 欄位替換 — `training/nodes.py` 的 `evaluate_model` 改用 schema
- **Step 5.4** ✅ Inference Pipeline 欄位替換 — `nodes_pandas.py`、`nodes_spark.py`、`pipeline.py` 改用 schema
- **Step 5.5** ✅ Evaluation 模組欄位替換 — `metrics.py`、`baselines.py` 改用 schema（optional parameters 參數）
- **Step 5.6** ✅ `core/logging.py` — RunContext（含 run_id 產生）、JsonFormatter（JSON lines）、ConsoleFormatter（人類可讀）、setup_logging（從 config 設定）
- **Step 5.7** ✅ Runner 結構化日誌 — pipeline_started/completed/failed、node_started/completed/failed 事件，含 duration_seconds、status 等 extra 欄位
- **Step 5.8** ✅ CLI 整合 — `__main__.py` 改用 RunContext + setup_logging，run_id 寫入 manifest
- **Step 5.9** ✅ `conf/base/parameters.yaml` — 新增 `schema.columns` 與 `logging` section
- **Step 5.10** ✅ 測試 — `test_core/test_schema.py`（11 tests）、`test_core/test_logging.py`（10 tests）、既有測試全數通過

## 待完成階段

### Phase 5：Source Data ETL Pipeline

- SQL-based 資料轉換（PySpark），產出 feature table 與 label table
- SQL 檔案數量與執行順序由 YAML 設定
- 來源資料表新鮮度檢查
- 資料品質驗證（空值、重複、分佈等）
- 整合 Hive table 讀寫

### Phase 6：進階功能

- ✅ 進階評估指標：precision@K、recall@K、nDCG、MRR（evaluation/metrics.py）
- ✅ 指標切面：依整體、產品個別、自定義客群分群（evaluation/segments.py）
- ✅ Macro/micro average：分產品、分客群、分產品×客群
- ✅ Baseline 比較：全域/客群熱門度排序（evaluation/baselines.py）
- ✅ 模型比較 CLI：analyze + compare 子命令（scripts/evaluate_model.py）
- ✅ Plotly HTML 互動報告：離線可用、自包含（evaluation/report.py）
- ✅ 分數分布 + 排名分布 + 校準曲線視覺化（evaluation/distributions.py, calibration.py）
- ⬚ 規則化重新排序（rule-based reranking）
- ⬚ 月度監控 pipeline（機率值分佈監控、資料筆數檢查）
- ⬚ Safe rerun 檢查點機制

### Phase 7：進階策略

- Strategy 2：One-vs-Rest 多模型
- Strategy 3：Strategy 1/2 + 單層排序（LambdaRank）
- Strategy 4：Strategy 1/2 + 雙層排序（大類 → 中類）
- 錯誤分析 notebook template

## 驗證方式

```bash
# 安裝專案
pip install -e ".[dev]"

# 執行單元測試
pytest tests/ -v

# 執行特定測試
pytest tests/test_core/test_config.py -v

# 跑完整 pipeline
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local

# 模型晉升（手動觸發）
python scripts/promote_model.py

# 模型評估
python scripts/evaluate_model.py analyze <model_version> --snap-date 2024-03-31
python scripts/evaluate_model.py compare <model_version> --baseline global_popularity --snap-date 2024-03-31
```
