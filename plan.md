# 實作歷程與路線圖

本文件是實作狀態與開發路線圖的 source of truth。需求與範圍請參閱 [PRD.md](PRD.md)。

## Context

商業銀行產品推薦排序模型專案（recsys_tfb）的實作歷程記錄。記錄各階段的關鍵決策與具體實作步驟。

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
│   │   ├─ json_dataset.py      # JSONDataset（category_mappings 等）
│   │   └─ model_adapter_dataset.py  # ModelAdapterDataset（model + model_meta.json sidecar + calibrator.pkl）
│   ├─ pipelines/
│   │   ├─ __init__.py          # Pipeline registry (get_pipeline, list_pipelines)
│   │   ├─ source_etl/
│   │   │   ├─ __init__.py
│   │   │   ├─ models.py        # dataclass：TableConfig, SourceCheckConfig, AuditRecord
│   │   │   ├─ sql_renderer.py  # SQL 讀取 + 模板替換 + INSERT OVERWRITE 組裝
│   │   │   ├─ checks.py        # SourceChecker + OutputChecker
│   │   │   ├─ audit.py         # AuditWriter（Hive audit table + structured logging）
│   │   │   └─ sql_runner.py    # SQLRunner 核心類別
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
│   ├─ models/
│   │   ├─ __init__.py              # Exports ModelAdapter, get_adapter, ADAPTER_REGISTRY, LightGBMAdapter, CalibratedModelAdapter
│   │   ├─ base.py                  # ModelAdapter ABC, ADAPTER_REGISTRY, get_adapter() factory
│   │   ├─ lightgbm_adapter.py     # LightGBMAdapter
│   │   └─ calibrated_adapter.py   # CalibratedModelAdapter（isotonic/sigmoid 校準 wrapper）
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
│   │   ├─ parameters_inference.yaml
│   │   └─ parameters_source_etl.yaml
│   ├─ local/
│   │   ├─ catalog.yaml
│   │   └─ parameters_source_etl.yaml
│   ├─ production/
│   │   ├─ catalog.yaml
│   │   └─ parameters_source_etl.yaml
│   └─ sql/etl/                    # Source ETL SQL 檔案
│       ├─ feature/                # 特徵表 SQL
│       ├─ label/                  # 標籤表 SQL
│       └─ sample_pool/            # 抽樣池 SQL
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

### Phase 6：框架增強 ✅

- **Step 6.1** ✅ Catalog Memory Release — `MemoryDataset.release()` 方法、`DataCatalog.get_dataset()` 存取器、`DataCatalog._auto_created` 追蹤集合、Runner `_build_last_consumer_map()` 靜態方法 + 自動釋放邏輯（僅釋放 auto-created 的 pipeline 中間產物）、`dataset_released` structured log event
- **Step 6.2** ✅ Sample Pool 分離 — `conf/base/catalog.yaml` 新增 `sample_pool` ParquetDataset、`select_sample_keys` 輸入從 `label_table` 改為 `sample_pool`（pandas/spark 雙後端）、`pipeline.py` 接線更新、假資料產生 `data/sample_pool.parquet`
- **Step 6.3** ✅ Val Sampling — `parameters_dataset.yaml` 新增 `val_sample_ratio: 1.0`、`prepare_model_input` 加入 val set 可選分層抽樣（pandas/spark 雙後端）、group keys fallback 機制
- **Step 6.4** ✅ 整合驗證 — 假資料重新產生、全部測試通過（351 passed）、dataset pipeline 端到端驗證、training pipeline 端到端驗證

### Phase 7a：Inference Sanity Checks + Spark 優化 ✅

- **Step 7a.1** ✅ Inference sanity checks — 6 項驗證（row count、null score、score range、duplicate、product coverage、score variance）+ ValidationError
- **Step 7a.2** ✅ Spark 優化 — 移除不必要 `.count()` 呼叫、predict_scores 按 `(snap_date, prod_name)` 分片
- **Step 7a.3** ✅ ParquetDataset 分區寫入 — `partition_cols` 支援

### Phase 7b：演算法抽象 ✅

- **Step 7b.1** ✅ ModelAdapter ABC — `base.py`：train / predict / save / load / feature_importance / suggest_hyperparameters / log_to_mlflow 抽象方法、ADAPTER_REGISTRY、get_adapter() factory
- **Step 7b.2** ✅ LightGBMAdapter — 封裝 LightGBM API，實作所有 ModelAdapter 方法
- **Step 7b.3** ✅ ModelAdapterDataset — `model_meta.json` sidecar（adapter_class、algorithm、saved_at）、自動 registry lookup、向後相容舊 pickle 檔
- **Step 7b.4** ✅ Training/Inference nodes 重構 — 改用 adapter 介面，不直接依賴 LightGBM API
- **Step 7b.5** ✅ Config 擴充 — `training.algorithm`、`training.algorithm_params`（含 objective/metric）、`training.search_space`

### Phase 7.5：5-Way Dataset Split 重構 ✅

- **Step 7.5.1** ✅ 資料切割改為 5-way — train / train-dev / calibration（optional）/ validation / test
- **Step 7.5.2** ✅ train & train-dev 共用日期按 cust_id ratio 切分
- **Step 7.5.3** ✅ calibration optional — `enable_calibration` flag 控制
- **Step 7.5.4** ✅ sample_ratio_overrides — per-group 自訂比例（多欄位以 `|` 組合）
- **Step 7.5.5** ✅ Pipeline 條件式建構

### Phase 7.6：Dataset Pipeline 重構 ✅

- **Step 7.6.1** ✅ Train 日期參數化 — `train_snap_date_start` / `train_snap_date_end`
- **Step 7.6.2** ✅ sample_pool 改為 customer-month-product 粒度（加入 prod_name）
- **Step 7.6.3** ✅ 整併 select_sample_keys & select_calibration_keys 為通用 `select_keys` 函數
- **Step 7.6.4** ✅ sample_group_keys 支援 `(cust_segment_typ, prod_name)` 組合做 per-product 抽樣
- **Step 7.6.5** ✅ build_dataset 動態 join key — 含 prod_name 時按產品 join label_table
- **Step 7.6.6** ✅ ETL SQL for sample_pool — `conf/sql/etl/sample_pool/`

### Phase 7c：Probability Calibration ✅

- **Step 7c.1** ✅ CalibratedModelAdapter — `models/calibrated_adapter.py`：isotonic/sigmoid post-hoc calibration wrapper。實作 `fit_calibrator()`、`predict()`、`predict_uncalibrated()`、`save()`/`load()` with calibrator.pkl sidecar。不註冊 ADAPTER_REGISTRY（wrapper pattern）。測試：`test_calibrated_adapter.py`（8 tests）
- **Step 7c.2** ✅ ModelAdapterDataset calibration sidecar — `save()` 偵測 CalibratedModelAdapter 並寫入 `calibrated`/`calibration_method` 到 `model_meta.json`。`load()` 自動偵測 meta 中的 calibrated flag 並 wrap base adapter。向後相容舊 meta 檔案
- **Step 7c.3** ✅ Training nodes — `calibrate_model()` 節點 wrap model with CalibratedModelAdapter。`evaluate_model()` 增加 uncalibrated metrics comparison（偵測 CalibratedModelAdapter 時自動比較）。`log_experiment()` 記錄 calibration params + `uncalibrated_overall_map` metric 到 MLflow。Extract `_compute_ranking_metrics()` helper 避免重複程式碼
- **Step 7c.4** ✅ Training pipeline conditional node — `create_pipeline()` 接受 `enable_calibration` kwarg，條件式插入 `calibrate_model` node。`trained_model` 為中間產物，`model` 為最終輸出。`__main__.py` 從 `parameters_training.yaml` 讀取 `calibration.enabled`
- **Step 7c.5** ✅ Inference use_calibration flag — `predict_scores()` pandas/spark 雙後端支援 `use_calibration` config（預設 true）。當 flag=false 且 model 為 CalibratedModelAdapter 時呼叫 `predict_uncalibrated()`
- **Step 7c.6** ✅ Pipeline structure tests — 5 個測試驗證 `enable_calibration=True` 時 pipeline 結構（node count、calibrate_model presence、inputs、topological ordering）

### Phase 7d：Pipeline 重構：preprocessing 統一 ✅

（已合併於 Phase 7c 之後，細節見 git history）

### Phase 8a：Source ETL Pipeline ✅

- **Step 8a.1** ✅ models.py — `TableConfig`、`SourceCheckConfig`、`AuditRecord` dataclass + `from_dict()` factory
- **Step 8a.2** ✅ sql_renderer.py — SQL 讀取、`${var}` 模板替換、未解析變數偵測、`strip_header_comments()`、`build_insert_overwrite()` 組裝
- **Step 8a.3** ✅ checks.py — `SourceChecker`（partition exists / row count / schema drift）+ `OutputChecker`（row count / duplicate key ratio / null ratio）+ `CheckResult` dataclass
- **Step 8a.4** ✅ audit.py — `AuditWriter`（CREATE TABLE IF NOT EXISTS + INSERT INTO + structured logging + summary record）
- **Step 8a.5** ✅ sql_runner.py — `SQLRunner` 核心（config 解析、`_validate_order()` depends_on 驗證、主迴圈、dry-run 模式、restart_from 支援）
- **Step 8a.6** ✅ CLI 整合 — `__main__.py` 新增 `--snap-dates`、`--restart-from` 參數 + `source_etl` 分支（獨立執行器，不走 get_pipeline/Runner）
- **Step 8a.7** ✅ YAML 設定檔 — `parameters_source_etl.yaml`（base / local / production）
- **Step 8a.8** ✅ SQL 修改 — `feature_concat.sql`、`sample_pool.sql` 中繼表引用加 `${target_db}.` prefix
- **Step 8a.9** ✅ 測試 — 46 個單元/整合測試（models / sql_renderer / checks / audit / sql_runner）

## 待完成階段

| Phase | 名稱 | 內容 |
|-------|------|------|
| 8 | Evaluation Pipeline 化 | 獨立 evaluation pipeline（generate_predictions → compute_metrics → compute_baselines → generate_report）+ pipeline registry 註冊 + CLI 支援 `--pipeline evaluation` + catalog 新增 eval_predictions / eval_metrics / eval_report + Training pipeline evaluate_model node 保留（輕量 mAP 供 MLflow） |
| 9 | 可觀測性增強 | Data-quality profiling（`core/profiling.py` profile_dataframe() + Runner 自動呼叫 + `logging.profile_outputs` config 控制）+ Artifact/lineage logging（Catalog.save() 自動 emit `artifact_written` structured log event：filepath, dataset_type, upstream versions） |
| 10 | 版本管理增強 | manifest 擴充（git_commit_hash, library_versions, artifact_sizes, metrics_summary）+ 版本查詢 CLI（`versions list/show/diff` subcommand）+ rollback 機制（`promote_model.py --rollback`） |
| 11 | 去 hard-code 補完 + Tests | inference validation thresholds 參數化 + 剩餘 hard-coded 項盤點 + 新增 tests（test_models/, test_evaluation pipeline, test_profiling, test_model_adapter_dataset） |
| 12 | Safe rerun 檢查點 | 跳過已完成步驟，從失敗步驟接續執行 |
| — | Source ETL Phase 2 | Per-column data quality rules、automatic failure resume、更多 freshness checks、通知機制 |
| — | 記憶體優化 | 盤點 MemoryDataset 使用、大型中間產物改用 file-backed（目前實作已合理，僅在資料量大幅增加時需要） |
| — | 規則化重新排序 | rule-based reranking |
| — | 月度監控 | 機率值分佈監控、資料筆數檢查 |
| — | Strategy 2-4 | OVR 多模型、LambdaRank 排序、雙層排序 |
| — | 錯誤分析 notebook | template notebook |

## Phase 依賴關係

```
Phase 7b (演算法抽象) ✅
  ├── Phase 7.5 (5-Way Split) ✅
  │     └── Phase 7.6 (Dataset Pipeline 重構) ✅
  │           └── Phase 7c (Calibration) ✅
  │                 └── Phase 7d (Pipeline 重構：preprocessing 統一) ✅
  └── Phase 8 (Evaluation Pipeline) — 依賴 test split + ModelAdapter 介面
      └── Phase 11 (Tests) — 覆蓋上述所有新功能
Phase 8a (Source ETL) ✅ — 獨立，不依賴 Node/Pipeline/Runner
Phase 9 (可觀測性) — 獨立
Phase 10 (版本管理) — 獨立，可與 8 平行
```
