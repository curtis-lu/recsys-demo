# 專案骨架 + MVP Pipeline 實作計畫

## Context

這是一個全新的產品推薦排序模型專案（recsys_tfb），目前僅有 PRD 和設計原則文件，尚無任何程式碼。需要建立 Kedro-inspired 自建輕量框架作為基礎，然後實作 MVP（Dataset Building + Training Pipeline，Strategy 1 + mAP）。

## 已確認的關鍵決策

| 決策項目 | 選擇 |
|---------|------|
| 框架 | Kedro-inspired 自建輕量框架 + Ploomber 編排 |
| 資料存取層 | 抽象 I/O 層，開發用本地 Parquet，正式環境用 Hive |
| SQL ETL | 正式環境才執行，開發環境用假 Parquet 資料 |
| 訓練資料處理 | PySpark 前處理 + pandas 訓練 |
| MLflow | 本地檔案系統儲存 |
| Config | 環境分層 YAML（base/local/production） |
| 推論 | PySpark UDF-free 批次推論（mapInPandas） |
| 測試 | 本地 SparkSession + 小樣本 Parquet |
| Safe rerun | 檢查點機制，但不在 MVP |
| 特徵工程 | SQL ETL 已完成大部分，Dataset Building 最小化轉換 |
| MVP 範圍 | Dataset Building + Training |
| 開發順序 | 先骨架再 MVP |

## 專案結構

```
recsys_tfb/
├─ src/recsys_tfb/
│   ├─ __init__.py
│   ├─ __main__.py              # CLI 進入點 (typer)
│   ├─ core/
│   │   ├─ __init__.py
│   │   ├─ config.py            # ConfigLoader
│   │   ├─ catalog.py           # DataCatalog
│   │   ├─ node.py              # Node
│   │   ├─ pipeline.py          # Pipeline
│   │   └─ runner.py            # Runner
│   ├─ io/
│   │   ├─ __init__.py
│   │   ├─ base.py              # AbstractDataset
│   │   ├─ parquet_dataset.py   # ParquetDataset（支援 pandas & PySpark）
│   │   ├─ hive_dataset.py      # HiveDataset
│   │   └─ pickle_dataset.py    # PickleDataset（模型檔等）
│   ├─ pipelines/
│   │   ├─ __init__.py
│   │   ├─ dataset/
│   │   │   ├─ __init__.py
│   │   │   ├─ nodes.py         # 純函數：抽樣、切分、prepare
│   │   │   └─ pipeline.py      # Pipeline 定義
│   │   └─ training/
│   │       ├─ __init__.py
│   │       ├─ nodes.py         # 純函數：訓練、評估、記錄
│   │       └─ pipeline.py      # Pipeline 定義
│   └─ utils/
│       ├─ __init__.py
│       └─ spark.py             # SparkSession 建立等工具
├─ conf/
│   ├─ base/
│   │   ├─ catalog.yaml
│   │   ├─ parameters.yaml
│   │   ├─ parameters_dataset.yaml
│   │   └─ parameters_training.yaml
│   ├─ local/
│   │   └─ catalog.yaml
│   └─ production/
│       └─ catalog.yaml
├─ sql/                         # ETL SQL 檔案（後續開發）
├─ tests/
│   ├─ conftest.py              # SparkSession fixture
│   ├─ test_core/
│   ├─ test_io/
│   └─ test_pipelines/
├─ data/                        # 本地假資料（後續由使用者提供 SQL 產出）
├─ docs/
├─ pyproject.toml               # 專案定義與依賴
└─ CLAUDE.md
```

## 實作計畫

### Phase 1：專案骨架

**Step 1.1 - 專案初始化**
- 建立 `pyproject.toml`（定義套件、依賴、pytest 設定）
- 建立目錄結構和 `__init__.py`

**Step 1.2 - ConfigLoader**
- `src/recsys_tfb/core/config.py`
- 載入 `conf/{env}/` 下所有 YAML 檔
- base 設定 + env 設定深度合併（env 覆寫 base）
- 提供 `get_catalog_config()`, `get_parameters()` 方法
- 測試：`tests/test_core/test_config.py`

**Step 1.3 - I/O 抽象層**
- `src/recsys_tfb/io/base.py`：`AbstractDataset` 抽象類別，定義 `load()` / `save()` / `exists()`
- `src/recsys_tfb/io/parquet_dataset.py`：`ParquetDataset`，支援 pandas 和 PySpark 讀寫
- `src/recsys_tfb/io/pickle_dataset.py`：`PickleDataset`，用於模型檔和 preprocessor
- 測試：`tests/test_io/`

**Step 1.4 - DataCatalog**
- `src/recsys_tfb/core/catalog.py`
- 根據 catalog.yaml 的定義實例化對應的 Dataset 物件
- 提供 `load(name)` / `save(name, data)` / `exists(name)` 方法
- 測試：`tests/test_core/test_catalog.py`

**Step 1.5 - Node / Pipeline / Runner**
- `Node`：封裝函數 + 輸入/輸出名稱
- `Pipeline`：Node 集合，拓撲排序解析執行順序
- `Runner`：從 DataCatalog 取資料 → 執行 Node → 存回 DataCatalog，附帶日誌和計時
- 測試：`tests/test_core/`

**Step 1.6 - CLI**
- `src/recsys_tfb/__main__.py`：使用 Typer
- `python -m recsys_tfb run --pipeline <name> --env <env>`
- 測試：手動驗證 CLI help

**Step 1.7 - Config YAML 檔案**
- `conf/base/catalog.yaml`：定義所有資料集
- `conf/base/parameters.yaml`：全域參數
- `conf/local/catalog.yaml`：開發環境覆寫（Parquet 路徑）

### Phase 2：Dataset Building Pipeline

**Step 2.1 - 假資料準備**
- `data/` 下建立最小樣本 Parquet 檔（feature_table, label_table）
- label_table 欄位：snap_date, cust_id, cust_segment_typ, apply_start_date, apply_end_date, label, prod_name

**Step 2.2 - Dataset Building Nodes**
- `select_sample_keys(label_table, params) → sample_keys`：從 label table 取 key 欄位做分層抽樣，group by 欄位由 YAML `sample_group_keys` 設定
- `split_keys(sample_keys, label_table, params) → train_keys, train_dev_keys, val_keys`：依時間切分為三組（互不重疊），train_dev 有抽樣，val 為全量
- `build_dataset(keys, feature_table, label_table) → dataset`：join 完整特徵（共用函數，分別建立 train_set, train_dev_set, val_set）
- `prepare_model_input(train_set, train_dev_set, val_set, params) → X_train, y_train, X_train_dev, y_train_dev, X_val, y_val, preprocessor, category_mappings`

**Step 2.3 - Pipeline 定義**
- `src/recsys_tfb/pipelines/dataset/pipeline.py`：組裝 nodes 為 Pipeline（7 nodes）

**Step 2.4 - Config**
- `conf/base/parameters_dataset.yaml`：sample_group_keys、sample_ratio、train_dev_snap_dates、val_snap_dates

**Step 2.5 - I/O 擴充**
- `src/recsys_tfb/io/json_dataset.py`：JSONDataset，用於儲存 category_mappings
- `conf/base/catalog.yaml`：新增 category_mappings 條目

**Step 2.6 - 測試**
- `tests/test_pipelines/test_dataset/`：每個 node 的單元測試

### Phase 3：Training Pipeline

**Step 3.1 - Training Nodes**
- `train_model(X_train, y_train, params) → model`：LightGBM 二元分類
- `tune_hyperparameters(X_train, y_train, X_val, y_val, params) → best_params, tuning_results`：Optuna 搜尋
- `train_final_model(X_train, y_train, best_params) → final_model`
- `predict(model, X_train, X_val) → train_preds, val_preds`
- `evaluate(train_preds, val_preds, y_train, y_val, params) → metrics`：mAP（MVP 先做此指標）
- `log_experiment(model, params, metrics) → run_id`：MLflow 記錄
- `register_model(run_id, params) → model_version`

**Step 3.2 - Pipeline 定義**
- `src/recsys_tfb/pipelines/training/pipeline.py`

**Step 3.3 - Config**
- `conf/base/parameters_training.yaml`：模型參數、調參設定、評估設定

**Step 3.4 - 測試**
- `tests/test_pipelines/test_training/`

### Phase 4：端到端驗證

**Step 4.1 - 整合測試**
- 用假資料跑完 dataset → training 完整流程
- 確認 MLflow 記錄正確、模型檔可載入、metrics 有輸出

**Step 4.2 - 更新 CLAUDE.md**
- 加入 build/test/run 指令

## 驗證方式

```bash
# 安裝專案
pip install -e ".[dev]"

# 執行單元測試
pytest tests/ -v

# 執行特定測試
pytest tests/test_core/test_config.py -v

# 跑完整 MVP pipeline
python -m recsys_tfb run --pipeline dataset --env local
python -m recsys_tfb run --pipeline training --env local

# 檢查 MLflow 記錄
mlflow ui  # 開啟 MLflow UI 查看實驗結果
```
