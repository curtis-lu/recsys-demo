# recsys_tfb

商業銀行產品推薦排序模型。預測客戶對 22 個金融產品類別（信用卡、貸款、存款、基金、保險等）的興趣評分，幫助行銷 PM 確定觸達優先級。

- **推論**：每週批量評分，~1000 萬客戶 × 22 產品 × ~500 特徵
- **訓練**：月度快照（12 個月），按需執行
- **目標環境**：PySpark 3.3.2 on Hadoop/HDFS/Hive，Ploomber DAG 編排，無網路，CPU-only

## 目前狀態

- ✅ Kedro-inspired 核心框架（Node, Pipeline, Runner, Catalog, ConfigLoader）
- ✅ Dataset Building Pipeline（分層抽樣、train/train-dev/val 切分、特徵工程）
- ✅ Training Pipeline（Optuna 超參搜尋、LightGBM 訓練、mAP 評估、MLflow 追蹤）
- ✅ Inference Pipeline（批次打分、preprocessor 複用、排序）
- ✅ Hash-based 版本管理（manifest、symlink latest/best）
- ✅ pandas/PySpark 雙後端支援
- ✅ Strategy 1 MVP（單一二分類器 + mAP）
- ✅ 欄位設定彈性化（drop_columns/categorical_columns 可透過 YAML 設定）
- ✅ Inference output 使用實際 model hash + latest symlink
- ⬚ Source Data ETL Pipeline
- ✅ 評估模組（mAP, nDCG, precision@K, recall@K, MRR + macro/micro avg + baselines + Plotly HTML 報告 + 模型比較 CLI）
- ⬚ Strategy 2-4

## 架構

採用 Kedro 風格的框架設計：

```
Node → Pipeline → Runner → Catalog
```

- **Node**：封裝純函數的計算單元，宣告輸入/輸出
- **Pipeline**：節點的有向無環圖（DAG），拓撲排序（Kahn's algorithm）
- **Runner**：依序執行節點
- **Catalog**：資料 I/O 抽象層，支援 Parquet、Pickle、JSON 等適配器

### 4 條流水線

| 流水線 | 狀態 | 說明 |
|--------|------|------|
| Source Data ETL | ⬚ | SQL 驅動的特徵和標籤表建構（PySpark） |
| Dataset Building | ✅ | 分層抽樣、train/train-dev/val 切分、特徵工程。雙後端。輸出版本化 Parquet |
| Training | ✅ | Optuna 超參搜尋、LightGBM 訓練、mAP 評估、MLflow 追蹤、版本比較 |
| Inference | ✅ | 每週批量打分，複用 Dataset Building 前處理邏輯。雙後端。結果依版本/日期分區 |

### 流水線相依關係

三條 pipeline 必須依序執行，後者依賴前者的產出：

```
feature_table ──┐                                              feature_table ──┐
label_table ────┤                                              preprocessor ───┤
                ▼                                              model ──────────┤
        ┌───────────────┐      ┌──────────────┐               ▼
        │    Dataset     │      │   Training   │      ┌──────────────┐
        │   Building     │─────▶│              │─────▶│  Inference   │
        └───────────────┘      └──────────────┘      └──────────────┘
              │                       │                       │
              ▼                       ▼                       ▼
     X/y_train, X/y_val       model, best_params      ranked_predictions
     preprocessor              evaluation_results
     category_mappings
```

**跨 pipeline 共享的 catalog dataset：**

| 共享 Dataset | 產出 Pipeline | 消費 Pipeline | 說明 |
|--------------|---------------|---------------|------|
| `feature_table` | （外部輸入） | Dataset, Inference | 客戶特徵表，~500 欄位 |
| `label_table` | （外部輸入） | Dataset | 客戶標籤表 |
| `X_train`, `y_train` | Dataset | Training | 訓練集特徵矩陣與標籤 |
| `X_train_dev`, `y_train_dev` | Dataset | Training | 開發集（用於 early stopping） |
| `X_val`, `y_val` | Dataset | Training | 驗證集特徵矩陣與標籤 |
| `val_set` | Dataset | Training | 驗證集原始 DataFrame（mAP 分組計算用） |
| `preprocessor` | Dataset | Inference | 特徵編碼器（categorical mapping、欄位清單等） |
| `category_mappings` | Dataset | （參考用） | 產品類別對照表 |
| `model` | Training | Inference | LightGBM Booster 模型 |

**版本串接機制：**

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

- Dataset pipeline 的產出存放在 `data/dataset/${dataset_version}/`
- Training pipeline 的產出存放在 `data/models/${model_version}/`
- Inference pipeline 的產出存放在 `data/inference/${model_version}/${snap_date}/`，其中 `${model_version}` 為實際 hash 值。Model 與 preprocessor 讀取仍透過 `best` symlink 解析
- Inference pipeline 完成後自動更新 `data/inference/latest` symlink
- `model_version` 依賴 `dataset_version`，確保模型可追溯到其訓練資料

**關鍵不變量（跨 pipeline 必須一致）：**

- `preprocessor` 必須一致：Inference 使用的 preprocessor 必須與 Training 所用的 Dataset pipeline 產出相同，否則特徵編碼不一致會導致預測錯誤
- 產品清單必須一致：`inference.products` 必須是訓練時 `category_mappings` 中出現過的產品子集
- `feature_table` schema 必須一致：Inference 時的 feature_table 欄位必須與 Dataset Building 時一致

### 資料契約

以下說明各 pipeline 對輸入資料的 schema 要求與 hard-coded 欄位依賴。接手或修改 pipeline 前，請確認輸入資料符合這些約束。

#### feature_table schema

| 欄位 | 型別 | 說明 | 程式碼依賴 |
|------|------|------|-----------|
| `snap_date` | datetime | 快照日期 | join key、日期過濾、分群（hard-coded） |
| `cust_id` | string | 客戶 ID | join key（hard-coded） |
| 其餘數值欄位 | float | 特徵 | 動態偵測（非 join key / 非 drop 清單的欄位都會成為模型特徵） |

- join key `["snap_date", "cust_id"]` 是 hard-coded（`dataset/nodes_pandas.py:74,78`、`dataset/nodes_spark.py:89,92`、`inference/nodes_pandas.py:30`）
- 數值特徵欄位名稱不是 hard-coded，程式碼透過排除法（drop 掉 join key + label 相關欄位後，剩餘皆為特徵）

#### label_table schema

| 欄位 | 型別 | 說明 | 程式碼依賴 |
|------|------|------|-----------|
| `snap_date` | datetime | 快照日期 | join key（hard-coded） |
| `cust_id` | string | 客戶 ID | join key（hard-coded） |
| `prod_name` | string | 產品名稱 | 唯一 categorical 欄位，integer encoding（hard-coded） |
| `label` | int (0/1) | 二分類標籤 | 目標變數（hard-coded） |
| `apply_start_date` | datetime | 申請起始日 | `prepare_model_input` 時 drop（hard-coded） |
| `apply_end_date` | datetime | 申請結束日 | `prepare_model_input` 時 drop（hard-coded） |
| `cust_segment_typ` | string | 客戶分群 | `prepare_model_input` 時 drop（hard-coded） |

#### Hard-coded 欄位依賴總結

| 分類 | 欄位 | 說明 |
|------|------|------|
| **Join key** | `snap_date`, `cust_id` | 所有 pipeline 的合併 / 分群基礎 |
| **可設定 drop 的欄位（預設）** | `snap_date`, `cust_id`, `label`, `apply_start_date`, `apply_end_date`, `cust_segment_typ` | 在 `prepare_model_input` 中移除。可透過 `parameters_dataset.yaml` 的 `prepare_model_input.drop_columns` 覆蓋 |
| **可設定 categorical 欄位（預設）** | `prod_name` | integer encoding。可透過 `parameters_dataset.yaml` 的 `prepare_model_input.categorical_columns` 覆蓋 |

#### 日期格式與資料型別

- 所有日期欄位須為 `datetime64[ns]`（pandas Timestamp），字串形式 `"YYYY-MM-DD"` 會自動轉換
- `label` 必須為 0 或 1 的整數
- 數值特徵欄位為 float

#### 其他隱含約束

- **Inference cross-join**：推論時會將 `feature_table` 的客戶與 `inference.products` 清單做笛卡兒積（`inference/nodes_pandas.py:24-27`）
- **未知類別處理**：推論時遇到訓練集沒見過的 `prod_name` 值，會編碼為 `-1`
- **mAP 分群**：評估時依 `(snap_date, cust_id)` 分群計算 AP，每個 group 有多個 `prod_name`（`training/nodes.py:50,204`）
- **特徵欄位順序**：preprocessor 記錄了訓練時的欄位順序，推論時必須一致

### 客製化指引

#### 設定檔調整（不需改程式碼）

修改 `conf/base/parameters_*.yaml` 即可調整行為，改完重跑 pipeline 就會生效：

| 檔案 | 可調內容 | 範例 |
|------|----------|------|
| `parameters_dataset.yaml` | 抽樣率、分組欄位、train/val 日期切分 | 把 `sample_ratio` 從 1.0 改為 0.1 進行快速驗證 |
| `parameters_training.yaml` | Optuna 試驗次數、LightGBM 迭代數/早停、超參搜尋範圍 | 增加 `n_trials` 到 100 做更充分的超參搜尋 |
| `parameters_inference.yaml` | 推論日期、產品清單 | 新增產品代碼到 `inference.products` |
| `parameters.yaml` | 專案名稱、隨機種子 | 換 `random_seed` 測試穩定性 |
| `catalog.yaml` | 資料路徑、後端（pandas/spark）、存儲格式 | 把 `backend` 從 `pandas` 改為 `spark` |
| `conf/local/` 或 `conf/production/` | 環境覆蓋 | 在 `production/catalog.yaml` 中指定 HDFS 路徑 |

> 注意：修改 `parameters_dataset.yaml` 會產生新的 `dataset_version`，後續 training 和 inference 都需重跑。修改 `parameters_training.yaml` 會產生新的 `model_version`，inference 需重跑。

#### 節點邏輯修改（改 nodes 檔案）

Pipeline 節點都是純函數，可直接修改或替換：

| 要改的邏輯 | 修改檔案 | 函數 |
|-----------|----------|------|
| 特徵工程（新增/刪除特徵） | `pipelines/dataset/nodes_pandas.py`（及 `nodes_spark.py`） | `prepare_model_input` |
| 抽樣策略 | `pipelines/dataset/nodes_pandas.py` | `select_sample_keys` |
| 資料切分方式 | `pipelines/dataset/nodes_pandas.py` | `split_keys` |
| 超參搜尋空間/策略 | `pipelines/training/nodes.py` | `tune_hyperparameters` |
| 模型訓練邏輯 | `pipelines/training/nodes.py` | `train_model` |
| 評估指標 | `pipelines/training/nodes.py` | `evaluate_model` |
| 打分前處理 | `pipelines/inference/nodes_pandas.py` | `apply_preprocessor` |
| 排序/過濾邏輯 | `pipelines/inference/nodes_pandas.py` | `rank_predictions` |

> 如果同時支援雙後端，`nodes_pandas.py` 和 `nodes_spark.py` 需保持同步修改。

#### 框架層（不建議改動）

以下模組構成 pipeline 執行的基礎設施，修改前請確認完全理解其影響：

| 模組 | 路徑 | 說明 |
|------|------|------|
| 核心框架 | `core/node.py`, `core/pipeline.py`, `core/runner.py` | Node/Pipeline/Runner 執行引擎 |
| 設定載入 | `core/config.py` | YAML 合併策略 |
| Catalog | `core/catalog.py` | 資料集註冊與模板變數解析 |
| 版本管理 | `core/versioning.py` | hash 計算、manifest 寫入、symlink 管理 |
| I/O 適配器 | `io/` | Parquet/Pickle/JSON 讀寫 |
| CLI | `__main__.py` | Typer 命令列入口 |

### 版本管理

- **Dataset version**：`SHA-256(parameters_dataset.yaml)` 前 8 碼
- **Model version**：`SHA-256(parameters_training.yaml + dataset_version)` 前 8 碼
- 每次執行產出 JSON **manifest**（記錄 pipeline metadata）
- **Symlink**：`latest`（最近執行）、`best`（晉升版本）
- Catalog 路徑使用模板變數：`${dataset_version}`、`${model_version}`、`${snap_date}`

### 雙後端支援

| 後端 | 使用場景 | 節點檔案 |
|------|----------|----------|
| pandas | 本地開發、中小資料量 | `nodes_pandas.py` |
| PySpark | 正式環境、大規模資料 | `nodes_spark.py` |

Dataset 和 Inference Pipeline 支援雙後端；Training Pipeline 僅使用 pandas（接收準備好的 numpy array）。

### 配置管理

```
conf/
├── base/           # 基礎配置（跨環境共享）
│   ├── catalog.yaml
│   ├── parameters.yaml
│   ├── parameters_dataset.yaml
│   ├── parameters_training.yaml
│   └── parameters_inference.yaml
├── local/          # 本地開發環境覆蓋
├── production/     # 生產環境配置
└── sql/            # ETL SQL 檔案
```

#### 環境覆蓋機制

- `base/` 存放跨環境共享的預設值，`local/` 和 `production/` 可覆蓋同名 key
- 合併語義：nested dict 遞迴合併（deep merge），scalar 值直接替換
- 透過 `--env` 參數切換環境：`python -m recsys_tfb -p dataset -e local`
- `--env` 預設值為 `local`，即未指定時載入 `base/` 再合併 `local/` 配置

#### 全域參數 (`parameters.yaml`)

| 參數 | 型別 | 預設值 | 說明 |
|------|------|--------|------|
| `project_name` | str | `recsys_tfb` | 專案名稱標識，用於 MLflow 實驗命名等 |
| `random_seed` | int | `42` | 全域隨機種子，影響抽樣、Optuna sampler、LightGBM 訓練 |

#### Dataset 參數 (`parameters_dataset.yaml`)

| 參數 | 型別 | 預設值 | 說明 | 可調範圍 |
|------|------|--------|------|----------|
| `dataset.sample_ratio` | float | `1.0` | 分層抽樣率，1.0 表示全量 | 0.0 ~ 1.0 |
| `dataset.sample_group_keys` | list[str] | `["snap_date"]` | 分層抽樣的分組欄位 | feature_table 中存在的欄位名 |
| `dataset.train_dev_snap_dates` | list[str] | `["2024-02-29"]` | 訓練集 + 開發集使用的快照日期 | feature_table 中存在的日期 |
| `dataset.val_snap_dates` | list[str] | `["2024-03-31"]` | 驗證集使用的快照日期 | feature_table 中存在的日期 |
| `prepare_model_input.drop_columns` | list[str] | `["snap_date", "cust_id", "label", "apply_start_date", "apply_end_date", "cust_segment_typ"]` | `prepare_model_input` 時要移除的欄位 | label_table 中存在的欄位名 |
| `prepare_model_input.categorical_columns` | list[str] | `["prod_name"]` | 需做 integer encoding 的類別欄位 | label_table 中存在的欄位名 |

#### Training 參數 (`parameters_training.yaml`)

| 參數 | 型別 | 預設值 | 說明 | 可調範圍 |
|------|------|--------|------|----------|
| `training.n_trials` | int | `20` | Optuna 超參搜尋試驗次數 | ≥ 1 |
| `training.num_iterations` | int | `500` | LightGBM boosting 迭代輪數 | ≥ 1 |
| `training.early_stopping_rounds` | int | `50` | 早停耐心值（驗證指標無改善的容忍輪數） | ≥ 1，且應 < `num_iterations` |
| `training.search_space.learning_rate` | {low, high} | `{0.001, 0.1}` | 學習率搜尋範圍 | 0.001 ~ 1.0 |
| `training.search_space.num_leaves` | {low, high} | `{4, 64}` | 葉節點數搜尋範圍 | 2 ~ 256 |
| `training.search_space.max_depth` | {low, high} | `{3, 8}` | 最大樹深搜尋範圍 | 1 ~ 16 |
| `training.search_space.min_child_samples` | {low, high} | `{5, 100}` | 葉節點最小樣本數搜尋範圍 | 1 ~ 1000 |
| `training.search_space.subsample` | {low, high} | `{0.6, 1.0}` | 行抽樣比例搜尋範圍 | 0.1 ~ 1.0 |
| `training.search_space.colsample_bytree` | {low, high} | `{0.6, 1.0}` | 列抽樣比例搜尋範圍 | 0.1 ~ 1.0 |
| `mlflow.experiment_name` | str | `recsys_tfb` | MLflow 實驗名稱 | 任意字串 |
| `mlflow.tracking_uri` | str | `mlruns` | MLflow tracking 儲存路徑 | 本地路徑或 MLflow server URI |

#### Inference 參數 (`parameters_inference.yaml`)

| 參數 | 型別 | 預設值 | 說明 | 可調範圍 |
|------|------|--------|------|----------|
| `inference.snap_dates` | list[str] | `["2024-03-31"]` | 推論使用的快照日期 | feature_table 中存在的日期 |
| `inference.products` | list[str] | `["bond", "fx", ...]` | 要打分的產品代碼列表 | 訓練時 category_mappings 中出現過的產品 |

#### Catalog 配置 (`catalog.yaml`)

每個 catalog entry 包含以下欄位：

| 欄位 | 必填 | 說明 |
|------|------|------|
| `type` | 是 | 資料集型別：`ParquetDataset`、`PickleDataset`、`JSONDataset` |
| `filepath` | 是 | 檔案路徑（本地相對路徑或 `hdfs:///` 絕對路徑），支援模板變數 |
| `backend` | 否 | 僅 `ParquetDataset` 適用：`pandas`（預設）或 `spark` |

Catalog 路徑支援模板變數替換：`${dataset_version}`、`${model_version}`、`${snap_date}`

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

#### 參數相依關係

| 約束條件 | 說明 |
|----------|------|
| `train_dev_snap_dates` ∩ `val_snap_dates` = ∅ | 訓練集與驗證集日期不可重疊，否則會造成資料洩漏 |
| `train_dev_snap_dates` ∪ `val_snap_dates` ⊆ feature_table 日期 | 所有指定日期必須存在於 feature_table 中 |
| `sample_ratio` < 1.0 時 | 各 split（train / train_dev / val）的資料量會等比縮小 |
| `random_seed` | 同時影響 `sample_ratio` 抽樣結果、Optuna sampler 初始化、LightGBM 訓練 seed |
| `inference.products` ⊆ category_mappings 的產品集合 | 推論產品必須是訓練時出現過的產品，否則模型無法產出有效預測 |
| `inference.snap_dates` ⊆ feature_table 日期 | 推論日期必須存在於 feature_table 中 |
| `early_stopping_rounds` < `num_iterations` | 否則早停永遠不會被觸發（訓練會跑完全部迭代） |
| `search_space.*.low` < `search_space.*.high` | 每個超參的搜尋下界必須嚴格小於上界 |

## 專案結構

```
src/recsys_tfb/
  __main__.py           — CLI 入口（Typer）
  core/
    config.py           — ConfigLoader（YAML base + env 合併）
    catalog.py          — DataCatalog（dataset registry & 路徑解析）
    node.py             — Node（函數封裝 + 命名 I/O）
    pipeline.py         — Pipeline（Kahn's algorithm 拓撲排序）
    runner.py           — Runner（依序執行）
    versioning.py       — Hash-based 版本管理、manifest、symlink
  io/
    base.py             — AbstractDataset 介面
    parquet_dataset.py  — ParquetDataset（pandas/spark 雙後端）
    pickle_dataset.py   — PickleDataset
    json_dataset.py     — JSONDataset
  pipelines/
    __init__.py         — Pipeline registry（get_pipeline, list_pipelines）
    dataset/            — Dataset building（nodes_pandas.py, nodes_spark.py, pipeline.py）
    training/           — Training（nodes.py, pipeline.py）
    inference/          — Inference（nodes_pandas.py, nodes_spark.py, pipeline.py）
  evaluation/
    metrics.py          — 排序指標（mAP, nDCG, precision@K, recall@K, MRR）
    distributions.py    — 分數/排名分布圖表
    calibration.py      — 校準曲線
    segments.py         — 客群/持有產品組合分析
    baselines.py        — 全域/客群熱門度 baseline
    report.py           — HTML 報告產生（Plotly 離線內嵌）
    compare.py          — 模型比較邏輯與視覺化
  utils/
    spark.py            — Spark 工具函數

conf/                   — YAML 配置 + SQL
scripts/
  generate_synthetic_data.py  — 產生合成假資料
  promote_model.py            — 模型版本晉升（手動觸發）
  evaluate_model.py           — 模型評估 CLI（analyze/compare）
tests/
  test_evaluation/      — 評估模組測試
  scripts/              — 腳本測試
data/                   — 開發用合成資料
```

## 快速開始

### 環境需求

- Python 3.10+

### 安裝

```bash
pip install -e ".[dev]"
```

### 執行流水線

```bash
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local
python -m recsys_tfb --pipeline inference --env local --model-version ab12cd34  # 指定模型版本
python -m recsys_tfb -p dataset -e local  # 簡寫
```

### 模型評估

```bash
# 單一模型分析（產出 Plotly HTML 報告 + metrics.json）
python scripts/evaluate_model.py analyze <model_version> --snap-date 2024-03-31

# 兩個模型版本比較（指標差異 + 分數分布比較）
python scripts/evaluate_model.py compare <model_a> <model_b> --snap-date 2024-03-31

# 模型 vs baseline 比較（global_popularity 或 segment_popularity）
python scripts/evaluate_model.py compare <model_version> --baseline global_popularity --snap-date 2024-03-31
```

- model_version 可使用版本 hash、`latest` 或 `best`
- `--k-values 3,5,10` 可自訂 K 值（預設 5, all）
- 報告輸出至 `data/evaluation/` 下對應版本目錄

### 模型晉升

```bash
python scripts/promote_model.py
```

### 執行測試

```bash
pytest
pytest tests/ -v
pytest tests/test_core/test_config.py -v  # 單一測試
```

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
| SHAP | 0.42.1 |
| Typer | 0.20.1 |
| pytest | 7.3.1 |

## 模型策略

| 策略 | 狀態 | 說明 |
|------|------|------|
| Strategy 1（MVP） | ✅ | 單一二分類器，產品名稱作為特徵，mAP 評估 |
| Strategy 2 | ⬚ | 每個產品獨立的 One-vs-Rest 分類器 |
| Strategy 3 | ⬚ | Strategy 1/2 + 排序層（如 LambdaRank） |
| Strategy 4 | ⬚ | Strategy 1/2 + 兩級排序（品類 → 子品類） |
