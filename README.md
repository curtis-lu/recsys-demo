# recsys_tfb

商業銀行產品推薦排序模型。預測客戶對 22 個金融產品類別（信用卡、貸款、存款、基金、保險等）的興趣評分，幫助行銷 PM 確定觸達優先級。

- **推論**：每週批量評分，~1000 萬客戶 × 22 產品 × ~500 特徵
- **訓練**：月度快照（12 個月），按需執行
- **目標環境**：PySpark 3.3.2 on Hadoop/HDFS/Hive，Ploomber DAG 編排，無網路，CPU-only

## 架構

採用 Kedro 風格的框架設計：

```
Node → Pipeline → Runner → Catalog
```

- **Node**：封裝純函數的計算單元，宣告輸入/輸出
- **Pipeline**：節點的有向無環圖（DAG）
- **Runner**：拓撲排序後依序執行節點
- **Catalog**：資料 I/O 抽象層，支援 Parquet、Pickle、JSON 等適配器

### 4 條流水線

| 流水線 | 說明 |
|--------|------|
| Source Data ETL | SQL 驅動的特徵和標籤表建構（PySpark） |
| Dataset Building | 分層抽樣、訓練/驗證/測試集切分、特徵工程 |
| Training | Optuna 超參搜尋、MLflow 實驗追蹤、多維度評估 |
| Inference | 每週批量打分、月度監控，複用 ETL 和 Dataset 元件 |

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
- 未指定 `--env` 時，僅載入 `base/` 配置

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

#### Training 參數 (`parameters_training.yaml`)

| 參數 | 型別 | 預設值 | 說明 | 可調範圍 |
|------|------|--------|------|----------|
| `training.n_trials` | int | `20` | Optuna 超參搜尋試驗次數 | ≥ 1 |
| `training.num_iterations` | int | `500` | LightGBM boosting 迭代輪數 | ≥ 1 |
| `training.early_stopping_rounds` | int | `50` | 早停耐心值（驗證指標無改善的容忍輪數） | ≥ 1，且應 < `num_iterations` |
| `training.search_space.learning_rate` | {low, high} | `{0.01, 0.3}` | 學習率搜尋範圍 | 0.001 ~ 1.0 |
| `training.search_space.num_leaves` | {low, high} | `{16, 128}` | 葉節點數搜尋範圍 | 2 ~ 256 |
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
| `filepath` | 是 | 檔案路徑（本地相對路徑或 `hdfs:///` 絕對路徑） |
| `backend` | 否 | 僅 `ParquetDataset` 適用：`pandas`（預設）或 `spark` |

`backend` 差異：

- `pandas`：使用 pyarrow 讀寫，適合本地開發與中小資料量
- `spark`：使用 PySpark DataFrame 讀寫，適合生產環境大規模資料

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
  __main__.py       — CLI 入口（Typer）
  core/             — 框架核心（node, pipeline, runner, catalog, config）
  io/               — 資料集適配器（parquet, pickle, json）
  pipelines/        — 流水線模組（dataset, training）
  utils/            — 工具函數（Spark 等）

conf/               — YAML 配置 + SQL
tests/              — pytest 測試
data/               — 開發用合成資料
scripts/            — 輔助腳本
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
python -m recsys_tfb -p dataset -e local  # 簡寫
```

### 執行測試

```bash
pytest
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

| 策略 | 說明 |
|------|------|
| Strategy 1（MVP） | 單一二分類器，產品名稱作為特徵 |
| Strategy 2 | 每個產品獨立的 One-vs-Rest 分類器 |
| Strategy 3 | Strategy 1/2 + 排序層（如 LambdaRank） |
| Strategy 4 | Strategy 1/2 + 兩級排序（品類 → 子品類） |

目前從 Strategy 1 + mAP 評估開始。
