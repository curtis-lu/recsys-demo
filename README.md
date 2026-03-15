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
│   └── parameters_dataset.yaml
├── local/          # 本地開發環境覆蓋
├── production/     # 生產環境配置
└── sql/            # ETL SQL 檔案
```

環境配置透過 `--env` 參數切換，`local/` 覆蓋 `base/` 中的同名配置項。

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
