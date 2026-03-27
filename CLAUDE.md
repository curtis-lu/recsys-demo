# CLAUDE.md

Claude Code 在此 repo 工作時的最小規範。

## Project Overview

商業銀行產品推薦排序模型。預測客戶對 22 類金融產品的興趣分數，供行銷 PM 排序推薦優先順序。

- **Inference**：每週批次推論，~10M 客戶 × 22 產品 × ~500 特徵
- **Training**：12 個月月底快照，不定期手動執行
- **Target environment**：PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Source of Truth

| 文件 | 職責 |
|------|------|
| **[PRD.md](PRD.md)** | 需求與範圍：專案目標、產品清單、功能需求、技術要求、設計原則 |
| **[plan.md](plan.md)** | 實作狀態與路線圖：關鍵決策、專案結構、已完成/待完成 Phase、step breakdown |
| **[kedro_design_philosophy.md](kedro_design_philosophy.md)** | Kedro-inspired 設計哲學與最佳實踐 |

> 若 CLAUDE.md 與上述文件衝突，以 source-of-truth 文件為準。

## Tech Stack

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1 | Typer 0.20.1

## Commands

```bash
# Install
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run pipelines
python -m recsys_tfb --pipeline source_etl --env local --snap-dates 2024-01-31,2024-02-29
python -m recsys_tfb --pipeline source_etl --env local --snap-dates 2024-01-31 --restart-from feature_concat
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local
python -m recsys_tfb --pipeline inference --env local --model-version ab12cd34

# Promote model (manual trigger, do not run automatically)
python scripts/promote_model.py
```

## Versioning

- **Dataset version**：`hash(parameters_dataset.yaml)` → 前 8 hex chars
- **Model version**：`hash(parameters_training.yaml + dataset_version)` → 前 8 hex chars
- 每個版本產出 JSON **manifest**；**symlinks**：`latest`（最近一次）、`best`（已晉升）
- Catalog 路徑使用模板變數：`${dataset_version}`、`${model_version}`、`${snap_date}`

## Dual Backend

- `nodes_pandas.py`（local dev）/ `nodes_spark.py`（production），由 `catalog.yaml` 的 `backend` 欄位控制
- Dataset 與 Inference pipelines 支援雙後端；Training pipeline 內部統一轉 pandas

## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation
- Dev 環境使用合成假資料取代 Hive tables
- Storage：Parquet（local dev）/ Parquet on HDFS（production）
