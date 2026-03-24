# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Product recommendation ranking model for a commercial bank. Predicts customer interest scores across 22 financial product categories (credit cards, loans, deposits, funds, insurance, etc.) to help marketing PMs prioritize outreach.

- **Inference**: weekly batch scoring for ~10M customers x 22 products x ~500 features
- **Training**: monthly snapshots (12 months), run on-demand
- **Target environment**: PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG orchestration, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Related Documents

- **[PRD.md](PRD.md)** — 產品需求文件：專案目標、產品清單、4大 pipeline 功能需求、技術要求、設計原則
- **[plan.md](plan.md)** — 實作歷程記錄：關鍵決策、各 Phase 的具體實作步驟（Step breakdown）
- **[kedro_design_philosophy.md](kedro_design_philosophy.md)** — 設計哲學：Kedro-inspired 架構原則與最佳實踐

## Tech Stack & Versions

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1 | Typer 0.20.1

## Current Implementation Status

- ✅ Kedro-inspired core framework (Node, Pipeline, Runner, Catalog, ConfigLoader)
- ✅ I/O adapters: ParquetDataset, PickleDataset, JSONDataset (with pandas/spark dual backend)
- ✅ Dataset Building Pipeline (stratified sampling, train/train-dev/val splits, feature engineering)
- ✅ Training Pipeline (Optuna hyperparameter tuning, LightGBM training, mAP evaluation, MLflow logging, model version comparison)
- ✅ Inference Pipeline (batch scoring, preprocessor reuse, ranking, actual model hash for output paths)
- ✅ Hash-based artifact versioning with manifests and symlinks (latest/best)
- ✅ Dual backend support: pandas (dev) / PySpark (production)
- ✅ CLI entry point with `--pipeline`, `--env`, `--dataset-version`, `--model-version` options
- ✅ Model promotion script (`scripts/promote_model.py`)
- ✅ Synthetic data generator (`scripts/generate_synthetic_data.py`)
- ✅ Comprehensive test suite
- ✅ Evaluation module (mAP, nDCG, precision@K, recall@K, MRR + macro/micro avg + baselines + Plotly HTML reports + model comparison CLI)
- ✅ Config-driven column schema (`get_schema()` from `parameters.yaml`, all pipelines use dynamic column names)
- ✅ Structured logging (Pipeline-level, Node-level JSON structured events, RunContext with run_id, dual console+file output)
- ✅ 欄位設定彈性化（drop_columns/categorical_columns 可透過 YAML 設定）
- ✅ Inference output 使用實際 model hash + latest symlink
- ✅ 框架增強（Catalog memory release、Sample pool 分離、Val sampling）
- ✅ Inference sanity checks（6 項驗證 + ValidationError）、Spark 優化（移除不必要 .count()、分片粒度細化）、ParquetDataset 分區寫入
- ✅ 演算法抽象（ModelAdapter ABC + LightGBMAdapter + ModelAdapterDataset I/O + training/inference nodes 重構為 adapter 介面）
- ⬚ Probability calibration layer（可選 isotonic/sigmoid，CalibratedModelAdapter wrapper）
- ⬚ Evaluation pipeline 化（獨立 pipeline：generate_predictions → compute_metrics → compute_baselines → generate_report）
- ⬚ Data-quality profiling（core/profiling.py + Runner 自動呼叫 + config 控制）
- ⬚ Artifact/lineage logging（Catalog.save 自動記錄 structured log event）
- ⬚ 版本管理增強（manifest 擴充 git_commit_hash/library_versions/artifact_sizes、版本查詢 CLI list/show/diff、rollback 機制）
- ⬚ Inference validation thresholds 參數化
- ⬚ Safe rerun 檢查點
- ⬚ Source Data ETL Pipeline
- ⬚ Strategy 2-4、規則化重新排序、月度監控、錯誤分析 notebook

## Architecture: 5 Pipelines

1. **Source Data ETL** *(not yet implemented)* - SQL-based transforms (PySpark) producing feature and label tables. SQL files defined and ordered via YAML config.
2. **Dataset Building** ✅ - Stratified sampling, train/train-dev/val splits, feature engineering. Outputs versioned Parquet files. Preprocessing logic reused in inference without data leakage. Dual pandas/spark backend.
3. **Training** ✅ - Optuna hyperparameter search, ModelAdapter-based training (config-driven algorithm selection), mAP evaluation, MLflow experiment tracking, model version comparison. Outputs versioned model artifacts. *(planned: probability calibration)*
4. **Inference** ✅ - Weekly batch scoring reusing dataset building preprocessing. Results partitioned by `${model_version}/${snap_date}`. Dual pandas/spark backend.
5. **Evaluation** *(planned)* - 獨立的模型評估 pipeline，針對指定 model_version 進行完整分析（metrics、baselines、calibration comparison、HTML report）。

## Project Structure

```
src/recsys_tfb/
  __main__.py           — CLI entry point (Typer)
  core/
    config.py           — ConfigLoader (YAML base + env merge)
    catalog.py          — DataCatalog (dataset registry & resolution)
    node.py             — Node (function wrapper with named I/O)
    pipeline.py         — Pipeline (topological sort via Kahn's algorithm)
    runner.py           — Runner (sequential execution, structured log events)
    versioning.py       — Hash-based versioning, manifests, symlinks
    schema.py           — get_schema() config-driven column names
    logging.py          — RunContext, JsonFormatter, ConsoleFormatter, setup_logging
  io/
    base.py             — AbstractDataset interface
    parquet_dataset.py  — ParquetDataset (pandas/spark dual backend)
    pickle_dataset.py   — PickleDataset
    json_dataset.py     — JSONDataset
    model_adapter_dataset.py — ModelAdapterDataset (model + model_meta.json sidecar)
  pipelines/
    __init__.py         — Pipeline registry (get_pipeline, list_pipelines)
    dataset/            — Dataset building (nodes_pandas.py, nodes_spark.py, pipeline.py)
    training/           — Training (nodes.py, pipeline.py)
    inference/          — Inference (nodes_pandas.py, nodes_spark.py, pipeline.py)
    evaluation/         — Evaluation pipeline (planned: nodes.py, pipeline.py)
  models/
    __init__.py         — Exports ModelAdapter, get_adapter, ADAPTER_REGISTRY, LightGBMAdapter
    base.py             — ModelAdapter ABC, ADAPTER_REGISTRY, get_adapter() factory
    lightgbm_adapter.py — LightGBMAdapter (train/predict/save/load/feature_importance/log_to_mlflow)
  evaluation/
    metrics.py          — Ranking metrics (mAP, nDCG, precision@K, recall@K, MRR)
    distributions.py    — Score/rank distribution plots
    calibration.py      — Calibration curves
    segments.py         — Segment/holding-combo analysis
    baselines.py        — Global/segment popularity baselines
    report.py           — HTML report generation (Plotly, offline-capable)
    compare.py          — Model comparison logic and visualizations
  utils/
    spark.py            — Spark utilities

conf/
  base/                 — Shared config (catalog.yaml, parameters*.yaml)
  local/                — Local dev overrides
  production/           — Production overrides
  sql/                  — ETL SQL files (future)

scripts/
  generate_synthetic_data.py  — Generate dev test data
  promote_model.py            — Promote model version (manual trigger)
  evaluate_model.py           — 模型評估 CLI（analyze 單一模型分析 / compare 模型間或 vs baseline 比較）

tests/                  — pytest test suite
data/                   — Local synthetic data (Parquet)
```

## Build / Test / Run Commands

```bash
# Install (with dev dependencies)
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run specific pipeline
python -m recsys_tfb --pipeline dataset --env local
python -m recsys_tfb --pipeline training --env local
python -m recsys_tfb --pipeline inference --env local
python -m recsys_tfb --pipeline inference --env local --model-version ab12cd34  # 指定模型版本

# Promote a model version to "best" symlink (manual trigger, do not run automatically)
python scripts/promote_model.py
```

## Versioning

Artifact versioning uses deterministic SHA-256 hashes of pipeline parameters:

- **Dataset version**: `hash(parameters_dataset.yaml)` → first 8 hex chars
- **Model version**: `hash(parameters_training.yaml + dataset_version)` → first 8 hex chars
- Each version produces a JSON **manifest** recording pipeline metadata
- **Symlinks**: `latest` points to most recent run, `best` points to promoted version
- Catalog paths use template variables: `${dataset_version}`, `${model_version}`, `${snap_date}`

## Dual Backend

Pipeline nodes have separate implementations for pandas and PySpark:

- `nodes_pandas.py` — used in local dev (`backend: pandas` in catalog)
- `nodes_spark.py` — used in production (`backend: spark` in catalog)
- Backend is selected per-dataset in `catalog.yaml` and per-pipeline via `create_pipeline(backend=...)` in pipeline registry
- Dataset and Inference pipelines support dual backend; Training pipeline runs on pandas only (receives prepared numpy arrays)

## Model Strategies

- **Strategy 1** (MVP) ✅: Single binary classifier, product name as feature. Evaluation: mAP only.
- **Strategy 2** *(planned)*: One-vs-rest per product
- **Strategy 3** *(planned)*: Strategy 1/2 + single ranking layer (e.g., LambdaRank)
- **Strategy 4** *(planned)*: Strategy 1/2 + two-tier ranking (category then subcategory)

## Design Philosophy (Kedro-inspired)

Follow the principles in `kedro_design_philosophy.md`. Key rules:

- **Separate transformation from I/O** - pure functions for logic, config/adapters for storage
- **Pipeline-oriented** - explicit nodes with defined inputs/outputs, not monolithic scripts
- **Externalize configuration** - all parameters, paths, thresholds in YAML config files
- **Reproducibility** - deterministic transforms, versioned artifacts, explicit dependencies
- **Safe rerun** - pipelines must handle interruption; skip completed steps when possible
- **Observability** - structured logging, step timing, data volume tracking, Spark config recording
- **No data leakage** - shared preprocessing logic between training and inference, but keep training-only and inference-only concerns separate

## Development Approach

Build incrementally per the PRD:

1. ~~Minimal working version first (Strategy 1 + mAP)~~ ✅ Done
2. Add features one at a time
3. Test after each addition
4. Skip error analysis notebooks initially

## Development Roadmap

### 已完成

| Phase | 名稱 | 內容 |
|-------|------|------|
| 1 | 專案骨架 ✅ | pyproject.toml、ConfigLoader、I/O 抽象層、DataCatalog、Node/Pipeline/Runner、CLI、Config YAML |
| 2 | Dataset Building Pipeline ✅ | 假資料、nodes（pandas/spark）、Pipeline 定義、parameters_dataset.yaml、JSONDataset、測試 |
| 3 | Training Pipeline ✅ | Optuna 調參、LightGBM 訓練、mAP 評估、MLflow 記錄、Pipeline 定義、測試 |
| 4 | Inference Pipeline + 版本管理 ✅ | 推論 nodes（pandas/spark）、Hash-based 版本管理、模型晉升、Catalog 模板變數、測試 |
| 4.5 | 修正已知問題 + 欄位彈性化 ✅ | README 修正、inference output 改用實際 model hash、欄位抽取到 YAML |
| 5 | Config-driven schema + Structured logging ✅ | `get_schema()` 取代 hard-coded 欄位、RunContext + 雙輸出、Runner 結構化事件 |
| 6 | 框架增強 ✅ | Catalog memory release、Sample pool 分離、Val sampling |
| 7a | Inference Sanity Checks + Spark 優化 ✅ | 6 項 sanity checks（ValidationError）、Spark .count() 移除、predict_scores 按 (snap_date, prod_name) 分片、ParquetDataset partition_cols 支援 |
| 7b | 演算法抽象 ✅ | ModelAdapter ABC + LightGBMAdapter + ModelAdapterDataset I/O（model_meta.json sidecar、向後相容）+ training/inference nodes 重構為 adapter 介面 + config 擴充（algorithm, algorithm_params） |

### 待完成

| Phase | 名稱 | 內容 |
|-------|------|------|
| 7c | Probability Calibration | CalibratedModelAdapter wrapper（可選 isotonic/sigmoid）+ `parameters_training.yaml` calibration section + MLflow 條件式 log_model |
| 8 | Evaluation Pipeline 化 | 獨立 evaluation pipeline（generate_predictions → compute_metrics → compute_baselines → generate_report）+ pipeline registry 註冊 + CLI 支援 `--pipeline evaluation` + catalog 新增 eval_predictions / eval_metrics / eval_report + Training pipeline evaluate_model node 保留（輕量 mAP 供 MLflow） |
| 9 | 可觀測性增強 | Data-quality profiling（`core/profiling.py` profile_dataframe() + Runner 自動呼叫 + `logging.profile_outputs` config 控制）+ Artifact/lineage logging（Catalog.save() 自動 emit `artifact_written` structured log event：filepath, dataset_type, upstream versions） |
| 10 | 版本管理增強 | manifest 擴充（git_commit_hash, library_versions, artifact_sizes, metrics_summary）+ 版本查詢 CLI（`versions list/show/diff` subcommand）+ rollback 機制（`promote_model.py --rollback`） |
| 11 | 去 hard-code 補完 + Tests | inference validation thresholds 參數化 + 剩餘 hard-coded 項盤點 + 新增 tests（test_models/, test_evaluation pipeline, test_profiling, test_model_adapter_dataset） |
| 12 | Safe rerun 檢查點 | 跳過已完成步驟，從失敗步驟接續執行 |
| — | Source Data ETL Pipeline | SQL 轉換、Hive 整合、資料驗證 |
| — | 記憶體優化 | 盤點 MemoryDataset 使用、大型中間產物改用 file-backed（目前實作已合理，僅在資料量大幅增加時需要） |
| — | 規則化重新排序 | rule-based reranking |
| — | 月度監控 | 機率值分佈監控、資料筆數檢查 |
| — | Strategy 2-4 | OVR 多模型、LambdaRank 排序、雙層排序 |
| — | 錯誤分析 notebook | template notebook |

### Phase 依賴關係

```
Phase 7b (演算法抽象) ✅
  ├── Phase 7c (Calibration) — 依賴 ModelAdapter 介面
  └── Phase 8 (Evaluation Pipeline) — 依賴 ModelAdapter 介面
      └── Phase 11 (Tests) — 覆蓋上述所有新功能
Phase 9 (可觀測性) — 獨立
Phase 10 (版本管理) — 獨立，可與 8 平行
```


## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation
- Dev environment uses synthetic data in place of Hive tables (see data spec docs)
- Storage format: Parquet (local dev) / Parquet on HDFS (production)

