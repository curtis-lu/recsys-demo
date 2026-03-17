# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Product recommendation ranking model for a commercial bank. Predicts customer interest scores across 22 financial product categories (credit cards, loans, deposits, funds, insurance, etc.) to help marketing PMs prioritize outreach.

- **Inference**: weekly batch scoring for ~10M customers x 22 products x ~500 features
- **Training**: monthly snapshots (12 months), run on-demand
- **Target environment**: PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG orchestration, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

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
- ✅ CLI entry point with `--pipeline`, `--env`, `--dataset-version` options
- ✅ Model promotion script (`scripts/promote_model.py`)
- ✅ Synthetic data generator (`scripts/generate_synthetic_data.py`)
- ✅ Comprehensive test suite
- ⬚ Source Data ETL Pipeline (not yet implemented)
- ⬚ Advanced metrics (precision@K, recall@K, nDCG, MRR)
- ⬚ Probability calibration and rule-based reranking
- ⬚ Strategy 2-4
- ⬚ Monthly monitoring pipeline
- ⬚ 結構化日誌（Pipeline-level, Node-level, Data-quality, Artifact/lineage）
- ✅ 欄位設定彈性化（drop_columns/categorical_columns 可透過 YAML 設定）
- ✅ Inference output 使用實際 model hash + latest symlink

## Architecture: 4 Pipelines

1. **Source Data ETL** _(not yet implemented)_ - SQL-based transforms (PySpark) producing feature and label tables. SQL files defined and ordered via YAML config.
2. **Dataset Building** ✅ - Stratified sampling, train/train-dev/val splits, feature engineering. Outputs versioned Parquet files. Preprocessing logic reused in inference without data leakage. Dual pandas/spark backend.
3. **Training** ✅ - Optuna hyperparameter search, LightGBM binary classification (Strategy 1), mAP evaluation, MLflow experiment tracking, model version comparison. Outputs versioned model artifacts.
4. **Inference** ✅ - Weekly batch scoring reusing dataset building preprocessing. Results partitioned by `${model_version}/${snap_date}`. Dual pandas/spark backend.

## Project Structure

```
src/recsys_tfb/
  __main__.py           — CLI entry point (Typer)
  core/
    config.py           — ConfigLoader (YAML base + env merge)
    catalog.py          — DataCatalog (dataset registry & resolution)
    node.py             — Node (function wrapper with named I/O)
    pipeline.py         — Pipeline (topological sort via Kahn's algorithm)
    runner.py           — Runner (sequential execution)
    versioning.py       — Hash-based versioning, manifests, symlinks
  io/
    base.py             — AbstractDataset interface
    parquet_dataset.py  — ParquetDataset (pandas/spark dual backend)
    pickle_dataset.py   — PickleDataset
    json_dataset.py     — JSONDataset
  pipelines/
    __init__.py         — Pipeline registry (get_pipeline, list_pipelines)
    dataset/            — Dataset building (nodes_pandas.py, nodes_spark.py, pipeline.py)
    training/           — Training (nodes.py, pipeline.py)
    inference/          — Inference (nodes_pandas.py, nodes_spark.py, pipeline.py)
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
python -m recsys_tfb run --pipeline dataset --env local
python -m recsys_tfb run --pipeline training --env local
python -m recsys_tfb run --pipeline inference --env local

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
- **Strategy 2** _(planned)_: One-vs-rest per product
- **Strategy 3** _(planned)_: Strategy 1/2 + single ranking layer (e.g., LambdaRank)
- **Strategy 4** _(planned)_: Strategy 1/2 + two-tier ranking (category then subcategory)

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

| Phase | 名稱 | 內容 |
|-------|------|------|
| 1 | 修正已知問題 + 欄位彈性化 ✅ | README `--env` 文件修正、inference output 改用實際 model hash、dataset pipeline hard-coded 欄位抽取到 YAML |
| 2 | 結構化日誌框架 | run_id 產生、Pipeline-level + Node-level 結構化日誌 |
| 3 | Data-quality + Artifact log | Data-quality log、Artifact/lineage log |
| 4 | 版本管理增強 | manifest 擴充、版本查詢 CLI、rollback 機制 |
| 5 | Safe rerun 檢查點 | 跳過已完成步驟，從失敗步驟接續執行 |

## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation
- Dev environment uses synthetic data in place of Hive tables (see data spec docs)
- Storage format: Parquet (local dev) / Parquet on HDFS (production)
