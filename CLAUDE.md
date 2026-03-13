# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Product recommendation ranking model for a commercial bank. Predicts customer interest scores across 22 financial product categories (credit cards, loans, deposits, funds, insurance, etc.) to help marketing PMs prioritize outreach.

- **Inference**: weekly batch scoring for ~10M customers x 22 products x ~500 features
- **Training**: monthly snapshots (12 months), run on-demand
- **Target environment**: PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG orchestration, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Tech Stack & Versions

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1

## Architecture: 4 Pipelines

1. **Source Data ETL** - SQL-based transforms (PySpark) producing feature and label Hive tables. SQL files defined and ordered via YAML config. Includes source freshness checks and data quality validation (nulls, duplicates, distributions).
2. **Dataset Building** - Stratified sampling, train/validation splits, feature engineering. Outputs Hive tables. Components must be reusable in inference without data leakage.
3. **Training** - Model experiments with hyperparameter search (Optuna), experiment tracking (MLflow). Evaluation metrics: mAP, precision@K, recall@K, nDCG, MRR - sliced by overall, per-product, and custom segments. Supports probability calibration and rule-based reranking.
4. **Inference** - Weekly batch scoring, monthly monitoring. Reuses ETL and dataset building components. Results partitioned by snap_date & prod_code.

## Model Strategies

- **Strategy 1** (MVP): Single binary classifier, product name as feature
- **Strategy 2**: One-vs-rest per product
- **Strategy 3**: Strategy 1/2 + single ranking layer (e.g., LambdaRank)
- **Strategy 4**: Strategy 1/2 + two-tier ranking (category then subcategory)

Start with Strategy 1 and mAP evaluation only.

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

1. Minimal working version first (Strategy 1 + mAP)
2. Add features one at a time
3. Test after each addition
4. Skip error analysis notebooks initially

## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation
- Dev environment uses synthetic data in place of Hive tables (see data spec docs)
- Storage format: Parquet on HDFS

