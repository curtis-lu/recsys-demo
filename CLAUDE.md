# CLAUDE.md

Claude Code 在此 repo 工作時的最小規範。

## Project Overview

商業銀行產品推薦排序模型。預測客戶對 22 類金融產品的興趣分數，供行銷 PM 排序推薦優先順序。

- **Inference**：每週批次推論，~10M 客戶 × 22 產品 × ~1500 特徵
- **Training**：12 個月月底快照，不定期手動執行
- **Target environment**：PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Tech Stack

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1 | Typer 0.20.1


## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation
- Dev 環境使用合成假資料取代 Hive tables
- Storage：Parquet（local dev）/ Parquet on HDFS（production）

## Local Spark backend testing

互動測試 backend=spark 的 pipeline：

- **本機環境**：`~/dev-cluster/`（Docker Spark+HDFS+Hive Metastore），詳見其 README。
- **Hive 來源表 setup**：`scripts/setup_hive_dev.py` 把 `data/{feature_table,label_table,sample_pool}.parquet` 寫成 `ml_recsys.<table>` Hive managed table。**跳過 source_etl**（合成資料已是 feature/label 粒度，沒有上游 `feature_concat`/`label_ccard` 等表）。
- **跑 pipeline**：`source ~/dev-cluster/scripts/client-env.sh && .venv/bin/python -m recsys_tfb <pipeline> --env production`。host 端讀 Hive 資料前需 `/etc/hosts` 加 `127.0.0.1 namenode datanode hive-metastore spark-master`，否則 `hdfs://namenode:9000/...` resolve 不到（dev-cluster README §「已知限制」第 3 點）。

## graphify

This project has a graphify knowledge graph at graphify-out/.

**MANDATORY**: For any architecture, refactoring, or codebase exploration task —
read `graphify-out/GRAPH_REPORT.md` BEFORE launching Explore agents or reading raw files.
Do not substitute an Explore agent for this step.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
