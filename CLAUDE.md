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
- **Hive 來源表 setup**：`scripts/setup_hive_dev.py` 把 `data/{feature_table,label_table,sample_pool}.parquet` 寫成 `ml_recsys.<table>` Hive managed table。**跳過 source_etl**（合成資料已是 feature/label 粒度，沒有上游 `feature_concat`/`label_ccard` 等表）。腳本內**必須把 `snap_date` cast 成 DATE**（合成 parquet 是 timestamp[us]，不轉的話 Spark 對 `'YYYY-MM-DD'` 字串 filter 會 0 row，val/test/calibration 全空）。
- **Ad-hoc PySpark 腳本（setup_hive_dev / nuke_ml_recsys / `SHOW PARTITIONS` 等）**：跟 pipeline 一樣從 host venv 跑，**不要 docker exec 進 spark-master/worker container**（apache/spark image 是 JVM-only，沒有 python3）。詳見 `dev-cluster-spark` skill SOP-6。
  ```bash
  source ~/dev-cluster/scripts/client-env.sh
  .venv/bin/python scripts/<your_script>.py
  ```
- **/etc/hosts**：host 端讀 Hive 資料前需加 `127.0.0.1 namenode datanode hive-metastore spark-master`，否則 `hdfs://namenode:9000/...` resolve 不到（dev-cluster README §「已知限制」第 3 點）。

### Pipeline 與 SPARK_CONF_DIR 的對應

`--env production` 的 catalog `cached_*_model_input` 與 model artifact (`model.txt` / `calibrator.pkl` / `*.json`) 都用 host-local `file://` 路徑（cache 跨 run 重用、artifact 用 Python `open()` 寫不認 hdfs scheme）。因此 pipeline 必須照下表選對 `SPARK_CONF_DIR`：

| Pipeline | `SPARK_CONF_DIR` | spark.master | 為什麼 |
|---|---|---|---|
| `dataset` / `inference` / `evaluation` / `baselines` / `*_etl` | `~/dev-cluster/client-template/spark`（client-env.sh 預設） | `spark://localhost:7077` | 寫 Hive managed table 走 HDFS，需要 worker container |
| `training` | **`~/dev-cluster/client-template-local/spark`** | `local[*]` | cached_* 跟 model artifact 都寫 host fs；distributed worker 看不到 host fs |

執行：
```bash
source ~/dev-cluster/scripts/client-env.sh                              # 設 HADOOP_CONF_DIR、JDK17 add-opens
# dataset / inference / etc.
.venv/bin/python -m recsys_tfb <pipeline> --env production
# training（必須切 local conf）
export SPARK_CONF_DIR=~/dev-cluster/client-template-local/spark
.venv/bin/python -m recsys_tfb training --env production
```

走錯 conf 的典型 trap（深入排查見 `dev-cluster-spark` skill）：
- training 用 `spark://` → `cached_*` 在 host 端只剩 `_SUCCESS`、tune_hyperparameters 報 `Unable to infer schema for Parquet`
- training 用 `spark://` 又把 catalog filepath 寫成 `hdfs://` → Python `open()` 在 cwd 建出 literal `./hdfs:/namenode:9000/...` 假目錄
- `client-template-local` 缺 hive-site.xml symlink → `Table or view not found: ml_recsys.<table>`

## graphify

This project has a graphify knowledge graph at graphify-out/.

**MANDATORY**: For any architecture, refactoring, or codebase exploration task —
read `graphify-out/GRAPH_REPORT.md` BEFORE launching Explore agents or reading raw files.
Do not substitute an Explore agent for this step.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
