# CLAUDE.md

Claude Code 在此 repo 工作時的最小規範。

## Project Overview

商業銀行產品推薦排序模型。預測客戶對 22 類金融產品的興趣分數，供行銷 PM 排序推薦優先順序。

- **Inference**：每週批次推論，~10M 客戶 × 22 產品 × ~1500 特徵
- **Training**：N 個月底 snapshot（顯式 `train_snap_dates` list 配置），不定期手動執行
- **Target environment**：PySpark 3.3.2 on Hadoop/HDFS/Hive, Ploomber DAG, no internet, no extra packages, CPU-only (4 core, 128GB RAM)

## Tech Stack

Python 3.10+ | PySpark 3.3.2 | LightGBM 4.6.0 | scikit-learn 1.5.0 | MLflow 3.1.0 | Optuna 4.5.0 | Ploomber 0.23.3 | pandas 1.5.3 | numpy 1.25.0 | pyarrow 14.0.1 | pytest 7.3.1 | SHAP 0.42.1 | Typer 0.20.1


## Production Constraints

- No UDFs in Spark
- No network access
- No additional package installation

## 測試效能與避免長時間阻塞

pytest 的 `spark` fixture 是 function-scoped（每個測試重啟 SparkSession）。**跑整包 `tests/test_evaluation` 約需 ~33 分鐘**，整包 `tests/` 更久。為避免卡住：

- **永遠只跑與改動相關的特定測試檔**，不要把整包 Spark 測試當「保險再驗一次」重跑。例：改 `metrics_spark.py` → 只跑 `tests/test_evaluation/test_metrics_spark*.py`。
- **controller / code review 驗證用 `git diff <base>..<head>`（SHA-based，秒級）＋ 針對性 grep**，不要重跑 subagent 已經驗過的測試。
- 真要跑可能 >2 分鐘的指令，明確設短 timeout 或拆子集；**絕不讓 30+ 分鐘的 Spark 全量測試擋住流程**（曾因此空轉整晚）。
- 全量回歸只在「最終整合驗證」做一次，並預期它很慢、用 background 執行且不阻塞其他進度。
- 跨 worktree 驗證一律用絕對路徑或 `git -C <worktree>`；Bash cwd 會在 skill/`cd` 後被 reset，相對路徑可能讀到 stale 的 main tree。

## Local dev-cluster testing

在本機 dev-cluster 互動測試 pipeline：

- **本機環境**：`~/dev-cluster/`（Docker Spark+HDFS+Hive Metastore），詳見其 README。
- **Hive 來源表 setup**：`scripts/setup_hive_dev.py` 把 `data/{feature_table,label_table,sample_pool}.parquet` 寫成 `ml_recsys.<table>` Hive managed table。**跳過 source_etl**（合成資料已是 feature/label 粒度，沒有上游 `feature_concat`/`label_ccard` 等表）。腳本內**必須把 `snap_date` cast 成 DATE**（合成 parquet 是 timestamp[us]，不轉的話 Spark 對 `'YYYY-MM-DD'` 字串 filter 會 0 row，val/test/calibration 全空）。
- **Ad-hoc / admin PySpark 腳本（setup_hive_dev / nuke_ml_recsys / `SHOW PARTITIONS` 等）**：用 `scripts/dev_admin.sh` wrapper，跑在 transient `devcluster/pyspark` container 內 + `--master local[N]`（README §line 77-91 推薦的 admin pattern）。**不要 host venv**（standalone init 3+ min、`file://<host>` 派給 worker container 找不到）；**也不要 docker exec spark-master**（無 python3）。腳本內 path 寫 `/workspace/...` 不是 host 絕對路徑。詳見 `dev-cluster-spark` skill SOP-6。
  ```bash
  scripts/dev_admin.sh scripts/nuke_ml_recsys.py
  scripts/dev_admin.sh scripts/setup_hive_dev.py
  ```
- **/etc/hosts**：host 端讀 Hive 資料前需加 `127.0.0.1 namenode datanode hive-metastore spark-master`，否則 `hdfs://namenode:9000/...` resolve 不到（dev-cluster README §「已知限制」第 3 點）。

### Pipeline 與 SPARK_CONF_DIR 的對應

`--env production` 的 training cache 跟 model artifact (`model.txt` / `calibrator.pkl` / `*.json`) 都駐留在 driver-local fs：cache 由 `_materialize_parquet_handle`（`src/recsys_tfb/pipelines/training/nodes.py`）自己從 HDFS `copyToLocal` 拉下來（不經 catalog `ParquetDataset`、不依賴 `spark.master` 模式；cache node output 是 `ParquetHandle`，由 framework auto-MemoryDataset 在 DAG 中銜接；dev/test 也必須走 `cache.root`，不再有 `enabled=false` 繞行路徑）；artifact 走 Python `open()` 寫不認 `hdfs://` scheme。Pipeline 依下表選對 `SPARK_CONF_DIR`：

| Pipeline | `SPARK_CONF_DIR` | spark.master | 為什麼 |
|---|---|---|---|
| `dataset` / `inference` / `evaluation` / `baselines` / `*_etl` | `~/dev-cluster/client-template/spark`（client-env.sh 預設） | `spark://localhost:7077` | 寫 Hive managed table 走 HDFS，需要 worker container |
| `training` | **`~/dev-cluster/client-template-local/spark`** | `local[*]` | LightGBM 是 driver 單機訓練，distributed cluster 沒幫助；model artifact 駐留 driver-local；cache 由 cache node 自己從 HDFS 拉；evaluate_model 將 test-set 預測寫入 Hive `ml_recsys.training_eval_predictions`（hive-site.xml 已 symlink 進 `client-template-local/spark/`） |

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
- 把 catalog 上 model / best_params / evaluation_results 的 filepath 寫成 `hdfs://` → Python `open()` 在 cwd 建出 literal `./hdfs:/namenode:9000/...` 假目錄
- 早期版本 `client-template-local` 缺 hive-site.xml symlink，會出現 `Table or view not found: ml_recsys.<table>`；現已修正（symlink 至 `~/dev-cluster/client-template-local/hive-site.xml`）

## graphify

This project has a graphify knowledge graph at graphify-out/.

**MANDATORY**: For any architecture, refactoring, or codebase exploration task —
read `graphify-out/GRAPH_REPORT.md` BEFORE launching Explore agents or reading raw files.
Do not substitute an Explore agent for this step.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current
