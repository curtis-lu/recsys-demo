# 本機 Spark 測試環境

本機測試**不需要任何外部 container（HDFS / Hive Metastore / Spark cluster）**。Spark 在你的 venv 裡用 `local[*]`
跑，Hive 表是 managed、落在 `data/local_warehouse`，metastore 是 pyspark 自帶的內嵌 Derby
（`data/metastore_db`）。連線設定全在 `conf/spark-local/spark-defaults.conf`。

> 公司環境的 Spark 連線沒問題；這套**只為本機測試程式**。切到公司環境＝換 `SPARK_CONF_DIR`，
> 見 [`spark-connection-architecture.md`](spark-connection-architecture.md)。

## 一次性建置

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py        # 產合成資料 + 建 ml_recsys.* 表
```

`local_spark_setup.py` 會：合成 parquet（缺則自動跑 `generate_synthetic_data.py`）→ `snap_date`
cast DATE → `saveAsTable` 進 `ml_recsys.{feature_table,label_table,sample_pool}`。

## 跑 pipeline / scripts

```bash
export SPARK_CONF_DIR=$PWD/conf/spark-local      # 每個新 shell 都要
PYTHONPATH=src .venv/bin/python -m recsys_tfb <dataset|training|inference|evaluation> --env local
PYTHONPATH=src .venv/bin/python scripts/suggest_categorical_cols.py <args>
PYTHONPATH=src .venv/bin/python scripts/sampling_overrides_editor.py <args>
```

端到端一鍵：`bash scripts/local_e2e.sh`。

## 重置

```bash
PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py --reset   # rm warehouse+metastore 後重建
```

## 互動查表（ad-hoc）

開一個已連到本機 `ml_recsys` 的 Spark shell，直接查持久化的表（managed 表的資料在
`data/local_warehouse`、metadata 在內嵌 Derby `data/metastore_db`，跨行程持久）：

```bash
bash scripts/local_spark_shell.sh                                    # pyspark：Python REPL，spark 已建好
bash scripts/local_spark_shell.sh sql                                # spark-sql：純 SQL 提示符
bash scripts/local_spark_shell.sh sql -e "SHOW TABLES IN ml_recsys"  # 一行式非互動查詢
```

pyspark REPL 裡：

```python
spark.sql("SHOW TABLES IN ml_recsys").show(truncate=False)
spark.sql("SELECT * FROM ml_recsys.feature_table LIMIT 5").show()
spark.sql("SHOW PARTITIONS ml_recsys.recsys_prod_train_model_input").show(truncate=False)
```

離開：pyspark `exit()`／spark-sql `quit;`／Ctrl-D。wrapper 已內建「從 root 啟動 +
`SPARK_CONF_DIR` + 把 pyspark 的 python 釘到 venv（避免抓到系統 3.12）」。
**Derby 單行程鎖**：shell 開著時別同時跑 pipeline 或第二個 Spark session。

## 注意事項

- **一定從 repo/worktree root 跑**：`conf/spark-local` 的路徑是相對的（`data/local_warehouse` 等）。
- **stderr 的 `RpcEndpointNotFoundException: CoarseGrainedScheduler` 是 local[*] by-design 噪音**，看 stdout 成功訊號即可。
- **內嵌 Derby 單行程獨佔**：別同時跑兩個 Spark 行程碰同一 `data/metastore_db`（會撞鎖）；循序跑即可。
- **隔離**：`local_spark_setup.py --check-isolation` 確認 warehouse/metastore/cache 都在此 worktree、無指向 main。
