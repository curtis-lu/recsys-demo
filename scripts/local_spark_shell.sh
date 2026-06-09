#!/usr/bin/env bash
# 本機 ad-hoc 互動查表：開一個已連到本機 ml_recsys（內嵌 Derby + data/local_warehouse）的 Spark shell。
#
#   bash scripts/local_spark_shell.sh                       # pyspark：Python REPL，spark 已建好 → spark.sql("...").show()
#   bash scripts/local_spark_shell.sh sql                    # spark-sql：純 SQL 提示符（USE ml_recsys; SHOW TABLES; ...）
#   bash scripts/local_spark_shell.sh sql -e "SHOW TABLES IN ml_recsys"   # 一行式 ad-hoc 查詢（非互動）
#
# 模式後面多帶的參數會原樣傳給 pyspark / spark-sql。
#
# 注意：內嵌 Derby 單行程獨佔——shell 開著時別同時跑 pipeline 或第二個 Spark session（會撞鎖）。
# 看到 stderr 的 RpcEndpointNotFoundException 是 local[*] by-design 良性噪音，prompt 照樣可用。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"                                   # warehouse/metastore 路徑相對 root，必須在 root 啟動
export SPARK_CONF_DIR="$ROOT/conf/spark-local"
# 把 driver/worker python 釘到 venv（3.10.9），避免 pyspark 抓到系統 python 3.12。
export PYSPARK_PYTHON="$ROOT/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$ROOT/.venv/bin/python"

mode="${1:-py}"; shift 2>/dev/null || true   # 其餘參數原樣傳給底層 launcher
case "$mode" in
  py|pyspark)    exec "$ROOT/.venv/bin/pyspark" "$@" ;;
  sql|spark-sql) exec "$ROOT/.venv/bin/spark-sql" "$@" ;;
  *) echo "用法: $0 [py|sql] [extra args]   (py=pyspark REPL，sql=spark-sql CLI)" >&2; exit 2 ;;
esac
