"""廣告情境示例的本機 Spark 環境：清掉 data/、產生原始表、寫進 Hive 的 ad_raw 資料庫。

對應銀行示例的 scripts/local_spark_setup.py，差在兩件事：

- 寫進 Hive 的是**上游原始表**（ad_raw.*），不是框架的來源表。來源表由 source_etl
  從這裡算出來，所以本示例的 source_etl 是真的會跑的。
- 一切落在執行當下的目錄（應為 examples/ad/）底下：warehouse、metastore、模型、報表。
  spark-defaults.conf 的 warehouse 與 metastore 路徑是相對路徑，所以 SPARK_CONF_DIR
  直接指 repo 根目錄那份即可；本腳本在 Spark 起來後核對 warehouse 真的落在這裡，
  沒落在這裡就中止——那代表接下來每一條 pipeline 都會寫進別的示例的 data/。

用法：由 run_e2e.sh 呼叫；單獨跑時
    cd examples/ad && SPARK_CONF_DIR=<repo>/conf/spark-local TZ=Asia/Taipei \\
        <repo>/.venv/bin/python setup_local.py
"""
from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from generate_data import generate, write_parquet  # noqa: E402

RAW_DB = "ad_raw"
DATA = Path("data")


def main() -> None:
    if Path.cwd().resolve() != HERE:
        sys.exit(f"要在 {HERE} 底下執行（目前在 {Path.cwd()}）：conf/ 與 data/ 都相對目前目錄")
    if not os.environ.get("SPARK_CONF_DIR"):
        sys.exit("SPARK_CONF_DIR 沒設：沒有它就沒有 Hive metastore，表不會跨指令留存")

    # 整個 data/ 重來：模型、dataset 產物、warehouse 都由這份原始資料決定，
    # 留著舊的會讓基準 digest 比到上一輪的東西
    if DATA.exists():
        shutil.rmtree(DATA)
        print(f"[reset] removed {DATA.resolve()}")

    raw_dir = DATA / "raw"
    tables = generate()
    write_parquet(tables, raw_dir)
    print(f"[ok] 原始表 parquet → {raw_dir.resolve()}")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("ad_example_setup").getOrCreate()
    warehouse = spark.conf.get("spark.sql.warehouse.dir")
    expected = (DATA / "local_warehouse").resolve()
    if str(expected) not in warehouse:
        spark.stop()
        sys.exit(f"warehouse 落在 {warehouse}，不是 {expected}：SPARK_CONF_DIR 讀到的不是相對路徑那份設定")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {RAW_DB}")
    for name in tables:
        df = spark.read.parquet((raw_dir / f"{name}.parquet").resolve().as_uri())
        full = f"{RAW_DB}.{name}"
        df.write.mode("overwrite").saveAsTable(full)
        print(f"[ok] {full}: {spark.table(full).count()} rows, schema={df.dtypes}")
    spark.stop()


if __name__ == "__main__":
    main()
