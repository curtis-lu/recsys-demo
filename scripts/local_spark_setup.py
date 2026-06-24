"""本機 Spark 測試環境 setup：合成 parquet → ml_recsys.* managed 表（本機 warehouse）。

在 host venv 直接跑（無 Docker、無 transient container）。連線設定全來自 SPARK_CONF_DIR
（conf/spark-local）；warehouse / metastore 落在此 worktree 的 data/ 下，per-worktree 隔離。

用法（從 repo/worktree root）：
    export SPARK_CONF_DIR=$PWD/conf/spark-local
    PYTHONPATH=src .venv/bin/python scripts/local_spark_setup.py [--reset] [--check-isolation]
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

DB = "ml_recsys"
DATA = Path("data")
WAREHOUSE = DATA / "local_warehouse"
METASTORE = DATA / "metastore_db"
CACHE = DATA / "recsys_cache"
TABLES = {
    "feature_table": DATA / "feature_table.parquet",
    "label_table": DATA / "label_table.parquet",
    "sample_pool": DATA / "sample_pool.parquet",
    "inference_population": DATA / "inference_population.parquet",
}


def check_isolation() -> None:
    """跑 Spark 前 fast assert：本機狀態全在此 worktree 內、無指向 main。任一不過即 exit 1。"""
    root = Path.cwd()
    errors: list[str] = []

    conf = os.environ.get("SPARK_CONF_DIR", "")
    expected_conf = root / "conf" / "spark-local"
    if not conf or os.path.realpath(conf) != os.path.realpath(expected_conf):
        errors.append(f"SPARK_CONF_DIR={conf!r}，應為 {expected_conf}")

    for name, p in {"local_warehouse": WAREHOUSE, "metastore_db": METASTORE, "recsys_cache": CACHE}.items():
        if p.is_symlink():
            errors.append(f"data/{name} 是 symlink（應為 worktree 真目錄）→ {os.readlink(p)}")

    if errors:
        print("[check-isolation] FAIL:")
        for e in errors:
            print("  -", e)
        sys.exit(1)
    print(f"[check-isolation] OK：root={root}；warehouse/metastore/cache 皆 worktree 本地、無指向 main")


def reset() -> None:
    for p in (WAREHOUSE, METASTORE):
        if p.exists():
            shutil.rmtree(p)
            print(f"[reset] removed {p}")


def ensure_synthetic_data() -> None:
    missing = [str(p) for p in TABLES.values() if not p.exists()]
    if missing:
        print(f"[setup] 缺合成 parquet {missing} → 執行 generate_synthetic_data.py")
        subprocess.run([sys.executable, "scripts/generate_synthetic_data.py"], check=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="清 warehouse + metastore 後重建")
    ap.add_argument("--check-isolation", action="store_true", help="只做隔離 pre-flight 後結束")
    args = ap.parse_args()

    check_isolation()
    if args.check_isolation:
        return
    if args.reset:
        reset()

    ensure_synthetic_data()

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import to_date
    from pyspark.sql.types import TimestampType

    spark = SparkSession.builder.appName("local_spark_setup").getOrCreate()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    print(f"[ok] database ready: {DB}")
    for table, path in TABLES.items():
        df = spark.read.parquet(path.resolve().as_uri())
        # 合成 parquet 的 snap_date 是 timestamp[us]；不 cast DATE 的話對 'YYYY-MM-DD' 字串
        # filter 會 0 row（val/test/calibration 全空）。
        if "snap_date" in df.columns and isinstance(
            df.schema["snap_date"].dataType, TimestampType
        ):
            df = df.withColumn("snap_date", to_date("snap_date"))
        full = f"{DB}.{table}"
        df.write.mode("overwrite").saveAsTable(full)
        print(f"[ok] {full}: {spark.table(full).count()} rows, columns={df.columns}")
    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
