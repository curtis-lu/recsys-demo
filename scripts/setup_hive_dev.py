"""Bootstrap dev Hive: write synthetic parquet as ml_recsys.{feature,label,sample_pool}_table.

Run via the dev-cluster admin wrapper (transient devcluster/pyspark container,
local[N] master, no executor pool). See dev-cluster-spark skill SOP-6.

    scripts/dev_admin.sh scripts/setup_hive_dev.py

Do NOT run from host venv (.venv/bin/python ...): host driver + spark-worker
container fs mismatch makes file:///workspace not resolvable on the worker side.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import TimestampType

DB = "ml_recsys"
# FQ URI required: see dev-cluster-spark skill SOP-4 (CREATE DATABASE without
# LOCATION crashes if metastore container's fs.defaultFS resolves to itself).
DB_LOCATION = f"hdfs://namenode:9000/user/hive/warehouse/{DB}.db"

# Paths inside the transient container — project root is bind-mounted at /workspace.
TABLES = {
    "feature_table": "/workspace/data/feature_table.parquet",
    "label_table": "/workspace/data/label_table.parquet",
    "sample_pool": "/workspace/data/sample_pool.parquet",
}


def main() -> None:
    spark = (
        SparkSession.builder.appName("setup_hive_dev")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DB} LOCATION '{DB_LOCATION}'"
    )
    print(f"[ok] database ready: {DB} at {DB_LOCATION}")

    for table, path in TABLES.items():
        df = spark.read.parquet(f"file://{path}")
        if "snap_date" in df.columns and isinstance(
            df.schema["snap_date"].dataType, TimestampType
        ):
            df = df.withColumn("snap_date", to_date("snap_date"))
        full = f"{DB}.{table}"
        df.write.mode("overwrite").saveAsTable(full)
        n = spark.table(full).count()
        print(f"[ok] {full}: {n} rows, columns={df.columns}")

    print("\n[done] tables in", DB)
    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
