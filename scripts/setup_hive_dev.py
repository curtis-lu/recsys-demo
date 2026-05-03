"""Bootstrap dev Hive: write synthetic parquet as ml_recsys.{feature,label,sample_pool}_table.

Run inside dev-cluster spark container via spark-submit, with /workspace mounted to
the host project root.
"""

from pyspark.sql import SparkSession

DB = "ml_recsys"
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

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    print(f"[ok] database ready: {DB}")

    for table, path in TABLES.items():
        df = spark.read.parquet(f"file://{path}")
        full = f"{DB}.{table}"
        df.write.mode("overwrite").saveAsTable(full)
        n = spark.table(full).count()
        print(f"[ok] {full}: {n} rows, columns={df.columns}")

    print("\n[done] tables in", DB)
    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
