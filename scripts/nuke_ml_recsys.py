"""Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.

Run inside dev-cluster spark container via spark-submit, with /workspace mounted
to the host project root. Pair with setup_hive_dev.py to rebuild source tables.

This is intended for dev-cluster only. DO NOT run against production.
"""

from pyspark.sql import SparkSession

DB = "ml_recsys"


def main() -> None:
    spark = (
        SparkSession.builder.appName("nuke_ml_recsys")
        .enableHiveSupport()
        .getOrCreate()
    )

    existed = spark.catalog.databaseExists(DB)
    if existed:
        spark.sql(f"DROP DATABASE {DB} CASCADE")
        print(f"[ok] dropped database: {DB}")
    else:
        print(f"[skip] database does not exist: {DB}")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
    print(f"[ok] created empty database: {DB}")

    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
