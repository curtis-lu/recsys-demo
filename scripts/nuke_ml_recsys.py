"""Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.

Run from host venv with client-env.sh sourced (NOT docker exec into spark-master —
that container is JVM-only, no python3). See dev-cluster-spark skill SOP-6.

    source ~/dev-cluster/scripts/client-env.sh
    .venv/bin/python scripts/nuke_ml_recsys.py
    .venv/bin/python scripts/setup_hive_dev.py    # rebuild source tables

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
