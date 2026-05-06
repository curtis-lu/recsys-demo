"""Drop the ml_recsys database (CASCADE) for a clean dev-cluster reset.

Run via the dev-cluster admin wrapper (transient devcluster/pyspark container,
local[N] master, no executor pool). See dev-cluster-spark skill SOP-6.

    scripts/dev_admin.sh scripts/nuke_ml_recsys.py
    scripts/dev_admin.sh scripts/setup_hive_dev.py    # rebuild source tables

This is intended for dev-cluster only. DO NOT run against production.
"""

from pyspark.sql import SparkSession

DB = "ml_recsys"
# FQ URI required: metastore container's fs.defaultFS resolves localhost to itself,
# so a relative or localhost-keyed warehouse path crashes CREATE DATABASE.
# See dev-cluster-spark skill SOP-4.
DB_LOCATION = f"hdfs://namenode:9000/user/hive/warehouse/{DB}.db"


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

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {DB} LOCATION '{DB_LOCATION}'"
    )
    print(f"[ok] created empty database: {DB} at {DB_LOCATION}")

    spark.sql(f"SHOW TABLES IN {DB}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
