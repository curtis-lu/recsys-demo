import os
import sys

import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture
def spark():
    """SparkSession with local-mode test configs and Hive support.

    Session is reused across tests when alive (built once per pytest process,
    inherited by later tests). When the session has been stopped (e.g. by
    ``tune_hyperparameters``), ``get_or_create_spark_session`` rebuilds it
    with the same configs.

    Hive support is needed because ``HiveTableDataset._build_create_ddl``
    emits ``STORED AS PARQUET`` (Hive DDL); the round-trip test in
    ``test_evaluation_compare_pipeline.py`` exercises that path.
    """
    from recsys_tfb.utils.spark import get_or_create_spark_session

    test_configs = {
        "app_name": "recsys_tfb_test",
        "spark.master": "local[1]",
        "spark.sql.shuffle.partitions": "1",
        "spark.default.parallelism": "1",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "1g",
    }
    return get_or_create_spark_session(test_configs, enable_hive=True)
