import os
import sys

import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# loopback 綁定：macOS 換網路後 hostname 會解析到過期 IP，driver 綁不上 →
# 所有 Spark fixture 在 setup 秒炸（netty bind error）。測試都是 local[1]，
# 走 127.0.0.1 恆正確。setdefault 保留外部顯式覆寫空間。
# （2026-07-07，known-pitfalls.md §7）
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


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
