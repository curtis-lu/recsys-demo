import os
import sys

import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture
def spark():
    """Per-test SparkSession resolved via get_or_create_spark_session.

    Function-scoped on purpose: tune_hyperparameters explicitly stops the
    SparkSession to free JVM threads before its driver-local HPO loop, and
    a session-scoped fixture would then yield the same dead object to every
    later test (e.g. test_dataset_then_training calls .createDataFrame() on
    it -> AttributeError 'NoneType' has no attribute 'sc'). Function scope
    plus get_or_create's stopped-session detection means the next test
    after a stop() transparently gets a fresh session. No teardown — the
    session is reused across tests when alive and cleaned up by Python at
    process exit.
    """
    from pyspark.sql import SparkSession
    from recsys_tfb.utils.spark import _is_session_alive

    active = SparkSession.getActiveSession()
    if active is None or not _is_session_alive(active):
        # No active session or it's stopped; create a new one with Hive support.
        test_configs = {
            "app_name": "recsys_tfb_test",
            "spark.master": "local[1]",
            "spark.sql.shuffle.partitions": "1",
            "spark.default.parallelism": "1",
            "spark.ui.enabled": "false",
            "spark.driver.memory": "1g",
        }
        builder = SparkSession.builder.appName(test_configs["app_name"])
        for key, value in test_configs.items():
            if key == "app_name":
                continue
            builder = builder.config(key, value)
        session = builder.enableHiveSupport().getOrCreate()
        return session
    else:
        # Session is alive, just return it (enableHiveSupport() would have no effect)
        return active
