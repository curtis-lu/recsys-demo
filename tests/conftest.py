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
    from recsys_tfb.utils.spark import get_or_create_spark_session

    test_configs = {
        "app_name": "recsys_tfb_test",
        "spark.master": "local[1]",
        "spark.sql.shuffle.partitions": "1",
        "spark.default.parallelism": "1",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "1g",
    }
    return get_or_create_spark_session(test_configs)
