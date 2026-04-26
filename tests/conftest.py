import os
import sys

import pytest

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests."""
    from recsys_tfb.utils.spark import get_or_create_spark_session

    test_configs = {
        "app_name": "recsys_tfb_test",
        "spark.master": "local[1]",
        "spark.sql.shuffle.partitions": "1",
        "spark.default.parallelism": "1",
        "spark.ui.enabled": "false",
        "spark.driver.memory": "1g",
    }
    session = get_or_create_spark_session(test_configs)
    yield session
    session.stop()
