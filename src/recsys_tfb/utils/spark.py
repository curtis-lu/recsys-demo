"""SparkSession entrypoint with config-driven creation and fallback."""

import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_VALID_VALUE_TYPES = (str, int, bool)


def get_or_create_spark_session(
    spark_configs: dict[str, Any] | None = None,
) -> SparkSession:
    """Create or return the SparkSession.

    Two call modes:

    1. Pipeline entrypoint passes ``spark_configs`` (already deep-merged
       ``params["spark"]``). Builder is configured and a session is
       created. If an active session already exists, runtime configs are
       applied and the existing session is returned (cluster-level
       configs would be ignored by PySpark — a warning is logged).
    2. IO / SQLRunner / scripts call with ``None``. If an active session
       exists, return it directly. Otherwise fall back to loading the
       base ``parameters.yaml`` ``spark:`` block via ConfigLoader and
       create a session from that.

    Raises:
        TypeError: ``spark_configs`` is not a dict.
        ValueError: any value is not str / int / bool.
    """
    if spark_configs is None:
        return _fallback_create()

    if not isinstance(spark_configs, dict):
        raise TypeError(
            f"spark_configs must be a dict, got {type(spark_configs).__name__}"
        )
    _validate_values(spark_configs)

    if SparkSession.getActiveSession() is not None:
        logger.warning(
            "Active SparkSession already exists; cluster-level configs "
            "in spark_configs will be ignored by PySpark."
        )

    app_name = spark_configs.get("app_name", "recsys_tfb")
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_configs.items():
        if key == "app_name":
            continue
        builder = builder.config(key, value)
    return builder.getOrCreate()


def _validate_values(spark_configs: dict[str, Any]) -> None:
    bad = [
        k
        for k, v in spark_configs.items()
        if not isinstance(v, _VALID_VALUE_TYPES)
    ]
    if bad:
        raise ValueError(
            "spark_configs values must be str / int / bool. "
            f"Invalid keys: {bad}"
        )


def _fallback_create() -> SparkSession:
    """Stub for Task 2 — returns active session or raises."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    raise RuntimeError(
        "No active SparkSession and no spark_configs provided. "
        "Fallback to ConfigLoader is implemented in Task 2."
    )
