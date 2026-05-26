"""SparkSession entrypoint with config-driven creation and fallback."""

import logging
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_VALID_VALUE_TYPES = (str, int, bool)


def get_or_create_spark_session(
    spark_configs: dict[str, Any] | None = None,
    enable_hive: bool = False,
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

    enable_hive (default False): when True, the builder calls
        ``.enableHiveSupport()`` before ``getOrCreate()``. Required for
        ``HiveTableDataset`` write paths in tests (``STORED AS PARQUET``
        DDL needs Hive parser support). Production code paths leave this
        False; the cluster session inherits Hive support from
        ``SPARK_CONF_DIR``'s ``hive-site.xml`` rather than this flag.

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
    if enable_hive:
        builder = builder.enableHiveSupport()
    return builder.getOrCreate()


def _is_session_alive(session: SparkSession) -> bool:
    """True if the session's SparkContext is still attached to a live JVM."""
    try:
        return session.sparkContext._jsc is not None
    except Exception:
        return False


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
    """Return active session, or build one from base parameters.yaml.

    `getActiveSession()` can return a session whose SparkContext has been
    stopped (e.g. tune_hyperparameters explicitly calls `.stop()` to free
    JVM threads before the driver-local HPO loop). Treat a stopped session
    as if there were none and rebuild from yaml — otherwise downstream
    nodes call `.parallelize()` / `.createDataFrame()` on a dead context
    and hit AttributeError: 'NoneType' object has no attribute 'sc'.
    """
    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        return active

    import os
    from pathlib import Path

    from recsys_tfb.core.config import ConfigLoader

    env = os.environ.get("CONF_ENV", "local")
    conf_dir = Path.cwd() / "conf"
    if not conf_dir.is_dir():
        raise RuntimeError(
            f"No active SparkSession and conf/ not found at {conf_dir}. "
            "Cannot build fallback session."
        )
    loader = ConfigLoader(str(conf_dir), env=env)
    try:
        base_params = loader.get_parameters_by_name("parameters")
    except KeyError as exc:
        raise RuntimeError(
            "No active SparkSession and parameters.yaml not found in conf/."
        ) from exc
    spark_configs = base_params.get("spark", {})

    # Match the entrypoint path (__main__._load_spark_config): resolve
    # ${vdclient.*.*} placeholders before handing dict to the builder.
    # Otherwise yaml values like ${vdclient.cdp.driver_port} reach SparkConf
    # as literal strings → "spark.driver.port should be int".
    # ${env.*} placeholders are already resolved by ConfigLoader.
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders
    spark_configs = resolve_vdclient_placeholders(spark_configs)

    logger.info(
        "Fallback: building SparkSession (yaml=conf/%s/parameters.yaml, "
        "connection settings from SPARK_CONF_DIR)",
        env,
    )
    return get_or_create_spark_session(spark_configs or {"app_name": "recsys_tfb"})
