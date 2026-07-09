"""SparkSession entrypoint: config-driven creation with canonical-config memory."""

import logging
from typing import Any

from pyspark import SparkContext
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_VALID_VALUE_TYPES = (str, int, bool)

# Canonical configs, remembered from the first mode-1 call (a CLI entry).
# mode-2 rebuilds from these instead of re-reading yaml: the yaml path guesses
# the env from CONF_ENV (never set anywhere) and misses the per-pipeline
# `spark:` block that _load_spark_config merges in.
_canonical_configs: dict[str, Any] | None = None
_canonical_enable_hive: bool = False


def reset_spark_session_state() -> None:
    """Forget the canonical configs. Test-only: module state leaks across tests."""
    global _canonical_configs, _canonical_enable_hive
    _canonical_configs = None
    _canonical_enable_hive = False


def get_or_create_spark_session(
    spark_configs: dict[str, Any] | None = None,
    enable_hive: bool = False,
) -> SparkSession:
    """Create or return the SparkSession.

    Two call modes:

    1. Pipeline entrypoint passes ``spark_configs`` (already deep-merged
       ``params["spark"]``). The configs are remembered as canonical and a
       session is created. If an active session already exists, runtime
       configs are applied and the existing session is returned
       (cluster-level configs would be ignored by PySpark — a warning is
       logged).
    2. IO / SQLRunner / scripts call with ``None``. An alive active session is
       returned directly. Otherwise a session is rebuilt from the remembered
       canonical configs, or — if no mode-1 call ever happened (scripts,
       tests) — from the base ``parameters.yaml`` ``spark:`` block.

    enable_hive (default False): when True, the builder calls
        ``.enableHiveSupport()`` before ``getOrCreate()``. Required for
        ``HiveTableDataset`` write paths in tests (``STORED AS PARQUET``
        DDL needs Hive parser support). Production code paths leave this
        False; the cluster session inherits Hive support from
        ``SPARK_CONF_DIR``'s ``hive-site.xml`` rather than this flag.
        Remembered alongside the configs so a rebuild keeps Hive support.

    Raises:
        TypeError: ``spark_configs`` is not a dict.
        ValueError: any value is not str / int / bool.
    """
    global _canonical_configs, _canonical_enable_hive

    if spark_configs is None:
        return _rebuild_or_active()

    if not isinstance(spark_configs, dict):
        raise TypeError(
            f"spark_configs must be a dict, got {type(spark_configs).__name__}"
        )
    _validate_values(spark_configs)

    _canonical_configs = dict(spark_configs)
    _canonical_enable_hive = enable_hive

    active = SparkSession.getActiveSession()
    if active is not None and not _is_session_alive(active):
        _stop_and_clear(active)
    elif active is not None:
        logger.warning(
            "Active SparkSession already exists; cluster-level configs "
            "in spark_configs will be ignored by PySpark."
        )

    return _build(spark_configs, enable_hive)


def _rebuild_or_active() -> SparkSession:
    """Return the active session, or rebuild one (canonical configs, else yaml)."""
    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        return active

    if active is not None:
        _stop_and_clear(active)
    else:
        stale = SparkSession._instantiatedSession
        if stale is not None and not _is_session_alive(stale):
            _stop_and_clear(stale)

    if _canonical_configs is not None:
        return _build(_canonical_configs, _canonical_enable_hive)

    return _build_from_yaml()


def _build(spark_configs: dict[str, Any], enable_hive: bool) -> SparkSession:
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
    """True only if the session's SparkContext is still running.

    A non-None ``_jsc`` means the Python wrapper still holds a context
    object, not that the JVM context is live: a stopped SparkContext can
    leave ``_jsc`` non-None, and any subsequent ``parallelize`` /
    ``createDataFrame`` then raises ``IllegalStateException: Cannot call
    methods on a stopped SparkContext``. Probe ``isStopped()`` so a dead
    session reads as not-alive and the caller rebuilds a fresh one.
    """
    try:
        jsc = session.sparkContext._jsc
        return jsc is not None and not jsc.sc().isStopped()
    except Exception:
        return False


def _stop_and_clear(session: SparkSession) -> None:
    """Stop a session Python-side and clear PySpark's singletons.

    ``SparkSession.builder.getOrCreate()`` treats a session as reusable
    whenever ``_instantiatedSession._sc._jsc`` is not None. Only a Python-side
    ``SparkContext.stop()`` sets ``_jsc`` to None, so a JVM-side death leaves
    the singletons pointing at a corpse and every "rebuild" hands it back.
    Clearing them is what makes the next getOrCreate actually build.

    ``SparkSession.stop()`` clears the singletons only if its JVM calls
    succeed; when the py4j gateway itself is gone it raises partway through.
    The explicit assignments below are the belt-and-braces for that case.
    """
    try:
        session.stop()
    except Exception as exc:  # noqa: BLE001 — JVM/gateway may already be gone
        logger.warning("SparkSession.stop() raised while clearing: %s", exc)

    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    SparkContext._active_spark_context = None


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


def _build_from_yaml() -> SparkSession:
    """Build a session from base parameters.yaml. Only for never-configured callers.

    Reached by scripts and tests that call mode-2 without any prior mode-1
    call. Pipeline runs always go through the canonical configs instead.
    """
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

    # _build (not the mode-1 entry) — recursing through mode-1 would write
    # yaml configs into _canonical_configs and poison the memory. That skips
    # mode-1's _validate_values, so re-apply it here.
    spark_configs = spark_configs or {"app_name": "recsys_tfb"}
    _validate_values(spark_configs)

    logger.info(
        "Fallback: building SparkSession (yaml=conf/%s/parameters.yaml, "
        "connection settings from SPARK_CONF_DIR)",
        env,
    )
    return _build(spark_configs, False)
