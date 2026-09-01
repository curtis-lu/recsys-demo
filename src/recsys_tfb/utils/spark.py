"""SparkSession entrypoint: config-driven creation with canonical-config memory."""

import logging
import time
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

# Last application id we successfully built, and the last time we handed out a
# live session. Reported by the ``spark_context_dead`` event, which fires only
# when a context is found dead that *we* did not stop — i.e. the cluster took it.
#
# Reading the event: with ``spark_lifecycle.release_during_hpo`` on (the
# default) the HPO window holds no session at all, so an idle reclaim during HPO
# cannot happen and no event fires for it. An event under the default therefore
# means the context died while Spark was in active use — which already rules out
# idle reclaim and points at preemption / AM failure / token expiry. To let an
# idle-reclaim death happen and be attributed, set ``release_during_hpo: false``
# and read ``seconds_since_last_use``: a gap matching the HPO window is an idle
# reclaim; a gap unrelated to it is a fixed-lifetime death such as an expiring
# delegation token.
_last_app_id: str | None = None
_last_alive_ts: float | None = None


class SparkSessionUnavailableError(RuntimeError):
    """A SparkSession could not be created or rebuilt.

    Raised instead of letting py4j's ``Py4JNetworkError`` (dead JVM gateway —
    unrecoverable in-process, the run must be restarted) or Spark's
    ``IllegalStateException`` surface at an unrelated call site.
    """


def reset_spark_session_state() -> None:
    """Forget the canonical configs. Test-only: module state leaks across tests."""
    global _canonical_configs, _canonical_enable_hive, _last_app_id, _last_alive_ts
    _canonical_configs = None
    _canonical_enable_hive = False
    _last_app_id = None
    _last_alive_ts = None


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
        Enforced as a post-condition (``_ensure_hive_catalog``) on every
        build, because ``getOrCreate()`` returns any existing session
        untouched, which makes ``enableHiveSupport()`` a no-op. **The check
        covers builds, not the mode-2 shortcut**: a mode-2 call ignores this
        argument entirely and an alive active session is returned as-is
        (only its rebuild path goes through ``_build``). Ask for Hive in the
        mode-1 call that establishes the canonical configs.

    Raises:
        TypeError: ``spark_configs`` is not a dict.
        ValueError: any value is not str / int / bool.
        SparkSessionUnavailableError: the session could not be built, or
            ``enable_hive=True`` could not be satisfied even after stopping
            the context and rebuilding.
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
            "in spark_configs will be ignored by PySpark (unless "
            "enable_hive=True finds the session lacks Hive, in which case "
            "_ensure_hive_catalog stops it and the rebuild does apply them)."
        )

    return _build(spark_configs, enable_hive)


def stop_spark_session() -> bool:
    """Stop the current SparkSession, if any. True when one was actually stopped.

    Idempotent: safe on an already-dead session and on no session at all. The
    next mode-2 caller rebuilds from the canonical configs, so stopping is a
    way to hand the executors back — not to end the run.
    """
    session = SparkSession.getActiveSession() or SparkSession._instantiatedSession
    if session is None:
        return False

    app_id = _safe_app_id(session)
    _stop_and_clear(session)
    logger.info(
        "SparkSession released (application_id=%s)", app_id,
        extra={"event": "spark_session_released", "application_id": app_id},
    )
    return True


def release_spark_session(parameters: dict) -> bool:
    """Release the SparkSession before a long driver-local stretch (HPO).

    Holding an idle Spark application for hours invites the cluster to reclaim
    it; the context then dies JVM-side and every later Spark call fails. Give
    the executors back instead, and let the next mode-2 caller rebuild from the
    canonical configs.

    Returns True when a session was actually stopped.
    """
    lifecycle = parameters.get("spark_lifecycle") or {}
    if not lifecycle.get("release_during_hpo", True):
        logger.info(
            "spark_lifecycle.release_during_hpo=false; keeping SparkSession alive"
        )
        return False
    return stop_spark_session()


def _safe_app_id(session: SparkSession) -> str | None:
    try:
        return session.sparkContext.applicationId
    except Exception:  # noqa: BLE001 — the context may already be gone
        return None


def _rebuild_or_active() -> SparkSession:
    """Return the active session, or rebuild one (canonical configs, else yaml)."""
    global _last_alive_ts

    active = SparkSession.getActiveSession()
    if active is not None and _is_session_alive(active):
        _last_alive_ts = time.time()
        return active

    dead = active
    if dead is None:
        stale = SparkSession._instantiatedSession
        if stale is not None and not _is_session_alive(stale):
            dead = stale

    if dead is not None:
        idle = int(time.time() - (_last_alive_ts or time.time()))
        logger.warning(
            "Detected stopped SparkContext; rebuilding "
            "(last_application_id=%s, seconds_since_last_use=%d)",
            _last_app_id, idle,
            extra={
                "event": "spark_context_dead",
                "last_application_id": _last_app_id,
                "seconds_since_last_use": idle,
            },
        )
        _stop_and_clear(dead)

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

    session = _get_or_create(builder, app_name)

    if enable_hive:
        session = _ensure_hive_catalog(session, builder, app_name)

    _mark_alive(session)
    logger.info(
        "SparkSession ready (application_id=%s, app_name=%s)",
        _last_app_id, app_name,
        extra={
            "event": "spark_session_created",
            "application_id": _last_app_id,
            "app_name": app_name,
        },
    )
    return session


def _get_or_create(builder: Any, app_name: str) -> SparkSession:
    try:
        return builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001 — surface one readable error
        # Deliberately neutral: this also catches a first build failing on an
        # unreachable master, a port clash, or a queue rejection. The real cause
        # is chained via `from exc` and stays in the traceback.
        raise SparkSessionUnavailableError(
            f"Failed to build SparkSession (app_name={app_name!r}, "
            f"last_application_id={_last_app_id!r}). Check the spark configs "
            "and cluster availability. If the py4j gateway itself is dead the "
            "driver JVM is gone: the run cannot recover in-process and must be "
            "restarted."
        ) from exc


def _catalog_impl(session: SparkSession) -> str | None:
    """The session's ``spark.sql.catalogImplementation``, or None if unreadable."""
    try:
        return session.conf.get("spark.sql.catalogImplementation", None)
    except Exception:  # noqa: BLE001 — a dying session must not mask the check
        return None


def _ensure_hive_catalog(
    session: SparkSession, builder: Any, app_name: str
) -> SparkSession:
    """Post-condition for ``enable_hive=True``: the session really has Hive.

    ``getOrCreate()`` hands back the process's existing session untouched, so
    the builder's ``enableHiveSupport()`` is a silent no-op whenever one is
    already up — the caller asked for Hive, got a session without it, and
    nothing said so. The failure then surfaces far away, as an
    ``AnalysisException`` from the first Hive DDL.

    Rebuilding the SparkSession object alone would not help:
    ``spark.sql.catalogImplementation`` comes from the SparkContext's
    SparkConf, so the context has to be stopped first. That is why this
    goes through ``_stop_and_clear`` rather than another plain getOrCreate.

    This is the framework-side half of the fix #242 made in
    ``tests/conftest.py``'s consumer-side ``spark`` fixture; both layers stay
    (defence in depth). See docs/operations/known-pitfalls.md 5a.
    """
    actual = _catalog_impl(session)
    if actual == "hive":
        return session

    logger.warning(
        "Hive support was requested but the session handed back has "
        "catalogImplementation=%s; stopping the context and rebuilding "
        "(app_name=%s)",
        actual, app_name,
        extra={
            "event": "spark_hive_downgrade_repaired",
            "catalog_implementation": actual,
            "app_name": app_name,
        },
    )
    _stop_and_clear(session)
    session = _get_or_create(builder, app_name)

    rebuilt = _catalog_impl(session)
    if rebuilt != "hive":
        # Clear it before raising. A session left registered stays reachable:
        # the next mode-2 call takes the `getActiveSession()` shortcut in
        # `_rebuild_or_active` and hands back the very session this raise just
        # refused, without ever re-entering this check. Refusing to return it
        # is not the same as refusing to leak it.
        _stop_and_clear(session)
        raise SparkSessionUnavailableError(
            f"Hive support was requested (enable_hive=True) but the "
            f"SparkSession has catalogImplementation={rebuilt!r}, not 'hive' "
            f"(app_name={app_name!r}) — even after stopping the context and "
            "rebuilding. Refusing to hand back a downgraded session: the next "
            "Hive DDL would fail with an unrelated AnalysisException. Check "
            "that hive-site.xml is reachable via SPARK_CONF_DIR and that the "
            "Spark build includes Hive support."
        )
    return session


def _mark_alive(session: SparkSession) -> None:
    global _last_app_id, _last_alive_ts
    _last_app_id = _safe_app_id(session)
    _last_alive_ts = time.time()


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
    succeed; when the py4j gateway itself is gone it raises at
    ``clearDefaultSession()`` (pyspark/sql/session.py) and never reaches its own
    assignments. The explicit assignments below are the belt-and-braces for that
    case, and mirror every singleton ``SparkSession.stop()`` would have cleared.
    """
    from pyspark.sql.context import SQLContext

    try:
        session.stop()
    except Exception as exc:  # noqa: BLE001 — JVM/gateway may already be gone
        logger.warning("SparkSession.stop() raised while clearing: %s", exc)

    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    SparkContext._active_spark_context = None
    SQLContext._instantiatedContext = None


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
