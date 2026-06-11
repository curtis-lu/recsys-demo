"""Unit tests for recsys_tfb.utils.spark._is_session_alive.

Pure-logic tests over the SparkContext wrapper — no real SparkSession, so
they stay fast and run outside the `spark` marker.
"""

from unittest.mock import MagicMock

from recsys_tfb.utils.spark import _is_session_alive


def _session_with_jsc(jsc) -> MagicMock:
    session = MagicMock()
    session.sparkContext._jsc = jsc
    return session


def test_live_context_is_alive():
    jsc = MagicMock()
    jsc.sc.return_value.isStopped.return_value = False
    assert _is_session_alive(_session_with_jsc(jsc)) is True


def test_stopped_context_is_not_alive():
    """Regression: `_jsc` can be a non-None wrapper whose JVM context is stopped.

    The old check (`_jsc is not None`) reported such a session as alive, so
    `_fallback_create` returned a dead session and the downstream Hive write
    hit ``IllegalStateException: Cannot call methods on a stopped
    SparkContext`` (production HPO→predict failure). Probing ``isStopped()``
    detects it and lets the fallback rebuild a fresh session instead.
    """
    jsc = MagicMock()
    jsc.sc.return_value.isStopped.return_value = True
    assert _is_session_alive(_session_with_jsc(jsc)) is False


def test_missing_jsc_is_not_alive():
    # Clean PySpark stop() nulls `_jsc`; must short-circuit before calling
    # `.sc()` on None.
    assert _is_session_alive(_session_with_jsc(None)) is False


def test_probe_raising_is_not_alive():
    # Any failure while probing the context must read as "not alive" so the
    # caller rebuilds rather than propagating a low-level Py4J error.
    jsc = MagicMock()
    jsc.sc.side_effect = RuntimeError("gateway gone")
    assert _is_session_alive(_session_with_jsc(jsc)) is False
