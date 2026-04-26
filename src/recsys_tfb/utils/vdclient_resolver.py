"""Resolve ${...} placeholders in spark config values.

Two placeholder forms are supported:

  ``${vdclient.<name>}`` → ``vdclient.get_<name>()``
      ``vdclient`` is an environment-provided package not always available
      (laptops, CI). Lazy-imported once per call.

  ``${env.<NAME>}`` → ``os.environ["<NAME>"]``
      Reads the process environment.

Both resolvers share the same drop-on-missing semantics: if the lookup
fails (import error, missing getter, missing env var), the affected
spark config key is dropped from the returned dict and a warning is
logged, so PySpark falls back to its default.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)
_VDCLIENT_PATTERN = re.compile(r"\$\{vdclient\.(\w+)\}")
_ENV_PATTERN = re.compile(r"\$\{env\.(\w+)\}")


def resolve_vdclient_placeholders(spark_configs: dict) -> dict:
    """Resolve ${vdclient.<name>} placeholders by calling vdclient.get_<name>().

    For each value containing one or more placeholders:
      1. Lazy-import ``vdclient`` (once per call). Import failure → drop the
         key + warn.
      2. For each ``<name>``, look up ``get_<name>`` on the module. Missing
         getter → drop the key + warn.
      3. Otherwise call the getter and substitute the placeholder text.

    Values without placeholders pass through unchanged. Non-string values pass
    through unchanged.
    """
    resolved: dict = {}
    vdclient_mod = None
    vdclient_import_attempted = False

    for key, value in spark_configs.items():
        if not isinstance(value, str):
            resolved[key] = value
            continue
        names = _VDCLIENT_PATTERN.findall(value)
        if not names:
            resolved[key] = value
            continue

        if not vdclient_import_attempted:
            vdclient_import_attempted = True
            try:
                import vdclient as _vdclient_mod  # type: ignore[import]

                vdclient_mod = _vdclient_mod
            except ImportError:
                vdclient_mod = None
                logger.warning(
                    "vdclient not importable; spark config keys with "
                    "${vdclient.*} placeholders will be dropped."
                )

        if vdclient_mod is None:
            logger.warning(
                "Dropping spark config key '%s' (placeholders %s): "
                "vdclient unavailable",
                key,
                names,
            )
            continue

        new_value = value
        drop_this_key = False
        for name in names:
            getter = getattr(vdclient_mod, f"get_{name}", None)
            if getter is None:
                logger.warning(
                    "Dropping spark config key '%s': vdclient has no get_%s",
                    key,
                    name,
                )
                drop_this_key = True
                break
            new_value = new_value.replace(
                f"${{vdclient.{name}}}", str(getter())
            )
        if drop_this_key:
            continue
        resolved[key] = new_value

    return resolved


def resolve_env_placeholders(spark_configs: dict) -> dict:
    """Resolve ${env.<NAME>} placeholders by reading os.environ["<NAME>"].

    For each value containing one or more placeholders, look up each name in
    ``os.environ``. Missing variable → drop the key + warn. Non-string values
    pass through unchanged.
    """
    resolved: dict = {}

    for key, value in spark_configs.items():
        if not isinstance(value, str):
            resolved[key] = value
            continue
        names = _ENV_PATTERN.findall(value)
        if not names:
            resolved[key] = value
            continue

        new_value = value
        drop_this_key = False
        for name in names:
            env_value = os.environ.get(name)
            if env_value is None:
                logger.warning(
                    "Dropping spark config key '%s': env var '%s' not set",
                    key,
                    name,
                )
                drop_this_key = True
                break
            new_value = new_value.replace(f"${{env.{name}}}", env_value)
        if drop_this_key:
            continue
        resolved[key] = new_value

    return resolved
