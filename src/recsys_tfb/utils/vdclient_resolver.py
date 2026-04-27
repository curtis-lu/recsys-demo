"""Resolve ${...} placeholders in spark config values.

Two placeholder forms are supported:

  ``${vdclient.<cluster>.<field>}`` → ``vdclient_magic.spark_ports("<cluster>")``
      Returns the named field from the port tuple. Supported fields:
      ``driver_port`` (index 0), ``blockManager_port`` (index 1).
      ``vdclient_magic`` is production-only; unavailable on laptops / CI.
      Lazy-imported once; result cached per cluster per call.

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
_VDCLIENT_PATTERN = re.compile(r"\$\{vdclient\.(\w+)\.(\w+)\}")
_ENV_PATTERN = re.compile(r"\$\{env\.(\w+)\}")

# Fields returned positionally by spark_ports(cluster).
_SPARK_PORTS_FIELDS = ("driver_port", "blockManager_port")


def resolve_vdclient_placeholders(spark_configs: dict) -> dict:
    """Resolve ${vdclient.<cluster>.<field>} placeholders via vdclient_magic.spark_ports().

    For each value containing one or more placeholders:
      1. Lazy-import ``vdclient_magic`` (once per call). Import failure → drop
         the key + warn.
      2. Call ``spark_ports(cluster)`` and cache the (driver_port,
         blockManager_port) tuple. Unknown field name → drop the key + warn.
      3. Otherwise substitute the placeholder text.

    Values without placeholders pass through unchanged. Non-string values pass
    through unchanged.
    """
    resolved: dict = {}
    vdclient_mod = None
    vdclient_import_attempted = False
    spark_ports_cache: dict = {}

    for key, value in spark_configs.items():
        if not isinstance(value, str):
            resolved[key] = value
            continue
        matches = _VDCLIENT_PATTERN.findall(value)
        if not matches:
            resolved[key] = value
            continue

        if not vdclient_import_attempted:
            vdclient_import_attempted = True
            try:
                import vdclient_magic as _vdclient_mod  # type: ignore[import]

                vdclient_mod = _vdclient_mod
            except ImportError:
                vdclient_mod = None
                logger.warning(
                    "vdclient_magic not importable; spark config keys with "
                    "${vdclient.*.*} placeholders will be dropped."
                )

        if vdclient_mod is None:
            logger.warning(
                "Dropping spark config key '%s' (placeholders %s): "
                "vdclient_magic unavailable",
                key,
                matches,
            )
            continue

        new_value = value
        drop_this_key = False
        for cluster, field in matches:
            if cluster not in spark_ports_cache:
                try:
                    ports = vdclient_mod.spark_ports(cluster)
                    spark_ports_cache[cluster] = dict(
                        zip(_SPARK_PORTS_FIELDS, (str(p) for p in ports))
                    )
                except Exception as exc:
                    logger.warning(
                        "Dropping spark config key '%s': spark_ports(%r) failed: %s",
                        key,
                        cluster,
                        exc,
                    )
                    drop_this_key = True
                    break
            port_val = spark_ports_cache[cluster].get(field)
            if port_val is None:
                logger.warning(
                    "Dropping spark config key '%s': unknown vdclient field '%s' "
                    "(supported: %s)",
                    key,
                    field,
                    ", ".join(_SPARK_PORTS_FIELDS),
                )
                drop_this_key = True
                break
            new_value = new_value.replace(
                f"${{vdclient.{cluster}.{field}}}", port_val
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
