"""Preprocessing module: fit/transform/apply logic shared across pipelines.

Backends are split into separate submodules so that importing the pandas
path never loads pyspark:

- ``._pandas``  — pandas backend (no pyspark dependency)
- ``._spark``   — Spark backend  (imports pyspark at module level, safe
                  because only ``nodes_spark`` files import it)
- ``._common``  — backend-agnostic helpers
"""
