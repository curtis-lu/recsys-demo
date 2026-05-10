"""Preprocessing module: fit/transform/apply logic for the Spark pipeline.

- ``._spark``   — Spark backend  (imports pyspark at module level, safe
                  because only ``nodes_spark`` files import it)
- ``._common``  — backend-agnostic helpers
"""
