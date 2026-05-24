"""Load Model B raw predictions for 2-way comparison.

Two source kinds:
  * model_version  — read ``ranked_predictions`` filtered by ``model_version``
  * external_hive  — read external Hive table with column rename + prod_mapping

The full source dict is staged at ``parameters['evaluation']['compare']`` by
the CLI dispatcher (``__main__.py``).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def load_compare_predictions(parameters: dict, spark: SparkSession) -> SparkDataFrame:
    """Dispatch on ``compare.kind`` and return raw Model B predictions."""
    eval_params = parameters.get("evaluation", {}) or {}
    src = eval_params.get("compare")
    if not src:
        raise RuntimeError(
            "parameters['evaluation']['compare'] missing — CLI must dispatch "
            "the chosen compare source dict here before pipeline run."
        )
    snap_date = str(eval_params.get("snap_date") or "").strip()
    if not snap_date:
        raise RuntimeError("evaluation.snap_date missing")

    schema = get_schema(parameters)
    kind = src.get("kind")
    if kind == "model_version":
        return _load_model_version(src, snap_date, schema, spark)
    if kind == "external_hive":
        return _load_external_hive(src, snap_date, schema, spark)
    raise RuntimeError(f"unknown compare source kind={kind!r}")


def _load_model_version(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    mv = src["model_version"]
    time_col = schema["time"]
    df = (
        spark.table("ranked_predictions")
        .filter(F.col("model_version") == mv)
        .filter(F.col(time_col).cast("string") == snap_date)
    )
    if df.isEmpty():
        raise DataConsistencyError(
            f"compare model_version={mv!r} has no rows for snap_date={snap_date!r}"
        )
    logger.info("Loaded compare predictions: model_version=%s rows=%d", mv, df.count())
    return df


def _load_external_hive(
    src: dict, snap_date: str, schema: dict, spark: SparkSession
) -> SparkDataFrame:
    raise NotImplementedError("external_hive branch lands in Task 5")
