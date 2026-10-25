"""Keeping the evaluated ``model_version``'s predictions (Spark, lazy).

Which version is evaluated is decided in ``prepare_eval_data``; this module
holds how a predictions frame is kept to it.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


def restrict_to_model_version(
    caller_logger: logging.Logger, predictions: SparkDataFrame, model_version: str,
) -> SparkDataFrame:
    """``predictions`` kept to ``model_version``: filtered on its
    ``model_version`` column when it has one, returned as given otherwise.

    A frame without the column is taken as already pruned by the catalog:
    ``HiveTableDataset`` drops ``partition_filter`` columns after applying
    them. ``caller_logger`` is the node's: the lines keep the logger name they
    are filtered by.
    """
    if "model_version" in predictions.columns:
        caller_logger.info("Filtering predictions to model_version=%s", model_version)
        return predictions.filter(F.col("model_version") == model_version)
    caller_logger.info(
        "Predictions input has no model_version column; assuming catalog "
        "partition_filter already selected model_version=%s",
        model_version,
    )
    return predictions
