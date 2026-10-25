"""Keeping the evaluated ``model_version``'s predictions (Spark, lazy).

Which version is evaluated is decided in ``prepare_eval_data``; this module
holds how a predictions frame is kept to it.
"""

import logging

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F


def restrict_to_model_version(
    predictions: SparkDataFrame, model_version: str, caller_logger: logging.Logger,
) -> SparkDataFrame:
    """``predictions`` kept to ``model_version``: filtered on its
    ``model_version`` column when it has one, returned as given otherwise.

    A frame without the column is one the catalog already pruned:
    ``HiveTableDataset`` drops ``partition_filter`` columns after applying
    them, and both predictions sources of ``prepare_eval_data`` declare
    ``model_version`` as a static ``partition_filter``. The filter is for
    frames built outside the catalog (tests, ``--compare`` paths reading via
    ``spark.table``). ``caller_logger`` is the node's: the lines keep the logger
    name they are filtered by.
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
