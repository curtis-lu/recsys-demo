"""The rows of a ``--prediction-quality --scope`` run (ADR-0032 decision 7):
the scope table's keys, read and checked, and the evaluated rows they name."""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from recsys_tfb.core.consistency import (
    ConfigConsistencyError,
    DataConsistencyError,
    scope_table_errors,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_dates_without_rows,
    restrict_to_eval_snap_dates,
)


def read_scope_keys(
    spark: SparkSession, name: str, scope: dict, parameters: dict,
) -> SparkDataFrame:
    """Scope ``name``'s distinct keys on the evaluated dates, under the
    resolved column names its ``columns`` maps from.

    Pre-check (config): A67, off the metastore, before a row is read.

    Pre-check (input): every evaluated date has a row. A month the table
    lacks, or writes in another format, would drop out of every number
    without a word.
    """
    try:
        table_columns = spark.table(scope["table"]).columns
    except AnalysisException:
        table_columns = None
    errors = scope_table_errors(name, scope, table_columns)
    if errors:
        raise ConfigConsistencyError("\n".join(errors))
    keys = spark.table(scope["table"]).select(
        *[F.col(theirs).alias(ours) for ours, theirs in scope["columns"].items()])
    # The user's table: nothing says it is partitioned by the time column.
    missing = eval_snap_dates_without_rows(
        keys, parameters, time_partitioned=False)
    if missing:
        raise DataConsistencyError(
            f"scope {name!r}: table {scope['table']!r} has no row for the "
            f"evaluated date(s) {missing}; its time column must hold them as "
            f"the framework writes them.")
    return restrict_to_eval_snap_dates(keys, parameters).distinct()


def count_keys_not_scored(
    keys: SparkDataFrame, eval_predictions: SparkDataFrame,
) -> int:
    """How many of ``keys`` no evaluated row carries (an entity outside the
    scored population, an item no candidate holds)."""
    return keys.join(eval_predictions, keys.columns, "left_anti").count()
