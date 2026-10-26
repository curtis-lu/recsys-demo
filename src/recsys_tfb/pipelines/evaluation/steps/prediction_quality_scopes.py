"""The rows of a ``--prediction-quality --scope`` run (ADR-0032 decision 7):
which scope the run names, the scope table's keys, read and checked, and the
evaluated rows they name."""

from __future__ import annotations

from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from recsys_tfb.core.consistency import (
    UNSCOPED_PREDICTION_QUALITY,
    ConfigConsistencyError,
    scope_flag_errors,
    scope_table_errors,
)
from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import normalised
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_dates_without_rows,
    restrict_to_eval_snap_dates,
)

#: The runtime parameter the CLI sets to the ``--scope`` name, or
#: ``UNSCOPED_PREDICTION_QUALITY``; also the output directory's segment.
SCOPE_PARAMETER = "prediction_quality_scope"


def run_scope(parameters: dict) -> tuple[str, dict | None]:
    """The run's scope name and its ``evaluation.scopes`` entry, ``None``
    when unscoped (no name given counts as unscoped).

    Pre-check (config): A66's predicate again, the runtime backstop of the
    CLI's (a name ``evaluation.scopes`` does not register).
    """
    name = parameters.get(SCOPE_PARAMETER, UNSCOPED_PREDICTION_QUALITY)
    if name == UNSCOPED_PREDICTION_QUALITY:
        return name, None
    errors = scope_flag_errors(parameters, name, prediction_quality=True)
    if errors:
        raise ConfigConsistencyError("\n".join(errors))
    return name, parameters["evaluation"]["scopes"][name]


def scope_definition(parameters: dict) -> dict | None:
    """What the run's scope selects, as a result records it and a redraw
    compares it (``config_fingerprint.require_same_recorded_values``): its
    ``table`` and ``columns``, normalised; ``None`` when unscoped. The name
    is in the output path."""
    _, scope = run_scope(parameters)
    if scope is None:
        return None
    return normalised({"table": scope["table"], "columns": scope["columns"]})


def read_scope_keys(
    spark: SparkSession, name: str, scope: dict, row_types: dict,
    parameters: dict,
) -> tuple[SparkDataFrame, list[str]]:
    """Scope ``name``'s distinct keys on the evaluated dates, under the
    resolved column names, and the evaluated dates it holds no row for (a
    campaign that skips a month, or a month written in another format).
    ``row_types`` is the evaluated rows' type of each resolved column.

    Pre-check (config): A67, off the metastore, before a row is read.

    Costs one scan of the table per evaluated date (it need not be
    partitioned by time), on top of what the keys' readers scan.
    """
    try:
        table_types = dict(spark.table(scope["table"]).dtypes)
        unreadable = ""
    except AnalysisException as exc:
        table_types, unreadable = None, str(exc).splitlines()[0]
    errors = scope_table_errors(
        name, scope, table_types, row_types, unreadable=unreadable)
    if errors:
        raise ConfigConsistencyError("\n".join(errors))
    keys = spark.table(scope["table"]).select(
        *[F.col(theirs).alias(ours) for ours, theirs in scope["columns"].items()])
    without_rows = eval_snap_dates_without_rows(
        keys, parameters, time_partitioned=False)
    return restrict_to_eval_snap_dates(keys, parameters).distinct(), without_rows


def count_keys_not_scored(
    keys: SparkDataFrame, eval_predictions: SparkDataFrame,
) -> int:
    """How many of ``keys`` no evaluated row carries (an entity outside the
    scored population, an item no candidate holds). One anti-join."""
    return keys.join(eval_predictions, keys.columns, "left_anti").count()
