"""The evaluated month: which one it is, and keeping only its rows.

``enriched_eval_predictions`` holds every month this ``model_version`` has been
evaluated on: ``prepare_eval_data`` writes one time partition per run, and the
catalog entry's ``partition_filter`` prunes ``model_version`` only. So every
node that reads the table starts by keeping the evaluated month (ADR-0018
decision 1). A reader that forgets raises nothing and computes over every month;
``tests/test_pipelines/test_evaluation/test_pipeline.py`` fails any node wired to
the table whose body does not call :func:`restrict_to_eval_snap_date`.

Why not put the month in the catalog's ``partition_filter``:
``HiveTableDataset.load`` drops filter columns from the frame it returns, and the
time column is half of every query group.

Why the step takes ``parameters`` instead of the month: the config key is read
in one function, :func:`eval_snap_date`, so the S6 literal-column registry holds
one entry for it rather than one per reader.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.schema import get_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def eval_snap_date(parameters: dict) -> str:
    """``evaluation.snap_date``, stripped, as the text the partition stores.

    Not reformatted: the time partition is a STRING written from rows selected
    with this same setting, so comparing text with text is exact.
    """
    value = str(
        ((parameters.get("evaluation") or {}).get("snap_date")) or ""
    ).strip()
    if not value:
        raise ValueError(
            "evaluation.snap_date not configured. Set evaluation.snap_date "
            "(ISO YYYY-MM-DD) in conf/base/parameters_evaluation.yaml."
        )
    return value


def restrict_to_eval_snap_date(df: DataFrame, parameters: dict) -> DataFrame:
    """``df`` restricted to the evaluated month.

    ``cast("string")`` on a STRING partition column is a no-op, so Spark keeps
    pruning partitions. Not ``to_date``: whether that form still prunes a string
    partition was never measured. It is the comparison ``prepare_eval_data``
    already filters its predictions with.
    """
    time_col = get_schema(parameters)["time"]
    return df.filter(F.col(time_col).cast("string") == eval_snap_date(parameters))
