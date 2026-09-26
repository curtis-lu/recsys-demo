"""What the B8 numeric-precision gate measures and reports.

The rule lives in ``core/consistency.py`` (``numeric_precision_errors``,
``numeric_precision_rows``); the decisions — which columns, which months,
which policy — in ``validate_numeric_precision``. This module holds the
mechanisms between the two: each column's value grid read off its dtype, the
``ColumnPrecision`` pairs the rule takes, and the persisted report with the log
lines that carry it. Which files the entity-level facts come from is
``steps/footer_facts.py``, shared with B10.

Both tables the gate measures — the entity-level one from footers, the
candidate-level one from a scan — go through the same functions here, so their
report sections cannot drift apart.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping

import pandas as pd

from recsys_tfb.core.consistency import (
    ColumnPrecision,
    numeric_precision_rows,
    spark_dtype_value_step,
)

logger = logging.getLogger(__name__)


def value_steps(dtypes: Mapping[str, str], columns: Iterable[str]) -> dict[str, float]:
    """``{column: grid spacing}`` for the ``columns`` whose dtype states one.

    Float and double state no grid (``spark_dtype_value_step`` is ``None``), so
    no bound can be claimed for them and they are left out. The keys are
    therefore the columns the gate actually checks, which is why callers read
    them rather than ``columns``.
    """
    steps: dict[str, float] = {}
    for column in columns:
        step = spark_dtype_value_step(dtypes[column])
        if step is not None:
            steps[column] = step
    return steps


def column_precisions(
    max_abs: Mapping[str, float | None],
    steps: Mapping[str, float],
) -> dict[str, ColumnPrecision]:
    """Each checked column's measured ``max(|x|)`` paired with its grid spacing.

    Keyed by ``steps``: a column measured but without a grid is not checked,
    and a checked column must have been measured (``read_max_abs_stats`` and
    ``months_present_and_max_abs`` return a key for every column asked).
    """
    return {c: ColumnPrecision(max_abs[c], steps[c]) for c in sorted(steps)}


def precision_report(
    storage_type: str,
    policy: str,
    months: list,
    by_column: Mapping[str, ColumnPrecision],
    dtypes: Mapping[str, str],
) -> dict:
    """The persisted shape of one table's precision measurement.

    ``dtypes`` is carried into each row because a reader asking "why is this
    column's limit so low" needs the scale, and looking it up means finding the
    table this report is about.
    """
    rows = numeric_precision_rows(by_column, storage_type)
    for row in rows:
        row["dtype"] = dtypes.get(row["column"])
    return {
        "storage_type": storage_type,
        "policy": policy,
        "months": [pd.Timestamp(m).strftime("%Y-%m-%d") for m in months],
        "checked_columns": len(rows),
        "breaches": sum(1 for r in rows if r["verdict"] == "breach"),
        "unmeasured": sum(1 for r in rows if r["verdict"] == "unmeasured"),
        "columns": rows,
    }


def log_precision_report(report: dict) -> None:
    """Log the report as a fixed-width table, closest-to-breaching first.

    Logged as well as returned because ``block`` aborts the run before the
    catalog can write the artifact — the one case where an operator most needs
    the numbers is the one where the file does not exist.
    """
    for line in _report_lines(report):
        logger.info("%s", line)


def _report_lines(report: dict) -> list[str]:
    lines = [
        f"  {'column':<28} {'dtype':<16} {'max(|x|)':>18} "
        f"{'limit':>18} {'headroom':>12}  verdict"
    ]
    for row in report["columns"]:
        headroom = "-" if row["headroom"] is None else f"{row['headroom']:.3g}x"
        max_abs = "-" if row["max_abs"] is None else f"{row['max_abs']:,.10g}"
        lines.append(
            f"  {row['column']:<28} {str(row['dtype']):<16} {max_abs:>18} "
            f"{row['limit']:>18,.10g} {headroom:>12}  {row['verdict']}"
        )
    return lines
