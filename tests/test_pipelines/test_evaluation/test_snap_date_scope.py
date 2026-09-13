"""The step every reader of ``enriched_eval_predictions`` starts with.

The table holds every month a ``model_version`` was evaluated on (ADR-0018
decision 1), so a reader that keeps the wrong rows computes over several months
and raises nothing. The frames here therefore always hold a second month.
"""

import pytest

from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_date,
    restrict_to_eval_snap_date,
)


def _params(snap_date, time_col="as_of"):
    # The time column is deliberately not the example deployment's name: the
    # column comes from schema.time, never from the setting's key.
    return {
        "schema": {"columns": {
            "time": time_col, "entity": ["who"], "item": "what"}},
        "evaluation": {"snap_date": snap_date},
    }


def _two_months(spark):
    return spark.createDataFrame(
        [("2026-01-31", "a"), ("2026-02-28", "a"), ("2026-01-31", "b")],
        ["as_of", "who"],
    )


def test_keeps_only_the_evaluated_month(spark):
    out = restrict_to_eval_snap_date(_two_months(spark), _params("2026-01-31"))
    assert sorted((r.as_of, r.who) for r in out.collect()) == [
        ("2026-01-31", "a"), ("2026-01-31", "b"),
    ]


def test_a_month_with_no_rows_keeps_none(spark):
    """An absent partition is an empty frame here, not an error: the
    postcondition in ``compute_metrics`` is what refuses it."""
    out = restrict_to_eval_snap_date(_two_months(spark), _params("2025-12-31"))
    assert out.collect() == []


def test_the_setting_is_compared_stripped(spark):
    out = restrict_to_eval_snap_date(_two_months(spark), _params(" 2026-02-28 "))
    assert [(r.as_of, r.who) for r in out.collect()] == [("2026-02-28", "a")]


@pytest.mark.parametrize("evaluation", [{}, {"snap_date": None}, {"snap_date": "  "}])
def test_an_unset_month_raises_naming_the_key(evaluation):
    with pytest.raises(ValueError, match=r"evaluation\.snap_date not configured"):
        eval_snap_date({"evaluation": evaluation})
