"""The step every reader of ``enriched_eval_predictions`` starts with.

The table holds every month a ``model_version`` was evaluated on (ADR-0018
decision 1), so a reader that keeps the wrong rows computes over several months
and raises nothing. The frames here therefore always hold a month that is not
evaluated.
"""

import pytest

from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_dates,
    eval_snap_dates_without_rows,
    restrict_to_eval_snap_dates,
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


def _three_months(spark):
    return spark.createDataFrame(
        [("2026-01-31", "a"), ("2026-02-28", "a"), ("2026-03-31", "a"),
         ("2026-03-31", "b")],
        ["as_of", "who"],
    )


def _rows(df):
    return sorted((r.as_of, r.who) for r in df.collect())


def test_keeps_only_the_evaluated_month(spark):
    out = restrict_to_eval_snap_dates(_two_months(spark), _params("2026-01-31"))
    assert _rows(out) == [("2026-01-31", "a"), ("2026-01-31", "b")]


def test_a_month_with_no_rows_keeps_none(spark):
    """An absent partition is an empty frame here, not an error: the
    postcondition in ``compute_metrics`` is what refuses it."""
    out = restrict_to_eval_snap_dates(_two_months(spark), _params("2025-12-31"))
    assert out.collect() == []


def test_the_setting_is_compared_stripped(spark):
    out = restrict_to_eval_snap_dates(_two_months(spark), _params(" 2026-02-28 "))
    assert _rows(out) == [("2026-02-28", "a")]


def test_several_dates_keep_exactly_those_dates(spark):
    """Two of three months configured: both kept, the third dropped."""
    out = restrict_to_eval_snap_dates(
        _three_months(spark), _params(["2026-01-31", "2026-03-31"]))
    assert _rows(out) == [
        ("2026-01-31", "a"), ("2026-03-31", "a"), ("2026-03-31", "b"),
    ]


def test_one_date_keeps_the_equality_form_whose_pruning_was_measured(spark):
    """A single date filters with ``=``, the form ADR-0018 checked prunes a
    STRING partition; only several dates use ``IN``."""
    one = restrict_to_eval_snap_dates(_two_months(spark), _params("2026-01-31"))
    plan = one._jdf.queryExecution().analyzed().toString()
    assert "= 2026-01-31)" in plan, plan
    assert " IN (" not in plan, plan


@pytest.mark.parametrize("value, expected", [
    ("2026-01-31", ["2026-01-31"]),
    ([" 2026-03-31", "2026-01-31 "], ["2026-03-31", "2026-01-31"]),
])
def test_the_dates_are_read_as_a_list_in_configured_order(value, expected):
    assert eval_snap_dates({"evaluation": {"snap_date": value}}) == expected


@pytest.mark.parametrize(
    "evaluation",
    [{}, {"snap_date": None}, {"snap_date": "  "}, {"snap_date": []}],
)
def test_an_unset_month_raises_naming_the_key(evaluation):
    with pytest.raises(ValueError, match=r"evaluation\.snap_date not configured"):
        eval_snap_dates({"evaluation": evaluation})


def test_dates_without_rows_names_each_missing_date(spark):
    """2026-02-28 has no row: it is named, the two that have rows are not."""
    missing = eval_snap_dates_without_rows(
        _three_months(spark).filter("as_of != '2026-02-28'"),
        _params(["2026-01-31", "2026-02-28", "2026-03-31"]),
    )
    assert missing == ["2026-02-28"]


def test_dates_without_rows_is_empty_when_every_date_has_rows(spark):
    assert eval_snap_dates_without_rows(
        _three_months(spark), _params(["2026-01-31", "2026-03-31"])) == []
    assert eval_snap_dates_without_rows(
        _two_months(spark), _params("2026-01-31")) == []
    assert eval_snap_dates_without_rows(
        _two_months(spark), _params("2025-12-31")) == ["2025-12-31"]
