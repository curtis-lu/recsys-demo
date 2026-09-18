"""The step every reader of ``enriched_eval_predictions`` starts with.

The table holds every month a ``model_version`` was evaluated on (ADR-0018
decision 1), so a reader that keeps the wrong rows computes over several months
and raises nothing. The frames here therefore always hold a month that is not
evaluated.
"""

import pytest

from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
    PARTITION_FINGERPRINT_COLUMN,
)
from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
    eval_snap_dates,
    eval_snap_dates_without_rows,
    restrict_to_current_eval_partitions,
    restrict_to_eval_snap_dates,
    stamp_partition_fingerprint,
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


# --- Each partition carries the settings it was written under (#374) --------
#
# One date's partition can be written by a single-date run (``20260331/``) and
# by a several-date run (``20260131-20260331/``). The JSON fingerprint in each
# run's directory only speaks for what that directory's run wrote, so it cannot
# tell a reader that another run rewrote the partition since. Each row carries
# the fingerprint instead.

_Q1 = ["2026-01-31", "2026-02-28", "2026-03-31"]


def _settings(snap_date, segment_columns=("tier",), post_training=None):
    """Parameters that differ only in the partition-content settings."""
    params = _params(snap_date)
    params["evaluation"]["segment_columns"] = list(segment_columns)
    if post_training is not None:
        params["post_training"] = post_training
    return params


def _written(spark, months, params, joined=()):
    """What prepare_eval_data leaves in ``months`` when run with ``params``
    and it joined ``joined``."""
    frame = spark.createDataFrame(
        [(m, who) for m in months for who in ("a", "b")], ["as_of", "who"])
    return stamp_partition_fingerprint(frame, params, list(joined))


def _table(spark, *writes):
    """The table after ``writes`` — ``(months, params)`` or ``(months, params,
    joined)`` — in order: a later write replaces its months, as a dynamic
    partition overwrite does."""
    by_month = {}
    for months, params, *joined in writes:
        for month in months:
            by_month[month] = (params, joined[0] if joined else ())
    frames = [_written(spark, [m], p, j)
              for m, (p, j) in sorted(by_month.items())]
    table = frames[0]
    for frame in frames[1:]:
        table = table.unionByName(frame)
    return table


def _landed(params, joined=()):
    """The ``evaluation_segment_columns`` a run under ``params`` that joined
    ``joined`` lands in its directory."""
    from recsys_tfb.pipelines.evaluation.steps.config_fingerprint import (
        fingerprint,
    )

    return {"joined": list(joined), "config_fingerprint": fingerprint(params)}


def _read(table, params, joined=(), **kwargs):
    """A reader under ``params`` whose directory's JSON says ``joined``."""
    return restrict_to_current_eval_partitions(
        table, params, _landed(params, joined), **kwargs)


def test_a_month_rewritten_by_a_single_date_run_stops_the_range_resume(spark):
    """Q1 under settings A; March re-run alone under settings B; resuming the
    Q1 run (A) must not read B's March. Its directory's JSON still says A."""
    a_range, b_single = _settings(_Q1), _settings("2026-03-31", ("region",))
    table = _table(spark, (_Q1, a_range), (["2026-03-31"], b_single))
    with pytest.raises(ValueError) as excinfo:
        _read(table, a_range)
    message = str(excinfo.value)
    assert "2026-03-31" in message, message
    assert "2026-01-31" not in message and "2026-02-28" not in message, message
    assert "--from-node prepare_eval_data" in message, message


def test_a_month_rewritten_by_a_range_run_stops_the_single_date_resume(spark):
    """The other direction: March alone under A, then Q1 under B rewrites
    March; resuming the March run (A) must not read B's March."""
    a_single, b_range = _settings("2026-03-31"), _settings(_Q1, ("region",))
    table = _table(spark, (["2026-03-31"], a_single), (_Q1, b_range))
    with pytest.raises(ValueError, match=r"2026-03-31"):
        _read(table, a_single)


def test_a_run_mode_switch_is_a_different_partition(spark):
    """post_training is a partition-content setting: monitoring rows are not
    the post-training population."""
    monitoring = _settings("2026-03-31", post_training=False)
    table = _table(spark, (["2026-03-31"], monitoring))
    with pytest.raises(ValueError, match=r"2026-03-31"):
        _read(table, _settings("2026-03-31", post_training=True))


def test_same_settings_over_other_dates_do_not_block_each_other(spark):
    """A single-date run and a range run with the same settings write the same
    rows for the date they share, so neither refuses the other's partition."""
    a_range, a_single = _settings(_Q1), _settings("2026-03-31")
    table = _table(spark, (_Q1, a_range), (["2026-03-31"], a_single))

    as_range = _read(table, a_range)
    as_single = _read(table, a_single)

    assert as_range.frame.count() == 6
    assert as_single.frame.count() == 2
    assert PARTITION_FINGERPRINT_COLUMN not in as_range.frame.columns
    assert as_range.dates_without_rows == [] == as_single.dates_without_rows


def test_a_partition_written_before_fingerprints_is_named_as_such(spark):
    """The table's schema evolves on write, so a partition written before #374
    reads back with the column NULL; a table never rewritten has no column."""
    from pyspark.sql import functions as F

    a_range = _settings(_Q1)
    table = _table(spark, (_Q1, a_range)).withColumn(
        PARTITION_FINGERPRINT_COLUMN,
        F.when(F.col("as_of") == "2026-02-28", F.lit(None).cast("string"))
        .otherwise(F.col(PARTITION_FINGERPRINT_COLUMN)))
    with pytest.raises(ValueError) as excinfo:
        _read(table, a_range)
    message = str(excinfo.value)
    assert "2026-02-28: written before" in message, message
    assert "2026-01-31" not in message, message

    no_column = table.drop(PARTITION_FINGERPRINT_COLUMN)
    with pytest.raises(ValueError, match=r"2026-03-31: written before"):
        _read(no_column, _settings("2026-03-31"))


def test_a_month_written_with_other_segment_columns_joined_is_refused(spark):
    """Same settings, but February's run joined nothing (its population lacked
    ``tier``) while the range directory's JSON says ``[tier]``."""
    a_range, a_single = _settings(_Q1), _settings("2026-02-28")
    table = _table(spark, (_Q1, a_range, ["tier"]),
                   (["2026-02-28"], a_single, []))
    with pytest.raises(ValueError) as excinfo:
        _read(table, a_range, joined=["tier"])
    message = str(excinfo.value)
    assert "2026-02-28: written under other settings" in message, message
    assert "2026-01-31" not in message, message
    # Same joined list on every write: read.
    same = _table(spark, (_Q1, a_range, ["tier"]),
                  (["2026-02-28"], a_single, ["tier"]))
    assert _read(same, a_range, joined=["tier"]).frame.count() == 6


def test_compare_only_takes_the_settings_recorded_in_the_json(spark):
    """``recorded_settings``: a post-training run's partition, read with
    ``post_training`` unset today, matches what its directory's JSON records;
    today's settings would refuse it."""
    post_training = _settings("2026-03-31", post_training=True)
    table = _table(spark, (["2026-03-31"], post_training))
    today = _settings("2026-03-31", post_training=False)
    landed = _landed(post_training)
    kept = restrict_to_current_eval_partitions(
        table, today, landed, recorded_settings=True)
    assert kept.frame.count() == 2
    with pytest.raises(ValueError, match=r"2026-03-31: written under other"):
        restrict_to_current_eval_partitions(table, today, landed)


def test_every_stale_month_is_named_in_one_raise(spark):
    a_range, b = _settings(_Q1), _settings(_Q1, ("region",))
    table = _table(spark, (_Q1, b))
    with pytest.raises(ValueError) as excinfo:
        _read(table, a_range)
    for month in _Q1:
        assert month in str(excinfo.value)


def test_a_configured_month_without_rows_is_reported_not_raised(spark):
    """No rows is not a fingerprint mismatch: the caller decides (the
    --compare-only gate raises, compute_metrics' postcondition raises)."""
    a_range = _settings(_Q1)
    table = _table(spark, (["2026-01-31", "2026-03-31"], a_range))
    partitions = _read(table, a_range)
    assert partitions.dates_without_rows == ["2026-02-28"]
    assert partitions.frame.count() == 4


def _spy_actions(monkeypatch):
    from pyspark.sql import DataFrame

    calls = []
    real_collect = DataFrame.collect

    def collect(self):
        calls.append(("collect",
                      self._jdf.queryExecution().optimizedPlan().toString()))
        return real_collect(self)

    def is_empty(self):
        calls.append(("isEmpty", ""))
        raise AssertionError("per-date isEmpty is what the check replaces")

    monkeypatch.setattr(DataFrame, "collect", collect)
    monkeypatch.setattr(DataFrame, "isEmpty", is_empty)
    return calls


def _spark_jobs(spark, run):
    """How many Spark jobs ``run`` submits, counted by the status tracker."""
    import uuid

    group = f"cost-{uuid.uuid4().hex}"
    context = spark.sparkContext
    context.setJobGroup(group, group)
    try:
        run()
    finally:
        context.setLocalProperty("spark.jobGroup.id", None)
    return len(context.statusTracker().getJobIdsForGroup(group))


@pytest.mark.parametrize("dates", [["2026-03-31"], _Q1], ids=["one", "three"])
def test_the_check_is_one_limited_read_per_date_in_one_job(
    spark, monkeypatch, dates,
):
    """Every reader pays this, so its cost must not grow with the table: one
    action, one Spark job, and per date a one-row read of that partition
    (``GlobalLimit 1`` per date), never an aggregate over the column. A
    partition is written by one dynamic overwrite, so one row answers for it."""
    params = _settings(dates if len(dates) > 1 else dates[0])
    table = _table(spark, (_Q1, params))
    calls = _spy_actions(monkeypatch)
    jobs = _spark_jobs(spark, lambda: _read(table, params))
    plan = calls[0][1] if calls else ""
    assert "Aggregate" not in plan, plan
    assert plan.count("GlobalLimit 1") == len(dates), plan
    assert [kind for kind, _ in calls] == ["collect"]
    assert jobs == 1


def test_dates_without_rows_are_found_in_one_job(spark, monkeypatch):
    """The same shape serves the predictions' per-date check."""
    params = _params(_Q1)
    calls = _spy_actions(monkeypatch)
    jobs = _spark_jobs(spark, lambda: eval_snap_dates_without_rows(
        _two_months(spark), params))
    assert [kind for kind, _ in calls] == ["collect"]
    assert jobs == 1
