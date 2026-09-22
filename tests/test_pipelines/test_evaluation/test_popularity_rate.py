"""The positive-rate popularity baseline (#397): pipeline shape, the node pair
``build_popularity_period_counts`` → ``compute_baseline_metrics``, and the
recount rule of ``popularity_period_counts``.
"""

import logging
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml

from recsys_tfb.pipelines.dataset.month_plans import (
    landed_months,
    plan_incremental_snap_dates,
)
from recsys_tfb.pipelines.evaluation.pipeline import create_pipeline

_NODE = "build_popularity_period_counts"


def _rate_params(score="rate"):
    from tests.test_pipelines.test_evaluation.test_nodes import (
        TestComputeBaselineMetrics,
    )

    params = TestComputeBaselineMetrics._parameters()
    params["evaluation"]["baseline"]["score"] = score
    return params


def _segments(params):
    from tests.test_pipelines.test_evaluation.test_nodes import _no_segments

    return _no_segments(params)


def _eval_predictions(spark, params=None):
    """2025-01-31: c1 holds positives A and C, c2 holds B. Stamped with the
    fingerprint of ``params`` (the fixture's own by default), as
    prepare_eval_data under those settings would write it."""
    from recsys_tfb.pipelines.evaluation.steps.snap_date_scope import (
        stamp_partition_fingerprint,
    )
    from tests.test_pipelines.test_evaluation.test_nodes import (
        TestComputeBaselineMetrics,
    )

    frame = TestComputeBaselineMetrics._eval_predictions(spark).drop(
        "eval_partition_fingerprint")
    return stamp_partition_fingerprint(
        frame, params or TestComputeBaselineMetrics._parameters(), [])


def _history(spark):
    """2024-06-30 history. Candidates: A 10, B 2, C 5; positives: A 3, B 1.

    label_table holds the positive rows only (clicks), so its row count per
    item equals the positive count: a denominator taken from it gives every
    item with a positive a rate of 1.
    """
    pool = [("2024-06-30", f"h{i}", "A") for i in range(10)]
    pool += [("2024-06-30", f"h{i}", "B") for i in range(2)]
    pool += [("2024-06-30", f"h{i}", "C") for i in range(5)]
    labels = [("2024-06-30", f"h{i}", "A", 1) for i in range(3)]
    labels += [("2024-06-30", "h0", "B", 1)]
    return (
        spark.createDataFrame(pd.DataFrame(
            pool, columns=["snap_date", "cust_id", "prod_name"])),
        spark.createDataFrame(pd.DataFrame(
            labels, columns=["snap_date", "cust_id", "prod_name", "label"])),
    )


def _plan(periods, existing=(), rebuild=()):
    return plan_incremental_snap_dates(periods, existing, rebuild)


def _node(name, **pipeline_kwargs):
    return next(n for n in create_pipeline(**pipeline_kwargs).nodes
                if n.name == name)


def _call(node, **datasets):
    """As the Runner calls it: inputs by position, by their catalog names."""
    return node.func(*[datasets[name] for name in node.inputs])


class TestPipelineShape:
    @pytest.mark.parametrize("post_training", [False, True])
    def test_switch_off_is_the_pipeline_without_the_feature(self, post_training):
        default = create_pipeline(post_training=post_training)
        off = create_pipeline(post_training=post_training, baseline_rate=False)
        assert [(n.name, n.inputs, n.outputs) for n in off.nodes] == [
            (n.name, n.inputs, n.outputs) for n in default.nodes]
        assert _NODE not in {n.name for n in off.nodes}
        baseline = next(n for n in off.nodes
                        if n.name == "compute_baseline_metrics")
        assert baseline.inputs == [
            "enriched_eval_predictions", "label_table",
            "evaluation_segment_columns", "parameters"]

    def test_monitoring_off_reads_no_sample_pool(self):
        inputs = {i for n in create_pipeline().nodes for i in n.inputs}
        assert "sample_pool" not in inputs

    @pytest.mark.parametrize("post_training", [False, True])
    def test_switch_on_wires_the_counts_before_the_baseline(self, post_training):
        pipe = create_pipeline(post_training=post_training, baseline_rate=True)
        names = [n.name for n in pipe.nodes]
        node = pipe.nodes[names.index(_NODE)]
        assert node.inputs == ["sample_pool", "label_table",
                               "popularity_period_counts_month_plan", "parameters"]
        assert node.outputs == ["popularity_period_counts"]
        assert names.index(_NODE) < names.index("compute_baseline_metrics")
        baseline = pipe.nodes[names.index("compute_baseline_metrics")]
        assert baseline.inputs[-2:] == [
            "popularity_period_counts", "popularity_period_counts_month_plan"]

    def test_compare_only_wires_no_baseline_either_way(self):
        pipe = create_pipeline(compare_source={"kind": "hive"},
                               compare_only=True, baseline_rate=True)
        assert _NODE not in {n.name for n in pipe.nodes}


class TestTheCatalogEntry:
    @pytest.mark.parametrize("conf", ["conf/base", "examples/ad/conf/base"])
    def test_not_keyed_by_the_model(self, conf):
        """Another model_version evaluated on the same dates lists the same
        partitions, so it recounts nothing."""
        root = Path(__file__).resolve().parents[3]
        entry = yaml.safe_load(
            (root / conf / "catalog.yaml").read_text())["popularity_period_counts"]
        assert entry["partition_filter"] == {
            "popularity_source_version": "${popularity_source_version}"}
        assert [c["name"] for c in entry["partition_cols"]] == ["snap_date"]


def _rate_run(spark, params, pool, labels, plan, eval_predictions=None):
    """The two nodes as --post-training wires them, as the Runner calls them."""
    counts = _call(
        _node(_NODE, post_training=True, baseline_rate=True),
        sample_pool=pool, label_table=labels, parameters=params,
        popularity_period_counts_month_plan=plan)
    return _call(
        _node("compute_baseline_metrics", post_training=True, baseline_rate=True),
        enriched_eval_predictions=(
            eval_predictions if eval_predictions is not None
            else _eval_predictions(spark)),
        label_table=labels, evaluation_segment_columns=_segments(params),
        parameters=params, popularity_period_counts=counts,
        popularity_period_counts_month_plan=plan)


def test_post_training_rate_mode_scores_by_rate(spark):
    """Count ranks A > B > C, rate (A 0.3, B 0.5, C 0) ranks B > A > C, so the
    baseline's mAP moves:

    * count: c1 (A, C positive) A①B②C③ → AP (1 + 2/3) / 2; c2 (B) at ② → 1/2
    * rate:  c1 B①A②C③ → AP (1/2 + 2/3) / 2; c2 B at ① → 1
    """
    params = _rate_params()
    pool, labels = _history(spark)
    result = _rate_run(spark, params, pool, labels, _plan(["2024-06-30"]))

    block = result["popularity_rate"]
    assert block["candidates"] == {"A": 10, "B": 2, "C": 5}
    assert block["positives"] == {"A": 3, "B": 1, "C": 0}
    assert block["rate"] == pytest.approx({"A": 0.3, "B": 0.5, "C": 0.0})
    assert block["window_months_covered"] == {"2025-01-31": 1}
    assert block["monthly_positives"] == {
        "A": {"2024-06": 3}, "B": {"2024-06": 1}, "C": {"2024-06": 0}}
    rate_map = ((1 / 2 + 2 / 3) / 2 + 1) / 2
    assert result["overall"]["map@3"] == pytest.approx(rate_map)
    # label_table's counts stay as they were: the count mode's numbers.
    assert result["purchase_counts"] == {"A": 3, "B": 1}


def test_monitoring_with_score_rate_ranks_by_the_count(spark):
    """Monitoring scores the full grid, so the CLI wires no counts there even
    with score: rate; the node then ranks by the count (#397)."""
    from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

    params = _rate_params()
    params["post_training"] = False
    _pool, labels = _history(spark)
    result = compute_baseline_metrics(
        _eval_predictions(spark, params), labels, _segments(params), params)
    assert "popularity_rate" not in result
    count_map = ((1 + 2 / 3) / 2 + 1 / 2) / 2
    assert result["overall"]["map@3"] == pytest.approx(count_map)


def test_count_mode_writes_no_rate_keys(spark):
    from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

    params = _rate_params("count")
    _pool, labels = _history(spark)
    result = compute_baseline_metrics(
        _eval_predictions(spark), labels, _segments(params), params)
    assert "popularity_rate" not in result
    count_map = ((1 + 2 / 3) / 2 + 1 / 2) / 2
    assert result["overall"]["map@3"] == pytest.approx(count_map)


def test_an_item_never_a_candidate_scores_zero_not_null(spark):
    """D is evaluated but has no candidate row in the window."""
    from pyspark.sql import functions as F

    from recsys_tfb.evaluation.baselines import (
        build_baseline_frame,
        compute_period_candidate_counts,
        compute_positive_rates,
    )

    params = _rate_params()
    pool, labels = _history(spark)
    rates = compute_positive_rates(
        compute_period_candidate_counts(pool, labels, ["2024-06-30"], params),
        ["2025-01-31"], 12, params)
    evaluated = _eval_predictions(spark).unionByName(
        _eval_predictions(spark).filter(F.col("prod_name") == "A")
        .withColumn("prod_name", F.lit("D")))
    frame = build_baseline_frame(evaluated, rates, params)
    scores = {r["prod_name"]: r["score"]
              for r in frame.select("prod_name", "score").distinct().collect()}
    assert scores["D"] == 0.0


def test_post_training_rate_mode_needs_its_counts(spark):
    from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

    params = _rate_params()
    params["post_training"] = True
    _pool, labels = _history(spark)
    with pytest.raises(RuntimeError, match="popularity_period_counts"):
        compute_baseline_metrics(
            _eval_predictions(spark, params), labels, _segments(params), params)


def test_partial_coverage_does_not_raise(spark):
    """12-month window, candidate rows in one month: no raise, the covered
    months are recorded for the report to disclose."""
    params = _rate_params()
    pool, labels = _history(spark)
    result = _rate_run(spark, params, pool, labels, _plan(["2024-06-30"]))
    assert result["popularity_rate"]["window_months_covered"] == {
        "2025-01-31": 1}


def test_a_landed_period_sample_pool_no_longer_holds_is_not_summed(spark):
    """2024-05-31 was counted once (it is in the table) but sample_pool no
    longer holds it, so the plan does not list it: its counts stay out."""
    params = _rate_params()
    pool, labels = _history(spark)
    stale = spark.createDataFrame(pd.DataFrame(
        [("2024-05-31", "C", 1000, 900)],
        columns=["snap_date", "prod_name", "n_candidates", "n_positives"]))
    plan = _plan(["2024-06-30"])
    counts = _call(
        _node(_NODE, post_training=True, baseline_rate=True),
        sample_pool=pool, label_table=labels, parameters=params,
        popularity_period_counts_month_plan=plan).unionByName(stale)
    result = _call(
        _node("compute_baseline_metrics", post_training=True, baseline_rate=True),
        enriched_eval_predictions=_eval_predictions(spark), label_table=labels,
        evaluation_segment_columns=_segments(params), parameters=params,
        popularity_period_counts=counts,
        popularity_period_counts_month_plan=plan)
    assert result["popularity_rate"]["candidates"] == {"A": 10, "B": 2, "C": 5}
    assert result["popularity_rate"]["rate"]["C"] == 0.0


def test_several_dates_count_each_period_once_in_the_report_block(spark):
    """Two evaluated dates whose 12-month windows both cover 2024-06-30: the
    block's denominator is that period's 10 candidates, not 10 per window."""
    from tests.test_pipelines.test_evaluation.test_nodes import (
        TestEnrichedReadersKeepTheEvaluatedMonth,
    )

    params = _rate_params()
    params["evaluation"]["snap_date"] = ["2025-01-31", "2025-02-28"]
    pool, labels = _history(spark)
    result = _rate_run(
        spark, params, pool, labels, _plan(["2024-06-30"]),
        eval_predictions=TestEnrichedReadersKeepTheEvaluatedMonth._two_months(
            spark))
    block = result["popularity_rate"]
    assert block["candidates"] == {"A": 10, "B": 2, "C": 5}
    assert block["positives"] == {"A": 3, "B": 1, "C": 0}
    assert block["window_months_covered"] == {
        "2025-01-31": 1, "2025-02-28": 1}


def test_count_mode_trend_counts_a_positive_sample_pool_lacks(spark):
    """Switch off: the monthly trend is label_table's, so a positive with no
    sample_pool row is counted (the rate mode's numerator would not see it)."""
    from recsys_tfb.pipelines.evaluation.nodes import compute_baseline_metrics

    params = _rate_params("count")
    _pool, labels = _history(spark)
    orphan = spark.createDataFrame(pd.DataFrame(
        [("2024-07-31", "nobody", "C", 1)],
        columns=["snap_date", "cust_id", "prod_name", "label"]))
    result = compute_baseline_metrics(
        _eval_predictions(spark), labels.unionByName(orphan),
        _segments(params), params)
    assert result["monthly_counts"]["C"] == {"2024-07": 1}
    assert result["purchase_counts"]["C"] == 1


# ---------------------------------------------------------------------------
# The recount rule, against a real Hive table shaped like the catalog entry.
# ---------------------------------------------------------------------------


def _warehouse_table_dir(spark, db, table):
    raw = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    if raw.startswith("file:"):
        raw = raw[len("file:"):]
    return Path(raw) / f"{db}.db" / table


@pytest.fixture
def period_table(spark):
    """``popularity_period_counts`` on an isolated database, never ml_recsys
    (known-pitfalls §14: a test once dropped real artifacts)."""
    from recsys_tfb.io.hive_table_dataset import HiveTableDataset

    db, table = "test_popularity_period_counts", "popularity_period_counts"

    def _clean():
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
        d = _warehouse_table_dir(spark, db, table)
        if d.exists():
            shutil.rmtree(d)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    _clean()
    try:
        yield HiveTableDataset(
            database=db, table=table, columns="auto",
            partition_filter={"popularity_source_version": "SRC_X"},
            partition_cols=[{"name": "snap_date", "type": "STRING"}],
            external=False,
        )
    finally:
        _clean()


def _landed(ds):
    return landed_months(ds.existing_partition_values(), time_col="snap_date",
                         dataset_name="popularity_period_counts")


def _table(ds):
    return sorted((r["snap_date"], r["prod_name"], r["n_candidates"],
                   r["n_positives"]) for r in ds.load().collect())


def test_landed_periods_are_reused_until_named_by_rebuild(
    spark, period_table, caplog,
):
    """Both periods land; label_table is then backfilled with a positive in
    each. Without --rebuild-dates nothing is recounted and the log says so;
    with ``--rebuild-dates 2024-05-31`` that period alone is recounted."""
    params = _rate_params()
    node = _node(_NODE, baseline_rate=True)
    periods = ["2024-05-31", "2024-06-30"]
    pool = spark.createDataFrame(pd.DataFrame(
        [(p, "c0", "A") for p in periods] + [(p, "c1", "A") for p in periods],
        columns=["snap_date", "cust_id", "prod_name"]))
    labels = spark.createDataFrame(pd.DataFrame(
        [], columns=["snap_date", "cust_id", "prod_name", "label"]),
        "snap_date string, cust_id string, prod_name string, label int")

    def run(label_table, rebuild=()):
        plan = _plan(periods, _landed(period_table), rebuild)
        out = _call(node, sample_pool=pool, label_table=label_table,
                    parameters=params, popularity_period_counts_month_plan=plan)
        period_table.save(out)
        return plan

    run(labels)
    first = _table(period_table)
    assert first == [("2024-05-31", "A", 2, 0), ("2024-06-30", "A", 2, 0)]

    backfilled = spark.createDataFrame(pd.DataFrame(
        [(p, "c0", "A", 1) for p in periods],
        columns=["snap_date", "cust_id", "prod_name", "label"]))
    with caplog.at_level(logging.INFO):
        plan = run(backfilled)
    assert plan.to_process == []
    assert _table(period_table) == first
    assert "counted=- skipped=2024-05-31,2024-06-30" in caplog.text

    plan = run(backfilled, rebuild=["2024-05-31"])
    assert plan.to_process == [pd.Timestamp("2024-05-31")]
    assert _table(period_table) == [
        ("2024-05-31", "A", 2, 1), ("2024-06-30", "A", 2, 0)]
