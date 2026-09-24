"""Tests for compute_test_metrics — training's test scoring over
training_eval_predictions (ADR-0028).

The `spark` fixture is the session-scoped one from tests/conftest.py — do not
re-declare a local one (it would shadow conftest's, and an early teardown via
`.stop()` would kill the shared session mid-suite).
"""

import pytest


def _make_parameters() -> dict:
    return {
        "schema": {
            "columns": {
                "time": "snap_date",
                "entity": ["cust_id"],
                "item": "prod_name",
                "label": "label",
                "score": "score",
                "rank": "rank",
            },
        },
        "evaluation": {"k_values": ["all"]},
        # The scored months default to every test month; these tests' rows
        # sit in this one.
        "dataset": {"test_snap_dates": ["2025-01-31"]},
    }


def _make_df(spark, rows):
    """rows: list of dicts with cust_id, snap_date, prod_name, score,
    score_uncalibrated, label.
    """
    import pandas as pd
    pdf = pd.DataFrame(rows)
    return spark.createDataFrame(pdf)


def test_compute_mAP_spark_returns_one_flat_dict(spark):
    """One set of metrics, always.

    There used to be a second, "before calibration" set, emitted when `score`
    and `score_uncalibrated` disagreed. #411 removed calibration — the only
    thing that could make them disagree — so the node no longer reads
    `score_uncalibrated` and no longer emits the second set.
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        # cust c1 — positives on prod_A (correct top rank)
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.9, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.1, "label": 0},
        # cust c2 — positives on prod_B (correct top rank)
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.2, "score_uncalibrated": 0.2, "label": 0},
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.8, "score_uncalibrated": 0.8, "label": 1},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 4}

    result = compute_test_metrics(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # Both customers ranked their positives at top -> overall mAP == 1.0
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)
    assert "per_item_map_attr" in result
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 0


def test_a_disagreeing_score_uncalibrated_column_is_ignored(spark):
    """The discriminating half: a table whose deprecated column disagrees with
    `score` gets the same flat result, scored on `score` alone.

    Such a table can only come from a pre-#411 run. Reading it would revive the
    second metric set under a model that was never calibrated.
    """
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "score_uncalibrated": 0.1, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "score_uncalibrated": 0.9, "label": 0},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 2}

    result = compute_test_metrics(df, manifest, _make_parameters())

    assert "uncalibrated" not in result
    assert "calibration_method" not in result
    # `score` puts c1's positive on top, so the metric is the calibrated-side
    # number of the old two-set result — i.e. it scored `score`, not the other.
    assert result["overall_map"] == pytest.approx(1.0, abs=1e-6)


def test_test_map_matches_the_report_when_event_is_declared(spark):
    """#434: with ``event`` declared the metrics are keyed at the widest query
    group (4 rows here), not at the item count (3). The node used to count
    items itself, ask for ``map@3`` and log 0.0 for both numbers, with nothing
    raised.

    c1 was shown A three times and B once and clicked the B at rank 4; c3 was
    shown C and clicked it. So AP(c1) = 1/4, AP(c3) = 1, mAP@all = 0.625 —
    worked out by hand, and the same number the report prints under
    ``map@all``. Per item, B's one positive contributes 1/4 and C's 1.
    """
    from recsys_tfb.evaluation import report_builder as rb
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    params = _make_parameters()
    params["schema"]["columns"]["event"] = "imp_id"
    rows = [
        ("c1", "2025-01-31", "A", "i1", 0.9, 0),
        ("c1", "2025-01-31", "A", "i2", 0.9, 0),
        ("c1", "2025-01-31", "A", "i3", 0.4, 0),
        ("c1", "2025-01-31", "B", "i4", 0.1, 1),
        ("c2", "2025-01-31", "A", "i5", 0.2, 0),
        ("c3", "2025-01-31", "C", "i6", 0.7, 1),
    ]
    df = spark.createDataFrame(
        rows,
        schema=["cust_id", "snap_date", "prod_name", "imp_id", "score", "label"],
    )
    manifest = {"snap_dates": ["2025-01-31"], "items": ["A", "B", "C"],
                "model_version": "v_test", "n_rows_written": len(rows)}

    result = compute_test_metrics(df, manifest, params)

    assert result["overall_map"] == pytest.approx(0.625)
    assert result["per_item_map_attr"] == {
        "B": pytest.approx(0.25), "C": pytest.approx(1.0),
    }
    report_card = rb.build_overview_section(
        compute_all_metrics(df, params), params,
    ).tables[0]
    assert result["overall_map"] == pytest.approx(
        report_card.loc["map@all", "value"])


def test_test_map_does_not_follow_the_all_positive_switch(spark):
    """#376: ``evaluation.query_filter.drop_all_positive_groups`` is an
    evaluation setting. The test mAP is logged to mlflow to compare models, and
    a switch that moved it would make models trained before and after it
    incomparable with nothing in the model version to say so. So training
    never passes it, whatever ``parameters`` holds.

    c1 is all-positive (AP 1 whatever the order), c2 puts its positive second
    (AP 1/2): mAP 3/4 with c1, 1/2 without — the fixture can tell.
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "label": 1},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "label": 1},
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.2, "label": 1},
        {"cust_id": "c2", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.8, "label": 0},
    ]
    df = _make_df(spark, rows)
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": len(rows)}
    switched_on = _make_parameters()
    switched_on["evaluation"]["query_filter"] = {
        "drop_all_positive_groups": True}

    with_switch = compute_test_metrics(df, manifest, switched_on)
    without_switch = compute_test_metrics(df, manifest, _make_parameters())

    assert with_switch == without_switch
    assert with_switch["overall_map"] == pytest.approx(0.75)
    dropped = compute_all_metrics(
        df, _make_parameters(), drop_all_positive_groups=True)
    assert dropped["overall"]["map@2"] == pytest.approx(0.5)


@pytest.mark.parametrize("categories", [
    # Column mode: the table is read off sample_pool by evaluation's
    # prepare_eval_data; training has none to pass.
    {"enabled": True, "unmapped": "singleton", "column": "family"},
    # A hand mapping naming an item the item list lacks: raises the moment
    # the category pass reads it.
    {"enabled": True, "unmapped": "singleton",
     "mapping": {"x": ["not_an_item"]}},
], ids=["column", "mapping"])
def test_test_map_never_computes_categories(spark, categories):
    """#379: the test mAP reads the fine-grained keys only, so its
    call switches the category pass off instead of inheriting
    ``evaluation.item_categories``. Either conf above raises the moment the
    category pass is attempted; the result equals the categories-off one."""
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_A",
         "score": 0.9, "label": 0},
        {"cust_id": "c1", "snap_date": "2025-01-31", "prod_name": "prod_B",
         "score": 0.1, "label": 1},
    ]
    manifest = {"snap_dates": ["2025-01-31"], "items": ["prod_A", "prod_B"],
                "model_version": "v_test", "n_rows_written": 2}
    params = _make_parameters()
    params["schema"]["categorical_values"] = {"prod_name": ["prod_A", "prod_B"]}
    off = compute_test_metrics(_make_df(spark, rows), manifest, params)

    params["evaluation"]["item_categories"] = categories
    on = compute_test_metrics(_make_df(spark, rows), manifest, params)
    assert on == off
    assert on["overall_map"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# ADR-0028: the four values without K, the scored months, the metric set
# ---------------------------------------------------------------------------

import logging

import numpy as np

JAN, FEB, DEC = "2025-01-31", "2025-02-28", "2024-12-31"
#: One month, two query groups, a tie. c1 ranks A, B (tie with C at 0.5, B
#: first by item), C: positives at 1 and 3 -> AP 5/6. c2 ranks A, B: its
#: positive second -> AP 1/2. mAP 2/3; per item A 1, B 1/2, C 2/3.
ROWS = [
    ("c1", JAN, "A", 0.9, 1), ("c1", JAN, "B", 0.5, 0), ("c1", JAN, "C", 0.5, 1),
    ("c2", JAN, "A", 0.8, 0), ("c2", JAN, "B", 0.3, 1), ("c2", JAN, "C", 0.2, 0),
]
COLUMNS = ["cust_id", "snap_date", "prod_name", "score", "label"]
MANIFEST = {"snap_dates": [JAN], "model_version": "v_test"}


def _frame(spark, rows=ROWS):
    return spark.createDataFrame(rows, schema=COLUMNS)


def _with(params, **blocks):
    out = dict(params)
    for key, value in blocks.items():
        out[key] = {**(params.get(key) or {}), **value}
    return out


def _legacy(result):
    return {k: result[k] for k in (
        "overall_map", "per_item_map_attr", "n_queries", "n_excluded_queries")}


@pytest.mark.parametrize("evaluation", [
    {"k_values": [5]},
    {"k_values": [1]},
    {"k_values": [1, "all"], "metric": {"k": 1}},
    {"k_values": ["all"], "item_categories": {
        "enabled": True, "mapping": {"AB": ["A", "B"]}}},
], ids=["no-all", "k1", "metric-k1", "categories-on"])
def test_the_four_values_do_not_follow_evaluation_settings(spark, evaluation):
    """ADR-0028 background one: the node used to read two of its values off
    keys built from the K ``"all"`` resolved to, so ``k_values`` without
    ``"all"`` logged 0.0 for both; other K settings and the category switch
    changed what was computed. Now nothing under ``evaluation`` is read: every
    setting gives what the report's ``map@all`` path gives with the default
    ones."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    df = _frame(spark)
    reference = compute_all_metrics(df, _make_parameters())
    expected = {
        "overall_map": reference["overall"]["map@3"],
        "per_item_map_attr": {
            k: v["map_attr@3"] for k, v in reference["per_item"].items()},
        "n_queries": reference["n_queries"],
        "n_excluded_queries": reference["n_excluded_queries"],
    }
    assert expected["overall_map"] == pytest.approx(2 / 3)

    params = _make_parameters()
    params["evaluation"] = evaluation
    params["schema"]["categorical_values"] = {"prod_name": ["A", "B", "C"]}

    assert _legacy(compute_test_metrics(df, MANIFEST, params)) == expected


def test_only_the_scored_months_are_scored(spark):
    """February alone: c1 puts its positive second (AP 1/2). January, where it
    ranks it first, is in the table and in test_snap_dates but not scored."""
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        ("c1", JAN, "A", 0.9, 1), ("c1", JAN, "B", 0.1, 0),
        ("c1", FEB, "A", 0.2, 1), ("c1", FEB, "B", 0.8, 0),
    ]
    params = _with(_make_parameters(), dataset={"test_snap_dates": [JAN, FEB]})

    both = compute_test_metrics(_frame(spark, rows), MANIFEST, params)
    feb = compute_test_metrics(
        _frame(spark, rows), MANIFEST,
        _with(params, test_metrics={"snap_date": FEB}))

    assert (both["overall_map"], both["n_queries"]) == (pytest.approx(0.75), 2)
    assert both["snap_dates"] == [JAN, FEB]
    assert (feb["overall_map"], feb["n_queries"]) == (pytest.approx(0.5), 1)
    assert feb["per_item_map_attr"] == {"A": pytest.approx(0.5)}
    assert feb["snap_dates"] == [FEB]


def test_months_in_the_table_but_not_scored_are_named(spark, tmp_path, caplog):
    """Read from a table partitioned by time, as the catalog's is: the months
    present but not scored come off the partition listing, with no scan."""
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    rows = [
        ("c1", DEC, "A", 0.1, 1), ("c1", DEC, "B", 0.9, 0),
        ("c1", JAN, "A", 0.9, 1), ("c1", JAN, "B", 0.1, 0),
        ("c1", FEB, "A", 0.2, 1), ("c1", FEB, "B", 0.8, 0),
    ]
    path = str(tmp_path / "predictions")
    _frame(spark, rows).write.partitionBy("snap_date").parquet(path)
    table = spark.read.parquet(path)
    params = _with(_make_parameters(), dataset={"test_snap_dates": [JAN, FEB]},
                   test_metrics={"snap_date": [JAN]})

    with caplog.at_level(logging.INFO):
        result = compute_test_metrics(table, MANIFEST, params)

    assert (result["overall_map"], result["n_queries"]) == (pytest.approx(1.0), 1)
    skipped = [r.getMessage() for r in caplog.records
               if "not scored" in r.getMessage()]
    assert len(skipped) == 1
    assert "['2024-12-31', '2025-02-28']" in skipped[0]


def test_a_scored_month_without_predictions_stops_the_node(spark):
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    params = _with(_make_parameters(), dataset={"test_snap_dates": [JAN, FEB]})
    with pytest.raises(ValueError, match=r"\['2025-02-28'\].*predict"):
        compute_test_metrics(_frame(spark), MANIFEST, params)


def test_the_record_names_its_months_its_metrics_and_their_names(spark):
    """Both ranking metrics against the numpy functions HPO scores val with
    (the tie at 0.5 included), and the names promote will read."""
    from recsys_tfb.evaluation.metrics import (
        compute_macro_per_item_map, compute_mean_ap,
    )
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    params = _with(_make_parameters(), training={"hpo_objective": "macro_per_item_map"})
    result = compute_test_metrics(_frame(spark), MANIFEST, params)

    groups = np.array([0, 0, 0, 1, 1, 1])
    items = np.array([r[2] for r in ROWS])
    y = np.array([r[4] for r in ROWS])
    score = np.array([r[3] for r in ROWS])
    assert result["metrics"] == {
        "mean_ap": pytest.approx(compute_mean_ap(groups, items, y, score)),
        "macro_per_item_map": pytest.approx(
            compute_macro_per_item_map(groups, items, y, score)),
    }
    assert result["metrics"]["macro_per_item_map"] == pytest.approx(13 / 18)
    assert result["snap_dates"] == [JAN]
    assert result["hpo_objective"] == "macro_per_item_map"
    assert result["selection_metric"] == "macro_per_item_map"
    assert result["metrics_not_computed"] == {}

    chosen = compute_test_metrics(_frame(spark), MANIFEST, _with(
        params, test_metrics={"selection_metric": "mean_ap"}))
    assert chosen["selection_metric"] == "mean_ap"
    assert chosen["hpo_objective"] == "macro_per_item_map"


def test_a_binary_prediction_metric_has_no_value_on_test_yet(spark, caplog):
    """Asked for as the HPO objective, the selection metric or in
    ``test_metrics.metrics``, a binary-prediction metric comes back without a
    value and with the reason, and the node still returns (#452: the test-side
    algorithm and the rule that blocks it land together, next)."""
    from recsys_tfb.evaluation.metric_registry import NO_TEST_ALGORITHM
    from recsys_tfb.pipelines.training.nodes import compute_test_metrics

    params = _with(
        _make_parameters(),
        training={"hpo_objective": "pooled_average_precision"},
        test_metrics={"metrics": ["macro_per_item_average_precision"]},
    )
    with caplog.at_level(logging.WARNING):
        result = compute_test_metrics(_frame(spark), MANIFEST, params)

    assert result["selection_metric"] == "pooled_average_precision"
    assert set(result["metrics"]) == {"mean_ap", "macro_per_item_map"}
    assert result["metrics_not_computed"] == {
        "pooled_average_precision": NO_TEST_ALGORITHM,
        "macro_per_item_average_precision": NO_TEST_ALGORITHM,
    }
    assert result["overall_map"] == pytest.approx(2 / 3)
    assert any("pooled_average_precision" in r.getMessage()
               and NO_TEST_ALGORITHM in r.getMessage() for r in caplog.records)
