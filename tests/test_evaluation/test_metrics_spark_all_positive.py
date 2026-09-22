"""All-positive query groups (#376): the switch that drops them from the
metrics, and the count written whether or not it is on.

An all-positive query group is one whose every row has a positive label.
Whatever the order, every per-query metric of such a group is the same, so it
cannot tell two rankings apart. ``evaluation.query_filter.drop_all_positive_groups``
drops them from the measurement metrics and the popularity baseline; the
count ``n_all_positive_queries`` is written either way.

The fixture ``_df`` is built so that the two grains disagree on which groups
are all-positive. That is the point of it: the category pass ranks a
different frame (one row per (query group, category), label = the max of its
children), so it must decide for itself, and a keyword that fails to reach it
leaves the category grain silently unfiltered.

    group  rows (item, score, label)                 fine grain      category grain
    c1     fund_stock .9 1 | exchange_fx .2 1        all-positive    all-positive (fund 1, exchange_fx 1)
    c2     fund_stock .3 1 | fund_bond   .8 0        mixed, AP 1/2   all-positive (fund 1, one row)
    c3     fund_bond  .9 0 | exchange_fx .1 1        mixed, AP 1/2   mixed, AP 1/2
    c4     fund_stock .5 0 | exchange_fx .4 0        no positive     no positive

    fine grain     : map@3  switch off = (1 + 1/2 + 1/2) / 3 = 2/3,  on = 1/2
    category grain : map@2  switch off = (1 + 1 + 1/2) / 3   = 5/6,  on = 1/2
"""

import logging

import pytest

from recsys_tfb.evaluation import metrics_spark as ms
from recsys_tfb.evaluation.metrics import ALL_POSITIVE_KEY

#: The keys a bundle's metrics live under — everything but the counts, which
#: count the frame as given and so differ from a frame with rows removed.
_METRIC_KEYS = ("overall", "per_segment", "per_item", "per_item_segment",
                "macro_avg", "observation_items")


def _params(categories=True):
    return {
        "schema": {
            "columns": {
                "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
                "label": "label", "score": "score", "rank": "rank",
            },
            "categorical_values": {"prod_name": [
                "fund_stock", "fund_bond", "exchange_fx"]},
        },
        "evaluation": {
            "k_values": [1, "all"],
            "item_categories": {
                "enabled": categories, "unmapped": "singleton",
                "mapping": {"fund": ["fund_stock", "fund_bond"]}},
        },
    }


_ROWS = [
    ("20240331", "c1", "fund_stock", 0.9, 1),
    ("20240331", "c1", "exchange_fx", 0.2, 1),
    ("20240331", "c2", "fund_stock", 0.3, 1),
    ("20240331", "c2", "fund_bond", 0.8, 0),
    ("20240331", "c3", "fund_bond", 0.9, 0),
    ("20240331", "c3", "exchange_fx", 0.1, 1),
    ("20240331", "c4", "fund_stock", 0.5, 0),
    ("20240331", "c4", "exchange_fx", 0.4, 0),
]
_COLS = ["snap_date", "cust_id", "prod_name", "score", "label"]


def _df(spark, without=()):
    """The fixture, less the query groups (customers) in ``without``."""
    return spark.createDataFrame(
        [r for r in _ROWS if r[1] not in without], schema=_COLS)


def _metrics(bundle):
    return {k: bundle[k] for k in _METRIC_KEYS}


def _switch(value):
    """Keyword arguments for the switch; ``None`` leaves it out (the default
    every caller outside evaluation — training — relies on)."""
    return {} if value is None else {"drop_all_positive_groups": value}


# ---------------------------------------------------------------------------
# compute_all_metrics (the measurement metrics, and the comparison report)
# ---------------------------------------------------------------------------


def test_the_switch_drops_the_fine_grained_all_positive_groups(spark):
    """On, the fine-grained bundle is what the switch-off bundle of the frame
    without c1 — the one fine-grained all-positive group — would be."""
    on = ms.compute_all_metrics(
        _df(spark), _params(), drop_all_positive_groups=True)
    assert on["overall"]["map@3"] == pytest.approx(0.5)
    assert on["overall"]["map@1"] == pytest.approx(0.0)
    assert _metrics(on) == _metrics(ms.compute_all_metrics(
        _df(spark, without={"c1"}), _params()))
    # c1's two positives are gone from the per-item family as well.
    assert {i: m["n_pos"] for i, m in on["per_item"].items()} == {
        "fund_stock": 1, "exchange_fx": 1}


def test_the_switch_drops_the_category_grain_all_positive_groups(spark):
    """The category grain decides on its own frame: c2 is mixed row by row
    but all-positive once fund_stock and fund_bond fold into one category, so
    it is dropped there and only there. A category pass that did not get the
    keyword keeps c1 and c2 and scores 5/6."""
    on = ms.compute_all_metrics(
        _df(spark), _params(), drop_all_positive_groups=True)
    assert on["category"]["overall"]["map@2"] == pytest.approx(0.5)
    assert _metrics(on["category"]) == _metrics(ms.compute_all_metrics(
        _df(spark, without={"c1", "c2"}), _params())["category"])


def test_switched_off_the_bundle_is_what_it_was_plus_the_count(spark):
    """Off is the default, value for value; it differs from on on this
    fixture, so the two paths are not the same by accident of the data; and
    the only new key is the count, at the top level of each grain."""
    default = ms.compute_all_metrics(_df(spark), _params())
    off = ms.compute_all_metrics(
        _df(spark), _params(), drop_all_positive_groups=False)
    on = ms.compute_all_metrics(
        _df(spark), _params(), drop_all_positive_groups=True)

    assert off == default
    assert off["overall"]["map@3"] == pytest.approx(2 / 3)
    assert off["category"]["overall"]["map@2"] == pytest.approx(5 / 6)
    assert off["overall"] != on["overall"]
    assert off["category"]["overall"] != on["category"]["overall"]

    assert set(off) == {
        "overall", "per_segment", "per_item", "per_item_segment",
        "macro_avg", "observation_items", "n_queries", "n_excluded_queries",
        "n_all_positive_queries", "dataset_overview", "category",
    }
    assert set(off["category"]) == {
        "overall", "per_segment", "per_item", "per_item_segment",
        "macro_avg", "observation_items", "n_queries", "n_excluded_queries",
        "n_all_positive_queries", "dataset_overview",
    }
    # Not a metric: the comparison report takes every key of `overall` for one.
    assert ALL_POSITIVE_KEY not in off["overall"]


@pytest.mark.parametrize("switch", [None, False, True])
def test_each_grain_counts_its_own_all_positive_groups(spark, switch):
    """Written whether or not the switch is on, and counted per grain: one
    fine-grained all-positive group (c1), two at the category grain (c1, c2).
    ``n_excluded_queries`` keeps counting only the groups holding no positive
    (c4) — the report reads ``n_queries - n_excluded_queries`` as the groups
    holding a positive."""
    out = ms.compute_all_metrics(_df(spark), _params(), **_switch(switch))
    assert out[ALL_POSITIVE_KEY] == 1
    assert out["category"][ALL_POSITIVE_KEY] == 2
    assert (out["n_queries"], out["n_excluded_queries"]) == (4, 1)
    assert (out["category"]["n_queries"],
            out["category"]["n_excluded_queries"]) == (4, 1)


@pytest.mark.parametrize("switch", [None, False, True])
def test_no_positive_anywhere_still_writes_the_count(spark, switch):
    df = spark.createDataFrame(
        [r for r in _ROWS if r[1] == "c4"], schema=_COLS)
    out = ms.compute_all_metrics(
        df, _params(categories=False), **_switch(switch))
    assert out["overall"] == {}
    assert (out["n_queries"], out["n_excluded_queries"],
            out[ALL_POSITIVE_KEY]) == (1, 1, 0)


def test_nothing_left_once_dropped_is_empty_and_says_why(spark, caplog):
    """Every group holding a positive is all-positive: on, nothing is left to
    score. The bundle is empty but keeps its counts, and the log names the
    switch rather than claiming there was no positive."""
    df = spark.createDataFrame(
        [r for r in _ROWS if r[1] in ("c1", "c4")], schema=_COLS)
    with caplog.at_level(logging.WARNING, logger=ms.logger.name):
        out = ms.compute_all_metrics(
            df, _params(categories=False), drop_all_positive_groups=True)
    assert out["overall"] == {} and out["per_item"] == {}
    assert (out["n_queries"], out["n_excluded_queries"],
            out[ALL_POSITIVE_KEY]) == (2, 1, 1)
    said = [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING]
    assert any("drop_all_positive_groups" in m for m in said), said
    assert not any("No queries with positive labels" in m for m in said), said


# ---------------------------------------------------------------------------
# `event` declared: all-positive is decided row by row, not item by item
# ---------------------------------------------------------------------------


def _event_params():
    p = _params(categories=False)
    p["schema"]["columns"]["event"] = "imp_id"
    return p


def test_with_event_a_group_is_all_positive_only_if_every_row_is(spark):
    """c1 was shown item A twice and responded once: its only item has a
    positive, but one of its two rows does not, so its AP depends on the
    order (1/2 here) and it stays. c2 (A once, positive) and c3 (B twice,
    both positive) are all-positive and go. Judging by item instead would
    drop c1 too and leave nothing."""
    df = spark.createDataFrame(
        [
            ("20240331", "c1", "A", "i1", 0.9, 0),
            ("20240331", "c1", "A", "i2", 0.4, 1),
            ("20240331", "c2", "A", "i3", 0.7, 1),
            ("20240331", "c3", "B", "i4", 0.6, 1),
            ("20240331", "c3", "B", "i5", 0.6, 1),
            ("20240331", "c4", "B", "i6", 0.3, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "imp_id", "score",
                "label"],
    )
    on = ms.compute_all_metrics(
        df, _event_params(), drop_all_positive_groups=True)
    # "all" is the widest query group once `event` is declared: 2 rows.
    assert on["overall"]["map@2"] == pytest.approx(0.5)
    assert on[ALL_POSITIVE_KEY] == 2
    assert (on["n_queries"], on["n_excluded_queries"]) == (4, 1)
    assert set(on["per_item"]) == {"A"}
    off = ms.compute_all_metrics(df, _event_params())
    assert off["overall"]["map@2"] == pytest.approx((0.5 + 1 + 1) / 3)


# ---------------------------------------------------------------------------
# compute_overall_per_item (the popularity baseline's slim path)
# ---------------------------------------------------------------------------


def test_the_slim_path_drops_the_fine_grained_all_positive_groups(spark):
    on = ms.compute_overall_per_item(
        _df(spark), _params(), drop_all_positive_groups=True)
    assert on["overall"]["map@3"] == pytest.approx(0.5)
    slim_without = ms.compute_overall_per_item(
        _df(spark, without={"c1"}), _params())
    assert on["overall"] == slim_without["overall"]
    assert on["per_item"] == slim_without["per_item"]


def test_the_slim_path_drops_the_category_grain_all_positive_groups(spark):
    """Through the recursion for the category pass: without the keyword there,
    the category grain keeps c1 and c2 and scores 5/6."""
    on = ms.compute_overall_per_item(
        _df(spark), _params(), with_category=True,
        drop_all_positive_groups=True)
    assert on["category"]["overall"]["map@2"] == pytest.approx(0.5)
    without = ms.compute_overall_per_item(
        _df(spark, without={"c1", "c2"}), _params(), with_category=True)
    assert on["category"]["overall"] == without["category"]["overall"]
    assert on["category"]["per_item"] == without["category"]["per_item"]


def test_the_slim_path_matches_the_full_path_with_the_switch_on(spark):
    """The baseline is read against the model's metrics; both sides must drop
    the same groups. It writes no count: the report's ledger reads the
    model's bundle, and the baseline scores the same groups."""
    slim = ms.compute_overall_per_item(
        _df(spark), _params(), with_category=True,
        drop_all_positive_groups=True)
    full = ms.compute_all_metrics(
        _df(spark), _params(), drop_all_positive_groups=True)
    assert slim["overall"] == full["overall"]
    assert slim["per_item"] == full["per_item"]
    assert slim["category"]["overall"] == full["category"]["overall"]
    assert set(slim) == {"overall", "per_item", "category"}
    assert set(slim["category"]) == {"overall", "per_item"}


def test_the_slim_path_switched_off_is_what_it_was(spark):
    default = ms.compute_overall_per_item(
        _df(spark), _params(), with_category=True)
    off = ms.compute_overall_per_item(
        _df(spark), _params(), with_category=True,
        drop_all_positive_groups=False)
    assert off == default
    assert off["overall"]["map@3"] == pytest.approx(2 / 3)
    assert off["category"]["overall"]["map@2"] == pytest.approx(5 / 6)


def test_the_slim_path_with_nothing_left_says_why(spark, caplog):
    df = spark.createDataFrame(
        [r for r in _ROWS if r[1] in ("c1", "c4")], schema=_COLS)
    with caplog.at_level(logging.WARNING, logger=ms.logger.name):
        out = ms.compute_overall_per_item(
            df, _params(categories=False), drop_all_positive_groups=True)
    assert out == {"overall": {}, "per_item": {}}
    said = [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING]
    assert any("drop_all_positive_groups" in m for m in said), said
    assert not any("No queries with positive labels" in m for m in said), said
