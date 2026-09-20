"""The within-query ranking rule shared by inference, evaluation and the
driver-side numpy paths (ADR-0020 bug 11, #355).

Ties among equal scores are broken by item ascending. The rule lives in one
module (``utils.ranking``) because it used to be written six times — inference's
``rank_predictions``, evaluation's ``rank_within_query`` and four numpy sorts on
the driver — and was undefined every time, so the same data could get different
ranks on every run and in every place.
"""

import numpy as np
import pandas as pd
import pytest

SNAP = "2024-03-31"

# Two entities, each with a three-way tie below one clear winner. The tied rows
# arrive in opposite orders for the two entities, so a rank that follows arrival
# order cannot get both right.
TIED_ROWS = [
    ("C001", "fund_stock", 0.5),
    ("C001", "exchange_fx", 0.5),
    ("C001", "ccard_ins", 0.5),
    ("C001", "fund_bond", 0.9),
    ("C002", "ccard_ins", 0.5),
    ("C002", "exchange_fx", 0.5),
    ("C002", "fund_stock", 0.5),
    ("C002", "fund_bond", 0.9),
]
# Written out, not derived: score descending, then item name ascending.
EXPECTED = {
    (cid, item): rank
    for cid in ("C001", "C002")
    for item, rank in (
        ("fund_bond", 1), ("ccard_ins", 2), ("exchange_fx", 3), ("fund_stock", 4),
    )
}


def _ranks(pdf: pd.DataFrame, rank_col: str) -> dict:
    return {
        (cid, item): int(r)
        for cid, item, r in zip(pdf["cust_id"], pdf["prod_name"], pdf[rank_col])
    }


def test_numpy_callers_still_import_without_pyspark():
    """``evaluation/metrics.py`` calls itself a pure-numpy leaf and
    ``diagnosis/metric/_common.py`` a pyspark-free one; both now import
    ``utils.ranking``. That keeps pyspark out only if neither the module nor
    the ``recsys_tfb.utils`` package imports it on load.

    A subprocess with pyspark blocked, because in this session pyspark is
    already loaded and ``sys.modules`` could not tell."""
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-c",
         "import sys; sys.modules['pyspark'] = None;"
         "import recsys_tfb.evaluation.metrics;"
         "import recsys_tfb.diagnosis.metric._common"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr[-2000:]


def _numpy_ranks(groups, score, items) -> np.ndarray:
    """1-based rank of every input row, from the numpy order."""
    from recsys_tfb.utils.ranking import order_by_score_then_item

    order = order_by_score_then_item(groups, score, items)
    ranks = np.empty(len(order), dtype=np.int64)
    g_sorted = groups[order]
    start = 0
    for end in [*(np.flatnonzero(np.diff(g_sorted)) + 1), len(order)]:
        ranks[order[start:end]] = np.arange(1, end - start + 1)
        start = end
    return ranks


def test_numpy_order_breaks_ties_by_item_whatever_order_the_rows_arrive_in():
    rng = np.random.default_rng(0)
    for trial in range(5):
        rows = [TIED_ROWS[i] for i in rng.permutation(len(TIED_ROWS))]
        cust = np.array([r[0] for r in rows], dtype=object)
        items = np.array([r[1] for r in rows], dtype=object)
        score = np.array([r[2] for r in rows])
        ranks = _numpy_ranks(pd.factorize(cust)[0], score, items)
        got = {(c, i): int(r) for c, i, r in zip(cust, items, ranks)}
        assert got == EXPECTED, trial


def test_numpy_order_compares_numeric_items_as_numbers():
    """Spark sorts an integer item column numerically, so the numpy twin must
    too: 2 before 10, not the string order where "10" < "2"."""
    groups = np.zeros(3, dtype=np.int64)
    score = np.full(3, 0.5)
    items = np.array([10, 2, 1])
    assert _numpy_ranks(groups, score, items).tolist() == [3, 2, 1]


@pytest.mark.spark
def test_ties_are_broken_by_item_name_whatever_order_the_rows_arrive_in(spark):
    from recsys_tfb.utils.ranking import rank_by_score_then_item

    schema = "cust_id STRING, prod_name STRING, score DOUBLE"
    orders = {
        "as written": TIED_ROWS,
        "reversed": list(reversed(TIED_ROWS)),
    }
    for label, rows in orders.items():
        df = spark.createDataFrame(rows, schema).repartition(3)
        ranked = df.withColumn(
            "pos", rank_by_score_then_item(["cust_id"], "score", "prod_name"),
        )
        assert _ranks(ranked.toPandas(), "pos") == EXPECTED, label


@pytest.mark.spark
def test_inference_and_evaluation_rank_the_same_tied_data_identically(spark):
    """Inference publishes ``rank``; evaluation re-ranks when the source has none
    (``--post-training``) and after the comparison mode shrinks the candidate
    set. Both must land on the same ranks for the same rows."""
    from recsys_tfb.pipelines.evaluation.nodes import (
        make_prepare_eval_data_node,
    )
    from recsys_tfb.pipelines.inference.nodes import rank_predictions

    rows = pd.DataFrame(TIED_ROWS, columns=["cust_id", "prod_name", "score"])
    rows["snap_date"] = SNAP
    schema_block = {"columns": {
        "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
        "label": "label", "score": "score", "rank": "rank",
    }}

    inference_ranked = rank_predictions(
        spark.createDataFrame(rows.assign(entity_bucket="0")),
        {},
        {"schema": schema_block, "inference": {"snap_dates": [SNAP]}},
    ).toPandas()

    evaluation_frame, _segments = make_prepare_eval_data_node(
        "inference_population"
    )(
        spark.createDataFrame(rows),
        spark.createDataFrame(
            [("C001", SNAP, "fund_bond", 1)],
            "cust_id STRING, snap_date STRING, prod_name STRING, label INT",
        ),
        spark.createDataFrame([], "snap_date STRING, cust_id STRING"),
        {"schema": schema_block, "model_version": "v1",
         "evaluation": {"snap_date": SNAP}},
    )
    evaluation_ranked = evaluation_frame.toPandas()

    assert _ranks(inference_ranked, "rank") == EXPECTED
    assert _ranks(evaluation_ranked, "rank") == EXPECTED


# Integer item ids chosen so that their string order ("10" < "100" < "2") is
# not their numeric order (2 < 10 < 100): an engine that compares items as
# strings ranks the tied rows differently from one that compares numbers.
NUMERIC_ITEM = {"ccard_ins": 2, "exchange_fx": 10, "fund_stock": 100, "fund_bond": 7}


@pytest.mark.spark
@pytest.mark.parametrize("item_type", ["STRING", "INT"])
def test_numpy_and_spark_rank_the_same_tied_rows_identically(spark, item_type):
    """The published rank comes from Spark; the diagnosis and the HPO score rank
    on the driver with numpy. Same rows, same ranks — also when the item column
    is numeric."""
    from recsys_tfb.utils.ranking import rank_by_score_then_item

    rows = (
        TIED_ROWS if item_type == "STRING"
        else [(c, NUMERIC_ITEM[i], s) for c, i, s in TIED_ROWS]
    )
    spark_ranked = spark.createDataFrame(
        rows, f"cust_id STRING, item {item_type}, score DOUBLE",
    ).repartition(3).withColumn(
        "pos", rank_by_score_then_item(["cust_id"], "score", "item"),
    ).toPandas()
    spark_ranks = {
        (c, i): int(r)
        for c, i, r in zip(spark_ranked["cust_id"], spark_ranked["item"],
                           spark_ranked["pos"])
    }

    pdf = pd.DataFrame(rows, columns=["cust_id", "item", "score"])
    numpy_ranks = _numpy_ranks(
        pd.factorize(pdf["cust_id"])[0],
        pdf["score"].to_numpy(),
        pdf["item"].to_numpy(),
    )
    assert {
        (c, i): int(r) for c, i, r in zip(pdf["cust_id"], pdf["item"], numpy_ranks)
    } == spark_ranks


# ---------------------------------------------------------------------------
# `event`: the same item more than once inside one query group (#378)
# ---------------------------------------------------------------------------

#: One query group, one item, three impressions, all tied on score — the shape
#: a config that declares `event` without per-impression features produces for
#: every group (nothing distinguishes the rows, so everything ties). Impression
#: ids out of order on purpose: the rank must come from the value, not the row.
EVENT_TIED_ROWS = [
    ("c1", "fund_bond", "imp03", 0.5),
    ("c1", "fund_bond", "imp01", 0.5),
    ("c1", "ccard_ins", "imp02", 0.5),
    ("c1", "fund_bond", "imp02", 0.5),
]

#: item ascending first, then event ascending — ADR-0025's rule, written out.
EVENT_EXPECTED = {
    ("c1", "ccard_ins", "imp02"): 1,
    ("c1", "fund_bond", "imp01"): 2,
    ("c1", "fund_bond", "imp02"): 3,
    ("c1", "fund_bond", "imp03"): 4,
}


def _numpy_ranks_with_event(groups, score, items, event_keys) -> np.ndarray:
    from recsys_tfb.utils.ranking import order_by_score_then_item

    order = order_by_score_then_item(groups, score, items, event_keys)
    ranks = np.empty(len(order), dtype=np.int64)
    g_sorted = groups[order]
    start = 0
    for end in [*(np.flatnonzero(np.diff(g_sorted)) + 1), len(order)]:
        ranks[order[start:end]] = np.arange(1, end - start + 1)
        start = end
    return ranks


def test_numpy_order_breaks_same_item_ties_by_event_whatever_the_row_order():
    """Shuffling the input rows must not move a rank. Without the event key the
    three fund_bond rows are indistinguishable and numpy's sort leaves them in
    arrival order, so this is the assertion that actually needs the change."""
    rng = np.random.default_rng(0)
    for trial in range(5):
        rows = [EVENT_TIED_ROWS[i] for i in rng.permutation(len(EVENT_TIED_ROWS))]
        cust = np.array([r[0] for r in rows], dtype=object)
        items = np.array([r[1] for r in rows], dtype=object)
        events = np.array([r[2] for r in rows], dtype=object)
        score = np.array([r[3] for r in rows])
        ranks = _numpy_ranks_with_event(
            pd.factorize(cust)[0], score, items, [events],
        )
        got = {
            (c, i, e): int(r)
            for c, i, e, r in zip(cust, items, events, ranks)
        }
        assert got == EVENT_EXPECTED, trial


def test_numpy_item_still_outranks_event():
    """Item is the primary tie-break and `event` only the secondary: a row
    whose event sorts first must still lose to a smaller item. Pins the key
    order inside the lexsort, which is otherwise easy to invert unnoticed —
    both orders are deterministic and reproducible."""
    groups = np.zeros(2, dtype=np.int64)
    score = np.full(2, 0.5)
    items = np.array(["b", "a"], dtype=object)
    events = np.array(["imp01", "imp99"], dtype=object)
    assert _numpy_ranks_with_event(groups, score, items, [events]).tolist() == [2, 1]


def test_numpy_several_event_columns_apply_in_declared_order():
    """Two event columns: the first decides, the second only breaks its ties."""
    groups = np.zeros(3, dtype=np.int64)
    score = np.full(3, 0.5)
    items = np.array(["a", "a", "a"], dtype=object)
    first = np.array([2, 1, 1])
    second = np.array([0, 9, 3])
    ranks = _numpy_ranks_with_event(groups, score, items, [first, second])
    # (1, 3) then (1, 9) then (2, 0)
    assert ranks.tolist() == [3, 2, 1]


def test_numpy_no_event_keys_is_the_old_order():
    """The compatibility half: an empty `event_keys` must reproduce the
    pre-#378 order exactly, or every existing deployment's ranks move."""
    from recsys_tfb.utils.ranking import order_by_score_then_item

    groups = np.array([0, 0, 1, 1], dtype=np.int64)
    score = np.array([0.5, 0.5, 0.2, 0.9])
    items = np.array(["b", "a", "z", "y"], dtype=object)
    assert np.array_equal(
        order_by_score_then_item(groups, score, items),
        order_by_score_then_item(groups, score, items, []),
    )


@pytest.mark.spark
def test_spark_breaks_same_item_ties_by_event(spark):
    from recsys_tfb.utils.ranking import rank_by_score_then_item

    ranked = spark.createDataFrame(
        EVENT_TIED_ROWS,
        "cust_id STRING, item STRING, imp_id STRING, score DOUBLE",
    ).repartition(3).withColumn(
        "pos",
        rank_by_score_then_item(["cust_id"], "score", "item", ["imp_id"]),
    ).toPandas()
    assert {
        (c, i, e): int(r)
        for c, i, e, r in zip(
            ranked["cust_id"], ranked["item"], ranked["imp_id"], ranked["pos"],
        )
    } == EVENT_EXPECTED


@pytest.mark.spark
@pytest.mark.parametrize("event_type", ["STRING", "INT"])
def test_numpy_and_spark_rank_the_same_event_rows_identically(spark, event_type):
    """ADR-0025 says the rule is written twice and both writings agree. The INT
    case is the one that catches a numpy twin comparing the event column as a
    string: 2 before 10, not "10" before "2"."""
    from recsys_tfb.utils.ranking import rank_by_score_then_item

    numeric = {"imp01": 2, "imp02": 10, "imp03": 100}
    rows = (
        EVENT_TIED_ROWS if event_type == "STRING"
        else [(c, i, numeric[e], s) for c, i, e, s in EVENT_TIED_ROWS]
    )
    spark_ranked = spark.createDataFrame(
        rows,
        f"cust_id STRING, item STRING, imp_id {event_type}, score DOUBLE",
    ).repartition(3).withColumn(
        "pos",
        rank_by_score_then_item(["cust_id"], "score", "item", ["imp_id"]),
    ).toPandas()
    spark_ranks = {
        (c, i, e): int(r)
        for c, i, e, r in zip(
            spark_ranked["cust_id"], spark_ranked["item"],
            spark_ranked["imp_id"], spark_ranked["pos"],
        )
    }

    pdf = pd.DataFrame(rows, columns=["cust_id", "item", "imp_id", "score"])
    numpy_ranks = _numpy_ranks_with_event(
        pd.factorize(pdf["cust_id"])[0],
        pdf["score"].to_numpy(),
        pdf["item"].to_numpy(),
        [pdf["imp_id"].to_numpy()],
    )
    assert {
        (c, i, e): int(r)
        for c, i, e, r in zip(
            pdf["cust_id"], pdf["item"], pdf["imp_id"], numpy_ranks,
        )
    } == spark_ranks
