"""The within-query ranking rule shared by inference and evaluation (bug 11).

Ties among equal scores are broken by item name ascending. The rule lives in one
function (``utils.ranking``) because it used to be written twice — once in
inference's ``rank_predictions`` and once in evaluation's ``rank_within_query``
— and was undefined both times, so the same data could get different ranks on
every run and in every place.
"""

import pandas as pd
import pytest

pytestmark = pytest.mark.spark

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


def test_inference_and_evaluation_rank_the_same_tied_data_identically(spark):
    """Inference publishes ``rank``; evaluation re-ranks when the source has none
    (``--post-training``) and after the comparison mode shrinks the candidate
    set. Both must land on the same ranks for the same rows."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import (
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
