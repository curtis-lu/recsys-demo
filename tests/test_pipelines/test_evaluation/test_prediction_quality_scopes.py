"""``steps/prediction_quality_scopes``: the scope table's keys on the
evaluated dates (ADR-0032 decision 7)."""
from __future__ import annotations

import pandas as pd


def test_an_evaluated_date_without_scope_rows_is_named_not_refused(spark):
    """A campaign may run in some of the evaluated months: that month
    contributes nothing, and the page says which months those are."""
    from recsys_tfb.pipelines.evaluation.steps.prediction_quality_scopes import (
        read_scope_keys,
    )

    spark.createDataFrame(pd.DataFrame({
        "d": ["2025-01-31", "2025-01-31", "2025-03-31"],
        "c": ["c1", "c2", "c3"],
    })).createOrReplaceTempView("pq_scope_months")
    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank"}},
        "evaluation": {"snap_date": ["2025-01-31", "2025-02-28"]},
    }
    keys, without_rows = read_scope_keys(
        spark, "pq_scope_months",
        {"table": "pq_scope_months",
         "columns": {"snap_date": "d", "cust_id": "c"}},
        {"snap_date": "string", "cust_id": "string"}, params)
    assert without_rows == ["2025-02-28"]
    assert sorted(map(tuple, keys.collect())) == [
        ("2025-01-31", "c1"), ("2025-01-31", "c2")]
