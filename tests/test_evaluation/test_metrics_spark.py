"""Tests for evaluation.metrics_spark module."""


def test_module_imports():
    """Verify the new module imports without errors."""
    from recsys_tfb.evaluation import metrics_spark  # noqa: F401


def test_rank_within_query_assigns_1_based_pos(spark):
    from recsys_tfb.evaluation.metrics_spark import rank_within_query

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 0.5),
            ("20240331", "C0", 0.9),
            ("20240331", "C0", 0.1),
            ("20240331", "C1", 0.8),
            ("20240331", "C1", 0.3),
        ],
        schema=["snap_date", "cust_id", "score"],
    )
    result = rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    by_score = {(r["cust_id"], r["score"]): r["pos"] for r in result}
    # C0: 0.9 → 1, 0.5 → 2, 0.1 → 3
    assert by_score[("C0", 0.9)] == 1
    assert by_score[("C0", 0.5)] == 2
    assert by_score[("C0", 0.1)] == 3
    # C1: 0.8 → 1, 0.3 → 2
    assert by_score[("C1", 0.8)] == 1
    assert by_score[("C1", 0.3)] == 2


def test_rank_within_query_independent_groups(spark):
    """pos is per-query, not global."""
    from recsys_tfb.evaluation.metrics_spark import rank_within_query

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 0.9),
            ("20240331", "C1", 0.9),
        ],
        schema=["snap_date", "cust_id", "score"],
    )
    result = rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    # Both rows get pos=1 within their own query.
    assert all(r["pos"] == 1 for r in result)


def test_add_query_aggregates_total_rel_per_query(spark):
    from recsys_tfb.evaluation.metrics_spark import add_query_aggregates

    df = spark.createDataFrame(
        [
            ("20240331", "C0", 1),
            ("20240331", "C0", 0),
            ("20240331", "C0", 1),
            ("20240331", "C1", 0),
            ("20240331", "C1", 0),
            ("20240331", "C2", 1),
        ],
        schema=["snap_date", "cust_id", "label"],
    )
    result = add_query_aggregates(df, ["snap_date", "cust_id"], "label").collect()
    by_cust = {r["cust_id"]: r["total_rel"] for r in result}
    # Same value should repeat across all rows of the same query.
    assert by_cust["C0"] == 2
    assert by_cust["C1"] == 0
    assert by_cust["C2"] == 1
