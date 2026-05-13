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


def _basic_enriched_input(spark):
    """A query (C0) of 3 items already ranked by score (pos column included)."""
    return spark.createDataFrame(
        [
            # snap_date, cust_id, prod, score, label, pos, total_rel
            ("20240331", "C0", "A", 0.9, 1, 1, 2),
            ("20240331", "C0", "B", 0.5, 0, 2, 2),
            ("20240331", "C0", "C", 0.1, 1, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )


def test_add_row_contributions_basic_columns(spark):
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = _basic_enriched_input(spark)
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()

    # cum_rel: 1, 1, 2
    assert [r["cum_rel"] for r in result] == [1, 1, 2]
    # prec_at_pos: 1/1, 1/2, 2/3
    assert result[0]["prec_at_pos"] == 1.0
    assert result[1]["prec_at_pos"] == 0.5
    assert abs(result[2]["prec_at_pos"] - 2 / 3) < 1e-12
    # dcg_term: label / log2(pos+1) → 1/log2(2)=1.0, 0/log2(3)=0, 1/log2(4)=0.5
    assert result[0]["dcg_term"] == 1.0
    assert result[1]["dcg_term"] == 0.0
    assert result[2]["dcg_term"] == 0.5
    # top_k@3: all in top 3
    assert all(r["top_k@3"] == 1.0 for r in result)
    # ap_contrib@3 = prec_at_pos * label * top_k → 1.0, 0, 2/3
    assert result[0]["ap_contrib@3"] == 1.0
    assert result[1]["ap_contrib@3"] == 0.0
    assert abs(result[2]["ap_contrib@3"] - 2 / 3) < 1e-12


def test_add_row_contributions_top_k_cutoff(spark):
    """top_k@2 should be 0 for pos > 2; ap_contrib@2 should follow."""
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = _basic_enriched_input(spark)
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[2]
    ).orderBy("pos").collect()

    assert [r["top_k@2"] for r in result] == [1.0, 1.0, 0.0]
    # pos 3 was a hit (label=1) but cut off by top_k@2 → ap_contrib@2 = 0
    assert result[2]["ap_contrib@2"] == 0.0
