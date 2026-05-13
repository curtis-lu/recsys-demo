"""Tests for evaluation.metrics_spark module."""

from pyspark.sql import functions as F  # noqa: F401


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


def test_add_row_contributions_ndcg_contrib_perfect_ranking(spark):
    """Two positives at pos 1,2; K=3, total_rel=2.

    iDCG@3 = 1/log2(2) + 1/log2(3) = 1.0 + 0.6309... = 1.6309...
    nDCG contributions only at pos 1,2 (label=1): 1.0/iDCG and (1/log2(3))/iDCG.
    Sum of ndcg_contrib@3 over query = iDCG/iDCG = 1.0  → perfect ranking nDCG=1.
    """
    import math
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1, 1, 2),
            ("20240331", "C0", "B", 0.5, 1, 2, 2),
            ("20240331", "C0", "C", 0.1, 0, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()

    idcg3 = 1.0 / math.log2(2) + 1.0 / math.log2(3)
    assert abs(result[0]["ndcg_contrib@3"] - (1.0 / math.log2(2)) / idcg3) < 1e-9
    assert abs(result[1]["ndcg_contrib@3"] - (1.0 / math.log2(3)) / idcg3) < 1e-9
    assert result[2]["ndcg_contrib@3"] == 0.0  # label=0
    total = sum(r["ndcg_contrib@3"] for r in result)
    assert abs(total - 1.0) < 1e-9


def test_add_row_contributions_ndcg_contrib_outside_top_k(spark):
    """K=1: only first row contributes; positive at pos 2 is cut off."""
    import math
    from recsys_tfb.evaluation.metrics_spark import add_row_contributions

    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 0, 1, 1),
            ("20240331", "C0", "B", 0.5, 1, 2, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    result = add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[1]
    ).orderBy("pos").collect()

    # iDCG@1 with total_rel=1: 1/log2(2) = 1.0
    # pos 1 (label=0): dcg_term=0, ndcg_contrib@1 = 0
    # pos 2 (label=1): top_k@1=0 → ndcg_contrib@1 = 0
    assert result[0]["ndcg_contrib@1"] == 0.0
    assert result[1]["ndcg_contrib@1"] == 0.0


def _full_enriched(spark, k_values=(3,)):
    """End-to-end enriched DF for 2 customers, 3 products, ready for aggregators."""
    from recsys_tfb.evaluation.metrics_spark import (
        add_query_aggregates,
        add_row_contributions,
        rank_within_query,
    )

    raw = spark.createDataFrame(
        [
            # C0: A(score 0.9, label 1), B(0.5, 0), C(0.1, 1)
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C0", "C", 0.1, 1),
            # C1: B(0.8, 1), C(0.6, 0), A(0.3, 0)
            ("20240331", "C1", "A", 0.3, 0),
            ("20240331", "C1", "B", 0.8, 1),
            ("20240331", "C1", "C", 0.6, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    group_cols = ["snap_date", "cust_id"]
    df = rank_within_query(raw, group_cols, "score")
    df = add_query_aggregates(df, group_cols, "label")
    df = df.filter(F.col("total_rel") > 0)
    df = add_row_contributions(df, group_cols, "label", list(k_values))
    return df


def test_aggregate_overall_returns_expected_keys(spark):
    from pyspark.sql import functions as F  # noqa: F401 (used inside _full_enriched)
    from recsys_tfb.evaluation.metrics_spark import aggregate_overall

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_overall(enriched, ["snap_date", "cust_id"], "label", [3])
    assert set(result.keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def test_aggregate_overall_known_values(spark):
    """Hand-computed values.

    C0: ranking A(1) B(2) C(3), labels [1,0,1], total_rel=2
        AP@3 = (1/1 + 2/3) / 2 = 5/6
        precision@3 = 2/3, recall@3 = 2/2 = 1
    C1: ranking B(1) C(2) A(3), labels [1,0,0], total_rel=1
        AP@3 = 1/1 / 1 = 1.0
        precision@3 = 1/3, recall@3 = 1
    Overall = mean over queries:
        map@3 = (5/6 + 1.0) / 2 = 11/12
        precision@3 = (2/3 + 1/3) / 2 = 0.5
        recall@3 = 1.0
    """
    import math
    from recsys_tfb.evaluation.metrics_spark import aggregate_overall

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_overall(enriched, ["snap_date", "cust_id"], "label", [3])
    assert abs(result["map@3"] - 11 / 12) < 1e-9
    assert abs(result["precision@3"] - 0.5) < 1e-9
    assert abs(result["recall@3"] - 1.0) < 1e-9
    # nDCG@3 sanity: must be between 0 and 1
    assert 0 < result["ndcg@3"] <= 1.0


def test_aggregate_by_row_dimension_keyed_by_dim_value(spark):
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    # 3 products, but only A, B, C had label=1 somewhere; check keys are strings.
    assert set(result.keys()) == {"A", "B", "C"}
    for prod, metrics in result.items():
        assert set(metrics.keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def test_aggregate_by_row_dimension_known_values(spark):
    """Same fixture as aggregate_overall.

    Per-product label=1 rows:
      A: only C0 (label=1 at pos 1) → prec_at_pos=1.0 → map@3 = 1.0
      B: only C1 (label=1 at pos 1) → prec_at_pos=1.0 → map@3 = 1.0
      C: only C0 (label=1 at pos 3) → prec_at_pos=2/3 → map@3 = 2/3
    """
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    assert abs(result["A"]["map@3"] - 1.0) < 1e-9
    assert abs(result["B"]["map@3"] - 1.0) < 1e-9
    assert abs(result["C"]["map@3"] - 2 / 3) < 1e-9
    # precision@K == recall@K == mean(top_k@K) for matched rows (matches pandas semantic)
    for prod in result:
        assert result[prod]["precision@3"] == result[prod]["recall@3"]


def test_aggregate_by_row_dimension_filters_to_label_1(spark):
    """label=0 rows contribute nothing; A's metrics should not be diluted by C1's A (label=0)."""
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    enriched = _full_enriched(spark, k_values=[3])
    result = aggregate_by_row_dimension(enriched, ["prod_name"], "label", [3])
    # A only has label=1 at C0 pos 1, so map@3 must be exactly 1.0 (not diluted).
    assert result["A"]["map@3"] == 1.0


def test_aggregate_by_row_dimension_multi_column_key(spark):
    """Multi-column dim → key is '_'.join of values."""
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_row_dimension

    # Add a segment column to the input.
    enriched = _full_enriched(spark, k_values=[3])
    enriched = enriched.withColumn(
        "seg", F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent"))
    )
    result = aggregate_by_row_dimension(enriched, ["prod_name", "seg"], "label", [3])
    # Only label=1 rows: (A, mass), (B, affluent), (C, mass)
    assert set(result.keys()) == {"A_mass", "B_affluent", "C_mass"}


def test_aggregate_by_query_dimension_equal_customer_weight(spark):
    """C0 in 'mass', C1 in 'affluent'.

    Per-query AP@3:  C0 = 5/6,  C1 = 1.0  (from aggregate_overall fixture).
    Per-segment:
        mass     → mean over {C0} = 5/6
        affluent → mean over {C1} = 1.0
    """
    from recsys_tfb.evaluation.metrics_spark import aggregate_by_query_dimension

    enriched = _full_enriched(spark, k_values=[3])
    enriched = enriched.withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    result = aggregate_by_query_dimension(
        enriched, "seg", ["snap_date", "cust_id"], "label", [3]
    )
    assert set(result.keys()) == {"mass", "affluent"}
    assert abs(result["mass"]["map@3"] - 5 / 6) < 1e-9
    assert abs(result["affluent"]["map@3"] - 1.0) < 1e-9
    for seg in result:
        assert set(result[seg].keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def _make_parameters(k_values=(3,), segment_columns=()):
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
        "evaluation": {
            "k_values": list(k_values),
            "segment_columns": list(segment_columns),
        },
    }


def _make_eval_predictions(spark, with_segment=False):
    rows = [
        # C0: A(0.9, 1), B(0.5, 0), C(0.1, 1)
        ("20240331", "C0", "A", 0.9, 1, "mass"),
        ("20240331", "C0", "B", 0.5, 0, "mass"),
        ("20240331", "C0", "C", 0.1, 1, "mass"),
        # C1: B(0.8, 1), C(0.6, 0), A(0.3, 0)
        ("20240331", "C1", "A", 0.3, 0, "affluent"),
        ("20240331", "C1", "B", 0.8, 1, "affluent"),
        ("20240331", "C1", "C", 0.6, 0, "affluent"),
    ]
    schema_cols = ["snap_date", "cust_id", "prod_name", "score", "label"]
    if with_segment:
        schema_cols = schema_cols + ["cust_segment_typ"]
    else:
        rows = [r[:5] for r in rows]
    return spark.createDataFrame(rows, schema=schema_cols)


def test_compute_all_metrics_returns_expected_keys(spark):
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert set(result.keys()) == {
        "overall",
        "per_product",
        "per_segment",
        "per_product_segment",
        "macro_avg",
        "n_queries",
        "n_excluded_queries",
    }


def test_compute_all_metrics_per_product_map_known_values(spark):
    """Mirrors pandas test_per_product_map_known_values:
        per_product["A"].map@3 == 1.0
        per_product["B"].map@3 == 1.0
        per_product["C"].map@3 == 2/3
    """
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters(k_values=[3])
    result = compute_all_metrics(eval_df, params)
    pp = result["per_product"]
    assert abs(pp["A"]["map@3"] - 1.0) < 1e-9
    assert abs(pp["B"]["map@3"] - 1.0) < 1e-9
    assert abs(pp["C"]["map@3"] - 2 / 3) < 1e-9


def test_compute_all_metrics_no_segment_column(spark):
    """No segment column in df → per_segment / per_product_segment are empty."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert result["per_segment"] == {}
    assert result["per_product_segment"] == {}
    assert "by_segment" not in result["macro_avg"]
    assert "by_product_segment" not in result["macro_avg"]
    assert "by_product" in result["macro_avg"]


def test_compute_all_metrics_with_segment_column(spark):
    """Segment column present → per_segment / per_product_segment populated."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = compute_all_metrics(eval_df, params)
    assert set(result["per_segment"].keys()) == {"mass", "affluent"}
    assert "by_segment" in result["macro_avg"]
    assert "by_product_segment" in result["macro_avg"]


def test_compute_all_metrics_excluded_queries_counted(spark):
    """A query with no positives is excluded; n_excluded_queries reflects that."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    # C2 has no positives.
    eval_df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C2", "A", 0.9, 0),
            ("20240331", "C2", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = _make_parameters(k_values=[2])
    result = compute_all_metrics(eval_df, params)
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 1


def test_compute_all_metrics_default_k_values_resolves_all(spark):
    """k_values defaults to [5, 'all']; 'all' resolves to n_products."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters()
    params["evaluation"].pop("k_values")  # use the default
    result = compute_all_metrics(eval_df, params)
    # 3 products → 'all' resolves to 3; together with default 5, sorted unique = [3, 5]
    overall_keys = set(result["overall"].keys())
    assert "map@3" in overall_keys
    assert "map@5" in overall_keys


def test_compute_all_metrics_all_queries_excluded(spark):
    """No positives anywhere → early return with empty dicts and counts."""
    from recsys_tfb.evaluation.metrics_spark import compute_all_metrics

    eval_df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 0),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C1", "A", 0.9, 0),
            ("20240331", "C1", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    params = _make_parameters(k_values=[2])
    result = compute_all_metrics(eval_df, params)
    assert result["overall"] == {}
    assert result["per_product"] == {}
    assert result["per_segment"] == {}
    assert result["per_product_segment"] == {}
    assert result["macro_avg"] == {}
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 2
