"""Tests for evaluation.metrics_spark — first-principles redesign.

Layout mirrors the module's layers:

    Layer 1 row-level     : rank_within_query / add_query_total_rel
                            / add_row_contributions
    Layer 2 per-query     : compute_per_query_metrics
    Layer 3 aggregations  : aggregate_overall / aggregate_per_segment
                            / aggregate_per_item / macro_average
    Layer 4 orchestrator  : compute_all_metrics

There is intentionally NO pandas-parity test — the redesigned Spark module
is the sole source of truth for these metrics.
"""

import math

import pytest
from pyspark.sql import functions as F

from recsys_tfb.evaluation import metrics_spark as ms


# ===========================================================================
# Shared fixtures
# ===========================================================================


def _two_customer_raw(spark):
    """Predictions for 2 customers, 3 products. Used across many tests.

    C0: A(score 0.9, label 1), B(0.5, 0), C(0.1, 1)   total_rel=2
        After ranking: pos A=1, B=2, C=3.
    C1: B(score 0.8, label 1), C(0.6, 0), A(0.3, 0)   total_rel=1
        After ranking: pos B=1, C=2, A=3.
    """
    return spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C0", "C", 0.1, 1),
            ("20240331", "C1", "A", 0.3, 0),
            ("20240331", "C1", "B", 0.8, 1),
            ("20240331", "C1", "C", 0.6, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def _enriched(spark, k_values=(3,)):
    df = _two_customer_raw(spark)
    group_cols = ["snap_date", "cust_id"]
    df = ms.rank_within_query(df, group_cols, "score")
    df = ms.add_query_total_rel(df, group_cols, "label")
    df = df.filter(F.col("total_rel") > 0)
    df = ms.add_row_contributions(df, group_cols, "label", list(k_values))
    return df


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


# ===========================================================================
# _resolve_k_values
# ===========================================================================


def test_resolve_k_values_basic():
    assert ms._resolve_k_values([5, 10], n_products=20) == [5, 10]


def test_resolve_k_values_all_resolves_to_n_products():
    assert ms._resolve_k_values([5, "all"], n_products=8) == [5, 8]


def test_resolve_k_values_case_insensitive():
    assert ms._resolve_k_values(["ALL"], n_products=4) == [4]


def test_resolve_k_values_dedup_and_sort():
    # 'all' resolves to 5, deduped with literal 5; final sorted.
    assert ms._resolve_k_values([5, "all", 3, 5], n_products=5) == [3, 5]


# ===========================================================================
# Layer 1 — row-level enrichment
# ===========================================================================


def test_rank_within_query_assigns_1_based_pos(spark):
    df = spark.createDataFrame(
        [
            ("d", "C0", 0.5),
            ("d", "C0", 0.9),
            ("d", "C0", 0.1),
            ("d", "C1", 0.8),
            ("d", "C1", 0.3),
        ],
        schema=["snap_date", "cust_id", "score"],
    )
    result = ms.rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    by_score = {(r["cust_id"], r["score"]): r["pos"] for r in result}
    assert by_score[("C0", 0.9)] == 1
    assert by_score[("C0", 0.5)] == 2
    assert by_score[("C0", 0.1)] == 3
    assert by_score[("C1", 0.8)] == 1
    assert by_score[("C1", 0.3)] == 2


def test_rank_within_query_independent_groups(spark):
    df = spark.createDataFrame(
        [("d", "C0", 0.9), ("d", "C1", 0.9)],
        schema=["snap_date", "cust_id", "score"],
    )
    result = ms.rank_within_query(df, ["snap_date", "cust_id"], "score").collect()
    assert all(r["pos"] == 1 for r in result)


def test_add_query_total_rel(spark):
    df = spark.createDataFrame(
        [
            ("d", "C0", 1),
            ("d", "C0", 0),
            ("d", "C0", 1),
            ("d", "C1", 0),
            ("d", "C1", 0),
            ("d", "C2", 1),
        ],
        schema=["snap_date", "cust_id", "label"],
    )
    result = ms.add_query_total_rel(df, ["snap_date", "cust_id"], "label").collect()
    by_cust = {r["cust_id"]: r["total_rel"] for r in result}
    assert by_cust["C0"] == 2
    assert by_cust["C1"] == 0
    assert by_cust["C2"] == 1


def test_add_row_contributions_basic_columns(spark):
    # Single C0: A(label=1, pos=1), B(label=0, pos=2), C(label=1, pos=3); total_rel=2
    df = spark.createDataFrame(
        [
            ("d", "C0", "A", 0.9, 1, 1, 2),
            ("d", "C0", "B", 0.5, 0, 2, 2),
            ("d", "C0", "C", 0.1, 1, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    rows = ms.add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()

    assert [r["cum_rel"] for r in rows] == [1, 1, 2]
    assert rows[0]["prec_at_pos"] == 1.0
    assert rows[1]["prec_at_pos"] == 0.5
    assert abs(rows[2]["prec_at_pos"] - 2 / 3) < 1e-12
    assert rows[0]["dcg_term"] == 1.0           # 1/log2(2) = 1
    assert rows[1]["dcg_term"] == 0.0           # label=0
    assert rows[2]["dcg_term"] == 0.5           # 1/log2(4) = 0.5
    assert all(r["top_k@3"] == 1.0 for r in rows)
    assert rows[0]["ap_contrib@3"] == 1.0
    assert rows[1]["ap_contrib@3"] == 0.0
    assert abs(rows[2]["ap_contrib@3"] - 2 / 3) < 1e-12


def test_add_row_contributions_top_k_cutoff(spark):
    df = spark.createDataFrame(
        [
            ("d", "C0", "A", 0.9, 1, 1, 2),
            ("d", "C0", "B", 0.5, 0, 2, 2),
            ("d", "C0", "C", 0.1, 1, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    rows = ms.add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[2]
    ).orderBy("pos").collect()
    assert [r["top_k@2"] for r in rows] == [1.0, 1.0, 0.0]
    assert rows[2]["ap_contrib@2"] == 0.0   # hit at pos 3 cut off by K=2


def test_add_row_contributions_ndcg_perfect_ranking_sums_to_one(spark):
    """Two positives at pos 1,2; K=3, total_rel=2 → sum of ndcg_contrib@3 = 1.0."""
    df = spark.createDataFrame(
        [
            ("d", "C0", "A", 0.9, 1, 1, 2),
            ("d", "C0", "B", 0.5, 1, 2, 2),
            ("d", "C0", "C", 0.1, 0, 3, 2),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    rows = ms.add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[3]
    ).orderBy("pos").collect()
    idcg3 = 1.0 / math.log2(2) + 1.0 / math.log2(3)
    assert abs(rows[0]["ndcg_contrib@3"] - (1.0 / math.log2(2)) / idcg3) < 1e-9
    assert abs(rows[1]["ndcg_contrib@3"] - (1.0 / math.log2(3)) / idcg3) < 1e-9
    assert rows[2]["ndcg_contrib@3"] == 0.0
    assert abs(sum(r["ndcg_contrib@3"] for r in rows) - 1.0) < 1e-9


def test_add_row_contributions_ndcg_outside_top_k_zero(spark):
    df = spark.createDataFrame(
        [
            ("d", "C0", "A", 0.9, 0, 1, 1),
            ("d", "C0", "B", 0.5, 1, 2, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "pos", "total_rel"],
    )
    rows = ms.add_row_contributions(
        df, ["snap_date", "cust_id"], "label", k_values=[1]
    ).orderBy("pos").collect()
    assert rows[0]["ndcg_contrib@1"] == 0.0  # label=0
    assert rows[1]["ndcg_contrib@1"] == 0.0  # cut off by K=1


# ===========================================================================
# Layer 2 — compute_per_query_metrics
# ===========================================================================


def test_compute_per_query_metrics_shape(spark):
    enriched = _enriched(spark, k_values=[3])
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    cols = set(per_query.columns)
    assert {"snap_date", "cust_id", "total_rel"} <= cols
    assert {"map@3", "ndcg@3", "precision@3", "recall@3"} <= cols
    # One row per (snap_date, cust_id) — 2 customers in the fixture.
    assert per_query.count() == 2


def test_compute_per_query_metrics_known_values(spark):
    """C0: ranking A(1) B(2) C(3), labels [1,0,1], total_rel=2
            AP@3 = (1/1 + 2/3) / 2 = 5/6
            precision@3 = 2/3, recall@3 = 1.0
       C1: ranking B(1) C(2) A(3), labels [1,0,0], total_rel=1
            AP@3 = 1/1 / 1 = 1.0
            precision@3 = 1/3, recall@3 = 1.0
    """
    enriched = _enriched(spark, k_values=[3])
    rows = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    ).collect()
    by_cust = {r["cust_id"]: r for r in rows}

    assert abs(by_cust["C0"]["map@3"] - 5 / 6) < 1e-9
    assert abs(by_cust["C0"]["precision@3"] - 2 / 3) < 1e-9
    assert abs(by_cust["C0"]["recall@3"] - 1.0) < 1e-9

    assert abs(by_cust["C1"]["map@3"] - 1.0) < 1e-9
    assert abs(by_cust["C1"]["precision@3"] - 1 / 3) < 1e-9
    assert abs(by_cust["C1"]["recall@3"] - 1.0) < 1e-9


def test_compute_per_query_metrics_carry_cols(spark):
    """carry_cols pulls customer-level attributes (e.g. segment) into per_query."""
    enriched = _enriched(spark, k_values=[3]).withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3], carry_cols=["seg"]
    ).collect()
    by_cust = {r["cust_id"]: r["seg"] for r in per_query}
    assert by_cust["C0"] == "mass"
    assert by_cust["C1"] == "affluent"


# ===========================================================================
# Layer 3 — aggregate_overall
# ===========================================================================


def test_aggregate_overall_known_values(spark):
    """Using the same fixture:
       map@3       = (5/6 + 1.0) / 2 = 11/12
       precision@3 = (2/3 + 1/3) / 2 = 0.5
       recall@3    = 1.0
    """
    enriched = _enriched(spark, k_values=[3])
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    overall = ms.aggregate_overall(per_query, [3])
    assert set(overall.keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}
    assert abs(overall["map@3"] - 11 / 12) < 1e-9
    assert abs(overall["precision@3"] - 0.5) < 1e-9
    assert abs(overall["recall@3"] - 1.0) < 1e-9
    assert 0 < overall["ndcg@3"] <= 1.0


def test_aggregate_overall_recall_at_k_equals_n_products_is_one(spark):
    """At K = n_products, every query has all positives ranked in top-K → recall == 1.0."""
    enriched = _enriched(spark, k_values=[3])  # n_products=3 in fixture
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    overall = ms.aggregate_overall(per_query, [3])
    assert abs(overall["recall@3"] - 1.0) < 1e-12


def test_aggregate_overall_precision_at_k_equals_n_products_is_base_rate(spark):
    """At K = n_products, per-query precision degenerates to total_rel / n_products.

    Fixture: C0 total_rel=2, C1 total_rel=1; n_products=3 (A/B/C).
        per-query precision@3 = (2/3 + 1/3) / 2 = 0.5  (== base rate of positives)
    """
    enriched = _enriched(spark, k_values=[3])
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    overall = ms.aggregate_overall(per_query, [3])
    # base rate of positives = total positives / (n_queries * n_products) = 3 / 6 = 0.5
    assert abs(overall["precision@3"] - 0.5) < 1e-12


# ===========================================================================
# Layer 3 — aggregate_per_segment
# ===========================================================================


def test_aggregate_per_segment_equal_customer_weight(spark):
    """C0 → 'mass' (AP@3=5/6), C1 → 'affluent' (AP@3=1.0).
       Segment with one customer each → seg metric = that customer's metric.
    """
    enriched = _enriched(spark, k_values=[3]).withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3], carry_cols=["seg"]
    )
    per_seg = ms.aggregate_per_segment(per_query, "seg", [3])
    assert set(per_seg.keys()) == {"mass", "affluent"}
    assert abs(per_seg["mass"]["map@3"] - 5 / 6) < 1e-9
    assert abs(per_seg["affluent"]["map@3"] - 1.0) < 1e-9
    for seg in per_seg:
        assert set(per_seg[seg].keys()) == {"map@3", "ndcg@3", "precision@3", "recall@3"}


def test_aggregate_per_segment_non_string_keys_stringified(spark):
    """seg column with integer values still produces str dict keys."""
    enriched = _enriched(spark, k_values=[3]).withColumn(
        "seg_int",
        F.when(F.col("cust_id") == "C0", F.lit(0)).otherwise(F.lit(1)),
    )
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3], carry_cols=["seg_int"]
    )
    per_seg = ms.aggregate_per_segment(per_query, "seg_int", [3])
    assert set(per_seg.keys()) == {"0", "1"}


# ===========================================================================
# Layer 3 — aggregate_per_item
# ===========================================================================


def test_aggregate_per_item_emits_attribution_keys_not_precision_recall(spark):
    """per_item emits hit_rate / map_attr / ndcg_attr / mean_pos — NOT precision / recall."""
    enriched = _enriched(spark, k_values=[3])
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])
    assert set(per_item.keys()) == {"A", "B", "C"}
    for prod, m in per_item.items():
        assert set(m.keys()) == {"mean_pos", "hit_rate@3", "map_attr@3", "ndcg_attr@3"}
        assert "precision@3" not in m
        assert "recall@3" not in m


def test_aggregate_per_item_known_values(spark):
    """Same fixture:
        A: label=1 only at (C0, pos=1) → top_k@3=1, prec_at_pos=1.0
            hit_rate@3 = 1.0
            map_attr@3 = 1.0
            mean_pos   = 1.0
        B: label=1 only at (C1, pos=1) → identical pattern
            hit_rate@3 = 1.0
            map_attr@3 = 1.0
            mean_pos   = 1.0
        C: label=1 only at (C0, pos=3) → top_k@3=1, prec_at_pos=2/3
            hit_rate@3 = 1.0
            map_attr@3 = 2/3
            mean_pos   = 3.0
    """
    enriched = _enriched(spark, k_values=[3])
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])

    assert abs(per_item["A"]["hit_rate@3"] - 1.0) < 1e-12
    assert abs(per_item["A"]["map_attr@3"] - 1.0) < 1e-12
    assert per_item["A"]["mean_pos"] == 1.0

    assert abs(per_item["B"]["hit_rate@3"] - 1.0) < 1e-12
    assert abs(per_item["B"]["map_attr@3"] - 1.0) < 1e-12
    assert per_item["B"]["mean_pos"] == 1.0

    assert abs(per_item["C"]["hit_rate@3"] - 1.0) < 1e-12
    assert abs(per_item["C"]["map_attr@3"] - 2 / 3) < 1e-12
    assert per_item["C"]["mean_pos"] == 3.0


def test_aggregate_per_item_hit_rate_below_one_when_pos_above_k(spark):
    """K=1: C is at pos=3 → hit_rate@1 = 0 for C, but still 1 for A and B."""
    enriched = _enriched(spark, k_values=[1])
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [1])
    assert per_item["A"]["hit_rate@1"] == 1.0
    assert per_item["B"]["hit_rate@1"] == 1.0
    assert per_item["C"]["hit_rate@1"] == 0.0


def test_aggregate_per_item_multi_column_key(spark):
    """dim_cols=[item, seg] → key joined with '_'. Only label=1 rows kept.
        label=1 rows: (A, mass), (B, affluent), (C, mass).
    """
    enriched = _enriched(spark, k_values=[3]).withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    per_ips = ms.aggregate_per_item(enriched, ["prod_name", "seg"], "label", [3])
    assert set(per_ips.keys()) == {"A_mass", "B_affluent", "C_mass"}


def test_aggregate_per_item_filters_label_zero_rows(spark):
    """A appears at (C1, pos=3) with label=0; its mean_pos must NOT be polluted by that."""
    enriched = _enriched(spark, k_values=[3])
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])
    assert per_item["A"]["mean_pos"] == 1.0   # would be (1+3)/2 = 2.0 if label=0 leaked in


# ===========================================================================
# macro_average
# ===========================================================================


def test_macro_average_basic():
    per_dim = {
        "A": {"hit_rate@3": 1.0, "map_attr@3": 0.5},
        "B": {"hit_rate@3": 0.5, "map_attr@3": 0.7},
    }
    assert ms.macro_average(per_dim) == {"hit_rate@3": 0.75, "map_attr@3": 0.6}


def test_macro_average_empty():
    assert ms.macro_average({}) == {}


def test_macro_average_skips_missing_keys():
    """If a key is missing in some dim entries, it is averaged only over the
    entries where it appears."""
    per_dim = {
        "A": {"hit_rate@3": 1.0, "map_attr@3": 0.5},
        "B": {"hit_rate@3": 0.5},                       # no map_attr@3
    }
    avg = ms.macro_average(per_dim)
    assert avg["hit_rate@3"] == 0.75
    assert avg["map_attr@3"] == 0.5   # only A contributes


# ===========================================================================
# Layer 4 — compute_all_metrics
# ===========================================================================


def _make_eval_predictions(spark, with_segment: bool):
    rows = [
        ("20240331", "C0", "A", 0.9, 1, "mass"),
        ("20240331", "C0", "B", 0.5, 0, "mass"),
        ("20240331", "C0", "C", 0.1, 1, "mass"),
        ("20240331", "C1", "A", 0.3, 0, "affluent"),
        ("20240331", "C1", "B", 0.8, 1, "affluent"),
        ("20240331", "C1", "C", 0.6, 0, "affluent"),
    ]
    cols = ["snap_date", "cust_id", "prod_name", "score", "label", "cust_segment_typ"]
    if not with_segment:
        rows = [r[:5] for r in rows]
        cols = cols[:5]
    return spark.createDataFrame(rows, schema=cols)


def test_compute_all_metrics_returns_expected_keys(spark):
    df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = ms.compute_all_metrics(df, params)
    assert set(result.keys()) == {
        "overall", "per_segment", "per_item", "per_item_segment",
        "macro_avg", "n_queries", "n_excluded_queries", "dataset_overview",
    }


def test_compute_all_metrics_per_item_known_values(spark):
    """map_attr@3: A=1.0, B=1.0, C=2/3 (same as the layer-3 test, end-to-end)."""
    df = _make_eval_predictions(spark, with_segment=False)
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[3]))
    pi = result["per_item"]
    assert abs(pi["A"]["map_attr@3"] - 1.0) < 1e-9
    assert abs(pi["B"]["map_attr@3"] - 1.0) < 1e-9
    assert abs(pi["C"]["map_attr@3"] - 2 / 3) < 1e-9


def test_compute_all_metrics_overall_per_query_aggregation(spark):
    """Overall map@3 = mean of per-query AP@3 = (5/6 + 1.0) / 2 = 11/12."""
    df = _make_eval_predictions(spark, with_segment=False)
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[3]))
    assert abs(result["overall"]["map@3"] - 11 / 12) < 1e-9


def test_compute_all_metrics_no_segment_column(spark):
    df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = ms.compute_all_metrics(df, params)
    assert result["per_segment"] == {}
    assert result["per_item_segment"] == {}
    assert "by_segment" not in result["macro_avg"]
    assert "by_item_segment" not in result["macro_avg"]
    assert "by_item" in result["macro_avg"]


def test_compute_all_metrics_with_segment_column(spark):
    df = _make_eval_predictions(spark, with_segment=True)
    params = _make_parameters(k_values=[3], segment_columns=["cust_segment_typ"])
    result = ms.compute_all_metrics(df, params)
    assert set(result["per_segment"].keys()) == {"mass", "affluent"}
    assert set(result["per_item_segment"].keys()) == {"A_mass", "B_affluent", "C_mass"}
    assert "by_segment" in result["macro_avg"]
    assert "by_item_segment" in result["macro_avg"]


def test_compute_all_metrics_excluded_queries_counted(spark):
    """A query with no positives is excluded; n_excluded_queries reflects that."""
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C2", "A", 0.9, 0),
            ("20240331", "C2", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[2]))
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 1


def test_compute_all_metrics_default_k_values_resolves_all(spark):
    """Default k_values=[5, 'all']; with 3 products → resolves to [3, 5]."""
    df = _make_eval_predictions(spark, with_segment=False)
    params = _make_parameters()
    params["evaluation"].pop("k_values")
    result = ms.compute_all_metrics(df, params)
    keys = set(result["overall"].keys())
    assert "map@3" in keys
    assert "map@5" in keys


def test_compute_all_metrics_all_queries_excluded(spark):
    """No positives anywhere → early return with empty dicts."""
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 0),
            ("20240331", "C0", "B", 0.5, 0),
            ("20240331", "C1", "A", 0.9, 0),
            ("20240331", "C1", "B", 0.5, 0),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[2]))
    assert result["overall"] == {}
    assert result["per_segment"] == {}
    assert result["per_item"] == {}
    assert result["per_item_segment"] == {}
    assert result["macro_avg"] == {}
    assert result["n_queries"] == 2
    assert result["n_excluded_queries"] == 2


def test_compute_all_metrics_segment_column_auto_detection_picks_first_present(spark):
    """When multiple segment columns are configured but only one is in the df,
    that one is picked.
    """
    df = _make_eval_predictions(spark, with_segment=True)
    # cust_segment_typ present; xxx_missing not in df → cust_segment_typ wins.
    params = _make_parameters(
        k_values=[3], segment_columns=["xxx_missing", "cust_segment_typ"]
    )
    result = ms.compute_all_metrics(df, params)
    assert set(result["per_segment"].keys()) == {"mass", "affluent"}


def test_compute_all_metrics_precision_at_n_products_is_base_rate(spark):
    """K=n_products fixture: 3 products, 6 rows, 3 positives → base rate = 0.5.
       overall.precision@3 must equal 0.5 (degenerate K=all case).
    """
    df = _make_eval_predictions(spark, with_segment=False)
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[3]))
    assert abs(result["overall"]["precision@3"] - 0.5) < 1e-12


def test_macro_per_item_map_numpy_matches_spark(spark):
    """compute_macro_per_item_map (numpy, HPO) == compute_all_metrics
    macro_avg.by_item.map_attr@all (Spark) on identical data.

    k_values=(3,) and 3 products => k=3 == n_products == 'all'.
    Scores are distinct so lexsort tie-order vs Spark row_number is moot.
    """
    import numpy as np

    from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

    df = _two_customer_raw(spark)
    params = _make_parameters(k_values=(3,))
    result = ms.compute_all_metrics(df, params)
    spark_macro = result["macro_avg"]["by_item"]["map_attr@3"]

    rows = df.collect()
    group_ids = {("20240331", "C0"): 0, ("20240331", "C1"): 1}
    groups = np.array([group_ids[(r["snap_date"], r["cust_id"])] for r in rows])
    items = np.array([r["prod_name"] for r in rows])
    y = np.array([r["label"] for r in rows])
    score = np.array([r["score"] for r in rows], dtype=np.float64)

    numpy_macro = compute_macro_per_item_map(groups, items, y, score)
    assert numpy_macro == pytest.approx(spark_macro, rel=1e-12)
