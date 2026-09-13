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
    df = ms.rank_within_query(df, group_cols, "score", "prod_name")
    df = ms.add_query_total_rel(df, group_cols, "label")
    df = df.filter(F.col("total_rel") > 0)
    df = ms.add_row_contributions(df, group_cols, "label", list(k_values))
    return df


def _make_parameters(k_values=(3,), segment_columns=(), metric=None):
    params = {
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
    if metric is not None:
        params["evaluation"]["metric"] = dict(metric)
    return params


# ===========================================================================
# _resolve_k_values
# ===========================================================================


def test_resolve_k_values_basic():
    assert ms._resolve_k_values([5, 10], n_items=20) == [5, 10]


def test_resolve_k_values_all_resolves_to_n_items():
    assert ms._resolve_k_values([5, "all"], n_items=8) == [5, 8]


def test_resolve_k_values_case_insensitive():
    assert ms._resolve_k_values(["ALL"], n_items=4) == [4]


def test_resolve_k_values_dedup_and_sort():
    # 'all' resolves to 5, deduped with literal 5; final sorted.
    assert ms._resolve_k_values([5, "all", 3, 5], n_items=5) == [3, 5]


# ===========================================================================
# Layer 1 — row-level enrichment
# ===========================================================================


def test_rank_within_query_assigns_1_based_pos(spark):
    df = spark.createDataFrame(
        [
            ("d", "C0", "A", 0.5),
            ("d", "C0", "B", 0.9),
            ("d", "C0", "C", 0.1),
            ("d", "C1", "A", 0.8),
            ("d", "C1", "B", 0.3),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score"],
    )
    result = ms.rank_within_query(
        df, ["snap_date", "cust_id"], "score", "prod_name"
    ).collect()
    by_score = {(r["cust_id"], r["score"]): r["pos"] for r in result}
    assert by_score[("C0", 0.9)] == 1
    assert by_score[("C0", 0.5)] == 2
    assert by_score[("C0", 0.1)] == 3
    assert by_score[("C1", 0.8)] == 1
    assert by_score[("C1", 0.3)] == 2


def test_rank_within_query_independent_groups(spark):
    df = spark.createDataFrame(
        [("d", "C0", "A", 0.9), ("d", "C1", "A", 0.9)],
        schema=["snap_date", "cust_id", "prod_name", "score"],
    )
    result = ms.rank_within_query(
        df, ["snap_date", "cust_id"], "score", "prod_name"
    ).collect()
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


def test_aggregate_overall_recall_at_k_equals_n_items_is_one(spark):
    """At K = n_items, every query has all positives ranked in top-K → recall == 1.0."""
    enriched = _enriched(spark, k_values=[3])  # n_items=3 in fixture
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    overall = ms.aggregate_overall(per_query, [3])
    assert abs(overall["recall@3"] - 1.0) < 1e-12


def test_aggregate_overall_precision_at_k_equals_n_items_is_base_rate(spark):
    """At K = n_items, per-query precision degenerates to total_rel / n_items.

    Fixture: C0 total_rel=2, C1 total_rel=1; n_items=3 (A/B/C).
        per-query precision@3 = (2/3 + 1/3) / 2 = 0.5  (== base rate of positives)
    """
    enriched = _enriched(spark, k_values=[3])
    per_query = ms.compute_per_query_metrics(
        enriched, ["snap_date", "cust_id"], "label", [3]
    )
    overall = ms.aggregate_overall(per_query, [3])
    # base rate of positives = total positives / (n_queries * n_items) = 3 / 6 = 0.5
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
        assert set(m.keys()) == {"mean_pos", "n_pos", "hit_rate@3", "map_attr@3", "ndcg_attr@3"}
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
    """dim_cols=[item, seg] → two-level {item: {seg: metrics}}. Only label=1 rows kept.
        label=1 rows: (A, mass), (B, affluent), (C, mass).
    """
    enriched = _enriched(spark, k_values=[3]).withColumn(
        "seg",
        F.when(F.col("cust_id") == "C0", F.lit("mass")).otherwise(F.lit("affluent")),
    )
    per_ips = ms.aggregate_per_item(enriched, ["prod_name", "seg"], "label", [3])
    assert {item: set(by_seg) for item, by_seg in per_ips.items()} == {
        "A": {"mass"}, "B": {"affluent"}, "C": {"mass"},
    }
    assert per_ips["A"]["mass"]["n_pos"] == 1


def test_aggregate_per_item_rejects_three_dim_cols(spark):
    enriched = _enriched(spark, k_values=[3])
    with pytest.raises(ValueError, match="1 or 2 dim_cols"):
        ms.aggregate_per_item(
            enriched, ["prod_name", "cust_id", "snap_date"], "label", [3]
        )


def test_aggregate_per_item_filters_label_zero_rows(spark):
    """A appears at (C1, pos=3) with label=0; its mean_pos must NOT be polluted by that."""
    enriched = _enriched(spark, k_values=[3])
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])
    assert per_item["A"]["mean_pos"] == 1.0   # would be (1+3)/2 = 2.0 if label=0 leaked in


def test_aggregate_per_item_emits_n_pos(spark):
    """n_pos = 該 item 的正例列數（weight_alpha/min_positives/shrinkage_k 的 P_j 來源）。"""
    enriched = _enriched(spark)
    per_item = ms.aggregate_per_item(enriched, ["prod_name"], "label", [3])
    # _two_customer_raw：A 正例 1 列（C0）、B 1 列（C1）、C 1 列（C0）
    assert per_item["A"]["n_pos"] == 1
    assert per_item["B"]["n_pos"] == 1
    assert per_item["C"]["n_pos"] == 1
    assert isinstance(per_item["A"]["n_pos"], int)


def test_macro_average_excludes_n_pos_from_output(spark=None):
    per_dim = {
        "A": {"map_attr@3": 0.75, "n_pos": 2},
        "B": {"map_attr@3": 1.0, "n_pos": 1},
    }
    avg = ms.macro_average(per_dim)
    assert avg == {"map_attr@3": pytest.approx(0.875)}
    assert "n_pos" not in avg


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
        "macro_avg", "observation_items", "n_queries", "n_excluded_queries",
        "dataset_overview",
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
    params = _make_parameters(k_values=[3])
    result = ms.compute_all_metrics(
        df, params, segment_columns=["cust_segment_typ"]
    )
    assert set(result["per_segment"].keys()) == {"mass", "affluent"}
    assert {
        item: set(by_seg) for item, by_seg in result["per_item_segment"].items()
    } == {"A": {"mass"}, "B": {"affluent"}, "C": {"mass"}}
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


def test_unmatched_segment_is_named_counted_and_kept_out_of_macro(spark):
    """A query whose segment is NULL (the population table has the column but
    no value for that key) is its own "(unmatched)" group: shown, counted, but
    not averaged into any macro (ADR-0020 bug 6). Before, it was a group
    called "None" weighted like a real one.

    C0 mass AP@3 = 5/6, C1 affluent AP@3 = 1.0, C2 unmatched: B positive at
    rank 2 -> AP@3 = 1/2. Macro over matched segments = (5/6 + 1) / 2 = 11/12;
    with C2 in it would be 7/9. Per-item-segment map_attr@3 (per-row P@rank):
    A_mass 1, C_mass 2/3, B_affluent 1, B_unmatched 1/2 -> matched macro 8/9.
    """
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1, "mass"),
            ("20240331", "C0", "B", 0.5, 0, "mass"),
            ("20240331", "C0", "C", 0.1, 1, "mass"),
            ("20240331", "C1", "A", 0.3, 0, "affluent"),
            ("20240331", "C1", "B", 0.8, 1, "affluent"),
            ("20240331", "C1", "C", 0.6, 0, "affluent"),
            ("20240331", "C2", "A", 0.9, 0, None),
            ("20240331", "C2", "B", 0.5, 1, None),
            ("20240331", "C2", "C", 0.1, 0, None),
        ],
        schema="snap_date string, cust_id string, prod_name string, "
               "score double, label int, cust_segment_typ string",
    )
    params = _make_parameters(k_values=[3])
    result = ms.compute_all_metrics(
        df, params, segment_columns=["cust_segment_typ"]
    )

    assert set(result["per_segment"]) == {"mass", "affluent", "(unmatched)"}
    assert result["per_segment"]["(unmatched)"]["map@3"] == pytest.approx(0.5)
    assert result["macro_avg"]["by_segment"]["map@3"] == pytest.approx(11 / 12)
    assert result["macro_avg"]["by_item_segment"]["map_attr@3"] == \
        pytest.approx(8 / 9)

    by_seg = result["dataset_overview"]["by_segment"]
    assert by_seg["(unmatched)"]["n_queries"] == 1
    assert by_seg["(unmatched)"]["query_share"] == pytest.approx(1 / 3)


def _with_stale_all_null_segment(spark):
    """The segment fixture plus a column another run mode joined in once: the
    enriched table is shared by both modes, so its schema is the union and
    this run's rows carry the other mode's column as all NULL."""
    return _make_eval_predictions(spark, with_segment=True).withColumn(
        "stale_seg", F.lit(None).cast("string")
    )


def test_segments_come_from_the_passed_list_not_the_frame(spark):
    """The metric layer segments by the columns it is handed (the landed
    evaluation_segment_columns), never by "configured and present in the
    frame". Otherwise the all-NULL stale column, listed first in config and
    present in the frame, is picked and grows a fake "(unmatched)" group.
    """
    df = _with_stale_all_null_segment(spark)
    params = _make_parameters(
        k_values=[3], segment_columns=["stale_seg", "cust_segment_typ"]
    )
    params["schema"]["categorical_values"] = {"prod_name": ["A", "B", "C"]}
    params["evaluation"]["item_categories"] = {
        "enabled": True, "unmapped": "singleton", "mapping": {"AB": ["A", "B"]}}

    full = ms.compute_all_metrics(df, params, segment_columns=["cust_segment_typ"])
    assert set(full["per_segment"]) == {"mass", "affluent"}
    assert set(full["dataset_overview"]["by_segment"]) == {"mass", "affluent"}
    assert set(full["category"]["per_segment"]) == {"mass", "affluent"}

    slim = ms.compute_overall_per_item(
        df, params, segment_columns=["cust_segment_typ"]
    )
    assert set(slim["per_segment"]) == {"mass", "affluent"}

    none = ms.compute_all_metrics(df, params, segment_columns=[])
    assert none["per_segment"] == {}
    assert "by_segment" not in none["dataset_overview"]


def test_a_passed_segment_column_missing_from_the_frame_raises(spark):
    """The list and the frame come from the same producer; disagreeing means
    the wiring is wrong, and silently dropping the column would hide it."""
    df = _make_eval_predictions(spark, with_segment=False)
    with pytest.raises(ValueError, match="cust_segment_typ.*not in the frame"):
        ms.compute_all_metrics(
            df, _make_parameters(k_values=[3]),
            segment_columns=["cust_segment_typ"],
        )


def test_compute_all_metrics_precision_at_n_items_is_base_rate(spark):
    """K=n_items fixture: 3 products, 6 rows, 3 positives → base rate = 0.5.
       overall.precision@3 must equal 0.5 (degenerate K=all case).
    """
    df = _make_eval_predictions(spark, with_segment=False)
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[3]))
    assert abs(result["overall"]["precision@3"] - 0.5) < 1e-12


def test_macro_per_item_map_numpy_matches_spark(spark):
    """compute_macro_per_item_map (numpy, HPO) == compute_all_metrics
    macro_avg.by_item.map_attr@all (Spark) on identical data.

    k_values=(3,) and 3 products => k=3 == n_items == 'all'.
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


# ===========================================================================
# macro_average parameterization (weight_alpha / min_positives / shrinkage_k)
# + compute_all_metrics observation_items / evaluation.metric wiring
# ===========================================================================


def _three_customer_raw(spark):
    """A 正例 2 個 query（contrib 1.0、0.5 → AP 0.75, n_pos=2）、B 1 個（1.0, n_pos=1）。"""
    return spark.createDataFrame(
        [
            ("20240331", "C0", "A", 0.9, 1),
            ("20240331", "C0", "B", 0.1, 0),
            ("20240331", "C1", "A", 0.1, 1),
            ("20240331", "C1", "B", 0.9, 0),
            ("20240331", "C2", "A", 0.1, 0),
            ("20240331", "C2", "B", 0.9, 1),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label"],
    )


def test_macro_average_weighted_by_n_pos():
    per_dim = {
        "A": {"map_attr@2": 0.75, "n_pos": 2},
        "B": {"map_attr@2": 1.0, "n_pos": 1},
    }
    assert ms.macro_average(per_dim, weight_alpha=1.0) == {
        "map_attr@2": pytest.approx(5 / 6)
    }
    assert ms.macro_average(per_dim, min_positives=2) == {
        "map_attr@2": pytest.approx(0.75)
    }
    assert ms.macro_average(per_dim, shrinkage_k=1.0) == {
        "map_attr@2": pytest.approx(61 / 72)
    }
    assert ms.macro_average(per_dim, min_positives=5) == {}


def test_macro_average_missing_n_pos_fails_loud():
    per_dim = {"A": {"map_attr@2": 0.75}, "B": {"map_attr@2": 1.0}}
    with pytest.raises(ValueError, match="n_pos"):
        ms.macro_average(per_dim, weight_alpha=1.0)


def test_compute_all_metrics_observation_items_and_param_macro(spark):
    df = _three_customer_raw(spark)
    # 預設參數：additive 鍵存在且為空、macro 不變
    base = ms.compute_all_metrics(df, _make_parameters(k_values=[2]))
    assert base["observation_items"] == []
    assert base["macro_avg"]["by_item"]["map_attr@2"] == pytest.approx(0.875)
    # min_positives=2：B 進觀察名單、macro 只剩 A
    params = _make_parameters(
        k_values=[2],
        metric={"weight_alpha": 0.0, "min_positives": 2, "shrinkage_k": 0},
    )
    result = ms.compute_all_metrics(df, params)
    assert result["observation_items"] == ["B"]
    assert result["macro_avg"]["by_item"]["map_attr@2"] == pytest.approx(0.75)


def test_param_macro_numpy_matches_spark(spark):
    """參數化後 numpy／Spark 兩實作同輸入同結果（spec Phase 1 parity 要求）。"""
    import numpy as np

    from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

    df = _three_customer_raw(spark)
    # weight_alpha 與 shrinkage_k 刻意用相異值：兩者同值時，_compute_core 把
    # 兩個 config 鍵接反（swap bug）也測不出來（審查用 fault injection 實證過）。
    metric = {"weight_alpha": 1.0, "min_positives": 0, "shrinkage_k": 2.0}
    result = ms.compute_all_metrics(df, _make_parameters(k_values=[2], metric=metric))
    spark_macro = result["macro_avg"]["by_item"]["map_attr@2"]

    rows = df.collect()
    group_ids = {("20240331", f"C{i}"): i for i in range(3)}
    groups = np.array([group_ids[(r["snap_date"], r["cust_id"])] for r in rows])
    items = np.array([r["prod_name"] for r in rows])
    y = np.array([r["label"] for r in rows])
    score = np.array([r["score"] for r in rows], dtype=np.float64)

    numpy_macro = compute_macro_per_item_map(
        groups, items, y, score,
        weight_alpha=1.0, min_positives=0, shrinkage_k=2.0,
    )
    assert numpy_macro == pytest.approx(spark_macro, rel=1e-12)


# ===========================================================================
# metric.k — truncation depth of the headline per-item family, an axis
# independent of k_values (ADR-0020 design H)
# ===========================================================================


def _metric_k_pdf():
    """3 items x 3 queries, no ties within a query. metric.k=2 < n_items=3,
    so the truncation actually bites.

    C0: A .9(1) B .5(0) C .1(1) -> A rank 1 adds 1; C rank 3 adds 2/3 (k=2 -> 0)
    C1: B .8(1) C .6(0) A .3(0) -> B rank 1 adds 1
    C2: C .7(0) A .6(1) B .2(1) -> A rank 2 adds 1/2; B rank 3 adds 2/3 (k=2 -> 0)

    Truncated at 2: A=.75, B=.5, C=0 -> macro 5/12; untruncated macro=.75.
    """
    import pandas as pd

    rows = [
        ("20240331", "C0", "A", 0.9, 1),
        ("20240331", "C0", "B", 0.5, 0),
        ("20240331", "C0", "C", 0.1, 1),
        ("20240331", "C1", "A", 0.3, 0),
        ("20240331", "C1", "B", 0.8, 1),
        ("20240331", "C1", "C", 0.6, 0),
        ("20240331", "C2", "A", 0.6, 1),
        ("20240331", "C2", "B", 0.2, 1),
        ("20240331", "C2", "C", 0.7, 0),
    ]
    return pd.DataFrame(
        rows, columns=["snap_date", "cust_id", "prod_name", "score", "label"]
    )


def _metric_k_params():
    params = _make_parameters(k_values=[1, "all"], metric={"k": 2})
    params["evaluation"]["diagnosis"] = {"ci": {"n_boot": 20}}
    return params


def test_metric_k_truncates_main_macro_same_as_ci_point(spark):
    """metric.k is not in k_values, yet the main path still emits
    map_attr@{metric.k}, and it equals the CI point estimate on the same
    data — they only match when both are truncated the same way.
    """
    from recsys_tfb.diagnosis.metric.uncertainty import bootstrap_per_item_ci

    pdf = _metric_k_pdf()
    params = _metric_k_params()
    result = ms.compute_all_metrics(spark.createDataFrame(pdf), params)
    main = result["macro_avg"]["by_item"]["map_attr@2"]
    assert main == pytest.approx(5 / 12)

    ci = bootstrap_per_item_ci(pdf, params)
    assert ci["macro"]["ap"] == pytest.approx(main)


def test_metric_k_grid_shared_by_slim_path(spark):
    """The baseline's slim path uses the same K grids as the main path
    (metric.k included on the per-item side), so the two sides line up."""
    pdf = _metric_k_pdf()
    params = _metric_k_params()
    slim = ms.compute_overall_per_item(spark.createDataFrame(pdf), params)
    full = ms.compute_all_metrics(spark.createDataFrame(pdf), params)
    assert "map_attr@2" in slim["per_item"]["A"]
    assert slim["per_item"] == full["per_item"]
    assert slim["overall"] == full["overall"]


def test_metric_k_reaches_only_the_per_item_family(spark):
    """``metric.k`` truncates the per-item macro family only; ``k_values``
    alone is the @K grid of the per-query families (ADR-0020 design H).

    ``k_values=[1, "all"]`` (all -> 3 items) with ``metric.k=2``: a leak would
    silently add ``map@2`` / ``precision@2`` / ``recall@2`` / ``ndcg@2`` to
    ``overall`` / ``per_segment``, and the comparison report prints every
    ``overall`` key as a row, so nobody-configured rows would appear.
    """
    pdf = _metric_k_pdf()
    pdf["seg"] = pdf["cust_id"].map({"C0": "s1", "C1": "s1", "C2": "s2"})
    params = _make_parameters(k_values=[1, "all"], metric={"k": 2})
    full = ms.compute_all_metrics(
        spark.createDataFrame(pdf), params, segment_columns=["seg"]
    )

    assert [k for k in full["overall"] if k.endswith("@2")] == []
    assert {"map@1", "map@3"} <= set(full["overall"])
    for seg, cell in full["per_segment"].items():
        assert [k for k in cell if k.endswith("@2")] == [], seg
    assert [k for k in full["macro_avg"]["by_segment"] if k.endswith("@2")] == []

    assert "map_attr@2" in full["per_item"]["A"]
    assert full["macro_avg"]["by_item"]["map_attr@2"] == pytest.approx(5 / 12)
    assert "map_attr@2" in full["per_item_segment"]["A"]["s1"]
    assert "map_attr@2" in full["macro_avg"]["by_item_segment"]

    slim = ms.compute_overall_per_item(
        spark.createDataFrame(pdf), params, segment_columns=["seg"]
    )
    assert slim["overall"] == full["overall"]
    assert slim["per_segment"] == full["per_segment"]
    assert slim["per_item"] == full["per_item"]


# ===========================================================================
# per_item_segment — two-level dict, no more "_"-joined keys (ADR-0020 bug 12)
# ===========================================================================


def test_per_item_segment_is_two_level_and_collision_free(spark):
    """(item="a", seg="b_c") and (item="a_b", seg="c") both used to join to
    "a_b_c", and the later write overwrote the earlier one.

    C0 (seg b_c): a .9(1)  a_b .1(0) -> cell (a, b_c)   map_attr@2 = 1
    C1 (seg c)  : a .9(0)  a_b .1(1) -> cell (a_b, c)   map_attr@2 = 1/2
    by_item_segment weights cells equally -> (1 + 1/2) / 2 = 0.75.
    """
    df = spark.createDataFrame(
        [
            ("20240331", "C0", "a", 0.9, 1, "b_c"),
            ("20240331", "C0", "a_b", 0.1, 0, "b_c"),
            ("20240331", "C1", "a", 0.9, 0, "c"),
            ("20240331", "C1", "a_b", 0.1, 1, "c"),
        ],
        schema=["snap_date", "cust_id", "prod_name", "score", "label", "seg"],
    )
    result = ms.compute_all_metrics(
        df, _make_parameters(k_values=[2]), segment_columns=["seg"]
    )
    pis = result["per_item_segment"]
    assert pis["a"]["b_c"]["map_attr@2"] == pytest.approx(1.0)
    assert pis["a_b"]["c"]["map_attr@2"] == pytest.approx(0.5)
    assert result["macro_avg"]["by_item_segment"]["map_attr@2"] == pytest.approx(0.75)
