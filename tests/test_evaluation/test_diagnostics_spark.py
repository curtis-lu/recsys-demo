"""Tests for evaluation.diagnostics_spark — Spark-side aggregation that
replaces row-level toPandas for the diagnostics figures. Each function reduces
the (potentially huge) eval_predictions DataFrame to a small pandas frame whose
size does not grow with the number of rows."""

import pandas as pd

from recsys_tfb.evaluation.diagnostics_spark import (
    calibration_bins,
    positive_rank_count_matrix,
    positive_rate_matrix,
    rank_count_matrix,
    score_box_stats,
    score_box_stats_by_label,
    score_histogram_counts,
)


def _sdf(spark, rows, cols):
    return spark.createDataFrame([tuple(r) for r in rows], list(cols))


class TestScoreHistogramCounts:
    def test_shared_global_bins_and_counts(self, spark):
        # Global min=0.0, max=1.0, nbins=2 -> width=0.5
        # bin(v) = min(1, floor(v/0.5)); 1.0 clamps into the top bin.
        rows = [
            ("A", 0.0), ("A", 0.4), ("A", 0.6), ("A", 1.0),
            ("B", 0.5),
        ]
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_histogram_counts(sdf, "item", "score", nbins=2)

        # bin centers for width 0.5 over [0,1]: 0.25 and 0.75
        centers = sorted(out["bin_center"].round(6).unique())
        assert centers == [0.25, 0.75]

        def cnt(item, center):
            m = out[(out["item"] == item) & (out["bin_center"].round(6) == center)]
            return int(m["count"].sum())

        assert cnt("A", 0.25) == 2   # 0.0, 0.4
        assert cnt("A", 0.75) == 2   # 0.6, 1.0
        assert cnt("B", 0.75) == 1   # 0.5 -> floor(1.0)=1 -> top bin
        assert out["bin_width"].round(6).unique().tolist() == [0.5]

    def test_constant_score_single_bin(self, spark):
        rows = [("A", 3.0), ("A", 3.0), ("A", 3.0)]
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_histogram_counts(sdf, "item", "score", nbins=4)
        # All identical -> one bin holding everything.
        assert int(out["count"].sum()) == 3
        assert len(out) == 1


class TestScoreBoxStats:
    def test_quartiles_and_clamped_fences(self, spark):
        rows = [("A", float(v)) for v in range(1, 101)]  # 1..100
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_box_stats(sdf, "item", "score").set_index("item")

        assert abs(out.loc["A", "median"] - 50.5) < 2.0
        assert out.loc["A", "q1"] <= out.loc["A", "median"] <= out.loc["A", "q3"]
        # Tukey fences fall outside the data range here, so they clamp to
        # the actual min/max (exact percentiles).
        assert out.loc["A", "lowerfence"] == 1.0
        assert out.loc["A", "upperfence"] == 100.0

    def test_one_row_per_item(self, spark):
        rows = [("A", 1.0), ("A", 2.0), ("B", 5.0), ("B", 9.0)]
        sdf = _sdf(spark, rows, ["item", "score"])
        out = score_box_stats(sdf, "item", "score")
        assert sorted(out["item"]) == ["A", "B"]
        assert set(out.columns) >= {
            "item", "q1", "median", "q3", "lowerfence", "upperfence"
        }


class TestScoreBoxStatsByLabel:
    def test_one_row_per_item_label(self, spark):
        rows = [
            ("A", 1.0, 1), ("A", 2.0, 1), ("A", 5.0, 0), ("A", 6.0, 0),
            ("B", 3.0, 1), ("B", 4.0, 0),
        ]
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = score_box_stats_by_label(sdf, "item", "score", "label")
        pairs = {(r["item"], int(r["label"])) for _, r in out.iterrows()}
        assert pairs == {("A", 1), ("A", 0), ("B", 1), ("B", 0)}


class TestRankCountMatrix:
    def test_counts_per_item_rank(self, spark):
        # 2 items, 2 customers; ranks 1..2 within each query.
        rows = [
            ("c1", "A", 1), ("c1", "B", 2),
            ("c2", "A", 2), ("c2", "B", 1),
        ]
        sdf = _sdf(spark, rows, ["cust", "item", "rank"])
        mat = rank_count_matrix(sdf, "item", "rank")
        # index = sorted items, columns = ranks 1..n_items
        assert list(mat.index) == ["A", "B"]
        assert list(mat.columns) == [1, 2]
        assert mat.loc["A", 1] == 1
        assert mat.loc["A", 2] == 1
        assert mat.loc["B", 1] == 1
        assert mat.loc["B", 2] == 1
        # Each item appears in every query exactly once -> row sums == n_queries
        assert mat.loc["A"].sum() == 2


class TestPositiveRankCountMatrix:
    def test_only_positive_rows_counted(self, spark):
        rows = [
            ("A", 1, 1), ("A", 2, 0),
            ("B", 1, 0), ("B", 2, 1), ("B", 1, 1),
        ]
        sdf = _sdf(spark, rows, ["item", "rank", "label"])
        mat = positive_rank_count_matrix(sdf, "item", "rank", "label")
        assert mat.loc["A", 1] == 1
        assert mat.loc["A", 2] == 0   # label 0, excluded
        assert mat.loc["B", 1] == 1
        assert mat.loc["B", 2] == 1
        # total counted == number of positive rows
        assert mat.values.sum() == 3


class TestPositiveRateMatrix:
    def test_rate_is_positive_over_total(self, spark):
        # Two items so the rank axis spans 1..2 (ranks 1..n_items convention).
        rows = [
            ("A", 1, 1), ("A", 1, 0), ("A", 1, 1),   # A rank1: 2/3
            ("A", 2, 0),                              # A rank2: 0/1
            ("B", 1, 1), ("B", 2, 0),                 # B present -> n_items=2
        ]
        sdf = _sdf(spark, rows, ["item", "rank", "label"])
        mat = positive_rate_matrix(sdf, "item", "rank", "label")
        assert abs(mat.loc["A", 1] - 2 / 3) < 1e-9
        assert mat.loc["A", 2] == 0.0
        assert mat.loc["B", 1] == 1.0
        assert ((mat.values >= 0.0) & (mat.values <= 1.0)).all()


class TestCalibrationBins:
    def test_bins_and_means(self, spark):
        # n_bins=5 over [0,1]; bin = min(4, floor(score*5)).
        rows = [
            ("A", 0.05, 0), ("A", 0.15, 1),   # bin0=0.05/lab0, bin0=0.15/lab1
            ("A", 0.45, 1), ("A", 0.55, 1),
            ("A", 0.95, 1),
        ]
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = calibration_bins(sdf, "item", "score", "label", n_bins=5)
        # 0.05 and 0.15 both land in bin 0 -> prob_pred=0.10, prob_true=0.5
        b0 = out[(out["item"] == "A") & (out["bin"] == 0)].iloc[0]
        assert abs(b0["prob_pred"] - 0.10) < 1e-9
        assert abs(b0["prob_true"] - 0.5) < 1e-9
        # all prob values within [0,1]
        assert ((out["prob_true"] >= 0) & (out["prob_true"] <= 1)).all()

    def test_skip_item_with_too_few_rows_or_no_positives(self, spark):
        rows = (
            [("A", 0.1 * i, 1 if i % 2 else 0) for i in range(1, 7)]  # 6 rows, has pos
            + [("B", 0.2, 1), ("B", 0.4, 0)]                          # 2 rows < n_bins
            + [("C", 0.1 * i, 0) for i in range(1, 7)]                # 6 rows, no pos
        )
        sdf = _sdf(spark, rows, ["item", "score", "label"])
        out = calibration_bins(sdf, "item", "score", "label", n_bins=5)
        items = set(out["item"])
        assert "A" in items
        assert "B" not in items   # too few rows
        assert "C" not in items   # no positives
