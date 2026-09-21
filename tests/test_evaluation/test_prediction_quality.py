"""Tests for evaluation.prediction_quality — the binary-prediction metric family
(ADR-0024): one ``groupBy(item, bin)`` over every evaluated row, and every
number the report shows derived from that one bin table.

The worked example used by the pure-function tests (4 fine bins over
[0, 1), width 0.25; bin 2 empty)::

    bin  rows  positives  score_sum
     0     4       0        0.4      (four 0.1s)
     1     2       1        0.8      (two 0.4s)
     3     2       2        1.8      (two 0.9s)

    P = 3 positives, N = 5 negatives, 8 rows.

Every expected value below is worked out by hand from that table, not from
the code under test; the sklearn cross-check in the comments uses the bin
index as the score (ties = one bin), which is what "binned" means here.
"""

import math

import pandas as pd
import pytest

from recsys_tfb.evaluation.prediction_quality import (
    aggregate_score_bins,
    binary_summary,
    coarse_bin_table,
    threshold_sweep,
)


def _bins():
    return pd.DataFrame(
        {"bin": [0, 1, 3], "n": [4, 2, 2], "n_pos": [0, 1, 2],
         "score_sum": [0.4, 0.8, 1.8]}
    )


class TestThresholdSweep:
    def test_one_row_per_non_empty_bin_highest_threshold_first(self):
        sweep = threshold_sweep(_bins(), lo=0.0, width=0.25)
        assert list(sweep["bin"]) == [3, 1, 0]
        assert list(sweep["threshold"]) == pytest.approx([0.75, 0.25, 0.0])

    def test_counts_at_each_bin_edge(self):
        sweep = threshold_sweep(_bins(), lo=0.0, width=0.25).set_index("bin")
        # threshold 0.75 -> bins >= 3 predicted positive: 2 rows, both positive
        assert (sweep.loc[3, "tp"], sweep.loc[3, "fp"],
                sweep.loc[3, "fn"], sweep.loc[3, "tn"]) == (2, 0, 1, 5)
        # threshold 0.25 -> bins >= 1: 4 rows, 3 positive
        assert (sweep.loc[1, "tp"], sweep.loc[1, "fp"],
                sweep.loc[1, "fn"], sweep.loc[1, "tn"]) == (3, 1, 0, 4)
        # threshold 0.0 -> everything predicted positive
        assert (sweep.loc[0, "tp"], sweep.loc[0, "fp"],
                sweep.loc[0, "fn"], sweep.loc[0, "tn"]) == (3, 5, 0, 0)

    def test_precision_recall_f1_at_each_bin_edge(self):
        sweep = threshold_sweep(_bins(), lo=0.0, width=0.25).set_index("bin")
        assert sweep.loc[3, "precision"] == pytest.approx(1.0)
        assert sweep.loc[3, "recall"] == pytest.approx(2 / 3)
        assert sweep.loc[3, "f1"] == pytest.approx(0.8)
        assert sweep.loc[1, "precision"] == pytest.approx(0.75)
        assert sweep.loc[1, "recall"] == pytest.approx(1.0)
        assert sweep.loc[1, "f1"] == pytest.approx(6 / 7)
        assert sweep.loc[0, "precision"] == pytest.approx(3 / 8)
        assert sweep.loc[0, "f1"] == pytest.approx(6 / 11)

    def test_no_positive_leaves_recall_undefined_not_zero(self):
        bins = pd.DataFrame({"bin": [0, 1], "n": [3, 2], "n_pos": [0, 0],
                             "score_sum": [0.3, 0.8]})
        sweep = threshold_sweep(bins, lo=0.0, width=0.5)
        assert sweep["recall"].isna().all()
        assert sweep["f1"].isna().all()
        assert list(sweep["precision"]) == pytest.approx([0.0, 0.0])


class TestBinarySummary:
    def test_population_totals(self):
        s = binary_summary(_bins(), lo=0.0, width=0.25)
        assert (s["n"], s["n_pos"]) == (8, 3)
        assert s["positive_rate"] == pytest.approx(3 / 8)

    def test_pr_auc_is_average_precision_of_the_binned_score(self):
        # sum over bins of (positives in bin / P) x precision at that bin's
        # lower edge: (2/3) x 1 + (1/3) x 0.75 + 0 = 0.91666...
        # sklearn.metrics.average_precision_score([1,1,1,0,0,0,0,0],
        #                                         [3,3,1,1,0,0,0,0]) agrees.
        s = binary_summary(_bins(), lo=0.0, width=0.25)
        assert s["pr_auc"] == pytest.approx(11 / 12)

    def test_roc_auc_counts_a_tie_within_a_bin_as_half(self):
        # 15 (positive, negative) pairs. The bin-1 negative is beaten by the
        # two bin-3 positives and tied with the bin-1 positive: 2.5. Each of
        # the four bin-0 negatives is beaten by all three positives: 12.
        # (2.5 + 12) / 15; sklearn.metrics.roc_auc_score on the bin index
        # agrees.
        s = binary_summary(_bins(), lo=0.0, width=0.25)
        assert s["roc_auc"] == pytest.approx(14.5 / 15)

    def test_best_f1_is_taken_at_a_bin_edge(self):
        s = binary_summary(_bins(), lo=0.0, width=0.25)
        best = s["best_f1"]
        assert best["threshold"] == pytest.approx(0.25)
        assert best["f1"] == pytest.approx(6 / 7)
        assert best["precision"] == pytest.approx(0.75)
        assert best["recall"] == pytest.approx(1.0)

    def test_undefined_metrics_are_none_without_positives(self):
        bins = pd.DataFrame({"bin": [0, 1], "n": [3, 2], "n_pos": [0, 0],
                             "score_sum": [0.3, 0.8]})
        s = binary_summary(bins, lo=0.0, width=0.5)
        assert s["pr_auc"] is None
        assert s["roc_auc"] is None
        assert s["best_f1"] is None
        assert s["n"] == 5

    def test_roc_auc_undefined_without_negatives(self):
        bins = pd.DataFrame({"bin": [0, 1], "n": [1, 2], "n_pos": [1, 2],
                             "score_sum": [0.3, 1.6]})
        s = binary_summary(bins, lo=0.0, width=0.5)
        assert s["roc_auc"] is None
        assert s["pr_auc"] == pytest.approx(1.0)

    def test_summary_is_json_native(self):
        # The payload lands as JSON; numpy scalars would not serialise.
        s = binary_summary(_bins(), lo=0.0, width=0.25)
        for key in ("n", "n_pos"):
            assert type(s[key]) in (int, float)
        for key in ("positive_rate", "pr_auc", "roc_auc"):
            assert type(s[key]) is float
        assert all(type(v) is float for v in s["best_f1"].values())


class TestAgreesWithSklearnOnTheBinnedScore:
    """``pr_auc`` / ``roc_auc`` are the exact values of the binned score, so
    scikit-learn run on the bin index (weights as ``sample_weight``) is an
    independent answer to check them against."""

    @pytest.mark.parametrize("seed", [0, 1, 2])
    def test_random_bins(self, seed):
        import numpy as np
        from sklearn.metrics import average_precision_score, roc_auc_score

        rng = np.random.default_rng(seed)
        bin_idx = rng.integers(0, 20, size=400)
        labels = (rng.random(400) < 0.05 + bin_idx / 40).astype(int)
        weights = rng.choice([1.0, 4.0], size=400)
        frame = pd.DataFrame({"bin": bin_idx, "n": weights,
                              "n_pos": labels * weights,
                              "score_sum": bin_idx * weights})
        bins = frame.groupby("bin", as_index=False)[
            ["n", "n_pos", "score_sum"]].sum()

        s = binary_summary(bins, lo=0.0, width=1.0)
        assert s["pr_auc"] == pytest.approx(average_precision_score(
            labels, bin_idx, sample_weight=weights))
        assert s["roc_auc"] == pytest.approx(roc_auc_score(
            labels, bin_idx, sample_weight=weights))


class TestCoarseBinTable:
    def test_fine_bins_merge_into_equal_width_display_bins(self):
        # 4 fine bins -> 2 display bins of 2 fine bins each (width 0.5).
        tbl = coarse_bin_table(_bins(), lo=0.0, width=0.25, n_bins=4,
                               n_display_bins=2)
        assert list(tbl["score_from"]) == pytest.approx([0.0, 0.5])
        assert list(tbl["score_to"]) == pytest.approx([0.5, 1.0])
        assert list(tbl["n"]) == [6, 2]
        assert list(tbl["n_pos"]) == [1, 2]

    def test_mean_score_and_positive_rate_per_display_bin(self):
        tbl = coarse_bin_table(_bins(), lo=0.0, width=0.25, n_bins=4,
                               n_display_bins=2)
        # (0.4 + 0.8) / 6 and 1.8 / 2
        assert list(tbl["mean_score"]) == pytest.approx([0.2, 0.9])
        assert list(tbl["positive_rate"]) == pytest.approx([1 / 6, 1.0])

    def test_threshold_columns_read_the_same_sweep_at_the_lower_edge(self):
        tbl = coarse_bin_table(_bins(), lo=0.0, width=0.25, n_bins=4,
                               n_display_bins=2)
        # lower edge 0.5 == fine edge 2: bins >= 2 -> the two 0.9 rows
        assert tbl.loc[1, "precision"] == pytest.approx(1.0)
        assert tbl.loc[1, "recall"] == pytest.approx(2 / 3)
        assert tbl.loc[1, "f1"] == pytest.approx(0.8)
        assert tbl.loc[0, "precision"] == pytest.approx(3 / 8)
        assert tbl.loc[0, "recall"] == pytest.approx(1.0)

    def test_empty_display_bin_is_kept_with_no_rate(self):
        bins = pd.DataFrame({"bin": [0, 3], "n": [2, 2], "n_pos": [0, 1],
                             "score_sum": [0.2, 1.8]})
        tbl = coarse_bin_table(bins, lo=0.0, width=0.25, n_bins=4,
                               n_display_bins=4)
        assert list(tbl["n"]) == [2, 0, 0, 2]
        assert math.isnan(tbl.loc[1, "mean_score"])
        assert math.isnan(tbl.loc[1, "positive_rate"])


def _frame(spark, rows, weight=False):
    cols = ["cust_id", "item", "score", "label"] + (["w"] if weight else [])
    return spark.createDataFrame([tuple(r) for r in rows], cols)


class TestAggregateScoreBins:
    """The one Spark pass: global equal-width bins over this data's
    ``min(score)``..``max(score)``, grouped by (item, bin)."""

    # Scores squeezed into 0.001..0.011, the way low-rate click scores are:
    # ten equal-width bins of 0.001. Bins: 0.001 -> 0, 0.0035 -> 2,
    # 0.0055 -> 4, 0.011 (the max) -> 9 (clamped into the top bin).
    ROWS = [
        ("c1", "A", 0.011, 1),
        ("c1", "B", 0.001, 0),
        ("c2", "A", 0.0055, 0),
        ("c2", "B", 0.0035, 1),
        # c3's query group has no positive at all: still counted.
        ("c3", "A", 0.0055, 0),
        ("c3", "B", 0.001, 0),
    ]

    def test_bin_range_is_this_datas_min_and_max_not_zero_to_one(self, spark):
        out = aggregate_score_bins(
            _frame(spark, self.ROWS), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=5)
        assert out["lo"] == pytest.approx(0.001)
        assert out["hi"] == pytest.approx(0.011)
        assert out["width"] == pytest.approx(0.001)

    def test_overall_bins_count_rows_positives_and_score_sum(self, spark):
        out = aggregate_score_bins(
            _frame(spark, self.ROWS), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=5)
        overall = out["overall"].set_index("bin")
        assert sorted(overall.index) == [0, 2, 4, 9]
        assert overall.loc[0, "n"] == 2 and overall.loc[0, "n_pos"] == 0
        assert overall.loc[4, "n"] == 2 and overall.loc[4, "n_pos"] == 0
        assert overall.loc[9, "n"] == 1 and overall.loc[9, "n_pos"] == 1
        assert overall.loc[2, "n_pos"] == 1
        assert overall.loc[4, "score_sum"] == pytest.approx(0.011)
        assert overall.loc[0, "score_sum"] == pytest.approx(0.002)

    def test_zero_positive_query_group_is_in_the_denominator(self, spark):
        # c3 has no positive; a `total_rel > 0` filter would drop its two
        # rows and leave 4.
        out = aggregate_score_bins(
            _frame(spark, self.ROWS), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=5)
        assert out["overall"]["n"].sum() == 6

    def test_per_item_bins_add_up_to_the_overall_bins(self, spark):
        out = aggregate_score_bins(
            _frame(spark, self.ROWS), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=5)
        per_item = out["per_item"].set_index(["item", "bin"])
        assert per_item.loc[("A", 4), "n"] == 2
        assert per_item.loc[("B", 0), "n"] == 2
        summed = out["per_item"].groupby("bin")[["n", "n_pos"]].sum()
        overall = out["overall"].set_index("bin")[["n", "n_pos"]]
        pd.testing.assert_frame_equal(
            summed.sort_index(), overall.sort_index(), check_dtype=False)

    def test_weight_column_multiplies_every_sum(self, spark):
        # c1/A, the positive in bin 9, weighs 2; c3's rows weigh 4.
        rows = [r + (w,) for r, w in zip(self.ROWS, [2.0, 1.0, 1.0, 1.0,
                                                      4.0, 4.0])]
        out = aggregate_score_bins(
            _frame(spark, rows, weight=True), item_col="item",
            score_col="score", label_col="label", n_bins=10, top_n=5,
            weight_col="w")
        overall = out["overall"].set_index("bin")
        # bin 4: c2/A (w 1) + c3/A (w 4)
        assert overall.loc[4, "n"] == pytest.approx(5.0)
        assert overall.loc[4, "score_sum"] == pytest.approx(0.0055 * 5)
        # bin 9: one positive of weight 2
        assert overall.loc[9, "n_pos"] == pytest.approx(2.0)
        assert out["overall"]["n"].sum() == pytest.approx(13.0)

    def test_top_n_keeps_the_items_with_most_rows(self, spark):
        rows = self.ROWS + [("c4", "B", 0.002, 0)]
        out = aggregate_score_bins(
            _frame(spark, rows), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=1)
        assert out["listed_items"] == ["B"]
        assert set(out["per_item"]["item"]) == {"B"}
        # Every item's totals are kept, listed or not.
        totals = out["item_totals"].set_index("item")
        assert totals.loc["A", "n"] == 3 and totals.loc["B", "n"] == 4
        # The overall bins still hold item A's rows.
        assert out["overall"]["n"].sum() == 7

    def test_top_n_zero_lists_no_item(self, spark):
        out = aggregate_score_bins(
            _frame(spark, self.ROWS), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=0)
        assert out["listed_items"] == []
        assert out["per_item"].empty
        assert out["overall"]["n"].sum() == 6

    def test_empty_frame_gives_empty_tables(self, spark):
        sdf = _frame(spark, self.ROWS).filter("score < 0")
        out = aggregate_score_bins(
            sdf, item_col="item", score_col="score", label_col="label",
            n_bins=10, top_n=5)
        assert out["lo"] is None and out["overall"].empty

    def test_identical_scores_fall_into_one_bin(self, spark):
        rows = [("c1", "A", 0.5, 1), ("c1", "B", 0.5, 0)]
        out = aggregate_score_bins(
            _frame(spark, rows), item_col="item", score_col="score",
            label_col="label", n_bins=10, top_n=5)
        assert out["width"] == 0.0
        assert list(out["overall"]["bin"]) == [0]
        assert out["overall"]["n"].sum() == 2


class TestBuildPayload:
    """What lands as ``prediction_quality.json``: the fine bin tables the
    report re-derives from, plus the headline numbers computed from them."""

    def _payload(self, spark, top_n=5):
        from recsys_tfb.evaluation.prediction_quality import build_payload

        agg = aggregate_score_bins(
            _frame(spark, TestAggregateScoreBins.ROWS), item_col="item",
            score_col="score", label_col="label", n_bins=10, top_n=top_n)
        return build_payload(
            agg, item_col="item", score_col="score", label_col="label",
            weight_col=None, n_bins=10, n_display_bins=5, top_n=top_n)

    def test_is_strict_json(self, spark):
        import json

        json.dumps(self._payload(spark), allow_nan=False)

    def test_bin_tables_round_trip_to_the_same_display_table(self, spark):
        from recsys_tfb.evaluation.diagnostics_spark import frame_from_json

        payload = self._payload(spark)
        bins = frame_from_json(payload["overall"]["bins"])
        tbl = coarse_bin_table(bins, lo=payload["bins"]["lo"],
                               width=payload["bins"]["width"], n_bins=10,
                               n_display_bins=5)
        # 5 display bins of 0.002 from 0.001: rows at 0.001 (x2) | 0.0035 |
        # 0.0055 (x2) | none | 0.011 -> 2, 1, 2, 0, 1.
        assert list(tbl["n"]) == [2, 1, 2, 0, 1]

    def test_headline_numbers_overall_and_per_listed_item(self, spark):
        payload = self._payload(spark)
        overall = payload["overall"]["summary"]
        assert (overall["n"], overall["n_pos"]) == (6, 2)
        assert set(payload["per_item"]["summary"]) == {"A", "B"}
        assert payload["per_item"]["summary"]["A"]["n"] == 3
        assert payload["per_item"]["n_items"] == 2

    def test_items_past_the_budget_are_counted_not_listed(self, spark):
        payload = self._payload(spark, top_n=1)
        assert payload["per_item"]["listed"] == ["A"]
        assert set(payload["per_item"]["summary"]) == {"A"}
        assert payload["per_item"]["n_items"] == 2


class TestDenseSweep:
    def test_every_edge_gets_a_row_and_an_empty_bin_reads_the_one_above(self):
        """``n_bins`` given: bin 2 is empty, so its edge predicts exactly what
        bin 3's edge does."""
        sweep = threshold_sweep(_bins(), lo=0.0, width=0.25, n_bins=4)
        assert list(sweep["bin"]) == [3, 2, 1, 0]
        by_bin = sweep.set_index("bin")
        for col in ("tp", "fp", "precision", "recall", "f1"):
            assert by_bin.loc[2, col] == pytest.approx(by_bin.loc[3, col])
        assert by_bin.loc[2, "n"] == 0
