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
    bin_areas,
    binary_summary,
    coarse_bin_table,
    item_top_k_metrics,
    macro_over_items,
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


#: The worked example's areas, as ``bin_areas`` hands them over.
_AREAS = {"pr_auc": 11 / 12, "roc_auc": 14.5 / 15}


class TestBinarySummary:
    def test_population_totals(self):
        s = binary_summary(_bins(), lo=0.0, width=0.25, areas=_AREAS)
        assert (s["n"], s["n_pos"]) == (8, 3)
        assert s["positive_rate"] == pytest.approx(3 / 8)

    def test_areas_are_the_ones_handed_in_not_recomputed(self):
        s = binary_summary(_bins(), lo=0.0, width=0.25,
                           areas={"pr_auc": 0.5, "roc_auc": float("nan")})
        assert s["pr_auc"] == 0.5
        assert s["roc_auc"] is None

    def test_best_f1_is_taken_at_a_bin_edge(self):
        s = binary_summary(_bins(), lo=0.0, width=0.25, areas=_AREAS)
        best = s["best_f1"]
        assert best["threshold"] == pytest.approx(0.25)
        assert best["f1"] == pytest.approx(6 / 7)
        assert best["precision"] == pytest.approx(0.75)
        assert best["recall"] == pytest.approx(1.0)

    def test_best_f1_is_none_without_positives(self):
        bins = pd.DataFrame({"bin": [0, 1], "n": [3, 2], "n_pos": [0, 0],
                             "score_sum": [0.3, 0.8]})
        s = binary_summary(bins, lo=0.0, width=0.5,
                           areas={"pr_auc": None, "roc_auc": None})
        assert s["best_f1"] is None
        assert s["n"] == 5

    def test_summary_is_json_native(self):
        # The payload lands as JSON; numpy scalars would not serialise.
        s = binary_summary(_bins(), lo=0.0, width=0.25, areas=_AREAS)
        for key in ("n", "n_pos"):
            assert type(s[key]) in (int, float)
        for key in ("positive_rate", "pr_auc", "roc_auc"):
            assert type(s[key]) is float
        assert all(type(v) is float for v in s["best_f1"].values())


class TestMacroOverItems:
    """``per_item.macro``: the plain mean over the items that have a value,
    with "x of y items" beside it (#541's first comment, 〈怎麼呈現〉 3-5)."""

    TABLE = pd.DataFrame({
        "item": ["A", "B", "C", "D"],
        "n_pos": [3, 1, 0, 5],
        "pr_auc": [0.9, 0.5, float("nan"), 0.4],
        # B has no negative: no ROC-AUC.
        "roc_auc": [0.8, float("nan"), float("nan"), 0.6],
    })

    def test_an_item_without_a_value_is_left_out_not_counted_as_zero(self):
        m = macro_over_items(self.TABLE, ["pr_auc", "roc_auc"],
                             min_positives=0)
        assert m["pr_auc"] == {"value": pytest.approx(0.6), "n_items": 3}
        assert m["roc_auc"] == {"value": pytest.approx(0.7), "n_items": 2}
        assert m["n_items"] == 4

    def test_min_positives_drops_items_with_fewer_positives(self):
        # min 3: only A (3) and D (5) count -> PR (0.9 + 0.4) / 2.
        m = macro_over_items(self.TABLE, ["pr_auc"], min_positives=3)
        assert m["pr_auc"] == {"value": pytest.approx(0.65), "n_items": 2}

    def test_no_item_with_a_value_gives_none(self):
        m = macro_over_items(self.TABLE, ["roc_auc"], min_positives=6)
        assert m["roc_auc"] == {"value": None, "n_items": 0}


def _spark_bins(spark, tables: dict):
    """``{item: bin table}`` as one Spark frame ``[item, bin, n, n_pos]``:
    what ``bin_areas`` reads, the shape of ``aggregate_score_bins``' cached
    ``groupBy(item, bin)``."""
    rows = [(item, int(r["bin"]), float(r["n"]), float(r["n_pos"]))
            for item, table in tables.items() for _, r in table.iterrows()]
    return spark.createDataFrame(rows, ["item", "bin", "n", "n_pos"])


def _areas_by_item(spark, tables: dict) -> dict:
    out = bin_areas(_spark_bins(spark, tables), ["item"]).toPandas()
    return out.set_index("item").to_dict("index")


class TestBinAreas:
    """``pr_auc`` / ``roc_auc`` of each item's bin table, computed in Spark:
    every item has them, and only one row per item leaves the executors."""

    def test_one_row_per_item_not_item_by_bin(self, spark):
        # Item A: the worked example's 3 bins; item B: 2 bins. A function
        # handing back item x bin would return 5 rows.
        b = pd.DataFrame({"bin": [0, 2], "n": [3, 1], "n_pos": [1, 1]})
        out = bin_areas(_spark_bins(spark, {"A": _bins(), "B": b}),
                        ["item"]).toPandas()
        assert len(out) == 2
        assert sorted(out["item"]) == ["A", "B"]
        assert list(out.columns) == ["item", "n", "n_pos", "pr_auc", "roc_auc"]

    def test_pr_auc_is_average_precision_of_the_binned_score(self, spark):
        # sum over bins of (positives in bin / P) x precision at that bin's
        # lower edge: (2/3) x 1 + (1/3) x 0.75 + 0 = 0.91666...
        # sklearn.metrics.average_precision_score([1,1,1,0,0,0,0,0],
        #                                         [3,3,1,1,0,0,0,0]) agrees.
        a = _areas_by_item(spark, {"A": _bins()})["A"]
        assert (a["n"], a["n_pos"]) == (8, 3)
        assert a["pr_auc"] == pytest.approx(11 / 12)

    def test_roc_auc_counts_a_tie_within_a_bin_as_half(self, spark):
        # 15 (positive, negative) pairs. The bin-1 negative is beaten by the
        # two bin-3 positives and tied with the bin-1 positive: 2.5. Each of
        # the four bin-0 negatives is beaten by all three positives: 12.
        # (2.5 + 12) / 15; sklearn.metrics.roc_auc_score on the bin index
        # agrees.
        a = _areas_by_item(spark, {"A": _bins()})["A"]
        assert a["roc_auc"] == pytest.approx(14.5 / 15)

    def test_each_item_is_ranked_within_its_own_bins(self, spark):
        # B alone: bin 2 holds 1 row, positive; bin 0 holds 3, one positive.
        # precision at bin 2's edge 1, at bin 0's edge 2/4 -> PR-AUC
        # (1 x 1 + 1 x 0.5) / 2 = 0.75. ROC: the two bin-0 negatives are
        # beaten by the bin-2 positive and tied with the bin-0 one: 1.5 each,
        # 3 / (2 x 2) = 0.75. Pooled with A they would come out otherwise.
        b = pd.DataFrame({"bin": [0, 2], "n": [3, 1], "n_pos": [1, 1]})
        areas = _areas_by_item(spark, {"A": _bins(), "B": b})
        assert areas["B"]["pr_auc"] == pytest.approx(0.75)
        assert areas["B"]["roc_auc"] == pytest.approx(0.75)
        assert areas["A"]["pr_auc"] == pytest.approx(11 / 12)

    def test_no_positive_leaves_both_areas_undefined(self, spark):
        bins = pd.DataFrame({"bin": [0, 1], "n": [3, 2], "n_pos": [0, 0]})
        a = _areas_by_item(spark, {"A": bins})["A"]
        assert pd.isna(a["pr_auc"]) and pd.isna(a["roc_auc"])
        assert a["n"] == 5

    def test_no_negative_leaves_roc_auc_undefined(self, spark):
        bins = pd.DataFrame({"bin": [0, 1], "n": [1, 2], "n_pos": [1, 2]})
        a = _areas_by_item(spark, {"A": bins})["A"]
        assert pd.isna(a["roc_auc"])
        assert a["pr_auc"] == pytest.approx(1.0)

    def test_no_key_column_gives_the_one_overall_row(self, spark):
        bins = _spark_bins(spark, {"A": _bins()}).drop("item")
        out = bin_areas(bins, []).toPandas()
        assert len(out) == 1
        assert out.loc[0, "pr_auc"] == pytest.approx(11 / 12)

    @pytest.mark.parametrize("seed", [0, 1])
    def test_agrees_with_sklearn_on_the_binned_score(self, spark, seed):
        """scikit-learn run on the bin index (weights as ``sample_weight``)
        is an independent answer: the areas are the binned score's exact
        values."""
        import numpy as np
        from sklearn.metrics import average_precision_score, roc_auc_score

        rng = np.random.default_rng(seed)
        bin_idx = rng.integers(0, 20, size=400)
        labels = (rng.random(400) < 0.05 + bin_idx / 40).astype(int)
        weights = rng.choice([1.0, 4.0], size=400)
        frame = pd.DataFrame({"bin": bin_idx, "n": weights,
                              "n_pos": labels * weights})
        bins = frame.groupby("bin", as_index=False)[["n", "n_pos"]].sum()

        a = _areas_by_item(spark, {"A": bins})["A"]
        assert a["pr_auc"] == pytest.approx(average_precision_score(
            labels, bin_idx, sample_weight=weights))
        assert a["roc_auc"] == pytest.approx(roc_auc_score(
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
        from recsys_tfb.evaluation.result_readers import frame_from_json

        payload = self._payload(spark)
        bins = frame_from_json(payload["overall"]["bins"])
        tbl = coarse_bin_table(bins, lo=payload["bins"]["lo"],
                               width=payload["bins"]["width"], n_bins=10,
                               n_display_bins=5)
        # 5 display bins of 0.002 from 0.001: rows at 0.001 (x2) | 0.0035 |
        # 0.0055 (x2) | none | 0.011 -> 2, 1, 2, 0, 1.
        assert list(tbl["n"]) == [2, 1, 2, 0, 1]

    def test_lands_the_display_bin_table_the_report_prints(self, spark):
        """#536: the report reads the display bins off the payload. Same
        worked example as above: 5 display bins holding 2, 1, 2, 0, 1 rows;
        item A's table holds A's 3 rows; the sweep starts at the highest
        non-empty fine bin's edge and ends at the data's minimum."""
        from recsys_tfb.evaluation.result_readers import (
            DISPLAY_BINS_KEY,
            THRESHOLD_SWEEP_KEY,
            frame_from_json,
        )

        payload = self._payload(spark)
        tbl = frame_from_json(payload["overall"][DISPLAY_BINS_KEY])
        assert list(tbl["n"]) == [2, 1, 2, 0, 1]
        items = frame_from_json(payload["per_item"][DISPLAY_BINS_KEY])
        assert set(items["item"]) == {"A", "B"}
        assert items.loc[items["item"] == "A", "n"].sum() == 3
        sweep = frame_from_json(payload["overall"][THRESHOLD_SWEEP_KEY])
        assert sweep["threshold"].iloc[-1] == pytest.approx(
            payload["bins"]["lo"])
        assert sweep["tp"].iloc[-1] + sweep["fp"].iloc[-1] == 6

    def test_headline_numbers_overall_and_per_listed_item(self, spark):
        payload = self._payload(spark)
        overall = payload["overall"]["summary"]
        assert (overall["n"], overall["n_pos"]) == (6, 2)
        assert set(payload["per_item"]["summary"]) == {"A", "B"}
        assert payload["per_item"]["summary"]["A"]["n"] == 3
        assert payload["per_item"]["n_items"] == 2

    def test_every_item_has_its_areas_and_the_macro_past_the_budget(
            self, spark):
        """``top_n`` only decides which bin tables are drawn: item B, not
        listed, still lands its areas. B's rows: a positive at 0.0035 above
        two negatives at 0.001 -> PR-AUC and ROC-AUC 1. A's: a positive at
        0.011, two negatives tied at 0.0055 -> 1 and 1."""
        from recsys_tfb.evaluation.result_readers import frame_from_json

        payload = self._payload(spark, top_n=1)
        totals = frame_from_json(
            payload["per_item"]["totals"]).set_index("item")
        assert totals.loc["B", "pr_auc"] == pytest.approx(1.0)
        assert totals.loc["B", "roc_auc"] == pytest.approx(1.0)
        macro = payload["per_item"]["macro"]
        assert macro["n_items"] == 2
        assert macro["roc_auc"] == {"value": pytest.approx(1.0), "n_items": 2}

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


class TestItemTopKMetrics:
    """Per-item precision@K and F1@K over **every** query group (#541's
    first comment): the denominator counts the times the item was ranked
    within the top K, in the groups without a positive too, each row times
    its weight.

    Worked example (group, item, rank, label, weight)::

        g1 (has a positive)   A 1 1 1 | B 2 0 1 | C 3 0 1
        g2 (has a positive)   B 1 1 1 | A 2 0 1 | C 3 1 1
        g3 (no positive, 1/r) A 1 0 2 | C 2 0 2 | B 3 0 2

    K = 1: A ranked first in g1 (1) and g3 (2) -> n_top 3, hits 1 ->
    precision 1/3, F1 2·1 / (3 + 1) = 1/2. B: n_top 1, hits 1 -> 1, F1 1.
    C: never first -> precision undefined, F1 2·0 / (0 + 1) = 0.
    K = 2: A n_top 1 + 1 + 2 = 4 -> 1/4; B 2 -> 1/2; C 2, hits 0 -> 0.
    Unweighted, A@1 would be 1/2; without g3, 1.
    """

    ROWS = [("g1", "A", 1, 1, 1.0), ("g1", "B", 2, 0, 1.0),
            ("g1", "C", 3, 0, 1.0), ("g2", "B", 1, 1, 1.0),
            ("g2", "A", 2, 0, 1.0), ("g2", "C", 3, 1, 1.0),
            ("g3", "A", 1, 0, 2.0), ("g3", "C", 2, 0, 2.0),
            ("g3", "B", 3, 0, 2.0)]

    def _out(self, spark, min_positives=0, weight_col="w"):
        sdf = spark.createDataFrame(
            self.ROWS, ["grp", "item", "rank", "label", "w"])
        return item_top_k_metrics(
            sdf, item_col="item", label_col="label", rank_col="rank",
            k_values=[1, 2], weight_col=weight_col,
            min_positives=min_positives)

    def test_precision_counts_the_groups_without_a_positive_weighted(
            self, spark):
        per_item = self._out(spark)["per_item"]
        assert per_item["A"]["precision@1"] == pytest.approx(1 / 3)
        assert per_item["B"]["precision@1"] == pytest.approx(1.0)
        assert per_item["A"]["precision@2"] == pytest.approx(1 / 4)
        assert per_item["B"]["precision@2"] == pytest.approx(1 / 2)
        assert per_item["C"]["precision@2"] == pytest.approx(0.0)

    def test_without_a_weight_every_row_counts_once(self, spark):
        per_item = self._out(spark, weight_col=None)["per_item"]
        assert per_item["A"]["precision@1"] == pytest.approx(1 / 2)

    def test_never_in_the_top_k_leaves_precision_undefined(self, spark):
        per_item = self._out(spark)["per_item"]
        assert "precision@1" not in per_item["C"]
        assert per_item["C"]["f1@1"] == pytest.approx(0.0)

    def test_f1_is_twice_hits_over_times_in_top_k_plus_positives(self, spark):
        per_item = self._out(spark)["per_item"]
        assert per_item["A"]["f1@1"] == pytest.approx(1 / 2)
        assert per_item["B"]["f1@1"] == pytest.approx(1.0)
        # A@2: hits 1, n_top 4, n_pos 1 -> 2 / 5.
        assert per_item["A"]["f1@2"] == pytest.approx(2 / 5)

    def test_lands_the_counts_beside_the_metrics(self, spark):
        shared = self._out(spark)["shared_numbers"]
        assert shared["A"]["n_top@1"] == pytest.approx(3.0)
        assert shared["A"]["hits@1"] == pytest.approx(1.0)
        assert shared["C"]["n_pos"] == pytest.approx(1.0)

    def test_macro_over_items_with_a_value(self, spark):
        macro = self._out(spark)["macro"]
        # precision@1: A 1/3 and B 1; C has none -> 2 of 3 items.
        assert macro["precision@1"] == {"value": pytest.approx(2 / 3),
                                        "n_items": 2}
        assert macro["f1@1"] == {"value": pytest.approx(1 / 2),
                                 "n_items": 3}
        assert macro["n_items"] == 3

    def test_macro_follows_min_positives(self, spark):
        # Every item holds one positive: min 2 leaves none.
        macro = self._out(spark, min_positives=2)["macro"]
        assert macro["precision@1"] == {"value": None, "n_items": 0}
