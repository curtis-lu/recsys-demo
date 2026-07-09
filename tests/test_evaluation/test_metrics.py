"""Tests for evaluation.metrics — numpy-only HPO primitives.

Scope: only ``compute_ap`` + ``compute_mean_ap``. The dict-shaped
``compute_all_metrics`` and all per-dimension helpers have moved to
``recsys_tfb.evaluation.metrics_spark``; see ``test_metrics_spark.py``.
"""

import numpy as np
import pytest

from recsys_tfb.evaluation.metrics import (
    compute_ap,
    compute_macro_per_item_map,
    compute_mean_ap,
    macro_from_per_item,
)


class TestComputeAP:
    def test_known_values(self):
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        ap = compute_ap(y_true, y_score)
        # Precision at pos 1: 1/1=1.0, pos 3: 2/3
        # AP = (1.0 + 2/3) / 2 = 5/6
        assert ap == pytest.approx(5 / 6)

    def test_no_positives(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) is None

    def test_all_positives(self):
        y_true = np.array([1, 1, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        assert compute_ap(y_true, y_score) == pytest.approx(1.0)

    def test_worst_case(self):
        y_true = np.array([0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7])
        # Only positive at rank 3: precision@3 = 1/3
        assert compute_ap(y_true, y_score) == pytest.approx(1 / 3)


class TestComputeMeanAP:
    def test_two_groups_mixed(self):
        # group 0: y=[1,0,1,0], score=[0.9,0.8,0.7,0.6] → AP = 5/6
        # group 1: y=[0,0,1], score=[0.9,0.8,0.7] → AP = 1/3
        groups = np.array([0, 0, 0, 0, 1, 1, 1])
        y_true = np.array([1, 0, 1, 0, 0, 0, 1])
        y_score = np.array([0.9, 0.8, 0.7, 0.6, 0.9, 0.8, 0.7])
        expected = (5 / 6 + 1 / 3) / 2
        assert compute_mean_ap(groups, y_true, y_score) == pytest.approx(expected)

    def test_skips_no_positive_group(self):
        # group 0: AP = 1.0  (single positive at top)
        # group 1: no positives → skipped
        # group 2: AP = 1.0  (single positive at top)
        groups = np.array([0, 0, 1, 1, 2, 2])
        y_true = np.array([1, 0, 0, 0, 1, 0])
        y_score = np.array([0.9, 0.1, 0.5, 0.4, 0.9, 0.1])
        assert compute_mean_ap(groups, y_true, y_score) == pytest.approx(1.0)

    def test_all_no_positive_returns_zero(self):
        groups = np.array([0, 0, 1, 1])
        y_true = np.array([0, 0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mean_ap(groups, y_true, y_score) == 0.0

    def test_single_group_equals_single_ap(self):
        groups = np.array([7, 7, 7, 7])
        y_true = np.array([1, 0, 1, 0])
        y_score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_mean_ap(groups, y_true, y_score) == pytest.approx(
            compute_ap(y_true, y_score)
        )

    def test_empty_inputs_return_zero(self):
        groups = np.array([], dtype=np.int64)
        y_true = np.array([], dtype=np.int64)
        y_score = np.array([], dtype=np.float64)
        assert compute_mean_ap(groups, y_true, y_score) == 0.0

    def test_stable_tie_breaking_within_group(self):
        """When y_score has ties within a group, the lexsort-based impl uses
        stable order — input position breaks the tie. This locks down the
        new invariant that the same input ALWAYS produces the same AP
        (the old quicksort-based impl was implementation-defined on ties).

        Group 0 layout (input already in score-desc; tied pairs):
          pos 0: score=0.9, y=1
          pos 1: score=0.9, y=0     (tied with pos 0; stable keeps it after)
          pos 2: score=0.5, y=1
          pos 3: score=0.5, y=0     (tied with pos 2; stable keeps it after)

        sorted y = [1, 0, 1, 0]
        cumsum   = [1, 1, 2, 2]
        pos      = [1, 2, 3, 4]
        precisions = [1.0, 0.5, 2/3, 0.5]
        AP = (1*1 + 0*0.5 + 1*2/3 + 0*0.5) / 2 = (1 + 2/3) / 2 = 5/6
        """
        groups = np.array([0, 0, 0, 0])
        y_score = np.array([0.9, 0.9, 0.5, 0.5])
        y_true = np.array([1, 0, 1, 0])
        assert compute_mean_ap(groups, y_true, y_score) == pytest.approx(5 / 6)

    def test_groups_unsorted_input(self):
        """Group ids in input do not need to be contiguous or sorted — the
        impl re-orders internally. Interleaved (group, score) input must
        yield the same AP as the contiguous-group form."""
        # Same data as test_two_groups_mixed but interleaved by group.
        # group 0 rows: y=[1,0,1,0], score=[0.9,0.8,0.7,0.6] → AP = 5/6
        # group 1 rows: y=[0,0,1],   score=[0.9,0.8,0.7]    → AP = 1/3
        groups = np.array([0, 1, 0, 1, 0, 1, 0])
        y_true = np.array([1, 0, 0, 0, 1, 1, 0])
        y_score = np.array([0.9, 0.9, 0.8, 0.8, 0.7, 0.7, 0.6])
        expected = (5 / 6 + 1 / 3) / 2
        assert compute_mean_ap(groups, y_true, y_score) == pytest.approx(expected)

    def test_random_many_groups_matches_naive_reference(self):
        """Random multi-group correctness check at moderate scale.

        Verifies the vectorized impl matches a slow per-group naive impl on
        random inputs with **distinct** scores (so tie-breaking is moot)."""
        rng = np.random.default_rng(42)
        n_groups = 200
        group_sizes = rng.integers(5, 30, size=n_groups)
        groups = np.repeat(np.arange(n_groups), group_sizes)
        n_rows = int(groups.shape[0])
        y_true = rng.integers(0, 2, size=n_rows).astype(np.int64)
        y_score = rng.uniform(size=n_rows)  # distinct floats — no ties

        expected_aps: list[float] = []
        for g in np.unique(groups):
            mask = groups == g
            y_g, s_g = y_true[mask], y_score[mask]
            if y_g.sum() == 0:
                continue
            order = np.argsort(-s_g)
            y_sorted = y_g[order]
            cumsum = np.cumsum(y_sorted)
            pos = np.arange(1, len(y_sorted) + 1)
            precisions = cumsum / pos
            expected_aps.append(
                float(np.sum(precisions * y_sorted) / np.sum(y_g))
            )
        expected = float(np.mean(expected_aps)) if expected_aps else 0.0

        actual = compute_mean_ap(groups, y_true, y_score)
        assert actual == pytest.approx(expected, rel=1e-12)


class TestComputeMacroPerItemMap:
    # Two customers, three products (mirrors the metrics_spark
    # _two_customer_raw fixture so the parity test shares the math):
    #   C0: A(0.9,1) B(0.5,0) C(0.1,1)  ranking A,B,C -> prec A=1.0, C=2/3
    #   C1: B(0.8,1) C(0.6,0) A(0.3,0)  ranking B,C,A -> prec B=1.0
    # per-item map_attr@all: A=1.0, B=1.0, C=2/3
    # macro = (1.0 + 1.0 + 2/3) / 3 = 8/9
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_full_map_macro_over_items(self):
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(8 / 9)

    def test_k_truncation_zeros_contrib_beyond_k(self):
        # k=1: C0 A pos1 -> 1.0, C pos3 -> 0.0 ; C1 B pos1 -> 1.0
        # per-item: A=1.0, B=1.0, C=0.0 ; macro = 2/3
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, k=1
        )
        assert result == pytest.approx(2 / 3)

    def test_skips_group_with_no_positives(self):
        # group 2 has no positives -> contributes nothing; A and C each 1.0
        groups = np.array([0, 0, 1, 1, 2, 2])
        items = np.array(["A", "C", "A", "C", "A", "C"])
        y = np.array([1, 0, 0, 1, 0, 0])
        score = np.array([0.9, 0.1, 0.4, 0.5, 0.9, 0.1])
        # C0: A(0.9,1) C(0.1,0) -> A prec 1.0 ; C1: C(0.5,1) A(0.4,0) -> C prec 1.0
        # per-item: A=1.0, C=1.0 ; macro = 1.0
        result = compute_macro_per_item_map(groups, items, y, score)
        assert result == pytest.approx(1.0)

    def test_all_no_positives_returns_zero(self):
        groups = np.array([0, 0, 1, 1])
        items = np.array(["A", "B", "A", "B"])
        y = np.array([0, 0, 0, 0])
        score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_macro_per_item_map(groups, items, y, score) == 0.0

    def test_empty_inputs_return_zero(self):
        empty = np.array([], dtype=np.int64)
        assert (
            compute_macro_per_item_map(
                empty, np.array([]), empty, np.array([], dtype=np.float64)
            )
            == 0.0
        )


class TestMacroFromPerItem:
    # 兩個 item：A 值 0.75、n=2；B 值 1.0、n=1。pooled = (2*0.75+1*1.0)/3 = 5/6
    VALUES = np.array([0.75, 1.0])
    N_POS = np.array([2, 1])

    def test_defaults_equal_plain_mean(self):
        assert macro_from_per_item(self.VALUES, self.N_POS) == pytest.approx(0.875)

    def test_weight_alpha_one_weights_by_n_pos(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, weight_alpha=1.0)
        assert r == pytest.approx(5 / 6)

    def test_min_positives_drops_cold_item(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, min_positives=2)
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_none(self):
        assert macro_from_per_item(self.VALUES, self.N_POS, min_positives=3) is None

    def test_shrinkage_known_value(self):
        # pooled=5/6；A'=(2*0.75+5/6)/3=7/9；B'=(1.0+5/6)/2=11/12；mean=61/72
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1.0)
        assert r == pytest.approx(61 / 72)

    def test_shrinkage_large_k_approaches_pooled(self):
        r = macro_from_per_item(self.VALUES, self.N_POS, shrinkage_k=1e9)
        assert r == pytest.approx(5 / 6, abs=1e-6)


class TestComputeMacroPerItemMapParams:
    # 3 queries、2 items。A 正例 2 列（contrib 1.0、0.5 → AP 0.75）、B 1 列（1.0）
    GROUPS = np.array([0, 0, 1, 1, 2, 2])
    ITEMS = np.array(["A", "B", "A", "B", "A", "B"])
    Y = np.array([1, 0, 1, 0, 0, 1])
    SCORE = np.array([0.9, 0.1, 0.1, 0.9, 0.1, 0.9])

    def test_defaults_unchanged(self):
        r = compute_macro_per_item_map(self.GROUPS, self.ITEMS, self.Y, self.SCORE)
        assert r == pytest.approx(0.875)

    def test_weight_alpha(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, weight_alpha=1.0
        )
        assert r == pytest.approx(5 / 6)

    def test_min_positives(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=2
        )
        assert r == pytest.approx(0.75)

    def test_min_positives_all_excluded_returns_zero(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, min_positives=3
        )
        assert r == 0.0

    def test_shrinkage(self):
        r = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, shrinkage_k=1.0
        )
        assert r == pytest.approx(61 / 72)
