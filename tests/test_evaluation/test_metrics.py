"""Tests for evaluation.metrics — numpy-only HPO primitives.

Scope: only ``compute_ap`` + ``compute_mean_ap``. The dict-shaped
``compute_all_metrics`` and all per-dimension helpers have moved to
``recsys_tfb.evaluation.metrics_spark``; see ``test_metrics_spark.py``.
"""

import numpy as np
import pytest

from recsys_tfb.evaluation.metrics import compute_ap, compute_mean_ap


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
