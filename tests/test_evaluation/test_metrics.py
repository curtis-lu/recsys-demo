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
