import math

import pytest

from recsys_tfb.tooling.sampling_suggest import suggest_ratio, suggest_weight


class TestSuggestRatio:
    def test_downsamples_negatives_to_target_ratio(self):
        # n_pos=10, n_neg=100, R=5 -> 5*10/100 = 0.5
        assert suggest_ratio(n_pos=10, n_neg=100, target_neg_pos=5) == 0.5

    def test_already_balanced_clamps_to_one(self):
        # 5*50/100 = 2.5 -> clamp 1.0
        assert suggest_ratio(n_pos=50, n_neg=100, target_neg_pos=5) == 1.0

    def test_zero_negatives_returns_one(self):
        assert suggest_ratio(n_pos=10, n_neg=0, target_neg_pos=5) == 1.0


class TestSuggestWeight:
    def test_inverse_frequency_with_sqrt_damping(self):
        # median=800, n_pos=200 -> (800/200)**0.5 = 2.0
        assert suggest_weight(n_pos=200, median_pos=800, alpha=0.5, w_max=5.0) == 2.0

    def test_hot_product_clamped_to_one(self):
        # n_pos >= median -> ratio<=1 -> clamp lower bound 1.0
        assert suggest_weight(n_pos=8000, median_pos=800, alpha=0.5, w_max=5.0) == 1.0

    def test_extreme_tail_capped_at_w_max(self):
        # (800/8)**0.5 = 10 -> cap 5.0
        assert suggest_weight(n_pos=8, median_pos=800, alpha=0.5, w_max=5.0) == 5.0

    def test_zero_pos_capped_at_w_max(self):
        assert suggest_weight(n_pos=0, median_pos=800, alpha=0.5, w_max=5.0) == 5.0
