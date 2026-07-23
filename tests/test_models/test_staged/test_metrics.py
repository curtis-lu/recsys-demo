import numpy as np
import pytest

from recsys_tfb.models.staged._metrics import binary_auc, binary_logloss


class TestBinaryAuc:
    def test_perfect_separation_is_one(self):
        y = np.array([0, 0, 1, 1])
        s = np.array([0.1, 0.2, 0.8, 0.9])
        assert binary_auc(y, s) == 1.0

    def test_reversed_is_zero(self):
        y = np.array([0, 0, 1, 1])
        s = np.array([0.9, 0.8, 0.2, 0.1])
        assert binary_auc(y, s) == 0.0

    def test_ties_average_rank(self):
        # 一正一負同分：AUC = 0.5（平手貢獻 0.5）
        y = np.array([0, 1])
        s = np.array([0.5, 0.5])
        assert binary_auc(y, s) == 0.5

    def test_single_class_returns_nan(self):
        assert np.isnan(binary_auc(np.array([1, 1]), np.array([0.2, 0.8])))

    def test_matches_bruteforce_pair_count(self):
        rng = np.random.default_rng(7)
        y = (rng.random(200) < 0.3).astype(int)
        s = rng.random(200)
        pos, neg = s[y == 1], s[y == 0]
        wins = (pos[:, None] > neg[None, :]).sum()
        ties = (pos[:, None] == neg[None, :]).sum()
        expected = (wins + 0.5 * ties) / (len(pos) * len(neg))
        assert binary_auc(y, s) == pytest.approx(expected, abs=1e-12)


class TestBinaryLogloss:
    def test_perfect_prediction_near_zero(self):
        y = np.array([0, 1])
        s = np.array([1e-9, 1 - 1e-9])
        assert binary_logloss(y, s) < 1e-6

    def test_uniform_prediction_is_log2(self):
        y = np.array([0, 1, 0, 1])
        s = np.full(4, 0.5)
        assert binary_logloss(y, s) == pytest.approx(np.log(2))

    def test_clips_extreme_scores(self):
        y = np.array([1])
        s = np.array([0.0])  # 未 clip 會是 inf
        assert np.isfinite(binary_logloss(y, s))
