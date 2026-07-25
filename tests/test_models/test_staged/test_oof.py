import numpy as np
import pytest

from recsys_tfb.models.staged.oof import assign_folds, oof_is_leakage_clean


class TestAssignFolds:
    def test_deterministic_and_in_range(self):
        ids = np.array([f"c{i}" for i in range(500)], dtype=object)
        a = assign_folds(ids, n_folds=5, seed=42)
        b = assign_folds(ids, n_folds=5, seed=42)
        np.testing.assert_array_equal(a, b)
        assert a.dtype == np.int64
        assert set(np.unique(a)) <= set(range(5))

    def test_entity_disjoint(self):
        # 同一 entity 的多列必落同折
        ids = np.array(["e1", "e2", "e1", "e3", "e2", "e1"], dtype=object)
        f = assign_folds(ids, n_folds=4, seed=7)
        assert f[0] == f[2] == f[5]
        assert f[1] == f[4]

    def test_seed_changes_assignment(self):
        ids = np.array([f"c{i}" for i in range(200)], dtype=object)
        assert not np.array_equal(assign_folds(ids, 5, seed=1),
                                  assign_folds(ids, 5, seed=2))

    def test_reasonably_balanced(self):
        ids = np.array([f"cust_{i}" for i in range(2000)], dtype=object)
        f = assign_folds(ids, n_folds=5, seed=42)
        counts = np.bincount(f, minlength=5)
        assert counts.min() > 0.5 * (2000 / 5)  # crc32 均勻性的寬鬆下界


class TestLeakageClean:
    def test_clean(self):
        folds = np.array([0, 1, 2, 0, 1])
        assert oof_is_leakage_clean(folds, folds.copy())

    def test_dirty_one_row(self):
        folds = np.array([0, 1, 2, 0, 1])
        producing = folds.copy()
        producing[3] = 1  # 這列被非自己折的模型評分
        assert not oof_is_leakage_clean(folds, producing)

    def test_length_mismatch_is_dirty(self):
        assert not oof_is_leakage_clean(np.array([0, 1]), np.array([0]))
