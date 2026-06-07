import numpy as np
from recsys_tfb.models.composite_train import assign_folds, oof_is_leakage_clean


def test_assign_folds_is_customer_disjoint_and_deterministic():
    custs = np.array(["c1", "c2", "c3", "c1", "c2"])  # c1,c2 repeat
    f1 = assign_folds(custs, n_folds=3, seed=7)
    f2 = assign_folds(custs, n_folds=3, seed=7)
    np.testing.assert_array_equal(f1, f2)            # deterministic
    # same customer -> same fold regardless of row
    assert f1[0] == f1[3]  # both c1
    assert f1[1] == f1[4]  # both c2
    assert set(f1.tolist()) <= {0, 1, 2}


def test_oof_clean_guard():
    # Each row must be scored by the booster of its OWN held-out fold, i.e.
    # producing_fold[i] == folds[i] for every row. Otherwise the scoring booster
    # trained on row i's fold -> leakage.
    folds = np.array([0, 1, 2, 0, 1])
    assert oof_is_leakage_clean(folds, producing_fold=folds.copy())          # clean
    dirty = np.array([1, 1, 2, 0, 1])  # row0 scored by fold1's booster
    assert not oof_is_leakage_clean(folds, producing_fold=dirty)
