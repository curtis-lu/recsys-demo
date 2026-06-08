import numpy as np
import pandas as pd
from recsys_tfb.io.handles import ParquetHandle
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


def _write_handle(tmp_path, name, n_cust, items, rng):
    rows = []
    for c in range(n_cust):
        for it in items:
            rows.append({
                "snap_date": "2025-01-31", "cust_id": f"c{c}", "prod_name": it,
                "f0": rng.rand(), "f1": rng.rand(),
                "label": int(rng.rand() < (0.5 if it == items[0] else 0.1)),
            })
    pdf = pd.DataFrame(rows)
    path = str(tmp_path / f"{name}.parquet")
    pdf.to_parquet(path)
    return ParquetHandle(path=path)


def test_train_composite_produces_routable_adapter(tmp_path):
    from recsys_tfb.models.composite_train import train_composite
    rng = np.random.RandomState(0)
    items = ["fund_a", "fund_b", "ccard_x"]
    train = _write_handle(tmp_path, "train", 30, items, rng)
    train_dev = _write_handle(tmp_path, "train_dev", 8, items, rng)
    val = _write_handle(tmp_path, "val", 8, items, rng)
    preprocessor_metadata = {
        "feature_columns": ["f0", "prod_name", "f1"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": items},  # value order == code
    }
    parameters = {
        "schema": {"columns": {"item": "prod_name", "entity": ["cust_id"],
                               "time": "snap_date", "label": "label"},
                   "categorical_values": {"prod_name": items}},
        "product_categories": {"mapping": {"fund": ["fund_a", "fund_b"]},
                               "unmapped": "singleton"},
        "training": {"model_structure": "per_group_plus_rank",
                     "algorithm_params": {"num_threads": 1},
                     "stage1": {"grouping": "category", "objective": "binary",
                                "metric": "binary_logloss", "n_folds": 3},
                     "stage2": {"objective": "lambdarank", "metric": "ndcg"}},
    }
    adapter = train_composite(train, train_dev, val, preprocessor_metadata, parameters)
    # groups: fund (fund_a+fund_b) and ccard_x singleton
    assert set(adapter._stage1.keys()) == {"fund", "ccard_x"}
    X = np.array([[0.3, 0, 0.7], [0.3, 2, 0.7]], dtype=float)  # code0=fund_a, code2=ccard_x
    out = adapter.predict(X)
    assert out.shape == (2,)
