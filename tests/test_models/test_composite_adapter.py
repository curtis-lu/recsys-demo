"""Tests for CompositeModelAdapter (inference contract)."""
import numpy as np
import lightgbm as lgb
import pytest

from recsys_tfb.models.composite_adapter import CompositeModelAdapter


def _tiny_booster(const: float, n_feat: int = 3) -> lgb.Booster:
    """A booster that predicts ~const regardless of input (constant labels)."""
    X = np.random.RandomState(0).rand(40, n_feat)
    y = np.full(40, const)
    ds = lgb.Dataset(X, label=y)
    return lgb.train({"objective": "regression", "min_data_in_leaf": 1,
                      "num_leaves": 2, "verbosity": -1}, ds, num_boost_round=1)


def _make_adapter():
    # feature_columns: f0, prod_name(idx1), f2 ; item codes 0->groupA, 1->groupB
    stage1 = {"A": _tiny_booster(0.2), "B": _tiny_booster(0.9)}  # 3 features (full X)
    # Stage-2 input = [stage1_score(1), cust_feats(=X without item col = 2), group_code(1)] = 4 feats
    stage2 = _tiny_booster(0.5, n_feat=4)
    return CompositeModelAdapter._from_parts(
        stage1_boosters=stage1,
        stage2_booster=stage2,
        item_col_index=1,
        item_code_to_group={0: "A", 1: "B"},
        group_to_code={"A": 0, "B": 1},
        n_features=3,
    )


def test_predict_routes_each_row_to_its_group():
    a = _make_adapter()
    # row0 item-code 0 -> group A (s1=0.2); row1 item-code 1 -> group B (s1=0.9)
    X = np.array([[0.1, 0.0, 0.3], [0.4, 1.0, 0.6]])
    s1 = a._stage1_scores(X)
    assert s1[0] == pytest.approx(0.2, abs=1e-6)
    assert s1[1] == pytest.approx(0.9, abs=1e-6)
    out = a.predict(X)
    assert out.shape == (2,)


def test_predict_unknown_item_code_raises():
    a = _make_adapter()
    X = np.array([[0.1, 7.0, 0.3]])  # code 7 not in item_code_to_group
    with pytest.raises(KeyError):
        a.predict(X)
