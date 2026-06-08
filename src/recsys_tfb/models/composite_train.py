"""Composite (two-stage) training orchestration.

Driver-local pandas/numpy, consistent with the existing single-machine LightGBM
training. Folds are customer-disjoint via zlib.crc32 (the IEEE-802.3 polynomial,
matching Spark's F.crc32 used by the dataset split) so the same customer never
appears in two folds.
"""
from __future__ import annotations

import zlib

import lightgbm as lgb
import numpy as np

from recsys_tfb.core.categories import resolve_groups
from recsys_tfb.core.group_utils import to_contiguous_groups
from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.composite_adapter import CompositeModelAdapter

_FOLD_SITE = "composite_oof"


def assign_folds(entity_ids: np.ndarray, n_folds: int, seed: int) -> np.ndarray:
    """Deterministic, customer-disjoint fold index in [0, n_folds) per row."""
    out = np.empty(len(entity_ids), dtype=np.int64)
    for i, e in enumerate(entity_ids):
        token = f"{_FOLD_SITE}|{seed}|{e}".encode()
        out[i] = zlib.crc32(token) % n_folds
    return out


def oof_is_leakage_clean(folds: np.ndarray, producing_fold: np.ndarray) -> bool:
    """True iff every row was scored out-of-fold.

    `producing_fold[i]` is the held-out fold whose booster scored row i. For a
    clean OOF, row i must be scored by the booster of its OWN fold (which trained
    on all OTHER folds), so `producing_fold[i] == folds[i]` for every row.
    """
    return bool(np.all(producing_fold == folds))


def _read_frame(handle, preprocessor_metadata, parameters):
    """Return (X, y, entity, item_codes) as numpy, columns in feature order."""
    schema = get_schema(parameters)
    feat_cols = preprocessor_metadata["feature_columns"]
    item_col = schema["item"]
    code_of = {v: i for i, v in enumerate(preprocessor_metadata["category_mappings"][item_col])}
    pdf = handle.to_pandas()
    X_df = pdf[feat_cols].copy()
    # encode the item column (the only deferred identity categorical) to codes
    X_df[item_col] = X_df[item_col].map(code_of).astype("int64")
    X = X_df.to_numpy(dtype=float)
    y = pdf[schema["label"]].to_numpy()
    entity = pdf[schema["entity"][0]].to_numpy()
    item_codes = pdf[item_col].map(code_of).to_numpy()
    return X, y, entity, item_codes


def _stage1_params(parameters):
    s1 = parameters["training"]["stage1"]
    return {"objective": s1.get("objective", "binary"),
            "metric": s1.get("metric", "binary_logloss"), "verbosity": -1,
            "num_threads": parameters["training"].get("algorithm_params", {}).get("num_threads", 0)}


def _stage2_params(parameters):
    s2 = parameters["training"]["stage2"]
    return {"objective": s2.get("objective", "lambdarank"),
            "metric": s2.get("metric", "ndcg"), "verbosity": -1,
            "num_threads": parameters["training"].get("algorithm_params", {}).get("num_threads", 0)}


def _fit_binary(X, y, params, num_boost_round=100):
    ds = lgb.Dataset(X, label=y, free_raw_data=False)
    return lgb.train({**params}, ds, num_boost_round=num_boost_round)


def _codes(values: np.ndarray) -> np.ndarray:
    """Stable int group id per distinct value, preserving first-seen order."""
    seen: dict = {}
    out = np.empty(len(values), dtype=np.int64)
    for i, v in enumerate(values):
        out[i] = seen.setdefault(v, len(seen))
    return out


def train_composite(train_handle, train_dev_handle, val_handle,
                    preprocessor_metadata, parameters) -> CompositeModelAdapter:
    """OOF cross-fit Stage-1 per grouping, refit on full train, train Stage-2."""
    schema = get_schema(parameters)
    item_col = schema["item"]
    item_idx = preprocessor_metadata["feature_columns"].index(item_col)
    grouping = parameters["training"]["stage1"].get("grouping", "category")
    n_folds = int(parameters["training"]["stage1"].get("n_folds", 5))

    item_to_group = resolve_groups(parameters, grouping)
    code_of = {v: i for i, v in enumerate(preprocessor_metadata["category_mappings"][item_col])}
    item_code_to_group = {code_of[v]: g for v, g in item_to_group.items()}
    groups = sorted(set(item_to_group.values()))
    group_to_code = {g: i for i, g in enumerate(groups)}

    Xtr, ytr, etr, code_tr = _read_frame(train_handle, preprocessor_metadata, parameters)
    group_tr = np.array([item_code_to_group[int(c)] for c in code_tr])

    # ---- OOF Stage-1 over train ----------------------------------------
    folds = assign_folds(etr, n_folds=n_folds, seed=42)
    s1_params = _stage1_params(parameters)
    oof = np.empty(len(Xtr), dtype=np.float64)
    producing_fold = np.empty(len(Xtr), dtype=np.int64)
    for g in groups:
        g_mask = group_tr == g
        for k in range(n_folds):
            fit_mask = g_mask & (folds != k)
            pred_mask = g_mask & (folds == k)
            if not pred_mask.any():
                continue
            if not fit_mask.any() or ytr[fit_mask].sum() == 0:
                oof[pred_mask] = float(ytr[g_mask].mean()) if g_mask.any() else 0.0
            else:
                booster = _fit_binary(Xtr[fit_mask], ytr[fit_mask], s1_params)
                oof[pred_mask] = booster.predict(Xtr[pred_mask])
            producing_fold[pred_mask] = k
    assert oof_is_leakage_clean(folds, producing_fold), "OOF leakage detected"

    # ---- refit Stage-1 on full train (used at inference) ---------------
    stage1_full: dict[str, lgb.Booster] = {}
    for g in groups:
        g_mask = group_tr == g
        if ytr[g_mask].sum() == 0:
            stage1_full[g] = _fit_binary(Xtr[g_mask], ytr[g_mask], s1_params, num_boost_round=1)
        else:
            stage1_full[g] = _fit_binary(Xtr[g_mask], ytr[g_mask], s1_params)

    # ---- Stage-2 (lambdarank, query=customer) --------------------------
    def stage2_matrix(X, code, s1):
        gcode = np.array([group_to_code[item_code_to_group[int(c)]] for c in code], dtype=float)
        cust = np.delete(X, item_idx, axis=1)
        return np.column_stack([s1, cust, gcode])

    X2_tr = stage2_matrix(Xtr, code_tr, oof)
    perm, grp_counts = to_contiguous_groups(_codes(etr))
    ds2 = lgb.Dataset(X2_tr[perm], label=ytr[perm], group=grp_counts, free_raw_data=False)

    Xv, yv, ev, code_v = _read_frame(val_handle, preprocessor_metadata, parameters)
    s1_val = np.array([stage1_full[item_code_to_group[int(c)]].predict(Xv[i:i+1])[0]
                       for i, c in enumerate(code_v)])
    X2_val = stage2_matrix(Xv, code_v, s1_val)
    permv, grpv = to_contiguous_groups(_codes(ev))
    ds2_val = lgb.Dataset(X2_val[permv], label=yv[permv], group=grpv,
                          reference=ds2, free_raw_data=False)

    stage2 = lgb.train(_stage2_params(parameters), ds2, num_boost_round=200,
                       valid_sets=[ds2_val],
                       callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)])

    return CompositeModelAdapter._from_parts(
        stage1_boosters=stage1_full, stage2_booster=stage2,
        item_col_index=item_idx, item_code_to_group=item_code_to_group,
        group_to_code=group_to_code, n_features=Xtr.shape[1],
    )
