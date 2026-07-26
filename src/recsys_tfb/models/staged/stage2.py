"""Stage-2 feature assembly + single-fit helper (spec D4).

Stage-2 matrix layout: ``[original features X | stage-1 score | group code]``.
Appending at the END keeps stage-1's categorical feature indices valid; the
group-code column is itself declared categorical.

Group code contract: code = rank of the group key in ``sorted(group_keys)``.
Derived, not persisted — sorted order is deterministic and the bundle
integrity check guarantees train/load see the same key set, so training and
inference always encode identically.
"""

import lightgbm as lgb
import numpy as np

from recsys_tfb.core.group_utils import to_contiguous_groups
from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter


STAGE2_EXTRA_FEATURES = ("stage1_score", "partition_gcode")


def stage2_feature_names(base_feature_cols) -> list:
    """[X | s1 | gcode] 對應的特徵名（與 stage2_matrix 欄序一致）。
    使用者特徵撞名時 lgb 以重複特徵名 fail-loud，屬 config 錯誤不防護。"""
    return list(base_feature_cols) + list(STAGE2_EXTRA_FEATURES)


def group_code_lookup(group_keys) -> dict:
    return {k: i for i, k in enumerate(sorted(group_keys))}


def encode_group_codes(keys: np.ndarray, lookup: dict) -> np.ndarray:
    """Map routing keys -> float codes. KeyError on an unknown key is
    deliberate: callers route/skip missing groups BEFORE encoding."""
    return np.array([lookup[k] for k in keys], dtype=np.float64)


def stage2_matrix(X, s1_scores, gcodes) -> np.ndarray:
    return np.column_stack([
        np.asarray(X, dtype=np.float64),
        np.asarray(s1_scores, dtype=np.float64),
        np.asarray(gcodes, dtype=np.float64),
    ])


def stage2_categorical_indices(base_cat_idx, n_base_features: int) -> list:
    """Stage-1 categorical indices stay valid; add the gcode column.
    Column order: [0..n_base-1]=X, n_base=s1 score, n_base+1=gcode.
    ``base_cat_idx=None`` means no stage-1 categoricals (the shape returned
    by ``LightGBMAdapter._categorical_indices`` when there are none) —
    treated the same as an empty list."""
    return list(base_cat_idx or []) + [int(n_base_features) + 1]


def fit_stage2(
    mode: str,
    X2_tr, y_tr, w_tr, qgroups_tr,
    X2_val, y_val, w_val, qgroups_val,
    params: dict, categorical_indices,
    feature_names=None,
) -> LightGBMAdapter:
    """One stage-2 fit, early-stopped on the provided validation set.

    ``qgroups_*``: per-row query-group ids (``extract_Xy_with_groups``
    convention); only consumed for lambdarank. Weights ride per-row and are
    perm-aligned for the ranking branch (mirrors the shared prepare layer).
    ``w_val`` rides the same way as ``w_tr`` — ``None`` means unweighted,
    matching the shared prepare layer where both train and val (dev) get
    weight= (lightgbm_adapter.py's ``ds_train``/``ds_dev``).
    """
    ds_kwargs = {}
    if feature_names is not None:
        ds_kwargs["feature_name"] = list(feature_names)
    if mode == "lambdarank":
        perm, counts = to_contiguous_groups(np.asarray(qgroups_tr))
        train_ds = lgb.Dataset(
            X2_tr[perm], label=np.asarray(y_tr)[perm],
            weight=None if w_tr is None else np.asarray(w_tr)[perm],
            group=counts, categorical_feature=list(categorical_indices),
            free_raw_data=False, **ds_kwargs,
        )
        permv, countsv = to_contiguous_groups(np.asarray(qgroups_val))
        val_ds = lgb.Dataset(
            X2_val[permv], label=np.asarray(y_val)[permv],
            weight=None if w_val is None else np.asarray(w_val)[permv],
            group=countsv, reference=train_ds, free_raw_data=False,
        )
    elif mode == "binary":
        train_ds = lgb.Dataset(
            X2_tr, label=y_tr, weight=w_tr,
            categorical_feature=list(categorical_indices),
            free_raw_data=False, **ds_kwargs,
        )
        val_ds = lgb.Dataset(
            X2_val, label=y_val, weight=w_val,
            reference=train_ds, free_raw_data=False,
        )
    else:
        raise ValueError(
            f"stage2 mode must be 'binary' or 'lambdarank', got {mode!r}")
    adapter = LightGBMAdapter()
    adapter.train(
        None, None, None, None, dict(params),
        train_dataset=train_ds, val_dataset=val_ds,
    )
    return adapter
