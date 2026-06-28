"""模型結構無關的特徵歸因接縫（SHAP）。

今天走 LightGBM booster 的 TreeExplainer；這是日後支援 composite（two-stage）
模型的唯一改點——上層診斷一律經 feature_attributions / attribution_budget_units，
不直接觸碰 model.booster。
"""
import numpy as np


def _resolve_booster(model):
    booster = getattr(model, "booster", None)
    if booster is None:
        raise TypeError(
            f"{type(model).__name__} 無 booster；SHAP 歸因不支援"
            "（請在此 seam 擴充 composite 模型）"
        )
    return booster


def feature_attributions(model, X, feature_names) -> np.ndarray:
    """回傳 (n_rows, n_features) 的 SHAP 值；去掉可能的 bias 欄。"""
    import shap

    booster = _resolve_booster(model)
    sv = np.asarray(shap.TreeExplainer(booster).shap_values(X))
    if sv.ndim == 3:                      # 某些版本回 [classes, n, feat]
        sv = sv[-1]
    return sv[:, : len(feature_names)]


def attribution_budget_units(model) -> int:
    """budget guard 的成本因子（今天 = booster 樹數）。boosterless 與
    feature_attributions 一致 fail-fast（raise TypeError）。"""
    return int(_resolve_booster(model).num_trees())
