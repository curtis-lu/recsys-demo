"""per-item SHAP 診斷 orchestrator。"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step

from ._util import _to_native
from .attribution import attribution_budget_units, feature_attributions
from .paths import diagnostics_dir
from .sampling import _stratified_item_sample

logger = logging.getLogger(__name__)


def _signed_profile(sv_subset, feature_cols, top_k):
    """回傳 (top_features[含 signed], mean_abs 向量)。mean_abs 向量供後續 divergence 用。"""
    ai = np.abs(sv_subset).mean(axis=0)
    si = sv_subset.mean(axis=0)
    order = np.argsort(ai)[::-1][:top_k]
    profile = [{"feature": feature_cols[i],
                "mean_abs_shap": _to_native(ai[i]),
                "mean_signed_shap": _to_native(si[i])} for i in order]
    return profile, ai


def compute_shap_diagnostics(model, test_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """SHAP 全域 / per-item（族群代表）/ 代表性個例。單次 shap_values 三用。"""
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("enabled", True):
        return {}

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    top_k = int(cfg.get("top_k", 30))
    n_examples = int(cfg.get("n_examples", 5))
    min_per_item = int(cfg.get("min_rows_per_item", 30))
    sample_rows = int(cfg.get("sample_rows", 2000))
    max_budget = int(cfg.get("max_budget", 4_000_000))
    positive_min_rows = int(cfg.get("positive_min_rows", 20))

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    feature_cols = list(preprocessor["feature_columns"])

    pdf = test_parquet_handle.to_pandas()

    n_trees = attribution_budget_units(model)
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d -> reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    idx = _stratified_item_sample(pdf, item_col, eff_sample, min_per_item, seed=42)
    if len(idx) == 0:
        logger.warning("shap diagnostics: empty sample after stratification; skipping")
        return {}
    sample_pdf = pdf.iloc[idx].reset_index(drop=True)

    X = _pdf_to_X(sample_pdf, preprocessor, parameters)
    scores = model.predict(X)

    with log_step(logger, "shap_values"):
        shap_values = feature_attributions(model, X, feature_cols)

    # ---- 全域 ----
    mean_abs = np.abs(shap_values).mean(axis=0)
    mean_signed = shap_values.mean(axis=0)
    order = np.argsort(mean_abs)[::-1][:top_k]
    global_top = [
        {"feature": feature_cols[i], "mean_abs_shap": _to_native(mean_abs[i]),
         "mean_signed_shap": _to_native(mean_signed[i])}
        for i in order
    ]

    # ---- per-item（族群代表 + 覆蓋率 metadata）----
    items = sample_pdf[item_col].values
    labels = sample_pdf[label_col].values if label_col in sample_pdf else np.zeros(len(sample_pdf))
    per_item = {}
    for item in pd.unique(items):
        mask = items == item
        prof_all, _ = _signed_profile(shap_values[mask], feature_cols, top_k)
        sc = scores[mask]
        pos_mask = mask & (labels == 1)
        n_pos = int(pos_mask.sum())
        if n_pos >= positive_min_rows:
            prof_pos, _ = _signed_profile(shap_values[pos_mask], feature_cols, top_k)
            pos_low = False
        else:
            prof_pos, pos_low = None, True
        per_item[str(item)] = {
            "top_features": prof_all,
            "n_sampled": int(mask.sum()),
            "n_positive": n_pos,
            "score_min": float(sc.min()), "score_max": float(sc.max()),
            "score_mean": float(sc.mean()),
            "low_coverage": bool(mask.sum() < min_per_item),
            "top_features_positive": prof_pos,
            "positive_low_coverage": pos_low,
        }

    # ---- 代表性個例（全域 high/low + 每 item 一筆高分）----
    def _example(i):
        return {"item": str(items[i]), "score": float(scores[i]),
                "shap": {feature_cols[j]: _to_native(shap_values[i, j]) for j in range(len(feature_cols))}}

    hi = np.argsort(scores)[::-1][:n_examples]
    lo = np.argsort(scores)[:n_examples]
    per_item_high = []
    for item in pd.unique(items):
        pos = np.where(items == item)[0]
        best = pos[np.argmax(scores[pos])]
        per_item_high.append(_example(best))
    examples = {"high": [_example(i) for i in hi],
                "low": [_example(i) for i in lo],
                "per_item_high": per_item_high}

    # ---- PNG ----
    d = diagnostics_dir(parameters)
    plt.figure()
    shap.summary_plot(shap_values, features=X, feature_names=feature_cols, show=False)
    plt.tight_layout()
    plt.savefig(d / "shap_summary.png", dpi=100)
    plt.close()
    for rank, i in enumerate(hi):
        plt.figure()
        shap.summary_plot(shap_values[[i]], features=X[[i]], feature_names=feature_cols,
                          plot_type="bar", show=False)
        plt.tight_layout()
        plt.savefig(d / f"waterfall_high_{rank}.png", dpi=100)
        plt.close()

    logger.info("shap diagnostics: n_sample=%d n_trees=%d items=%d",
                len(idx), n_trees, len(per_item))
    return {"global": {"top_features": global_top}, "per_item": per_item, "examples": examples}
