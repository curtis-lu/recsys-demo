"""per-item SHAP 診斷 orchestrator。"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step

from . import data_access
from ._util import _to_native
from .attribution import attribution_budget_units, feature_attributions
from .paths import per_item_summary_dir, safe_name, summary_dir
from .sampling import _stratified_item_sample

logger = logging.getLogger(__name__)


def _signed_profile(sv_subset, feature_cols, top_k):
    """回傳 (top_features[含 signed], mean_abs 向量)。mean_abs 向量供後續 divergence 用（全特徵向量，非 top-k 過濾）。"""
    ai = np.abs(sv_subset).mean(axis=0)
    si = sv_subset.mean(axis=0)
    order = np.argsort(ai)[::-1][:top_k]
    profile = [{"feature": feature_cols[i],
                "mean_abs_shap": _to_native(ai[i]),
                "mean_signed_shap": _to_native(si[i])} for i in order]
    return profile, ai


def _rankdata(a):
    """升序整數 rank（0..n-1）；平手以 argsort 穩定排序位置決定（非 scipy 的中點 rank）。"""
    order = np.argsort(a)
    ranks = np.empty(len(a), dtype=float)
    ranks[order] = np.arange(len(a), dtype=float)
    return ranks


def _divergence(item_abs, global_abs, metric, k, feature_cols):
    """per-item |SHAP| 排序 vs 全域排序的偏離度（0=一致, 1=完全不同）。
    回傳 (divergence float, idiosyncratic_features list)。"""
    k = min(int(k), len(feature_cols))
    i_order = np.argsort(item_abs)[::-1]
    g_top = set(np.argsort(global_abs)[::-1][:k].tolist())
    i_top = set(i_order[:k].tolist())
    if metric == "spearman":
        ir, gr = _rankdata(item_abs), _rankdata(global_abs)
        div = (1.0 - float(np.corrcoef(ir, gr)[0, 1])) / 2.0 if ir.std() and gr.std() else 0.0
    else:  # jaccard_topk
        inter, union = len(i_top & g_top), len(i_top | g_top)
        div = (1.0 - inter / union) if union else 0.0
    idio = [feature_cols[i] for i in i_order[:k] if i not in g_top]
    return float(div), idio


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
    divergence_metric = str(cfg.get("divergence_metric", "jaccard_topk"))
    divergence_top_k = int(cfg.get("divergence_top_k", 15))  # 通常比 top_k 小；只用於 Jaccard/idio 的 top-k 集合比較
    profile_positive = bool(cfg.get("profile_positive", True))

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    feature_cols = list(preprocessor["feature_columns"])

    path = test_parquet_handle.path

    n_trees = attribution_budget_units(model)
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d -> reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    # 只讀 item 分區欄做分層（避免全量物化 test）
    item_values = data_access.read_column(path, item_col)
    idx = _stratified_item_sample(item_values, eff_sample, min_per_item, seed=42)
    if len(idx) == 0:
        logger.warning("shap diagnostics: empty sample after stratification; skipping")
        return {}

    # 只取抽中的列 × (feature 欄 + item 欄 + label 欄)。生產上 item_col 通常即
    # categorical feature（已在 feature_cols 內），但診斷 fixture / cache 佈局未必；
    # 下游 per-item 分群需 sample_pdf[item_col]，故顯式確保 item/label 皆在 take_cols。
    names = data_access.schema_names(path)
    take_cols = list(feature_cols)
    for col in (item_col, label_col):
        if col in names and col not in take_cols:
            take_cols.append(col)
    sample_pdf = data_access.take_rows(path, idx, columns=take_cols).reset_index(drop=True)
    logger.info("shap diagnostics: n_total=%d n_sampled=%d n_cols=%d",
                len(item_values), len(sample_pdf), len(take_cols))

    X = _pdf_to_X(sample_pdf, preprocessor, parameters)
    scores = model.predict(X)

    with log_step(logger, "shap_values"):
        shap_values = feature_attributions(model, X, feature_cols)

    # ---- 全域 ----
    global_top, mean_abs = _signed_profile(shap_values, feature_cols, top_k)

    # ---- per-item（族群代表 + 覆蓋率 metadata）----
    items = sample_pdf[item_col].values
    labels = sample_pdf[label_col].values if label_col in sample_pdf else np.zeros(len(sample_pdf))
    per_item = {}
    for item in pd.unique(items):
        mask = items == item
        prof_all, ai = _signed_profile(shap_values[mask], feature_cols, top_k)
        sc = scores[mask]
        # -- divergence vs global driver ranking --
        div, idio = _divergence(ai, mean_abs, divergence_metric, divergence_top_k, feature_cols)
        # -- positive-only profile (adopters vs all-rows) --
        pos_mask = mask & (labels == 1)
        n_pos = int(pos_mask.sum())
        if not profile_positive:
            prof_pos, pos_low = None, False
        elif n_pos >= positive_min_rows:
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
            "positive_low_coverage": bool(pos_low),
            "divergence_from_global": _to_native(div),
            "idiosyncratic_features": idio,
        }

    item_idiosyncrasy = sorted(
        ({"item": k,
          "divergence_from_global": v["divergence_from_global"],
          "idiosyncratic_features": v["idiosyncratic_features"]}
         for k, v in per_item.items()),
        key=lambda r: r["divergence_from_global"],
        reverse=True,
    )

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

    # ---- PNG（best-effort：繪圖失敗不應中斷診斷/訓練，spec §4）----
    sdir = summary_dir(parameters)
    try:
        plt.figure()
        try:
            shap.summary_plot(shap_values, features=X, feature_names=feature_cols, show=False)
            plt.tight_layout()
            plt.savefig(sdir / "shap_summary_global.png", dpi=100)
        finally:
            plt.close()
    except Exception as e:
        logger.warning("global shap summary plot failed: %s", e)

    if cfg.get("per_item_beeswarm", True):
        pdir = per_item_summary_dir(parameters)
        for item in pd.unique(items):
            m = items == item
            try:
                plt.figure()
                try:
                    shap.summary_plot(shap_values[m], features=X[m],
                                      feature_names=feature_cols, show=False)
                    plt.tight_layout()
                    plt.savefig(pdir / f"shap_summary__{safe_name(item)}.png", dpi=100)
                finally:
                    plt.close()
            except Exception as e:
                logger.warning("per-item beeswarm failed for item %s: %s", item, e)

    logger.info("shap diagnostics: n_sample=%d n_trees=%d items=%d",
                len(idx), n_trees, len(per_item))
    return {"global": {"top_features": global_top}, "per_item": per_item, "examples": examples,
            "item_idiosyncrasy": item_idiosyncrasy}
