"""Summaries of a set of attributions, as the SHAP diagnoses report them: a
signed profile of the top features, and how far one item's ranking of the
features is from the global ranking.

Which rows each summary is taken over — the whole sample, one item, one
quadrant, an item's positives — is the node's decision; this module only
summarises the rows it is handed.
"""

import numpy as np

from recsys_tfb.pipelines.training.steps.diagnosis_artifacts import to_native


def signed_profile(sv_subset, feature_cols, top_k):
    """Returns (top features with their signed mean, the mean-|attribution|
    vector). The vector covers every feature, not only the top ``top_k``:
    ``divergence`` compares whole rankings with it."""
    ai = np.abs(sv_subset).mean(axis=0)
    si = sv_subset.mean(axis=0)
    order = np.argsort(ai)[::-1][:top_k]
    profile = [{"feature": feature_cols[i],
                "mean_abs_shap": to_native(ai[i]),
                "mean_signed_shap": to_native(si[i])} for i in order]
    return profile, ai


def _rankdata(a):
    """Ascending integer ranks (0..n-1); a tie goes to argsort's stable order,
    not to scipy's midpoint rank."""
    order = np.argsort(a)
    ranks = np.empty(len(a), dtype=float)
    ranks[order] = np.arange(len(a), dtype=float)
    return ranks


def divergence(item_abs, global_abs, metric, k, feature_cols):
    """How far an item's |SHAP| ranking is from the global one (0 = the same,
    1 = entirely different). Returns (divergence, idiosyncratic features: the
    item's top ``k`` that are not in the global top ``k``)."""
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
