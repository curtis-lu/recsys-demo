"""per-item AP 與 macro 的 cluster bootstrap CI（spec §3 Phase 1）.

cluster＝entity（cust_id）：同一客戶跨期整批重抽。關鍵簡化：重抽整個
cluster 不改變任何 query 內的排序，所以每列正例貢獻
（``evaluation.metrics.positive_row_contributions``）只算一次；每個
replicate 只是帶 cluster 乘數的重新聚合（bincount with weights），
n_boot=200 在 driver 端 numpy 上是毫秒級。

CI＝percentile bootstrap（2.5 / 97.5）。某 item 在部分 replicate 中可能
沒有正例列（該客戶群沒被抽到）——以 NaN 略過（nanpercentile）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import (
    macro_from_per_item,
    positive_row_contributions,
)


def bootstrap_per_item_ci(sample_pdf: pd.DataFrame, parameters: dict) -> dict:
    """在診斷抽樣上估 per-item AP 與 macro 的 CI。回傳可直接 JSON 序列化的 dict。

    點估計與每個 replicate 都套 ``evaluation.metric`` 的參數家族
    （weight_alpha / min_positives / shrinkage_k；k＝截斷）。
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    score_col = schema["score"]

    eval_params = parameters.get("evaluation", {}) or {}
    metric_cfg = eval_params.get("metric", {}) or {}
    k = metric_cfg.get("k", None)
    metric_params = {
        "weight_alpha": float(metric_cfg.get("weight_alpha", 0.0) or 0.0),
        "min_positives": int(metric_cfg.get("min_positives", 0) or 0),
        "shrinkage_k": float(metric_cfg.get("shrinkage_k", 0) or 0.0),
    }
    diag_cfg = eval_params.get("diagnosis", {}) or {}
    n_boot = int((diag_cfg.get("ci", {}) or {}).get("n_boot", 200))
    seed = int((diag_cfg.get("sample", {}) or {}).get("seed", 42))

    # query id（time × entity）與 cluster id（entity only）
    query_key = (
        sample_pdf[time_col].astype(str)
        + "|"
        + sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    )
    groups = pd.factorize(query_key)[0]
    cluster_key = sample_pdf[entity_cols].astype(str).agg("|".join, axis=1)
    clusters = pd.factorize(cluster_key)[0]

    y = sample_pdf[label_col].to_numpy()
    score = sample_pdf[score_col].to_numpy(dtype=np.float64)
    items = sample_pdf[item_col].astype(str).to_numpy()

    contrib, row_idx = positive_row_contributions(groups, y, score, k)
    if len(contrib) == 0:
        return {
            "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
            "metric_params": metric_params,
            "per_item": {}, "macro": None,
        }

    item_of = items[row_idx]
    cluster_of = clusters[row_idx]
    uniq_items, item_inv = np.unique(item_of, return_inverse=True)
    n_items = len(uniq_items)
    n_clusters = int(clusters.max()) + 1

    # ---- 點估計 ----
    sums = np.bincount(item_inv, weights=contrib, minlength=n_items)
    counts = np.bincount(item_inv, minlength=n_items).astype(np.float64)
    point = sums / counts
    macro_point = macro_from_per_item(point, counts, **metric_params)

    # ---- bootstrap：重抽 cluster、帶乘數重新聚合 ----
    rng = np.random.RandomState(seed)
    boot_items = np.full((n_boot, n_items), np.nan)
    boot_macro = np.full(n_boot, np.nan)
    for b in range(n_boot):
        draw = rng.randint(0, n_clusters, n_clusters)
        mult = np.bincount(draw, minlength=n_clusters).astype(np.float64)
        w = mult[cluster_of]
        s = np.bincount(item_inv, weights=contrib * w, minlength=n_items)
        c = np.bincount(item_inv, weights=w, minlength=n_items)
        present = c > 0
        vals = np.divide(s, c, out=np.full(n_items, np.nan), where=present)
        boot_items[b] = vals
        m = macro_from_per_item(
            vals[present], c[present], **metric_params
        )
        if m is not None:
            boot_macro[b] = m

    lo = np.nanpercentile(boot_items, 2.5, axis=0)
    hi = np.nanpercentile(boot_items, 97.5, axis=0)

    per_item = {
        str(uniq_items[j]): {
            "ap": float(point[j]),
            "ci_low": float(lo[j]),
            "ci_high": float(hi[j]),
            "n_pos": int(counts[j]),
        }
        for j in range(n_items)
    }
    macro = None
    if macro_point is not None and not np.all(np.isnan(boot_macro)):
        macro = {
            "ap": float(macro_point),
            "ci_low": float(np.nanpercentile(boot_macro, 2.5)),
            "ci_high": float(np.nanpercentile(boot_macro, 97.5)),
        }
    return {
        "enabled": True, "k": k, "n_boot": n_boot, "seed": seed,
        "metric_params": metric_params,
        "per_item": per_item, "macro": macro,
    }
