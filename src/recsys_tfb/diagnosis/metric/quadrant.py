"""象限組裝（框架 Ch2 的 2×2 象限）：合併兩軸與傷害觀測。

兩軸＝水準（gap_vs_global，取對帳層產物——行為觀測、不含歸因；歸因看
reconciliation 的 residual/verdict）×條件判別力（within-item AUC）。
象限標籤照框架手冊 Ch2 的表；「加害者」判準只看水準偏高、與判別力無關。
上游停用（reconciliation/metric_ci stub 或 None）→ 對應欄位 None、該軸
「無法評估」、notes 註記，不失敗（best-effort，沿 cases_manifest 慣例）。
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame as SparkDataFrame

from recsys_tfb.diagnosis.metric.cross_purchase import cross_purchase_matrix
from recsys_tfb.diagnosis.metric.discrimination import within_item_auc
from recsys_tfb.diagnosis.metric.occupancy_spark import (
    suppression_counts,
    top_slot_share,
)

logger = logging.getLogger(__name__)

_QUADRANT_LABELS = {
    ("正常", "好"): "健康",
    ("正常", "差"): "冷門受害者（水準對、判別力差）",
    ("偏高", "好"): "加害者（水準偏高、判別力好）",
    ("偏高", "差"): "加害者（常數高分型）",
    ("偏低", "好"): "受害者（水準偏低、判別力好）",
    ("偏低", "差"): "雙重受害",
}


def _level_status(gap_vs_global: float | None, band: float) -> str:
    if gap_vs_global is None:
        return "無法評估"
    if gap_vs_global > band:
        return "偏高"
    if gap_vs_global < -band:
        return "偏低"
    return "正常"


def _disc_status(auc: float | None, threshold: float) -> str:
    if auc is None:
        return "無法評估"
    return "好" if auc >= threshold else "差"


def build_quadrant_summary(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    metric_ci: dict | None,
    reconciliation: dict | None,
    parameters: dict,
) -> dict:
    """兩軸＋傷害觀測 → per-item 象限判定（JSON-ready）。"""
    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("quadrant", {}) or {}
    )
    auc_threshold = float(cfg.get("auc_threshold", 0.6))
    gap_band = float(cfg.get("gap_band", 0.35))
    top_k = int(cfg.get("top_k_occupancy", 1))

    auc = within_item_auc(eval_predictions, parameters)
    occupancy = top_slot_share(eval_predictions, parameters, top_k)
    suppression = suppression_counts(eval_predictions, parameters)
    prob, n_buyers = cross_purchase_matrix(label_table, parameters)

    recon_ok = bool(reconciliation and reconciliation.get("enabled"))
    recon_items = (reconciliation.get("by_item", {}) or {}) if recon_ok else {}
    ci_ok = bool(metric_ci and metric_ci.get("enabled"))
    ci_items = (metric_ci.get("per_item", {}) or {}) if ci_ok else {}

    notes: list[str] = []
    if not recon_ok:
        notes.append("reconciliation 停用或缺席——水準軸無法評估。")
    if not ci_ok:
        notes.append("metric_ci 停用或缺席——AP±CI 欄從缺。")

    by_item: dict[str, dict] = {}
    for item in sorted(auc):
        a = auc[item]
        gvg = (recon_items.get(item) or {}).get("gap_vs_global")
        level = _level_status(gvg, gap_band)
        disc = _disc_status(a.get("auc"), auc_threshold)
        if "無法評估" in (level, disc):
            label = "無法評估"
        else:
            label = _QUADRANT_LABELS[(level, disc)]
        ci = ci_items.get(item) or {}
        occ = occupancy["by_item"].get(item) or {}
        by_item[item] = {
            "auc": a.get("auc"),
            "auc_reason": a.get("reason"),
            "n_pos": a["n_pos"],
            "n_neg": a["n_neg"],
            "n_rows": a["n_rows"],
            "gap_vs_global": gvg,
            "level_status": level,
            "disc_status": disc,
            "quadrant": label,
            "is_aggressor": level == "偏高",
            "ap_sampled": ci.get("ap"),
            "ci_low": ci.get("ci_low"),
            "ci_high": ci.get("ci_high"),
            "top_share": occ.get("top_share"),
            "n_top": occ.get("n_top"),
            "y_rate": occ.get("y_rate"),
            "suppression_count": (
                (suppression["by_item"].get(item) or {})
                .get("suppression_count", 0)
            ),
        }

    return {
        "enabled": True,
        "thresholds": {
            "auc_threshold": auc_threshold,
            "gap_band": gap_band,
            "top_k_occupancy": top_k,
        },
        "n_queries": occupancy["n_queries"],
        "n_pos_queries": suppression["n_pos_queries"],
        "by_item": by_item,
        "cross_purchase": {
            "matrix": (
                {j: {k: float(prob.loc[j, k]) for k in prob.columns}
                 for j in prob.index}
                if not prob.empty else {}
            ),
            "n_buyers": (
                {j: int(n_buyers[j]) for j in n_buyers.index}
                if not n_buyers.empty else {}
            ),
        },
        "sources": {"reconciliation": recon_ok, "metric_ci": ci_ok},
        "notes": notes,
    }
