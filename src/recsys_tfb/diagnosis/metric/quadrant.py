"""象限組裝（框架 Ch2）：條件判別力軸合成＋傷害觀測。

原本的水準軸（``gap_vs_global``，取自對帳層）已隨對帳層一起退場——那組量是
為了回答「絕對水準對不對」，而該問題在純排序（macro per-item mAP）的推導鏈
上不存在。純排序的縱軸替代品（offset sweep 的 δ*_j）尚未定案，故本模組目前
只有條件判別力（within-item AUC）一軸；``quadrant`` 這組識別字沿用，等縱軸
補回來時名實即再度相符。
上游停用（metric_ci stub 或 None）→ 對應欄位 None、notes 註記，不失敗
（best-effort，沿 cases_manifest 慣例）。
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
    "好": "健康",
    "差": "冷門受害者（判別力差）",
}


def _disc_status(auc: float | None, threshold: float) -> str:
    if auc is None:
        return "無法評估"
    return "好" if auc >= threshold else "差"


def build_quadrant_summary(
    eval_predictions: SparkDataFrame,
    label_table: SparkDataFrame,
    metric_ci: dict | None,
    parameters: dict,
) -> dict:
    """條件判別力軸＋傷害觀測 → per-item 判定（JSON-ready）。"""
    cfg = (
        ((parameters.get("evaluation", {}) or {}).get("diagnosis", {}) or {})
        .get("quadrant", {}) or {}
    )
    auc_threshold = float(cfg.get("auc_threshold", 0.6))
    top_k = int(cfg.get("top_k_occupancy", 1))

    auc = within_item_auc(eval_predictions, parameters)
    occupancy = top_slot_share(eval_predictions, parameters, top_k)
    suppression = suppression_counts(eval_predictions, parameters)
    prob, n_buyers = cross_purchase_matrix(label_table, parameters)

    ci_ok = bool(metric_ci and metric_ci.get("enabled"))
    ci_items = (metric_ci.get("per_item", {}) or {}) if ci_ok else {}

    notes: list[str] = []
    if not ci_ok:
        notes.append("metric_ci 停用或缺席——AP±CI 欄從缺。")

    by_item: dict[str, dict] = {}
    for item in sorted(auc):
        a = auc[item]
        disc = _disc_status(a.get("auc"), auc_threshold)
        label = "無法評估" if disc == "無法評估" else _QUADRANT_LABELS[disc]
        ci = ci_items.get(item) or {}
        occ = occupancy["by_item"].get(item) or {}
        by_item[item] = {
            "auc": a.get("auc"),
            "auc_reason": a.get("reason"),
            "n_pos": a["n_pos"],
            "n_neg": a["n_neg"],
            "n_rows": a["n_rows"],
            "disc_status": disc,
            "quadrant": label,
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
        "sources": {"metric_ci": ci_ok},
        "notes": notes,
    }
