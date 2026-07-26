"""staged 模型診斷：分派 helper、Stage-1 總覽表、per-group runner。

分派契約（D-C3）：duck-typing，不 import staged 型別做 isinstance——
``LightGBMAdapter`` 沒有 ``predict_routed``／``stage2_mode``，shared 路徑
逐位元不變。
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)


def is_staged(model) -> bool:
    return hasattr(model, "predict_routed")


def has_stage2(model) -> bool:
    return getattr(model, "stage2_mode", "none") != "none"


def _routing_keys_for(model, pdf) -> np.ndarray:
    from recsys_tfb.models.staged.partition import routing_keys

    return routing_keys(pdf, model.partition_keys)


def resolve_attribution_inputs(model, pdf, X, feature_cols):
    """SHAP 系函式的輸入分派：stage2 存在 → ([X|s1|gcode], 名單+尾兩欄)；
    其他模型原樣通過。``pdf`` 需帶 partition key 欄（model_input cache 契約，
    spec §5：partition_keys ⊆ identity ∪ carry_columns）。"""
    if not has_stage2(model):
        return X, list(feature_cols)
    from recsys_tfb.models.staged.stage2 import stage2_feature_names

    keys = _routing_keys_for(model, pdf)
    return model.stage2_matrix_for(X, keys), stage2_feature_names(feature_cols)


def model_scores(model, pdf, X) -> np.ndarray:
    """模型分數（診斷抽樣列）：staged 走 routed（eval 資料缺群＝異常，raise）。"""
    if not is_staged(model):
        return model.predict(X)
    keys = _routing_keys_for(model, pdf)
    return model.predict_routed(X, keys, on_missing="raise")[0]


_STAGE2_SUMMARY_KEYS = ("mode", "oof_folds", "oof_rows", "n_groups", "best_params")


def compute_stage1_overview(stage1_groups_report, parameters: dict,
                            stage2_report=None) -> dict:
    """Stage-1 總覽表（spec §6）：每群一列＋彙總。只重排 stage1_groups.json
    既有事實不重算；只呈現資料不下結論（diagnosis-report-presentation.md）。
    ``stage2_report`` 走 trailing-default（pipeline.py log_experiment 同慣例）。"""
    groups = stage1_groups_report.get("groups", {})
    rows = []
    for key in sorted(groups):
        g = groups[key]
        n_rows, n_pos = int(g["n_rows"]), int(g["n_pos"])
        rows.append({
            "group": key,
            "n_rows": n_rows,
            "n_pos": n_pos,
            "pos_rate": (n_pos / n_rows) if n_rows else None,
            "metric": g.get("metric"),
            "score": g.get("score"),
            "train_seconds": g.get("train_seconds"),
            "best_params": g.get("best_params", {}),
        })
    out = {
        "partition_keys": stage1_groups_report.get("partition_keys"),
        "n_groups": len(rows),
        "total_rows": int(sum(r["n_rows"] for r in rows)),
        "total_positives": int(sum(r["n_pos"] for r in rows)),
        "total_train_seconds": float(sum(r["train_seconds"] or 0.0 for r in rows)),
        "groups": rows,
    }
    if stage2_report:
        out["stage2"] = {k: stage2_report[k] for k in _STAGE2_SUMMARY_KEYS
                         if k in stage2_report}
    logger.info("stage1 overview: %d group(s), stage2=%s",
                len(rows), bool(stage2_report))
    return out
