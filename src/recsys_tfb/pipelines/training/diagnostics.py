"""Training diagnostics: feature stats, native importance, SHAP.

純計算函式（over driver-local parquet / booster），無 Spark 依賴，供
training pipeline 的 diagnostic node 使用，產物由 log_experiment 上傳 MLflow。
產物路徑沿用 catalog 慣例 data/models/<model_version>/diagnostics/。
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def diagnostics_dir(parameters: dict) -> Path:
    """Resolve（並建立）診斷產物 dir，對齊 catalog 的
    data/models/${model_version}/diagnostics/ 慣例。"""
    mv = parameters["model_version"]
    d = Path("data") / "models" / str(mv) / "diagnostics"
    d.mkdir(parents=True, exist_ok=True)
    return d


def compute_feature_importance(model, parameters: dict) -> dict:
    """LightGBM split + gain importance，依 gain 排序，標出 dead features。"""
    cfg = parameters.get("diagnostics", {}).get("feature_importance", {})
    if not cfg.get("enabled", True):
        return {}
    split = model.feature_importance(kind="split")
    gain = model.feature_importance(kind="gain")
    ranked = sorted(
        ({"feature": f, "split": float(split[f]), "gain": float(gain[f])} for f in split),
        key=lambda r: r["gain"],
        reverse=True,
    )
    dead = sorted(f for f, v in split.items() if v == 0)
    logger.info("feature_importance: %d features, %d dead", len(ranked), len(dead))
    return {"ranked": ranked, "dead_features": dead}
