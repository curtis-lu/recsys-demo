"""P2 象限診斷:per-(item×象限) 聚合 signed profile。P2b-2 續加案例圖。"""

import logging

import pandas as pd

from recsys_tfb.core.logging import log_data_volume

from .attribution import feature_attributions
from .shap_per_item import _signed_profile

logger = logging.getLogger(__name__)

_QUADRANTS = ("TP", "FP", "FN", "TN")


def compute_quadrant_profiles(model, shap_population, preprocessor: dict, parameters: dict) -> dict:
    """per-(item×象限) 平均 signed profile。

    回傳 ``{"<item>": {"<quadrant>": {"top_features":[…], "n_sampled":int,
    "low_coverage":bool}}}``。``shap_population`` 為 ``select_shap_population`` 的小
    pandas(特徵 + item + quadrant)。None / 空 / ``quadrant_enabled=false`` → ``{}``。
    單次 SHAP。best-effort:失敗 log + 回 ``{}``,不中斷訓練。
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if shap_population is None or len(shap_population) == 0:
        logger.warning("quadrant profiles: empty population; skipping")
        return {}

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    top_k = int(cfg.get("top_k", 30))
    quadrant_min_rows = int(cfg.get("quadrant_min_rows", 10))
    item_col = get_schema(parameters)["item"]
    feature_cols = list(preprocessor["feature_columns"])

    try:
        pdf = shap_population.reset_index(drop=True)
        X = _pdf_to_X(pdf, preprocessor, parameters)
        log_data_volume(logger, "quadrant.X", X)
        shap_values = feature_attributions(model, X, feature_cols)
        items = pdf[item_col].values
        quads = pdf["quadrant"].values
        out: dict = {}
        for item in pd.unique(items):
            for q in _QUADRANTS:
                mask = (items == item) & (quads == q)
                n = int(mask.sum())
                if n == 0:
                    continue
                prof, _ = _signed_profile(shap_values[mask], feature_cols, top_k)
                out.setdefault(str(item), {})[q] = {
                    "top_features": prof,
                    "n_sampled": n,
                    "low_coverage": bool(n < quadrant_min_rows),
                }
    except Exception as e:  # best-effort:診斷失敗不中斷訓練
        logger.warning("quadrant profiles failed: %s", e)
        return {}
    logger.info("quadrant profiles: items=%d", len(out))
    return out
