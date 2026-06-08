"""Training diagnostics: feature stats, native importance, SHAP.

純計算函式（over driver-local parquet / booster），無 Spark 依賴，供
training pipeline 的 diagnostic node 使用，產物由 log_experiment 上傳 MLflow。
產物路徑沿用 catalog 慣例 data/models/<model_version>/diagnostics/。
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step

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


def _to_native(v):
    """np scalar / NaN → JSON-safe python scalar（NaN → None）。"""
    if v is None:
        return None
    f = float(v)
    return None if np.isnan(f) else f


def compute_feature_statistics(train_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """逐特徵 null_rate / mean,std,min,max（數值）/ n_distinct + single_value,high_null 旗標。"""
    cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    if not cfg.get("enabled", True):
        return {}
    sample_rows = int(cfg.get("sample_rows", 500000))
    high_null_threshold = float(cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])

    import pyarrow.parquet as pq

    table = pq.read_table(train_parquet_handle.path, columns=feature_cols)
    n = table.num_rows
    if n > sample_rows:
        idx = np.sort(np.random.RandomState(42).choice(n, size=sample_rows, replace=False))
        table = table.take(idx)
        logger.info("feature_statistics: sampled %d of %d rows", sample_rows, n)
    pdf = table.to_pandas()

    stats: dict = {}
    for col in feature_cols:
        s = pdf[col]
        null_rate = float(s.isna().mean())
        n_distinct = int(s.nunique(dropna=True))
        entry = {
            "null_rate": null_rate,
            "n_distinct": n_distinct,
            "single_value": n_distinct <= 1,
            "high_null": null_rate >= high_null_threshold,
        }
        if pd.api.types.is_numeric_dtype(s):
            entry["mean"] = _to_native(s.mean())
            entry["std"] = _to_native(s.std())
            entry["min"] = _to_native(s.min())
            entry["max"] = _to_native(s.max())
        stats[col] = entry
    logger.info("feature_statistics: %d features summarized", len(stats))
    return stats


def _stratified_item_sample(pdf, item_col, total, min_per_item, seed):
    """族群代表抽樣：依 item 分層，item 內純隨機；每 item 至少 min_per_item，
    不足全取（take-all）。回傳選中的 positional indices（對 pdf.iloc）。"""
    rng = np.random.RandomState(seed)
    groups = {item: np.where(pdf[item_col].values == item)[0]
              for item in pd.unique(pdf[item_col])}
    n_items = max(1, len(groups))
    per_item = max(int(min_per_item), total // n_items)
    selected = []
    for pos in groups.values():
        take = min(len(pos), per_item)
        selected.append(rng.choice(pos, size=take, replace=False))
    return np.sort(np.concatenate(selected)) if selected else np.array([], dtype=int)


def compute_shap_diagnostics(model, test_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """SHAP 全域 / per-item（族群代表）/ 代表性個例。單次 shap_values 三用。"""
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("enabled", True):
        return {}

    # Composite models hold N+1 boosters (no single `.booster`); single-booster
    # SHAP does not apply. Per-submodel SHAP is a future enhancement.
    from recsys_tfb.models.composite_adapter import CompositeModelAdapter
    base = getattr(model, "base", model)  # unwrap CalibratedModelAdapter if present
    if isinstance(base, CompositeModelAdapter):
        logger.info("SHAP diagnostics skipped for composite model "
                    "(per-submodel SHAP is a future enhancement).")
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

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    feature_cols = list(preprocessor["feature_columns"])

    pdf = test_parquet_handle.to_pandas()

    booster = model.booster
    n_trees = booster.num_trees()
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
        explainer = shap.TreeExplainer(booster)      # tree_path_dependent (default)
        shap_values = explainer.shap_values(X)        # SINGLE call
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 3:                         # 某些版本回傳 [classes, n, feat]
        shap_values = shap_values[-1]
    shap_values = shap_values[:, : len(feature_cols)] # 去掉可能的 bias 欄

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
        ai = np.abs(shap_values[mask]).mean(axis=0)
        o = np.argsort(ai)[::-1][:top_k]
        sc = scores[mask]
        per_item[str(item)] = {
            "top_features": [{"feature": feature_cols[i], "mean_abs_shap": _to_native(ai[i])} for i in o],
            "n_sampled": int(mask.sum()),
            "n_positive": int(np.sum(labels[mask] == 1)),
            "score_min": float(sc.min()), "score_max": float(sc.max()),
            "score_mean": float(sc.mean()),
            "low_coverage": bool(mask.sum() < min_per_item),
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
