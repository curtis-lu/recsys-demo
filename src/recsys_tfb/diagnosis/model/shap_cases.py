"""P2 象限診斷:per-(item×象限) 聚合 signed profile。P2b-2 續加案例圖。"""

import logging

import numpy as np
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


def _render_case(shap_row, feature_cols, top_k, item, quadrant, role, meta_row, out_dir):
    """畫單列 signed SHAP 橫條圖,回傳存檔 Path;失敗回 None(per-chart 隔離)。"""
    import matplotlib
    if matplotlib.get_backend().lower() != "agg":
        matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    from .paths import safe_name

    try:
        order = np.argsort(np.abs(shap_row))[::-1][:top_k]
        # barh 由下往上,反轉讓最大貢獻在最上方
        feats = [feature_cols[i] for i in order][::-1]
        vals = [float(shap_row[i]) for i in order][::-1]
        colors = ["tab:red" if v > 0 else "tab:blue" for v in vals]
        fig = plt.figure(figsize=(8, max(2.0, 0.4 * len(feats))))
        try:
            ax = fig.add_subplot(111)
            ax.barh(range(len(feats)), vals, color=colors)
            ax.set_yticks(range(len(feats)))
            ax.set_yticklabels(feats, fontsize=8)
            ax.axvline(0, color="black", linewidth=0.6)
            ax.set_xlabel("signed SHAP (log-odds)")
            ax.set_title(
                f"{item} · {quadrant} · {role} · score={float(meta_row['score']):.3f}"
                f" · rank={int(meta_row['rank'])} · label={int(meta_row['label'])}",
                fontsize=9)
            for y, v in enumerate(vals):
                ax.text(v, y, f" {v:+.3f}", va="center",
                        ha="left" if v >= 0 else "right", fontsize=7)
            fig.tight_layout()
            png_path = out_dir / safe_name(item) / f"{quadrant}_{role}.png"
            png_path.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(png_path, dpi=100)
            return png_path
        finally:
            plt.close(fig)
    except Exception as e:
        logger.warning("case chart failed (%s/%s/%s): %s", item, quadrant, role, e)
        return None


def _manifest_entry(meta_row, png_path, diag_dir, time_col, entity_cols):
    # manifest 鍵用實際 schema 欄名(泛用框架,不寫死銀行的 snap_date/cust)。
    base = {c: str(meta_row[c]) for c in [time_col] + entity_cols}
    base.update({"rank": int(meta_row["rank"]), "score": float(meta_row["score"]),
                 "label": int(meta_row["label"])})
    if png_path is None:
        return {"rendered": False, "reason": "render_failed", **base}
    return {"rendered": True, "png": str(png_path.relative_to(diag_dir)), **base}


def compute_quadrant_cases(model, case_rows, preprocessor: dict, parameters: dict) -> dict:
    """per-(item×象限) 全格極值案例的單列 signed SHAP 橫條圖 + 完整稽核 manifest。

    ``case_rows`` 為 ``select_shap_population`` 的第二輸出(每 item×象限 role=high/low
    各一列)。回傳
    ``{"<item>": {"<quadrant>": {"high"/"low": {rendered, png|reason, snap_date, cust,
    rank, score, label}}}}``。None / 空 / ``quadrant_enabled=false`` → ``{}``。
    單次 SHAP over 那幾十列極值。best-effort:整體失敗 log + 回 ``{}``;單張圖失敗只記該筆
    ``reason=render_failed``,不影響其餘。空格記 ``reason=empty``;單行格 low 記
    ``reason=single_row_same_as_high``(不產重複檔)。
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if case_rows is None or len(case_rows) == 0:
        logger.warning("quadrant cases: empty case_rows; skipping")
        return {}

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    from .paths import cases_dir, diagnostics_dir

    case_top_k = int(cfg.get("case_top_k", 15))
    schema = get_schema(parameters)
    item_col = schema["item"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    feature_cols = list(preprocessor["feature_columns"])

    try:
        pdf = case_rows.reset_index(drop=True)
        X = _pdf_to_X(pdf, preprocessor, parameters)
        log_data_volume(logger, "cases.X", X)
        shap_values = feature_attributions(model, X, feature_cols)

        cdir = cases_dir(parameters)
        ddir = diagnostics_dir(parameters)
        items = pdf[item_col].values
        quads = pdf["quadrant"].values
        roles = pdf["role"].values

        def _gkey(i):
            return tuple(str(pdf.iloc[i][c]) for c in [time_col] + entity_cols)

        manifest: dict = {}
        for item in pd.unique(items):
            item_entry: dict = {}
            for q in _QUADRANTS:
                idx = np.where((items == item) & (quads == q))[0]
                if len(idx) == 0:
                    item_entry[q] = {
                        "high": {"rendered": False, "reason": "empty"},
                        "low": {"rendered": False, "reason": "empty"}}
                    continue
                by_role = {roles[i]: i for i in idx}
                hi, lo = by_role.get("high"), by_role.get("low")
                cell: dict = {}
                # high(非空格通常必有;防禦性處理只有 low 的退化輸入)
                if hi is None:
                    cell["high"] = {"rendered": False, "reason": "empty"}
                else:
                    hi_png = _render_case(shap_values[hi], feature_cols, case_top_k,
                                          item, q, "high", pdf.iloc[hi], cdir)
                    cell["high"] = _manifest_entry(pdf.iloc[hi], hi_png, ddir,
                                                   time_col, entity_cols)
                # low:單行格(與 high 同列)→ 不重畫;只有 high 的退化輸入 → low 記 empty
                if lo is None:
                    cell["low"] = {"rendered": False, "reason": "empty"}
                elif hi is not None and _gkey(hi) == _gkey(lo):
                    cell["low"] = {"rendered": False,
                                   "reason": "single_row_same_as_high"}
                else:
                    lo_png = _render_case(shap_values[lo], feature_cols, case_top_k,
                                          item, q, "low", pdf.iloc[lo], cdir)
                    cell["low"] = _manifest_entry(pdf.iloc[lo], lo_png, ddir,
                                                  time_col, entity_cols)
                item_entry[q] = cell
            manifest[str(item)] = item_entry
    except Exception as e:  # best-effort:診斷失敗不中斷訓練
        logger.warning("quadrant cases failed: %s", e)
        return {}

    n_rendered = sum(1 for it in manifest.values() for cell in it.values()
                     for r in cell.values() if r.get("rendered"))
    logger.info("quadrant cases: items=%d rendered=%d", len(manifest), n_rendered)
    return manifest
