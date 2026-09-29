"""P2 象限診斷:per-(item×象限) 聚合 signed profile。P2b-2 續加案例圖。

Error policy (ADR-0030 decision 4): a model that cannot attribute
(``UnsupportedCapability``) skips the diagnosis with a warning and lands the
"model cannot" shape; any other exception stops the run. The case charts are
returned as drawing functions for the ``case_figures`` catalog entry, which
skips one that fails with a warning (decision 7).
"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_data_volume
from recsys_tfb.io.extract import pdf_to_X
from recsys_tfb.models.base import UnsupportedCapability
from recsys_tfb.models.feature_view import model_feature_view

from ._util import unsupported_artifact
from .figures import signed_bars
from .paths import safe_name
from .shap_per_item import _signed_profile

logger = logging.getLogger(__name__)

_QUADRANTS = ("TP", "FP", "FN", "TN")


def compute_quadrant_profiles(model, shap_population, preprocessor: dict, parameters: dict) -> dict:
    """per-(item×象限) 平均 signed profile。

    回傳 ``{"<item>": {"<quadrant>": {"top_features":[…], "n_sampled":int,
    "low_coverage":bool}}}``。``shap_population`` 為 ``select_shap_population`` 的小
    pandas(特徵 + item + quadrant)。None / 空 / ``quadrant_enabled=false`` → ``{}``。
    單次 SHAP。

    A model that cannot attribute lands the "model cannot" shape; any other
    failure stops the run (ADR-0030 decision 4).
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}
    if shap_population is None or len(shap_population) == 0:
        logger.warning("quadrant profiles: empty population; skipping")
        return {}

    from recsys_tfb.core.schema import get_schema

    top_k = int(cfg.get("top_k", 30))
    quadrant_min_rows = int(cfg.get("quadrant_min_rows", 10))
    item_col = get_schema(parameters)["item"]
    # Decision — which features, and in what order: ask the model, not
    # apply_feature_selection(preprocessor, parameters). This is not a drift fix:
    # the exclude list lives in the `training:` block, so editing it bumps
    # model_version, the model's catalog path moves with it, and the whole
    # training chain is pulled back — ADR-0014 decision 7 is explicit that the
    # version mechanism already blocks that, and that this is interface work, not
    # a bug fix. What it buys is addressability: model and preprocessor both have
    # catalog entries, while the config-derived view is memory-only and drags
    # select_features into every diagnosis-only slice.
    model_view = model_feature_view(model, preprocessor)
    feature_cols = list(model_view["feature_columns"])

    pdf = shap_population.reset_index(drop=True)
    X = pdf_to_X(pdf, model_view, parameters)
    log_data_volume(logger, "quadrant.X", X)
    # Decision — a model that cannot attribute skips this with a warning and
    # says so; nothing else is caught (ADR-0030 decision 4).
    try:
        shap_values = model.feature_attributions(X)
    except UnsupportedCapability as exc:
        logger.warning("quadrant profiles: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc)
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
    logger.info("quadrant profiles: items=%d", len(out))
    return out


#: Where the ``case_figures`` catalog entry writes, relative to the
#: diagnostics directory: the manifest's ``png`` paths are relative to that
#: directory, as they always were. The one place that knows the two catalog
#: entries sit side by side (``conf/base/catalog.yaml``).
_CASES_SUBDIR = "cases"


def _case_entry(meta_row, figure_path, case_label_cols):
    # manifest 鍵用實際 schema 欄名(泛用框架,不寫死銀行的 snap_date/cust)。
    # ``case_label_cols`` 見 ``compute_quadrant_cases``:identity 去掉 item。
    entry = {"rendered": True, "png": f"{_CASES_SUBDIR}/{figure_path}"}
    entry.update({c: str(meta_row[c]) for c in case_label_cols})
    entry.update({"rank": int(meta_row["rank"]), "score": float(meta_row["score"]),
                  "label": int(meta_row["label"])})
    return entry


def _case_title(item, quadrant, role, entry):
    return (f"{item} · {quadrant} · {role} · score={entry['score']:.3f}"
            f" · rank={entry['rank']} · label={entry['label']}")


def compute_quadrant_cases(
    model, case_rows, preprocessor: dict, parameters: dict,
) -> tuple[dict, dict]:
    """per-(item×象限) 全格極值案例的單列 signed SHAP 橫條圖 + 完整稽核 manifest。

    ``case_rows`` 為 ``select_shap_population`` 的第二輸出(每 item×象限 role=high/low
    各一列)。單次 SHAP over 那幾十列極值。空格記 ``reason=empty``;單行格 low 記
    ``reason=single_row_same_as_high``(不產重複檔)。

    Returns ``(cases_manifest, case_figures)``: the manifest
    ``{"<item>": {"<quadrant>": {"high"/"low": {rendered, png|reason, <identity
    columns but the item>, rank, score, label}}}}``, and ``{path under
    diagnostics/cases/: draw}`` for the charts. ``({}, {})`` for no case rows
    or ``quadrant_enabled: false``.

    ``rendered: True`` with a ``png`` says a chart for that case was handed to
    the catalog. The catalog draws it when it saves, so a chart that then
    fails to draw is a warning and a missing file (ADR-0030 decisions 4 and
    7); the manifest, written before any chart is drawn, cannot say
    ``render_failed`` any more. A model that cannot attribute → the "model
    cannot" shape and no charts; any other failure stops the run.
    """
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("quadrant_enabled", True):
        return {}, {}
    if case_rows is None or len(case_rows) == 0:
        logger.warning("quadrant cases: empty case_rows; skipping")
        return {}, {}

    from recsys_tfb.core.schema import get_schema

    case_top_k = int(cfg.get("case_top_k", 15))
    schema = get_schema(parameters)
    item_col = schema["item"]
    identity_cols = schema["identity_columns"]
    # manifest 標籤要指認「這張圖畫的是哪一列」,所以它是 identity——去掉 item
    # 只因為 item 已經是 manifest 的外層鍵,重複寫一次沒有資訊。
    #
    # 刻意不是 base key:base key 不隨 occasion 變寬(ADR-0025 決定 2),
    # 那會讓同一個 entity、同一個時段、不同場合的兩列拿到一模一樣的標籤——
    # 兩個不同的輸入映射成同一個結果。這裡從 identity 減一欄,identity 變寬
    # 它就跟著變寬。今天兩種寫法逐值相同,所以本次改動不動 manifest 的內容。
    case_label_cols = [c for c in identity_cols if c != item_col]
    # Decision — which features, and in what order: ask the model, not
    # apply_feature_selection(preprocessor, parameters). This is not a drift fix:
    # the exclude list lives in the `training:` block, so editing it bumps
    # model_version, the model's catalog path moves with it, and the whole
    # training chain is pulled back — ADR-0014 decision 7 is explicit that the
    # version mechanism already blocks that, and that this is interface work, not
    # a bug fix. What it buys is addressability: model and preprocessor both have
    # catalog entries, while the config-derived view is memory-only and drags
    # select_features into every diagnosis-only slice.
    model_view = model_feature_view(model, preprocessor)
    feature_cols = list(model_view["feature_columns"])

    pdf = case_rows.reset_index(drop=True)
    X = pdf_to_X(pdf, model_view, parameters)
    log_data_volume(logger, "cases.X", X)
    # Decision — a model that cannot attribute skips this with a warning and
    # says so; nothing else is caught (ADR-0030 decision 4).
    try:
        shap_values = model.feature_attributions(X)
    except UnsupportedCapability as exc:
        logger.warning("quadrant cases: skipped, the model cannot attribute: %s", exc)
        return unsupported_artifact(exc), {}

    items = pdf[item_col].values
    quads = pdf["quadrant"].values
    roles = pdf["role"].values

    def _gkey(i):
        return tuple(str(pdf.iloc[i][c]) for c in identity_cols)

    def _case(i, item, q, role):
        figure_path = f"{safe_name(item)}/{q}_{role}.png"
        entry = _case_entry(pdf.iloc[i], figure_path, case_label_cols)
        figures[figure_path] = signed_bars(
            shap_values[i], feature_cols, case_top_k,
            _case_title(item, q, role, entry))
        return entry

    manifest: dict = {}
    figures: dict = {}
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
                cell["high"] = _case(hi, item, q, "high")
            # low:單行格(與 high 同列)→ 不重畫;只有 high 的退化輸入 → low 記 empty
            if lo is None:
                cell["low"] = {"rendered": False, "reason": "empty"}
            elif hi is not None and _gkey(hi) == _gkey(lo):
                cell["low"] = {"rendered": False,
                               "reason": "single_row_same_as_high"}
            else:
                cell["low"] = _case(lo, item, q, "low")
            item_entry[q] = cell
        manifest[str(item)] = item_entry

    logger.info("quadrant cases: items=%d charts=%d", len(manifest), len(figures))
    return manifest, figures
