"""per-item SHAP 診斷 orchestrator。"""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_data_volume, log_step

from . import data_access
from ._util import _to_native
from .attribution import attribution_budget_units, feature_attributions
from .paths import per_item_summary_dir, safe_name, summary_dir
from .sampling import _positive_item_sample, _stratified_item_sample

logger = logging.getLogger(__name__)

_BACKGROUND_CAP = 128  # per-item interventional 背景列數上限；起手值，非 config 鍵


def _per_item_background(X_item, seed):
    """該 item 子母體（前景抽樣中屬於該 item 的列）當背景；超過 `_BACKGROUND_CAP`
    用固定 seed 的 RandomState 無放回抽樣至上限。"""
    n = len(X_item)
    if n <= _BACKGROUND_CAP:
        return X_item
    rng = np.random.RandomState(seed)
    idx = rng.choice(n, size=_BACKGROUND_CAP, replace=False)
    return X_item[idx]


def _signed_profile(sv_subset, feature_cols, top_k):
    """回傳 (top_features[含 signed], mean_abs 向量)。mean_abs 向量供後續 divergence 用（全特徵向量，非 top-k 過濾）。"""
    ai = np.abs(sv_subset).mean(axis=0)
    si = sv_subset.mean(axis=0)
    order = np.argsort(ai)[::-1][:top_k]
    profile = [{"feature": feature_cols[i],
                "mean_abs_shap": _to_native(ai[i]),
                "mean_signed_shap": _to_native(si[i])} for i in order]
    return profile, ai


def _rankdata(a):
    """升序整數 rank（0..n-1）；平手以 argsort 穩定排序位置決定（非 scipy 的中點 rank）。"""
    order = np.argsort(a)
    ranks = np.empty(len(a), dtype=float)
    ranks[order] = np.arange(len(a), dtype=float)
    return ranks


def _divergence(item_abs, global_abs, metric, k, feature_cols):
    """per-item |SHAP| 排序 vs 全域排序的偏離度（0=一致, 1=完全不同）。
    回傳 (divergence float, idiosyncratic_features list)。"""
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


def _positive_profiles(model, path, item_values, item_col, label_col, feature_cols,
                       take_cols, preprocessor, parameters, *, profile_positive,
                       per_item, min_rows, top_k):
    """針對正樣本(label==1)抽樣、單獨跑一次 SHAP,回傳每 item 的正例 profile。

    回傳 {item_str: (top_features_positive|None, n_positive, positive_low_coverage)}。
    profile_positive 關閉或資料無 label 欄 → 回傳 {}(呼叫端以預設處理)。與全域
    item 分層樣本解耦,避免稀疏正樣本 coverage 不足。
    """
    from recsys_tfb.io.extract import _pdf_to_X

    if not profile_positive or label_col not in data_access.schema_names(path):
        return {}
    all_labels = data_access.read_column(path, label_col)
    pos_idx = _positive_item_sample(item_values, all_labels, per_item, seed=42)
    if len(pos_idx) == 0:
        return {}
    pos_pdf = data_access.take_rows(path, pos_idx, columns=take_cols).reset_index(drop=True)
    log_data_volume(logger, "shap.positive_sample_pdf", pos_pdf, deep=True)
    X_pos = _pdf_to_X(pos_pdf, preprocessor, parameters)
    with log_step(logger, "shap_values_positive"):
        shap_pos = feature_attributions(model, X_pos, feature_cols)
    pos_items = pos_pdf[item_col].values
    out = {}
    for item in pd.unique(pos_items):
        m = pos_items == item
        n = int(m.sum())
        if n >= min_rows:
            prof, _ = _signed_profile(shap_pos[m], feature_cols, top_k)
            out[str(item)] = (prof, n, False)
        else:
            out[str(item)] = (None, n, True)
    return out


def compute_shap_diagnostics(model, test_parquet_handle, preprocessor: dict, parameters: dict) -> dict:
    """SHAP 全域 / per-item（族群代表）。單次 shap_values 兩用。"""
    cfg = parameters.get("diagnostics", {}).get("shap", {})
    if not cfg.get("enabled", True):
        return {}

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap

    from recsys_tfb.core.schema import get_schema
    from recsys_tfb.io.extract import _pdf_to_X

    top_k = int(cfg.get("top_k", 30))
    min_per_item = int(cfg.get("min_rows_per_item", 30))
    sample_rows = int(cfg.get("sample_rows", 2000))
    max_budget = int(cfg.get("max_budget", 4_000_000))
    positive_min_rows = int(cfg.get("positive_min_rows", 20))
    positive_sample_per_item = int(cfg.get("positive_sample_per_item", 30))
    divergence_metric = str(cfg.get("divergence_metric", "jaccard_topk"))
    divergence_top_k = int(cfg.get("divergence_top_k", 15))  # 通常比 top_k 小；只用於 Jaccard/idio 的 top-k 集合比較
    profile_positive = bool(cfg.get("profile_positive", True))
    background_mode = str(cfg.get("background", "global"))

    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    feature_cols = list(preprocessor["feature_columns"])

    path = test_parquet_handle.path

    n_trees = attribution_budget_units(model)
    eff_sample = sample_rows
    if eff_sample * max(1, n_trees) > max_budget:
        eff_sample = max(min_per_item, max_budget // max(1, n_trees))
        logger.warning(
            "shap budget guard: sample_rows %d * n_trees %d > max_budget %d -> reduce to %d",
            sample_rows, n_trees, max_budget, eff_sample,
        )

    # 只讀 item 分區欄做分層（避免全量物化 test）
    item_values = data_access.read_column(path, item_col)
    idx = _stratified_item_sample(item_values, eff_sample, min_per_item, seed=42)
    if len(idx) == 0:
        logger.warning("shap diagnostics: empty sample after stratification; skipping")
        return {}

    # 只取抽中的列 × (feature 欄 + item 欄 + label 欄)。生產上 item_col 通常即
    # categorical feature（已在 feature_cols 內），但診斷 fixture / cache 佈局未必；
    # 下游 per-item 分群需 sample_pdf[item_col]，故顯式確保 item/label 皆在 take_cols。
    names = data_access.schema_names(path)
    take_cols = list(feature_cols)
    for col in (item_col, label_col):
        if col in names and col not in take_cols:
            take_cols.append(col)
    sample_pdf = data_access.take_rows(path, idx, columns=take_cols).reset_index(drop=True)
    logger.info("shap diagnostics: n_total=%d n_sampled=%d n_cols=%d",
                len(item_values), len(sample_pdf), len(take_cols))
    log_data_volume(logger, "shap.sample_pdf", sample_pdf, deep=True)

    X = _pdf_to_X(sample_pdf, preprocessor, parameters)
    scores = model.predict(X)

    with log_step(logger, "shap_values"):
        shap_values = feature_attributions(model, X, feature_cols)

    # ---- per_item 能力探針（審查修復 2026-07-08）----
    # interventional TreeSHAP 需 shap 自行解析樹結構；shap 0.42.1 的 SingleTree
    # 以 float 陣列表示 threshold，無法表示 LightGBM 類別切分（"2||3||4"），而本
    # 框架模型必含 item 類別切點 → 真模型上必炸（實證：6059dcef 129/161 棵樹
    # SingleTree 解析失敗）。探針失敗＝整段降級回 global 行為＋notes 記錄
    # （best-effort，不炸訓練）。
    requested_background = background_mode
    degrade_note = None
    if background_mode == "per_item":
        try:
            probe = X[: min(len(X), 4)]
            feature_attributions(model, probe, feature_cols, background=probe,
                                 feature_perturbation="interventional")
        except Exception as exc:
            background_mode = "global"
            degrade_note = (
                "per_item 背景已降級為 global：interventional TreeSHAP 在目前"
                f"版本組合下無法解析類別切分（{type(exc).__name__}）。"
                "條件化背景不可行，見手冊已知限制。"
            )
            logger.warning("shap background=per_item 不可行，降級 global：%s", exc)

    # ---- 全域 ----
    global_top, mean_abs = _signed_profile(shap_values, feature_cols, top_k)

    # ---- 正例 profile（解耦的正樣本目標 sample B；獨立第二次 SHAP）----
    # per_item 背景模式下不跑這第二次全域 pass：正例 profile 改在下面迴圈內,
    # 直接從該 item 自己的 per-item SHAP 輸出切 label==1 列(見迴圈內註解)。
    if background_mode == "per_item":
        positive_profiles = {}
    else:
        positive_profiles = _positive_profiles(
            model, path, item_values, item_col, label_col, feature_cols, take_cols,
            preprocessor, parameters, profile_positive=profile_positive,
            per_item=positive_sample_per_item, min_rows=positive_min_rows, top_k=top_k)

    # ---- per-item（族群代表 + 覆蓋率 metadata）----
    items = sample_pdf[item_col].values
    label_present = background_mode == "per_item" and label_col in sample_pdf.columns
    labels = sample_pdf[label_col].values if label_present else None
    per_item = {}
    for item in pd.unique(items):
        mask = items == item
        if background_mode == "per_item":
            # 背景＝該 item 子母體（自己的前景列，上限 _BACKGROUND_CAP）；
            # interventional TreeSHAP。全域 top_features/divergence 的全域向量
            # 仍用 shap_values（global 背景）算，見下方 mean_abs 用法不變。
            X_item = X[mask]
            bg = _per_item_background(X_item, seed=42)
            with log_step(logger, "shap_values_per_item"):
                sv_item = feature_attributions(
                    model, X_item, feature_cols, background=bg,
                    feature_perturbation="interventional")
            prof_all, ai = _signed_profile(sv_item, feature_cols, top_k)
        else:
            sv_item = None
            prof_all, ai = _signed_profile(shap_values[mask], feature_cols, top_k)
        sc = scores[mask]
        # -- divergence vs global driver ranking --
        div, idio = _divergence(ai, mean_abs, divergence_metric, divergence_top_k, feature_cols)
        # -- positive-only profile --
        if background_mode == "per_item" and profile_positive and label_present:
            # 解耦的正樣本 sample B（_positive_profiles）在 per_item 模式下不跑；
            # 改用同一份 per-item 輸出中 label==1 的列（未額外抽樣，coverage 受限於
            # 前景抽樣本身，非 sample B 的針對性 oversampling）。
            pos_mask = labels[mask] == 1
            n_pos = int(pos_mask.sum())
            if n_pos >= positive_min_rows:
                prof_pos, _ = _signed_profile(sv_item[pos_mask], feature_cols, top_k)
                pos_low = False
            else:
                prof_pos, pos_low = None, True
        else:
            prof_pos, n_pos, pos_low = positive_profiles.get(
                str(item), (None, 0, bool(profile_positive)))
        per_item[str(item)] = {
            "top_features": prof_all,
            "n_sampled": int(mask.sum()),
            "n_positive": n_pos,
            "score_min": float(sc.min()), "score_max": float(sc.max()),
            "score_mean": float(sc.mean()),
            "low_coverage": bool(mask.sum() < min_per_item),
            "top_features_positive": prof_pos,
            "positive_low_coverage": bool(pos_low),
            "divergence_from_global": _to_native(div),
            "idiosyncratic_features": idio,
        }

    item_idiosyncrasy = sorted(
        ({"item": k,
          "divergence_from_global": v["divergence_from_global"],
          "idiosyncratic_features": v["idiosyncratic_features"]}
         for k, v in per_item.items()),
        key=lambda r: r["divergence_from_global"],
        reverse=True,
    )

    # ---- PNG（best-effort：繪圖失敗不應中斷診斷/訓練，spec §4）----
    sdir = summary_dir(parameters)
    try:
        plt.figure()
        try:
            shap.summary_plot(shap_values, features=X, feature_names=feature_cols, show=False)
            plt.tight_layout()
            plt.savefig(sdir / "shap_summary_global.png", dpi=100)
        finally:
            plt.close()
    except Exception as e:
        logger.warning("global shap summary plot failed: %s", e)

    if cfg.get("per_item_beeswarm", True):
        pdir = per_item_summary_dir(parameters)
        for item in pd.unique(items):
            m = items == item
            try:
                plt.figure()
                try:
                    shap.summary_plot(shap_values[m], features=X[m],
                                      feature_names=feature_cols, show=False)
                    plt.tight_layout()
                    plt.savefig(pdir / f"shap_summary__{safe_name(item)}.png", dpi=100)
                finally:
                    plt.close()
            except Exception as e:
                logger.warning("per-item beeswarm failed for item %s: %s", item, e)

    logger.info("shap diagnostics: n_sample=%d n_trees=%d items=%d",
                len(idx), n_trees, len(per_item))
    out = {"global": {"top_features": global_top}, "per_item": per_item,
           "item_idiosyncrasy": item_idiosyncrasy}
    if requested_background == "per_item":
        # global 模式的輸出 dict 不得多這個鍵（行為完全不變的宣稱），故只在
        # 「要求了 per_item」時才附加 notes——含降級情形（要求 per_item 但實際
        # 跑了 global，note 必須說明）。
        out["notes"] = [degrade_note] if degrade_note else [
            "shap background=per_item（interventional，背景=各 item 子母體，上限 128 列）；"
            "divergence 的全域向量仍為 global 背景——占比混入背景效應，判讀見手冊 §12"
        ]
    return out
