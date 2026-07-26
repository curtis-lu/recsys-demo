"""staged 模型診斷：分派 helper、Stage-1 總覽表、per-group runner。

分派契約（D-C3）：duck-typing，不 import staged 型別做 isinstance——
``LightGBMAdapter`` 沒有 ``predict_routed``／``stage2_mode``，shared 路徑
逐位元不變。
"""

import json
import logging
import time

import numpy as np

from recsys_tfb.core.logging import log_step

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


def compute_staged_group_diagnostics(model, train_parquet_handle,
                                     test_parquet_handle, preprocessor: dict,
                                     parameters: dict) -> dict:
    """stage2=none：每群核心四件（feature stats／importance／gain ledger／
    SHAP summary），落 diagnostics/groups/<slug>/，回傳 manifest。

    範圍裁決（2026-07-26，偏離 spec §6「完整」）：象限系列不做 per-group 版。
    單群失敗隔離記 error 不中斷（比照 quadrant best-effort 慣例）；但 partition
    key 欄缺失是 schema 問題 → fail-loud。
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import shap as shap_pkg

    from recsys_tfb.io.extract import _pdf_to_X
    from recsys_tfb.models.staged.partition import group_slug, routing_keys

    from . import data_access
    from .attribution import attribution_budget_units, feature_attributions
    from .feature_stats import _stats_from_pdf
    from .gain_ledger import compute_gain_ledger
    from .importance import compute_feature_importance
    from .paths import staged_group_dir
    from .shap_per_item import _signed_profile

    shap_cfg = parameters.get("diagnostics", {}).get("shap", {})
    fs_cfg = parameters.get("diagnostics", {}).get("feature_stats", {})
    top_k = int(shap_cfg.get("top_k", 30))
    shap_rows = int(shap_cfg.get("sample_rows", 2000))
    max_budget = int(shap_cfg.get("max_budget", 4_000_000))
    fs_rows = int(fs_cfg.get("sample_rows", 500000))
    high_null = float(fs_cfg.get("high_null_threshold", 0.5))
    feature_cols = list(preprocessor["feature_columns"])
    pkeys = list(model.partition_keys)

    def _keys_of(path):
        # 只讀 partition key 欄；routing_keys 走與訓練同一 str-join 慣例。
        # 缺欄在群迴圈之外直接炸（schema 問題，fail-loud）。
        import pandas as pd
        cols = {c: data_access.read_column(path, c) for c in pkeys}
        return routing_keys(pd.DataFrame(cols), pkeys)

    train_path = train_parquet_handle.path
    test_path = test_parquet_handle.path
    keys_tr = _keys_of(train_path)
    keys_te = _keys_of(test_path)

    manifest: dict = {"partition_keys": pkeys, "groups": {}}
    with log_step(logger, "staged_group_diagnostics"):
        for key in model.group_keys:
            slug = group_slug(key)
            t0 = time.monotonic()
            entry = {"group": key, "error": None}
            try:
                adapter = model._groups[key]
                gdir = staged_group_dir(parameters, slug)
                # 1) importance（真特徵名，Batch A 的果實）
                imp = compute_feature_importance(adapter, parameters)
                (gdir / "feature_importance.json").write_text(
                    json.dumps(imp, ensure_ascii=False, indent=1))
                # 2) gain ledger（函式只回 dict、不落檔 → 自己寫）
                ledger = compute_gain_ledger(adapter, preprocessor, parameters)
                (gdir / "gain_ledger.json").write_text(
                    json.dumps(ledger, ensure_ascii=False, indent=1))
                # 3) per-group train feature stats
                tr_idx = np.flatnonzero(keys_tr == key)
                entry["n_train_rows"] = int(tr_idx.size)
                if tr_idx.size > fs_rows:
                    tr_idx = np.sort(np.random.RandomState(42).choice(
                        tr_idx, size=fs_rows, replace=False))
                pdf_tr = data_access.take_rows(train_path, tr_idx, columns=feature_cols)
                stats = _stats_from_pdf(pdf_tr, feature_cols, high_null)
                (gdir / "feature_statistics.json").write_text(
                    json.dumps(stats, ensure_ascii=False, indent=1))
                # 4) SHAP summary（test 側、該群列；每群上限 shap.sample_rows，
                #    再受 max_budget 樹數預算閘限制——比照 shared 版
                #    compute_shap_diagnostics 的公式，見 shap_per_item.py:136-143）
                n_trees = attribution_budget_units(adapter)
                eff_shap_rows = shap_rows
                if eff_shap_rows * max(1, n_trees) > max_budget:
                    eff_shap_rows = max(1, max_budget // max(1, n_trees))
                    logger.warning(
                        "staged shap budget guard: group=%s sample_rows %d * "
                        "n_trees %d > max_budget %d -> reduce to %d",
                        slug, shap_rows, n_trees, max_budget, eff_shap_rows,
                    )
                te_idx = np.flatnonzero(keys_te == key)
                if te_idx.size > eff_shap_rows:
                    te_idx = np.sort(np.random.RandomState(42).choice(
                        te_idx, size=eff_shap_rows, replace=False))
                entry["n_shap_sampled"] = int(te_idx.size)
                if te_idx.size:
                    pdf_te = data_access.take_rows(test_path, te_idx,
                                                   columns=feature_cols)
                    X_g = _pdf_to_X(pdf_te, preprocessor, parameters)
                    sv = feature_attributions(adapter, X_g, feature_cols)
                    prof, _ = _signed_profile(sv, feature_cols, top_k)
                    (gdir / "shap_top_features.json").write_text(
                        json.dumps({"top_features": prof}, ensure_ascii=False,
                                   indent=1))
                    try:  # 圖 best-effort（比照 shap_per_item 慣例）
                        plt.figure()
                        try:
                            shap_pkg.summary_plot(sv, features=X_g,
                                                  feature_names=feature_cols,
                                                  show=False)
                            plt.tight_layout()
                            plt.savefig(gdir / "shap_summary.png", dpi=100)
                        finally:
                            plt.close()
                    except Exception as e:
                        logger.warning("group %s beeswarm failed: %s", slug, e)
            except Exception as e:  # 單群隔離：一群壞不拖垮其他群
                logger.warning("staged group diagnostics failed for %r: %s",
                               key, e)
                entry["error"] = f"{type(e).__name__}: {e}"
            entry["seconds"] = round(time.monotonic() - t0, 2)
            # 心跳：公司規模群多，逐群留時間戳（PR-B observability 同理由）
            logger.info("staged diagnostics group=%r slug=%s (%.1fs) error=%s",
                        key, slug, entry["seconds"], entry["error"])
            manifest["groups"][slug] = entry
    return manifest
