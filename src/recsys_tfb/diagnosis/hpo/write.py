"""HPO 搜尋診斷入口：稽核 JSON + 摘要 + 5 張自動圖（寫進 diagnostics/hpo/）。"""

from __future__ import annotations

import logging

from recsys_tfb.diagnosis.hpo._io import atomic_write_json
from recsys_tfb.diagnosis.hpo.collect import collect_trials
from recsys_tfb.diagnosis.hpo.paths import hpo_dir
from recsys_tfb.diagnosis.hpo.render import render_charts
from recsys_tfb.diagnosis.hpo.summary import build_summary

logger = logging.getLogger(__name__)


def write_hpo_diagnostics(
    study, search_space, parameters, *,
    search_id, hpo_objective, seed, n_trials_target, best_iteration,
):
    """為（完成或 resume 的）study 寫 HPO 搜尋診斷。

    受 diagnostics.hpo_search.enabled 控制（預設 True）。先原子寫 hpo_trials.json +
    hpo_summary.json，再 render 5 張圖。設計為由 tune_hyperparameters 尾端 best-effort 呼叫。
    """
    cfg = (parameters.get("diagnostics") or {}).get("hpo_search") or {}
    if not cfg.get("enabled", True):
        logger.info("diagnostics.hpo_search.enabled=false; skip HPO diagnostics")
        return
    patience = int(cfg.get("patience", 10))
    hi = float(cfg.get("boundary_hi", 0.98))
    lo = float(cfg.get("boundary_lo", 0.02))

    out = hpo_dir(parameters)
    payload = collect_trials(
        study, search_space,
        model_version=parameters["model_version"], search_id=search_id,
        hpo_objective=hpo_objective, seed=seed,
        n_trials_target=n_trials_target, best_iteration=best_iteration,
    )
    atomic_write_json(out / "hpo_trials.json", payload)
    summary = build_summary(study, payload, patience=patience, hi_thresh=hi, lo_thresh=lo)
    atomic_write_json(out / "hpo_summary.json", summary)
    written = render_charts(study, out)
    logger.info("HPO diagnostics written to %s (%d charts)", out, len(written))
