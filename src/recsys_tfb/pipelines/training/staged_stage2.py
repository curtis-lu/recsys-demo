"""train_stage2_model node: OOF stage-1 scores -> stage-2 model (PR-B).

Flow (spec §2.2 step 5): OOF gates (per group x fold trainability) ->
per-group K-fold OOF fits with that group's stage-1 best_params (train_dev
early stop) -> stage-2 matrix [X | oof_s1 | gcode] -> stage-2 fit/HPO,
early-stopped AND trial-scored on val (spec §2.2/§3.1; test untouched).

HPO reuses the shared path's persistent-study machinery by MODULE, not by
function: hpo_resume (journal study / resume / checkpoint), search_space,
nodes._hpo_score and write_hpo_diagnostics — tune_hyperparameters itself is
untouched (shared zero-regression). Stage-2 HPO reads the SAME flat
training.* keys as shared mode (spec §2.1), so compute_search_id's
"minus n_trials" semantics, --fresh-hpo and resume docs hold verbatim and
search_id covers only the stage-2 search (spec §3.2).

OOF checkpointing (PR-A 群級 checkpoint 同款，中斷成本考量): per-group OOF
score vectors under <wip_root>/<slug>/oof/ (scores.npy + meta.json +
_SUCCESS), keyed by model_version; restored only when n_rows/n_folds/seed
all match.
"""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import optuna

from recsys_tfb.core.logging import log_data_volume, log_peak_rss, log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.extract import (
    _composite_key_series, _pdf_to_X, _row_weights_from_pdf,
)
from recsys_tfb.models.staged.gates import check_oof_gates
from recsys_tfb.models.staged.oof import assign_folds, oof_is_leakage_clean
from recsys_tfb.models.staged.partition import (
    group_labels, group_seed, group_slug, routing_keys,
)
from recsys_tfb.models.staged.stage2 import (
    encode_group_codes, fit_stage2, group_code_lookup,
    stage2_categorical_indices, stage2_feature_names, stage2_matrix,
)
from recsys_tfb.models.staged.train_stage1 import _fit_adapter
from recsys_tfb.pipelines.training.staged import _wip_dir
from recsys_tfb.utils.spark import release_spark_session

logger = logging.getLogger(__name__)


def _group_oof(
    key, X_g, y_g, w_g, folds_g, X_dev_g, y_dev_g,
    params: dict, cat_idx, n_folds: int,
) -> np.ndarray:
    """OOF scores for ONE group's rows (group row order): each row scored by
    the booster that excluded its fold; leakage- and coverage-checked."""
    oof = np.full(len(y_g), np.nan, dtype=np.float64)
    producing = np.full(len(y_g), -1, dtype=np.int64)
    for k in range(int(n_folds)):
        pred_mask = folds_g == k
        if not pred_mask.any():
            continue
        fit_mask = ~pred_mask
        t0 = time.monotonic()
        adapter = _fit_adapter(
            X_g[fit_mask], y_g[fit_mask], w_g[fit_mask],
            X_dev_g, y_dev_g, dict(params), cat_idx,
        )
        oof[pred_mask] = adapter.predict(X_g[pred_mask])
        producing[pred_mask] = k
        # 心跳（Observability 要求 #1）：公司規模單折可達分鐘級，逐折留
        # 時間戳，長靜默段才能歸因到「哪群哪折」而不是被當成卡住。
        logger.info(
            "stage2 OOF group=%r fold=%d/%d fit_rows=%d pred_rows=%d (%.1fs)",
            key, k + 1, int(n_folds), int(fit_mask.sum()),
            int(pred_mask.sum()), time.monotonic() - t0,
        )
    if np.isnan(oof).any() or not oof_is_leakage_clean(folds_g, producing):
        raise RuntimeError(
            f"OOF integrity failed in group {key!r}: unscored rows or a row "
            "scored in-fold — this is a bug, not a data issue")
    return oof


def _load_oof_checkpoint(odir: Path, n_rows: int, n_folds: int, seed: int):
    if not (odir / "_SUCCESS").exists():
        return None
    meta = json.loads((odir / "meta.json").read_text())
    if (meta.get("n_rows"), meta.get("n_folds"), meta.get("seed")) != \
            (int(n_rows), int(n_folds), int(seed)):
        return None  # 形狀/設定不符 → 視同無 checkpoint（同 model_version 不應發生）
    return np.load(odir / "scores.npy")


def _write_oof_checkpoint(odir: Path, scores, n_rows, n_folds, seed) -> None:
    odir.mkdir(parents=True, exist_ok=True)
    np.save(odir / "scores.npy", np.asarray(scores))
    (odir / "meta.json").write_text(json.dumps(
        {"n_rows": int(n_rows), "n_folds": int(n_folds), "seed": int(seed)}))
    (odir / "_SUCCESS").touch()


def tune_stage2(
    mode: str, base_params: dict, cat_idx2,
    X2_tr, y_tr, w_tr, qg_tr,
    X2_val, y_val, w_val, qg_val, items_val,
    parameters: dict,
    *, feature_names2=None,
):
    """Persistent-study HPO for stage-2, or a single fit when n_trials==0.

    Returns (best_params, adapter, hpo_meta). Study/resume/--fresh-hpo/
    checkpoint semantics mirror tune_hyperparameters via hpo_resume; the
    loop itself is stage-2-specific (in-memory matrices, binary/lambdarank
    objective, val-scored trials with the shared ranking _hpo_score).
    """
    from recsys_tfb.pipelines.training import hpo_resume
    from recsys_tfb.pipelines.training.nodes import (
        HPO_OBJECTIVES, _hpo_score, _resolve_search_id,
    )
    from recsys_tfb.pipelines.training.search_space import build_trial_params

    training = parameters["training"]
    n_trials = int(training.get("n_trials", 0))
    if n_trials <= 0:
        adapter = fit_stage2(mode, X2_tr, y_tr, w_tr, qg_tr,
                             X2_val, y_val, w_val, qg_val, base_params,
                             cat_idx2, feature_names=feature_names2)
        return {}, adapter, {"n_trials": 0}

    hpo_objective = training.get("hpo_objective", "mean_ap")
    if hpo_objective not in HPO_OBJECTIVES:
        raise ValueError(
            f"unknown training.hpo_objective {hpo_objective!r}; "
            f"allowed: {', '.join(HPO_OBJECTIVES)}")
    search_space = training.get("search_space") or []
    seed = int(parameters.get("random_seed", 42))
    checkpointing = parameters.get("hpo_checkpointing", True)
    search_id = _resolve_search_id(parameters)
    study_dir = None
    best_state = {"score": -1.0, "model": None, "params": {}, "iteration": 0}

    def objective(trial):
        trial_params = build_trial_params(trial, search_space)
        # 起訖各一條（Observability 要求 #2；同 tune_hyperparameters 格式慣例）：
        # 公司規模單 trial＝一次全量 stage-2 訓練，start 行讓靜默段可歸因。
        logger.info("tune_stage2: trial=%d/%d start params=%s",
                    trial.number, n_trials, trial_params)
        t0 = time.monotonic()
        adapter = fit_stage2(
            mode, X2_tr, y_tr, w_tr, qg_tr, X2_val, y_val, w_val, qg_val,
            {**base_params, **trial_params}, cat_idx2,
            feature_names=feature_names2)
        score = _hpo_score(hpo_objective, qg_val, items_val, y_val,
                           adapter.predict(X2_val))
        if score > best_state["score"]:
            best_state.update(
                score=score, model=adapter, params=trial_params,
                iteration=adapter.booster.best_iteration)
            if checkpointing and study_dir is not None:
                hpo_resume.write_checkpoint(
                    study_dir, adapter, score=score,
                    best_iteration=adapter.booster.best_iteration,
                    best_params=trial_params, trial_number=trial.number,
                    search_id=search_id)
        logger.info(
            "tune_stage2: trial=%d/%d completed score=%.4f best=%.4f (%.1fs)",
            trial.number, n_trials, score, best_state["score"],
            time.monotonic() - t0)
        return score

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    if checkpointing:
        study_dir = hpo_resume.hpo_study_dir(search_id)
        if parameters.get("_fresh_hpo", False):
            logger.warning("--fresh-hpo: clearing %s", study_dir)
            hpo_resume.clear_study_dir(study_dir)
        study = hpo_resume.open_study(study_dir, search_id, seed)
        done = hpo_resume.count_completed(study)
        ckpt = hpo_resume.load_checkpoint(
            study_dir, training.get("algorithm", "lightgbm"))
        if ckpt is not None:
            best_state.update(score=ckpt["score"], model=ckpt["model"],
                              params=ckpt["params"],
                              iteration=ckpt["iteration"])
            logger.info(
                "stage-2 HPO resume: %d completed, best=%.4f; running %d "
                "more (target=%d)", done, ckpt["score"],
                max(0, n_trials - done), n_trials)
        remaining = max(0, n_trials - done)
    else:
        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(seed=seed))
        remaining = n_trials
    if remaining > 0:
        study.optimize(objective, n_trials=remaining)
    if best_state["model"] is None:
        # study 有 trial 但無可用 checkpoint 模型 → 補跑一次（shared 同款保底）
        study.enqueue_trial(study.best_params)
        study.optimize(objective, n_trials=1)

    # HPO 搜尋診斷：best-effort，失敗不影響回傳（同 nodes.py:605-614 慣例）
    try:
        from recsys_tfb.diagnosis.hpo import write_hpo_diagnostics
        write_hpo_diagnostics(
            study, search_space, parameters, search_id=search_id,
            hpo_objective=hpo_objective, seed=seed,
            n_trials_target=n_trials,
            best_iteration=best_state["iteration"])
    except Exception:
        logger.warning("stage-2 HPO diagnostics failed; training continues",
                       exc_info=True)

    meta = {"n_trials": n_trials, "search_id": search_id,
            "hpo_objective": hpo_objective,
            "score": float(best_state["score"]),
            "best_iteration": int(best_state["iteration"])}
    return dict(best_state["params"]), best_state["model"], meta


def train_stage2_model(
    stage1_model,
    stage1_groups_report: dict,
    train_parquet_handle,
    train_dev_parquet_handle,
    val_parquet_handle,
    preprocessor_view: dict,
    parameters: dict,
    wip_root=None,
):
    """Attach a stage-2 booster to the stage-1 adapter -> (model, report)."""
    # HPO 可能數小時；比照 tune_hyperparameters 首行主動釋放 Spark，
    # 由 predict 節點依 canonical configs 重建（nodes.py:408-419 同一理由）。
    release_spark_session(parameters)

    training = parameters["training"]
    staged_cfg = training["staged"]
    stage2_cfg = staged_cfg["stage2"]
    stage1_cfg = staged_cfg["stage1"]
    mode = stage2_cfg["mode"]
    n_folds = int(stage2_cfg.get("oof_folds", 5))
    partition_keys = stage1_model.partition_keys
    seed = int(parameters.get("random_seed", 42))
    schema = get_schema(parameters)
    label_col = schema["label"]
    wip = _wip_dir(parameters, wip_root)

    with log_step(logger, "stage2.load_train_frames"):
        pdf_tr = train_parquet_handle.to_pandas()
        pdf_dev = train_dev_parquet_handle.to_pandas()
    log_data_volume(logger, "stage2.pdf_tr", pdf_tr)
    log_data_volume(logger, "stage2.pdf_dev", pdf_dev)
    log_peak_rss(logger, "stage2.after_load_train_frames")
    labels_tr = group_labels(pdf_tr, partition_keys)
    labels_dev = group_labels(pdf_dev, partition_keys)
    y_tr_full = pdf_tr[label_col].values
    entity_tr = _composite_key_series(
        pdf_tr, list(schema["entity"])).to_numpy(dtype=object)
    folds = assign_folds(entity_tr, n_folds, seed)
    check_oof_gates(labels_tr, y_tr_full, folds, n_folds)

    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
    cat_idx = LightGBMAdapter._categorical_indices(preprocessor_view)
    algorithm_params = {
        **(training.get("algorithm_params") or {}),
        "num_iterations": training.get("num_iterations", 500),
        "early_stopping_rounds": training.get("early_stopping_rounds", 50),
    }
    best_by_group = {
        k: (v.get("best_params") or {})
        for k, v in (stage1_groups_report.get("groups") or {}).items()
    }
    group_keys = sorted(stage1_model.group_keys)
    tr_masks = {k: (labels_tr == k).to_numpy() for k in group_keys}
    dev_masks = {k: (labels_dev == k).to_numpy() for k in group_keys}

    def _one_group(key):
        odir = wip / group_slug(key) / "oof"
        g_mask = tr_masks[key]
        n_rows = int(g_mask.sum())
        cached = _load_oof_checkpoint(odir, n_rows, n_folds, seed)
        if cached is not None:
            return key, cached
        sub = pdf_tr.loc[g_mask]
        X_g = _pdf_to_X(sub, preprocessor_view, parameters)
        y_g = sub[label_col].values
        w_g = _row_weights_from_pdf(sub, parameters, preprocessor_view)
        sub_dev = pdf_dev.loc[dev_masks[key]]
        X_dev_g = _pdf_to_X(sub_dev, preprocessor_view, parameters)
        y_dev_g = sub_dev[label_col].values
        params = {**algorithm_params, **(stage1_cfg.get("params") or {}),
                  **best_by_group.get(key, {}),
                  "objective": "binary", "seed": group_seed(seed, key)}
        scores = _group_oof(key, X_g, y_g, w_g, folds[g_mask],
                            X_dev_g, y_dev_g, params, cat_idx, n_folds)
        _write_oof_checkpoint(odir, scores, n_rows, n_folds, seed)
        return key, scores

    max_workers = max(1, int(stage1_cfg.get("max_workers", 1)))
    logger.info("train_stage2_model: OOF %d group(s) x %d fold(s), "
                "max_workers=%d", len(group_keys), n_folds, max_workers)
    with log_step(logger, "stage2.oof"):
        if max_workers == 1:
            pairs = [_one_group(k) for k in group_keys]
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                pairs = list(pool.map(_one_group, group_keys))
    oof = np.full(len(pdf_tr), np.nan, dtype=np.float64)
    for key, scores in pairs:
        oof[tr_masks[key]] = scores
    if np.isnan(oof).any():
        # 不變量顯式化（最終審查建議）：train 每列的群都該在 group_keys 內；
        # NaN 流進 stage2_matrix 會被 LightGBM 當 missing value 靜默吞掉。
        raise RuntimeError(
            "OOF assembly left NaN rows — some train rows belong to no "
            "stage-1 group (group set drifted between nodes?)")
    log_peak_rss(logger, "stage2.after_oof")

    # ---- stage-2 訓練矩陣（[X | oof_s1 | gcode]，spec D4）----
    with log_step(logger, "stage2.build_matrices"):
        X_tr = _pdf_to_X(pdf_tr, preprocessor_view, parameters)
        w_tr = _row_weights_from_pdf(pdf_tr, parameters, preprocessor_view)
        lookup = group_code_lookup(group_keys)
        g_tr = encode_group_codes(routing_keys(pdf_tr, partition_keys), lookup)
        X2_tr = stage2_matrix(X_tr, oof, g_tr)
        n_base = X_tr.shape[1]
        del X_tr  # X2 已複製，釋放一份全量矩陣（spec §8 記憶體注意）
        qcols = [schema["time"], *schema["entity"]]
        qg_tr = pdf_tr.groupby(qcols, sort=False).ngroup().to_numpy(np.int64)

        pdf_val = val_parquet_handle.to_pandas()
        log_data_volume(logger, "stage2.pdf_val", pdf_val)
        X_v = _pdf_to_X(pdf_val, preprocessor_view, parameters)
        rk_val = routing_keys(pdf_val, partition_keys)
        # val 缺群＝異常（評估語意，spec D11 分流）→ on_missing="raise"
        s1_v, _mask = stage1_model.predict_routed(X_v, rk_val,
                                                  on_missing="raise")
        X2_v = stage2_matrix(X_v, s1_v, encode_group_codes(rk_val, lookup))
        del X_v
        y_v = pdf_val[label_col].values
        w_v = _row_weights_from_pdf(pdf_val, parameters, preprocessor_view)
        qg_v = pdf_val.groupby(qcols, sort=False).ngroup().to_numpy(np.int64)
        items_v = pdf_val[schema["item"]].to_numpy()
    log_data_volume(logger, "stage2.X2_tr", X2_tr)
    log_data_volume(logger, "stage2.X2_val", X2_v)
    log_peak_rss(logger, "stage2.after_train_matrix")

    from recsys_tfb.core.group_utils import default_metric_for_objective
    objective_name = "binary" if mode == "binary" else "lambdarank"
    stage2_params = dict(stage2_cfg.get("params") or {})
    base2 = {**algorithm_params, **stage2_params,
             "objective": objective_name, "seed": seed}
    # stage2.mode 是 objective 的唯一真實來源（與 stage-1 覆蓋行為對稱）；
    # ranking 下沿用 binary metric 會讓 early stopping 靜默失義 → 補 ndcg。
    base2["metric"] = default_metric_for_objective(
        objective_name, stage2_params.get("metric"))
    if not base2["metric"]:
        base2["metric"] = algorithm_params.get("metric")

    # _categorical_indices 無類別欄時回傳 None（lgb.Dataset 接受 None，
    # stage-1/OOF 路徑因此安全）；stage2_categorical_indices 自己處理
    # None（視同空列表），呼叫端不必再正規化。
    cat_idx2 = stage2_categorical_indices(cat_idx, n_base)
    with log_step(logger, "stage2.tune"):
        best_params2, s2_adapter, hpo_meta = tune_stage2(
            mode, base2, cat_idx2, X2_tr, y_tr_full, w_tr, qg_tr,
            X2_v, y_v, w_v, qg_v, items_v, parameters,
            feature_names2=stage2_feature_names(
                list(preprocessor_view["feature_columns"])))
    log_peak_rss(logger, "stage2.after_tune")

    stage2_meta = {"mode": mode, "oof_folds": n_folds,
                   "best_params": best_params2, **hpo_meta}
    stage1_model.set_stage2(s2_adapter, stage2_meta)
    report = {"mode": mode, "oof_folds": n_folds,
              "oof_rows": int(len(oof)),
              "n_groups": len(group_keys),
              "best_params": best_params2, **hpo_meta}
    logger.info("train_stage2_model: mode=%s folds=%d best_params=%s",
                mode, n_folds, best_params2)
    return stage1_model, report
