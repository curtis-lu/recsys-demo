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
    stage2_categorical_indices, stage2_matrix,
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
