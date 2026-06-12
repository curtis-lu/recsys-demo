"""Persistent HPO study + best-model checkpoint for crash-resumable tuning.

Keyed by ``search_id`` (recsys_tfb.core.versioning.compute_search_id) so a
crashed HPO run resumes only the remaining trials, and bumping
``training.n_trials`` extends the same search. Storage + checkpoint live under
``data/models/_hpo/<search_id>/`` (driver-local; same persistence guarantee as
the model_version artifacts).
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import optuna

logger = logging.getLogger(__name__)

JOURNAL = "study_journal.log"
CHECKPOINT_MODEL = "model.txt"
CHECKPOINT_META = "best_meta.json"


def hpo_study_dir(search_id: str) -> Path:
    """data/models/_hpo/<search_id>/ (relative; mirrors diagnostics_dir 慣例)."""
    return Path("data") / "models" / "_hpo" / str(search_id)


def open_study(study_dir: Path, search_id: str, seed: int) -> optuna.Study:
    """Open (or create) the persistent maximize study for this search_id."""
    study_dir.mkdir(parents=True, exist_ok=True)
    backend = optuna.storages.journal.JournalFileBackend(str(study_dir / JOURNAL))
    storage = optuna.storages.journal.JournalStorage(backend)
    return optuna.create_study(
        storage=storage,
        study_name=search_id,
        load_if_exists=True,
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed),
    )


def count_completed(study: optuna.Study) -> int:
    """Number of COMPLETE trials already recorded."""
    return sum(
        1 for t in study.get_trials(deepcopy=False)
        if t.state == optuna.trial.TrialState.COMPLETE
    )


def clear_study_dir(study_dir: Path) -> None:
    """Remove the study_dir subtree (--fresh-hpo). No-op if absent."""
    if study_dir.exists():
        shutil.rmtree(study_dir, ignore_errors=True)


def write_checkpoint(
    study_dir: Path,
    adapter,
    *,
    score: float,
    best_iteration: int,
    best_params: dict,
    trial_number: int,
    search_id: str,
) -> None:
    """Atomically persist current best adapter + meta under study_dir/checkpoint/."""
    ckpt = study_dir / "checkpoint"
    ckpt.mkdir(parents=True, exist_ok=True)

    tmp_model = ckpt / (CHECKPOINT_MODEL + ".tmp")
    adapter.save(str(tmp_model))
    os.replace(tmp_model, ckpt / CHECKPOINT_MODEL)

    meta = {
        "score": float(score),
        "best_iteration": int(best_iteration),
        "best_params": best_params,
        "trial_number": int(trial_number),
        "search_id": search_id,
    }
    fd, tmp_meta = tempfile.mkstemp(dir=str(ckpt), suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False, default=str)
    os.replace(tmp_meta, ckpt / CHECKPOINT_META)


def load_checkpoint(study_dir: Path, algorithm: str) -> Optional[dict]:
    """Load best-so-far checkpoint; None if absent/unreadable.

    Returns {score, iteration, params, trial_number, model(ModelAdapter)}.
    ``iteration`` 取自 meta（重載的 LightGBM booster 不保證保留 best_iteration）。
    """
    from recsys_tfb.models.base import get_adapter

    ckpt = study_dir / "checkpoint"
    meta_path = ckpt / CHECKPOINT_META
    model_path = ckpt / CHECKPOINT_MODEL
    if not (meta_path.exists() and model_path.exists()):
        return None
    adapter = get_adapter(algorithm)  # config error (unknown algorithm) must fail loud
    try:
        with open(meta_path) as f:
            meta = json.load(f)
        adapter.load(str(model_path))
    except Exception:
        logger.warning("HPO checkpoint unreadable at %s; ignoring", ckpt, exc_info=True)
        return None
    return {
        "score": float(meta["score"]),
        "iteration": int(meta["best_iteration"]),
        "params": meta.get("best_params", {}),
        "trial_number": int(meta.get("trial_number", -1)),
        "model": adapter,
    }
