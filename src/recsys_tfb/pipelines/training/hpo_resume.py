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
