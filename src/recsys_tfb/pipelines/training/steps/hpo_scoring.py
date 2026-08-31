"""How one HPO trial is scored, and who owns the best-so-far model.

``TrialScorer`` is handed straight to ``optuna.Study.optimize`` — Optuna only
requires a callable. It replaces a nested ``objective`` closure inside
``tune_hyperparameters`` that captured a dozen locals and reached back out to
mutate a ``best_state`` dict defined beside it. Same trial, same arithmetic;
what changes is that the search state is an attribute of the object named for
owning it, so a reader can see from the signature what the search carries.

**Nothing here unlocks parallel HPO, and the closure was never what blocked
it.** ``best["model"]`` is the trained ``ModelAdapter`` itself (the LightGBM
booster hangs off its ``.booster``), sitting in the driver's Python heap: under
multiprocessing each worker would refresh its own copy while the parent's stays
``None``, and ``tune_hyperparameters``'s last-resort branch would then quietly
refit ``study.best_params`` once — a whole extra training run, every run, in
the one setup whose point was to be faster. The real blocker, and the route
around it (read the winner back from the on-disk checkpoint rather than out of
memory), are recorded in ``docs/agents/architecture-constraints.md`` F3 —
including why today's checkpoint cannot carry that weight as written.
"""

import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
import optuna

from recsys_tfb.core.consistency import HPO_OBJECTIVES
from recsys_tfb.core.logging import log_data_volume, log_step
from recsys_tfb.evaluation.metrics import (
    compute_macro_per_item_map,
    compute_mean_ap,
)
from recsys_tfb.models.base import get_adapter
from recsys_tfb.pipelines.training.steps.hpo_resume import write_checkpoint
from recsys_tfb.pipelines.training.steps.search_space import build_trial_params

logger = logging.getLogger(__name__)


def _hpo_score(
    objective_name: str,
    groups: np.ndarray,
    items: Optional[np.ndarray],
    y_true: np.ndarray,
    y_score: np.ndarray,
) -> float:
    """Score val predictions for one HPO trial under the chosen objective.

    ``mean_ap``            — per-query mAP (``items`` unused).
    ``macro_per_item_map`` — macro average of per-item attributed mAP.

    Unknown ``objective_name`` raises ``ValueError``: a **pre-check** on the
    value handed in. A25 rejects the same value at CLI entry, so reaching this
    line means the caller assembled ``parameters`` without passing that gate.
    """
    if objective_name == "mean_ap":
        return compute_mean_ap(groups, y_true, y_score)
    if objective_name == "macro_per_item_map":
        return compute_macro_per_item_map(groups, items, y_true, y_score)
    raise ValueError(
        f"unknown training.hpo_objective {objective_name!r}; "
        f"allowed: {', '.join(HPO_OBJECTIVES)}"
    )


class TrialScorer:
    """Train one candidate, score it on val, and keep the search's winner.

    Everything the search needs for a trial is fixed at construction; the only
    thing that changes across calls is ``best``, this instance's record of the
    winning trial so far (``score`` / ``model`` / ``iteration`` / ``params``).
    ``tune_hyperparameters`` reads it back off the instance once the study is
    done — the model never travels through Optuna, which only ever sees the
    float this returns.

    ``study_dir=None`` means "do not checkpoint". It is the same condition as
    ``hpo_checkpointing: false``: without a study directory there is nowhere to
    refresh, and a crash simply costs the whole search.

    ``train_dev`` is the early-stopping val set for every trial while
    ``X_val`` / ``y_val`` decide the reported score — two different sets on
    purpose, so the number a trial is judged by is not the number its
    early stopping optimised against.
    """

    def __init__(
        self,
        *,
        train_lgb_handle,
        train_dev_lgb_handle,
        X_val,
        y_val: np.ndarray,
        groups_val: np.ndarray,
        items_val: Optional[np.ndarray],
        algorithm: str,
        algorithm_params: dict,
        search_space: dict,
        hpo_objective: str,
        seed: int,
        num_iterations: int,
        early_stopping_rounds: int,
        n_trials: int,
        search_id: str,
        study_dir: Optional[Path],
    ) -> None:
        self.train_lgb_handle = train_lgb_handle
        self.train_dev_lgb_handle = train_dev_lgb_handle
        self.X_val = X_val
        self.y_val = y_val
        self.groups_val = groups_val
        self.items_val = items_val
        self.algorithm = algorithm
        self.algorithm_params = algorithm_params
        self.search_space = search_space
        self.hpo_objective = hpo_objective
        self.seed = seed
        self.num_iterations = num_iterations
        self.early_stopping_rounds = early_stopping_rounds
        self.n_trials = n_trials
        self.search_id = search_id
        self.study_dir = study_dir
        self.best: dict = {
            "score": -1.0, "model": None, "iteration": 0, "params": {},
        }

    def adopt_checkpoint(self, checkpoint: dict) -> None:
        """Take over a previous run's winner when resuming a study.

        Without this the resumed search starts from ``score=-1.0``, so its
        first trial wins by default and replaces a possibly better model from
        the earlier run — and then checkpoints over it. No error either way: a
        worse model is still a valid model.
        """
        self.best.update(
            score=checkpoint["score"],
            model=checkpoint["model"],
            iteration=checkpoint["iteration"],
            params=checkpoint["params"],
        )

    def __call__(self, trial: optuna.Trial) -> float:
        trial_idx = trial.number
        trial_params = build_trial_params(trial, self.search_space)

        params = {
            **self.algorithm_params,
            "seed": self.seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": self.num_iterations,
            "early_stopping_rounds": self.early_stopping_rounds,
        }

        logger.info(
            "tune_hyperparameters: trial=%d/%d start params=%s",
            trial_idx, self.n_trials, trial_params,
        )
        t0 = time.monotonic()

        adapter = get_adapter(self.algorithm)
        construct_params = {"feature_pre_filter": False}
        with log_step(logger, "prepare_datasets"):
            ds_train = self.train_lgb_handle.load(params=construct_params).construct()
            ds_dev = self.train_dev_lgb_handle.load(
                reference=ds_train, params=construct_params
            ).construct()
        log_data_volume(logger, "tune.ds_train", ds_train)
        log_data_volume(logger, "tune.ds_dev", ds_dev)

        with log_step(logger, "train"):
            adapter.train(
                X_train=None, y_train=None, X_val=None, y_val=None,
                params=params,
                train_dataset=ds_train, val_dataset=ds_dev,
            )

        with log_step(logger, "predict"):
            y_pred = adapter.predict(self.X_val)

        with log_step(logger, "score"):
            score = _hpo_score(
                self.hpo_objective, self.groups_val, self.items_val,
                self.y_val, y_pred,
            )

        if score > self.best["score"]:
            self.best["score"] = score
            self.best["model"] = adapter
            self.best["iteration"] = adapter.booster.best_iteration
            self.best["params"] = trial_params
            if self.study_dir is not None:
                write_checkpoint(
                    self.study_dir, adapter,
                    score=score, best_iteration=adapter.booster.best_iteration,
                    best_params=trial_params, trial_number=trial_idx,
                    search_id=self.search_id,
                )

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed score=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, self.n_trials, score,
            adapter.booster.best_iteration, duration, self.best["score"],
        )

        return score
