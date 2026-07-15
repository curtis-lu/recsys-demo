"""從 Optuna Study 抽出自足的 trial 稽核基底（hpo_trials.json payload）。"""

from __future__ import annotations

import datetime as _dt

import optuna

SCHEMA_VERSION = 1


def _trial_row(t: optuna.trial.FrozenTrial) -> dict:
    return {
        "number": t.number,
        "value": t.value,
        "state": t.state.name,
        "params": dict(t.params),
        "duration_s": t.duration.total_seconds() if t.duration is not None else None,
    }


def collect_trials(
    study: optuna.Study,
    search_space: list,
    *,
    model_version: str,
    search_id: str,
    hpo_objective: str,
    seed: int,
    n_trials_target: int,
    best_iteration: int,
    generated_at: str | None = None,
) -> dict:
    """Build the self-contained hpo_trials.json payload from a live study."""
    completed = [
        t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE
    ]
    if generated_at is None:
        generated_at = _dt.datetime.now().isoformat(timespec="seconds")
    try:
        best = study.best_trial
        best_block = {
            "number": best.number,
            "value": best.value,
            "params": dict(best.params),
            "best_iteration": best_iteration,
        }
    except ValueError:  # no completed trials
        best_block = None
    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "model_version": str(model_version),
            "search_id": str(search_id),
            "hpo_objective": hpo_objective,
            "direction": study.direction.name.lower(),
            "sampler": type(study.sampler).__name__,
            "seed": seed,
            "n_trials_target": n_trials_target,
            "n_completed": len(completed),
            "search_space": search_space,
            "generated_at": generated_at,
        },
        "trials": [_trial_row(t) for t in study.trials],
        "best": best_block,
    }
