"""Train one stage-1 group model, optionally with per-group HPO.

Determinism contract (spec §3.1): sampler seed derives from
(random_seed, group_key); trials run SEQUENTIALLY inside a group
(parallelism only across groups); in-memory Optuna study — no resume,
interruption restarts the whole search.
"""

import logging
import time
from dataclasses import dataclass, field

import lightgbm as lgb
import numpy as np
import optuna

from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter
from recsys_tfb.models.staged._metrics import binary_auc, binary_logloss
from recsys_tfb.models.staged.partition import group_seed

logger = logging.getLogger(__name__)

# 靜音 per-trial INFO（每群 n_trials 條，N 群會刷爆 log）
optuna.logging.set_verbosity(optuna.logging.WARNING)


@dataclass
class GroupResult:
    group_key: str
    adapter: LightGBMAdapter
    best_params: dict
    score: float           # 該群 train_dev 上的原始 metric（auc 高好 / logloss 低好）
    metric: str
    n_rows: int
    n_pos: int
    train_seconds: float
    trial_values: list = field(default_factory=list)


def _fit_adapter(X_tr, y_tr, w_tr, X_dev, y_dev, params, categorical_indices):
    train_ds = lgb.Dataset(
        X_tr, label=y_tr, weight=w_tr,
        categorical_feature=categorical_indices, free_raw_data=False,
    )
    dev_ds = lgb.Dataset(
        X_dev, label=y_dev, reference=train_ds, free_raw_data=False,
    )
    adapter = LightGBMAdapter()
    adapter.train(
        X_tr, y_tr, X_dev, y_dev, dict(params),
        train_dataset=train_ds, val_dataset=dev_ds,
    )
    return adapter


def _score(metric: str, y_dev, preds) -> float:
    if metric == "logloss":
        return binary_logloss(y_dev, preds)
    return binary_auc(y_dev, preds)


def train_one_group(
    group_key: str,
    X_tr: np.ndarray, y_tr: np.ndarray, w_tr: np.ndarray,
    X_dev: np.ndarray, y_dev: np.ndarray, w_dev: np.ndarray,
    algorithm_params: dict,
    stage1_params: dict,
    hpo_cfg: dict,
    categorical_indices,
    base_seed: int,
) -> GroupResult:
    """Fixed-params train (n_trials=0) or sequential in-memory HPO then refit."""
    t0 = time.monotonic()
    metric = hpo_cfg.get("metric", "auc")
    n_trials = int(hpo_cfg.get("n_trials", 0))
    base_params = {**algorithm_params, **stage1_params,
                   "objective": "binary",
                   "seed": group_seed(base_seed, group_key)}

    best_params: dict = {}
    trial_values: list = []
    if n_trials > 0:
        # lazy import：search_space 機制屬 training pipeline，僅 HPO 用到
        from recsys_tfb.pipelines.training.search_space import build_trial_params

        search_space = hpo_cfg.get("search_space") or []
        sign = -1.0 if metric == "logloss" else 1.0

        def objective(trial):
            trial_params = build_trial_params(trial, search_space)
            trial_values.append(dict(trial_params))
            adapter = _fit_adapter(
                X_tr, y_tr, w_tr, X_dev, y_dev,
                {**base_params, **trial_params}, categorical_indices,
            )
            return sign * _score(metric, y_dev, adapter.predict(X_dev))

        study = optuna.create_study(
            direction="maximize",
            sampler=optuna.samplers.TPESampler(
                seed=group_seed(base_seed, group_key)),
        )
        study.optimize(objective, n_trials=n_trials, n_jobs=1)  # 群內序列
        best_params = dict(study.best_params)

    adapter = _fit_adapter(
        X_tr, y_tr, w_tr, X_dev, y_dev,
        {**base_params, **best_params}, categorical_indices,
    )
    score = _score(metric, y_dev, adapter.predict(X_dev))
    return GroupResult(
        group_key=group_key, adapter=adapter, best_params=best_params,
        score=float(score), metric=metric,
        n_rows=int(len(y_tr)), n_pos=int(np.asarray(y_tr).sum()),
        train_seconds=time.monotonic() - t0, trial_values=trial_values,
    )
