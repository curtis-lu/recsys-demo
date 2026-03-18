"""Pure functions for the training pipeline."""

import json
import logging
import re
from pathlib import Path

import lightgbm as lgb
import mlflow
import numpy as np
import optuna
import pandas as pd

logger = logging.getLogger(__name__)


from recsys_tfb.evaluation.metrics import compute_ap


def _compute_map(
    y_true: np.ndarray,
    y_score: np.ndarray,
    groups: pd.DataFrame,
) -> tuple[float, int]:
    """Compute mAP over query groups defined by (snap_date, cust_id).

    Returns (mAP, num_excluded_queries).
    """
    aps = []
    n_excluded = 0

    for _, idx in groups.groupby(["snap_date", "cust_id"]).groups.items():
        idx_arr = idx.values if hasattr(idx, "values") else np.array(idx)
        ap = compute_ap(y_true[idx_arr], y_score[idx_arr])
        if ap is None:
            n_excluded += 1
        else:
            aps.append(ap)

    mean_ap = float(np.mean(aps)) if aps else 0.0
    return mean_ap, n_excluded


def tune_hyperparameters(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_train_dev: pd.DataFrame,
    y_train_dev: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Search for optimal LightGBM hyperparameters using Optuna."""
    training_params = parameters["training"]
    n_trials = training_params["n_trials"]
    search_space = training_params["search_space"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)

    train_data = lgb.Dataset(X_train, label=y_train["label"].values, free_raw_data=False)
    dev_data = lgb.Dataset(X_train_dev, label=y_train_dev["label"].values, reference=train_data, free_raw_data=False)

    def objective(trial: optuna.Trial) -> float:
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "verbosity": -1,
            "seed": seed,
            "feature_pre_filter": False,
            "learning_rate": trial.suggest_float(
                "learning_rate",
                search_space["learning_rate"]["low"],
                search_space["learning_rate"]["high"],
                log=True,
            ),
            "num_leaves": trial.suggest_int(
                "num_leaves",
                search_space["num_leaves"]["low"],
                search_space["num_leaves"]["high"],
            ),
            "max_depth": trial.suggest_int(
                "max_depth",
                search_space["max_depth"]["low"],
                search_space["max_depth"]["high"],
            ),
            "min_child_samples": trial.suggest_int(
                "min_child_samples",
                search_space["min_child_samples"]["low"],
                search_space["min_child_samples"]["high"],
            ),
            "subsample": trial.suggest_float(
                "subsample",
                search_space["subsample"]["low"],
                search_space["subsample"]["high"],
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                search_space["colsample_bytree"]["low"],
                search_space["colsample_bytree"]["high"],
            ),
        }

        callbacks = [
            lgb.early_stopping(stopping_rounds=early_stopping_rounds),
            lgb.log_evaluation(period=0),
        ]
        booster = lgb.train(
            params,
            train_data,
            num_boost_round=num_iterations,
            valid_sets=[dev_data],
            valid_names=["train_dev"],
            callbacks=callbacks,
        )

        y_pred = booster.predict(X_train_dev)
        # Use a simple mAP: treat entire dev set as one query for tuning speed
        ap = compute_ap(y_train_dev["label"].values, y_pred)
        return ap if ap is not None else 0.0

    sampler = optuna.samplers.TPESampler(seed=seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study.optimize(objective, n_trials=n_trials)

    best_params = study.best_params
    logger.info("Best trial mAP: %.4f, params: %s", study.best_value, best_params)
    return best_params


def train_model(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_train_dev: pd.DataFrame,
    y_train_dev: pd.DataFrame,
    best_params: dict,
    parameters: dict,
) -> lgb.Booster:
    """Train a LightGBM binary classifier with early stopping."""
    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)

    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "verbosity": -1,
        "seed": seed,
        **best_params,
    }

    train_data = lgb.Dataset(X_train, label=y_train["label"].values)
    dev_data = lgb.Dataset(X_train_dev, label=y_train_dev["label"].values, reference=train_data)

    callbacks = [
        lgb.early_stopping(stopping_rounds=early_stopping_rounds),
        lgb.log_evaluation(period=50),
    ]
    booster = lgb.train(
        params,
        train_data,
        num_boost_round=num_iterations,
        valid_sets=[dev_data],
        valid_names=["train_dev"],
        callbacks=callbacks,
    )

    logger.info(
        "Model trained: %d iterations (best: %d)",
        booster.current_iteration(),
        booster.best_iteration,
    )
    return booster


def evaluate_model(
    model: lgb.Booster,
    X_val: pd.DataFrame,
    y_val: pd.DataFrame,
    val_set: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking-aware mAP with (snap_date, cust_id) as query groups."""
    y_score = model.predict(X_val)

    y_val_arr = y_val["label"].values
    groups = val_set[["snap_date", "cust_id"]].reset_index(drop=True)
    overall_map, n_excluded = _compute_map(y_val_arr, y_score, groups)

    # Per-product AP
    per_product_ap = {}
    for prod_name, idx in val_set.groupby("prod_name").groups.items():
        idx_arr = idx.values if hasattr(idx, "values") else np.array(idx)
        y_true_prod = y_val_arr[idx_arr]
        y_score_prod = y_score[idx_arr]
        ap = compute_ap(y_true_prod, y_score_prod)
        if ap is not None:
            per_product_ap[prod_name] = ap

    evaluation_results = {
        "overall_map": overall_map,
        "per_product_ap": per_product_ap,
        "n_queries": len(groups.drop_duplicates()),
        "n_excluded_queries": n_excluded,
    }

    logger.info(
        "Evaluation: mAP=%.4f, products=%d, excluded_queries=%d",
        overall_map,
        len(per_product_ap),
        n_excluded,
    )
    return evaluation_results


def log_experiment(
    model: lgb.Booster,
    best_params: dict,
    evaluation_results: dict,
    parameters: dict,
) -> None:
    """Log training results to MLflow."""
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metric("overall_map", evaluation_results["overall_map"])

        for prod, ap in evaluation_results.get("per_product_ap", {}).items():
            mlflow.log_metric(f"ap_{prod}", ap)

        mlflow.log_metric("n_queries", evaluation_results["n_queries"])
        mlflow.log_metric("n_excluded_queries", evaluation_results["n_excluded_queries"])

        mlflow.lightgbm.log_model(model, artifact_path="model")

    logger.info("MLflow experiment logged: %s", experiment_name)


_VERSION_TIMESTAMP_RE = re.compile(r"^\d{8}_\d{6}$")
_VERSION_HASH_RE = re.compile(r"^[0-9a-f]{8}$")


def _is_version_dir(name: str) -> bool:
    """Check if a directory name matches a known version format."""
    return bool(_VERSION_TIMESTAMP_RE.match(name) or _VERSION_HASH_RE.match(name))


def compare_model_versions(evaluation_results: dict, parameters: dict) -> dict:
    """Scan versioned model directories and produce a cross-version mAP comparison report."""
    models_dir = Path(parameters.get("models_dir", "data/models"))

    # Find version directories matching hash or timestamp formats
    versions = []
    if models_dir.is_dir():
        for d in sorted(models_dir.iterdir(), reverse=True):
            if d.is_dir() and not d.is_symlink() and _is_version_dir(d.name):
                eval_path = d / "evaluation_results.json"
                if eval_path.exists():
                    with open(eval_path) as f:
                        results = json.load(f)
                    versions.append({
                        "version": d.name,
                        "overall_map": results.get("overall_map", 0.0),
                        "per_product_ap": results.get("per_product_ap", {}),
                    })

    # Sort by mAP descending
    versions.sort(key=lambda v: v["overall_map"], reverse=True)

    # Detect current best version
    best_dir = models_dir / "best"
    current_best_version = None
    if best_dir.exists():
        # If best is a symlink, resolve to get the version name directly
        if best_dir.is_symlink():
            current_best_version = best_dir.resolve().name
        else:
            # Old format: best is a directory, match by mAP
            best_eval = best_dir / "evaluation_results.json"
            if best_eval.exists():
                with open(best_eval) as f:
                    best_results = json.load(f)
                best_map = best_results.get("overall_map")
                for v in versions:
                    if v["overall_map"] == best_map:
                        current_best_version = v["version"]
                        break

    # Log comparison table
    logger.info("=== Model Version Comparison ===")
    for v in versions:
        marker = " (current best)" if v["version"] == current_best_version else ""
        logger.info("  %s  mAP=%.4f%s", v["version"], v["overall_map"], marker)

    recommended = versions[0]["version"] if versions else None
    if recommended:
        logger.info("Recommended version: %s (mAP=%.4f)", recommended, versions[0]["overall_map"])

    return {
        "versions": versions,
        "recommended_version": recommended,
        "current_best_version": current_best_version,
    }
