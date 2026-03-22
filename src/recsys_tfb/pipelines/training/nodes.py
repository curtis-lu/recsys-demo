"""Pure functions for the training pipeline."""

import logging

import lightgbm as lgb
import mlflow
import numpy as np
import optuna
import pandas as pd

logger = logging.getLogger(__name__)


from recsys_tfb.evaluation.metrics import compute_all_metrics, compute_ap


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
    """Compute ranking-aware mAP with (snap_date, cust_id) as query groups.

    Delegates metric computation to evaluation.metrics.compute_all_metrics,
    ensuring per-product AP uses correct per-customer ranking semantics.
    """
    y_score = model.predict(X_val)

    # Build DataFrames expected by compute_all_metrics
    predictions = val_set[["snap_date", "cust_id", "prod_name"]].reset_index(drop=True).copy()
    predictions["score"] = y_score
    predictions["rank"] = (
        predictions.groupby(["snap_date", "cust_id"])["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    labels = val_set[["snap_date", "cust_id", "prod_name"]].reset_index(drop=True).copy()
    labels["label"] = y_val["label"].values

    metrics = compute_all_metrics(predictions, labels, k_values=["all"])

    # Extract map@N where N = number of unique products
    n_products = predictions["prod_name"].nunique()
    map_key = f"map@{n_products}"

    overall_map = metrics["overall"].get(map_key, 0.0)
    per_product_ap = {
        prod: vals.get(map_key, 0.0)
        for prod, vals in metrics["per_product"].items()
    }

    evaluation_results = {
        "overall_map": overall_map,
        "per_product_ap": per_product_ap,
        "n_queries": metrics["n_queries"],
        "n_excluded_queries": metrics["n_excluded_queries"],
    }

    logger.info(
        "Evaluation: mAP=%.4f, products=%d, excluded_queries=%d",
        overall_map,
        len(per_product_ap),
        metrics["n_excluded_queries"],
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
