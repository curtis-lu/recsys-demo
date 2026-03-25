"""Pure functions for the training pipeline."""

import logging

import mlflow
import numpy as np
import optuna
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.evaluation.metrics import compute_all_metrics, compute_ap
from recsys_tfb.models.base import ModelAdapter, get_adapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter

logger = logging.getLogger(__name__)


def tune_hyperparameters(
    X_train: pd.DataFrame,
    y_train: pd.DataFrame,
    X_train_dev: pd.DataFrame,
    y_train_dev: pd.DataFrame,
    X_val: pd.DataFrame,
    y_val: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Search for optimal hyperparameters using Optuna."""
    training_params = parameters["training"]
    n_trials = training_params["n_trials"]
    search_space = training_params["search_space"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    X_tr = X_train.values if hasattr(X_train, "values") else X_train
    y_tr = y_train["label"].values if isinstance(y_train, pd.DataFrame) else y_train
    X_dev = X_train_dev.values if hasattr(X_train_dev, "values") else X_train_dev
    y_dev = y_train_dev["label"].values if isinstance(y_train_dev, pd.DataFrame) else y_train_dev
    X_v = X_val.values if hasattr(X_val, "values") else X_val
    y_v = y_val["label"].values if isinstance(y_val, pd.DataFrame) else y_val

    def objective(trial: optuna.Trial) -> float:
        trial_params = {
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

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        adapter = get_adapter(algorithm)
        adapter.train(X_tr, y_tr, X_dev, y_dev, params)
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
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
) -> ModelAdapter:
    """Train a model using ModelAdapter with early stopping."""
    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    num_iterations = training_params.get("num_iterations", 500)
    early_stopping_rounds = training_params.get("early_stopping_rounds", 50)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = training_params.get("algorithm_params", {})

    params = {
        **algorithm_params,
        "seed": seed,
        **best_params,
        "num_iterations": num_iterations,
        "early_stopping_rounds": early_stopping_rounds,
    }

    X_tr = X_train.values if hasattr(X_train, "values") else X_train
    y_tr = y_train["label"].values if isinstance(y_train, pd.DataFrame) else y_train
    X_dev = X_train_dev.values if hasattr(X_train_dev, "values") else X_train_dev
    y_dev = y_train_dev["label"].values if isinstance(y_train_dev, pd.DataFrame) else y_train_dev

    adapter = get_adapter(algorithm)
    adapter.train(X_tr, y_tr, X_dev, y_dev, params)

    logger.info("Model trained with algorithm=%s", algorithm)
    return adapter


def calibrate_model(
    model: ModelAdapter,
    X_calibration: pd.DataFrame,
    y_calibration: pd.DataFrame,
    parameters: dict,
) -> ModelAdapter:
    """Wrap model with probability calibration."""
    method = (
        parameters.get("training", {})
        .get("calibration", {})
        .get("method", "isotonic")
    )

    X_cal = X_calibration.values if hasattr(X_calibration, "values") else X_calibration
    y_cal = (
        y_calibration["label"].values
        if isinstance(y_calibration, pd.DataFrame)
        else y_calibration
    )

    calibrated = CalibratedModelAdapter(model, method=method)
    calibrated.fit_calibrator(X_cal, y_cal)

    logger.info(
        "Model calibrated: method=%s, n_samples=%d", method, len(y_cal)
    )
    return calibrated


def _compute_ranking_metrics(
    y_score: np.ndarray,
    val_set: pd.DataFrame,
    labels: pd.DataFrame,
    schema: dict,
) -> tuple[float, dict[str, float], int, int]:
    """Compute ranking metrics from raw scores.

    Returns (overall_map, per_product_ap, n_queries, n_excluded_queries).
    """
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]
    group_cols = [time_col] + entity_cols

    predictions = val_set[identity_cols].reset_index(drop=True).copy()
    predictions[score_col] = y_score
    predictions[schema["rank"]] = (
        predictions.groupby(group_cols)[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    metrics = compute_all_metrics(predictions, labels, k_values=["all"])

    n_products = predictions[item_col].nunique()
    map_key = f"map@{n_products}"

    overall_map = metrics["overall"].get(map_key, 0.0)
    per_product_ap = {
        prod: vals.get(map_key, 0.0)
        for prod, vals in metrics["per_product"].items()
    }

    return overall_map, per_product_ap, metrics["n_queries"], metrics["n_excluded_queries"]


def evaluate_model(
    model: ModelAdapter,
    X_val: pd.DataFrame,
    y_val: pd.DataFrame,
    val_set: pd.DataFrame,
    parameters: dict,
) -> dict:
    """Compute ranking-aware mAP with (snap_date, cust_id) as query groups.

    Delegates metric computation to evaluation.metrics.compute_all_metrics,
    ensuring per-product AP uses correct per-customer ranking semantics.

    When the model is a CalibratedModelAdapter, also computes uncalibrated
    metrics for comparison.
    """
    schema = get_schema(parameters)
    label_col = schema["label"]

    X = X_val.values if hasattr(X_val, "values") else X_val
    y_score = model.predict(X)

    # Build labels DataFrame expected by _compute_ranking_metrics
    labels = val_set[schema["identity_columns"]].reset_index(drop=True).copy()
    labels[label_col] = y_val[label_col].values

    overall_map, per_product_ap, n_queries, n_excluded_queries = (
        _compute_ranking_metrics(y_score, val_set, labels, schema)
    )

    evaluation_results = {
        "overall_map": overall_map,
        "per_product_ap": per_product_ap,
        "n_queries": n_queries,
        "n_excluded_queries": n_excluded_queries,
    }

    # Uncalibrated comparison when model is calibrated
    if isinstance(model, CalibratedModelAdapter):
        y_score_uncal = model.predict_uncalibrated(X)
        uncal_map, uncal_per_product, _, _ = _compute_ranking_metrics(
            y_score_uncal, val_set, labels, schema
        )
        evaluation_results["uncalibrated"] = {
            "overall_map": uncal_map,
            "per_product_ap": uncal_per_product,
        }
        evaluation_results["calibration_method"] = model.method
        logger.info(
            "Uncalibrated mAP=%.4f vs Calibrated mAP=%.4f",
            uncal_map,
            overall_map,
        )

    logger.info(
        "Evaluation: mAP=%.4f, products=%d, excluded_queries=%d",
        overall_map,
        len(per_product_ap),
        n_excluded_queries,
    )
    return evaluation_results


def log_experiment(
    model: ModelAdapter,
    best_params: dict,
    evaluation_results: dict,
    parameters: dict,
) -> None:
    """Log training results to MLflow."""
    mlflow_params = parameters.get("mlflow", {})
    tracking_uri = mlflow_params.get("tracking_uri", "mlruns")
    experiment_name = mlflow_params.get("experiment_name", "recsys_tfb")
    algorithm = parameters.get("training", {}).get("algorithm", "lightgbm")

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_param("algorithm", algorithm)
        mlflow.log_metric("overall_map", evaluation_results["overall_map"])

        for prod, ap in evaluation_results.get("per_product_ap", {}).items():
            mlflow.log_metric(f"ap_{prod}", ap)

        mlflow.log_metric("n_queries", evaluation_results["n_queries"])
        mlflow.log_metric("n_excluded_queries", evaluation_results["n_excluded_queries"])

        # Calibration info
        if "uncalibrated" in evaluation_results:
            mlflow.log_param("calibrated", True)
            mlflow.log_param("calibration_method", evaluation_results["calibration_method"])
            mlflow.log_metric(
                "uncalibrated_overall_map",
                evaluation_results["uncalibrated"]["overall_map"],
            )
        else:
            mlflow.log_param("calibrated", False)

        model.log_to_mlflow()

    logger.info("MLflow experiment logged: %s", experiment_name)
