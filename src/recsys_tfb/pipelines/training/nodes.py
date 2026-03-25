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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_pandas(df):
    """Convert Spark DataFrame to pandas if needed (production backend)."""
    if hasattr(df, "toPandas"):
        return df.toPandas()
    return df


def _extract_Xy(
    model_input,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Extract feature matrix X and label vector y from model_input.

    Handles both pandas DataFrame and Spark DataFrame inputs.
    Encodes categorical identity columns (e.g., prod_name) that were
    deferred from the dataset pipeline's transform step.

    Returns:
        (X, y) as numpy arrays.
    """
    pdf = _to_pandas(model_input)
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    X_df = pdf[feature_cols].copy()

    # Encode categorical identity columns that were kept as original values
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    deferred_cats = [c for c in categorical_cols if c in identity_cols and c in X_df.columns]
    for col in deferred_cats:
        known = category_mappings[col]
        X_df[col] = pd.Categorical(X_df[col], categories=known).codes

    X = X_df.values
    y = pdf[label_col].values
    return X, y


# ---------------------------------------------------------------------------
# Pipeline nodes
# ---------------------------------------------------------------------------

def tune_hyperparameters(
    train_model_input,
    train_dev_model_input,
    val_model_input,
    preprocessor_metadata: dict,
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

    X_tr, y_tr = _extract_Xy(train_model_input, preprocessor_metadata, parameters)
    X_dev, y_dev = _extract_Xy(train_dev_model_input, preprocessor_metadata, parameters)
    X_v, y_v = _extract_Xy(val_model_input, preprocessor_metadata, parameters)

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
    train_model_input,
    train_dev_model_input,
    best_params: dict,
    preprocessor_metadata: dict,
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

    X_tr, y_tr = _extract_Xy(train_model_input, preprocessor_metadata, parameters)
    X_dev, y_dev = _extract_Xy(train_dev_model_input, preprocessor_metadata, parameters)

    adapter = get_adapter(algorithm)
    adapter.train(X_tr, y_tr, X_dev, y_dev, params)

    logger.info("Model trained with algorithm=%s", algorithm)
    return adapter


def calibrate_model(
    model: ModelAdapter,
    calibration_model_input,
    preprocessor_metadata: dict,
    parameters: dict,
) -> ModelAdapter:
    """Wrap model with probability calibration."""
    method = (
        parameters.get("training", {})
        .get("calibration", {})
        .get("method", "isotonic")
    )

    X_cal, y_cal = _extract_Xy(calibration_model_input, preprocessor_metadata, parameters)

    calibrated = CalibratedModelAdapter(model, method=method)
    calibrated.fit_calibrator(X_cal, y_cal)

    logger.info(
        "Model calibrated: method=%s, n_samples=%d", method, len(y_cal)
    )
    return calibrated


def _compute_ranking_metrics(
    y_score: np.ndarray,
    val_pdf: pd.DataFrame,
    schema: dict,
) -> tuple[float, dict[str, float], int, int]:
    """Compute ranking metrics from raw scores.

    Args:
        y_score: Predicted scores array.
        val_pdf: Pandas DataFrame containing identity columns and label.
        schema: Column schema dict.

    Returns (overall_map, per_product_ap, n_queries, n_excluded_queries).
    """
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    score_col = schema["score"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    group_cols = [time_col] + entity_cols

    predictions = val_pdf[identity_cols].reset_index(drop=True).copy()
    predictions[score_col] = y_score
    predictions[schema["rank"]] = (
        predictions.groupby(group_cols)[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    labels = val_pdf[identity_cols + [label_col]].reset_index(drop=True)

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
    val_model_input,
    preprocessor_metadata: dict,
    parameters: dict,
) -> dict:
    """Compute ranking-aware mAP with (snap_date, cust_id) as query groups.

    val_model_input contains identity columns, label, and encoded features,
    so no separate val_set is needed.

    When the model is a CalibratedModelAdapter, also computes uncalibrated
    metrics for comparison.
    """
    schema = get_schema(parameters)
    feature_cols = preprocessor_metadata["feature_columns"]

    val_pdf = _to_pandas(val_model_input)

    # Use _extract_Xy to handle deferred categorical encoding (e.g., prod_name)
    X, _ = _extract_Xy(val_model_input, preprocessor_metadata, parameters)
    y_score = model.predict(X)

    overall_map, per_product_ap, n_queries, n_excluded_queries = (
        _compute_ranking_metrics(y_score, val_pdf, schema)
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
            y_score_uncal, val_pdf, schema
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
