"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    evaluate_model,
    log_experiment,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(backend: str = "pandas") -> Pipeline:
    return Pipeline(
        [
            Node(
                tune_hyperparameters,
                inputs=["X_train", "y_train", "X_train_dev", "y_train_dev", "parameters"],
                outputs="best_params",
            ),
            Node(
                train_model,
                inputs=["X_train", "y_train", "X_train_dev", "y_train_dev", "best_params", "parameters"],
                outputs="model",
            ),
            Node(
                evaluate_model,
                inputs=["model", "X_val", "y_val", "val_set", "parameters"],
                outputs="evaluation_results",
            ),
            Node(
                log_experiment,
                inputs=["model", "best_params", "evaluation_results", "parameters"],
                outputs=None,
            ),
        ]
    )
