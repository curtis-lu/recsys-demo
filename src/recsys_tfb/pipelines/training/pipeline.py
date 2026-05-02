"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    evaluate_model,
    log_experiment,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(
            cache_train_model_input,
            inputs=["train_model_input", "parameters"],
            outputs="cached_train_model_input",
        ),
        Node(
            cache_train_dev_model_input,
            inputs=["train_dev_model_input", "parameters"],
            outputs="cached_train_dev_model_input",
        ),
        Node(
            cache_val_model_input,
            inputs=["val_model_input", "parameters"],
            outputs="cached_val_model_input",
        ),
    ]

    if enable_calibration:
        nodes.append(
            Node(
                cache_calibration_model_input,
                inputs=["calibration_model_input", "parameters"],
                outputs="cached_calibration_model_input",
            ),
        )

    nodes.extend([
        Node(
            tune_hyperparameters,
            inputs=[
                "cached_train_model_input", "cached_train_dev_model_input",
                "cached_val_model_input", "preprocessor", "parameters",
            ],
            outputs="best_params",
        ),
        Node(
            train_model,
            inputs=[
                "cached_train_model_input", "cached_train_dev_model_input",
                "best_params", "preprocessor", "parameters",
            ],
            outputs=train_model_output,
        ),
    ])

    if enable_calibration:
        nodes.append(
            Node(
                calibrate_model,
                inputs=[
                    "trained_model", "cached_calibration_model_input",
                    "preprocessor", "parameters",
                ],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "cached_val_model_input", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=["model", "best_params", "evaluation_results", "parameters"],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
