"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    calibrate_model,
    evaluate_model,
    log_experiment,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    # Determine train_model output name based on whether calibration is enabled
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(
            tune_hyperparameters,
            inputs=[
                "train_model_input", "train_dev_model_input", "val_model_input",
                "preprocessor", "parameters",
            ],
            outputs="best_params",
        ),
        Node(
            train_model,
            inputs=[
                "train_model_input", "train_dev_model_input",
                "best_params", "preprocessor", "parameters",
            ],
            outputs=train_model_output,
        ),
    ]

    if enable_calibration:
        nodes.append(
            Node(
                calibrate_model,
                inputs=["trained_model", "calibration_model_input", "preprocessor", "parameters"],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "val_model_input", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=["model", "best_params", "evaluation_results", "parameters"],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
