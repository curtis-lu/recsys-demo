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
    prepare_lgb_train_inputs,
    train_model,
    tune_hyperparameters,
)


def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    train_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        Node(
            cache_train_model_input,
            inputs=["train_model_input", "parameters"],
            outputs="train_parquet_handle",
        ),
        Node(
            cache_train_dev_model_input,
            inputs=["train_dev_model_input", "parameters"],
            outputs="train_dev_parquet_handle",
        ),
        Node(
            cache_val_model_input,
            inputs=["val_model_input", "parameters"],
            outputs="val_parquet_handle",
        ),
    ]

    if enable_calibration:
        nodes.append(
            Node(
                cache_calibration_model_input,
                inputs=["calibration_model_input", "parameters"],
                outputs="calibration_parquet_handle",
            ),
        )

    nodes.append(
        Node(
            prepare_lgb_train_inputs,
            inputs=[
                "train_parquet_handle", "train_dev_parquet_handle",
                "preprocessor", "parameters",
            ],
            outputs=["train_lgb_handle", "train_dev_lgb_handle"],
        ),
    )

    nodes.extend([
        Node(
            tune_hyperparameters,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
                "val_parquet_handle", "preprocessor", "parameters",
            ],
            outputs="best_params",
        ),
        Node(
            train_model,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
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
                    "trained_model", "calibration_parquet_handle",
                    "preprocessor", "parameters",
                ],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            evaluate_model,
            inputs=["model", "val_parquet_handle", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=["model", "best_params", "evaluation_results", "parameters"],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
