"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_test_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    evaluate_model,
    finalize_model,
    log_experiment,
    prepare_lgb_train_inputs,
    tune_hyperparameters,
)


def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    # finalize_model produces the trained model; under calibration it lands in
    # `trained_model` so calibrate_model can wrap it. Strategy
    # (hpo_best / refit_on_full) is read from parameters at runtime — not a
    # DAG-shape concern.
    final_model_output = "trained_model" if enable_calibration else "model"

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
        Node(
            cache_test_model_input,
            inputs=["test_model_input", "parameters"],
            outputs="test_parquet_handle",
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

    nodes.append(
        Node(
            tune_hyperparameters,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
                "val_parquet_handle", "preprocessor", "parameters",
            ],
            outputs=["best_params", "best_iteration", "hpo_best_model"],
        ),
    )

    nodes.append(
        Node(
            finalize_model,
            inputs=[
                "train_parquet_handle", "train_dev_parquet_handle",
                "hpo_best_model", "best_params", "best_iteration",
                "preprocessor", "parameters",
            ],
            outputs=final_model_output,
        ),
    )

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
            inputs=["model", "test_parquet_handle", "preprocessor", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            log_experiment,
            inputs=[
                "model", "best_params", "best_iteration",
                "evaluation_results", "parameters",
            ],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
