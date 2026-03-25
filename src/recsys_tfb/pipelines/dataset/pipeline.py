"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    if backend == "spark":
        from recsys_tfb.pipelines.dataset.nodes_spark import (
            build_dataset,
            fit_preprocessor_metadata,
            select_calibration_keys,
            select_test_keys,
            select_train_keys,
            select_val_keys,
            split_train_keys,
            transform_to_model_input,
        )
    else:
        from recsys_tfb.pipelines.dataset.nodes_pandas import (
            build_dataset,
            fit_preprocessor_metadata,
            select_calibration_keys,
            select_test_keys,
            select_train_keys,
            select_val_keys,
            split_train_keys,
            transform_to_model_input,
        )

    nodes = [
        # --- Key selection ---
        Node(
            select_train_keys,
            inputs=["sample_pool", "parameters"],
            outputs="sample_keys",
            name="select_sample_keys",
        ),
        Node(
            split_train_keys,
            inputs=["sample_keys", "parameters"],
            outputs=["train_keys", "train_dev_keys"],
        ),
        Node(
            select_val_keys,
            inputs=["label_table", "parameters"],
            outputs="val_keys",
        ),
        Node(
            select_test_keys,
            inputs=["label_table", "parameters"],
            outputs="test_keys",
        ),
        # --- Build split datasets ---
        Node(
            build_dataset,
            inputs=["train_keys", "feature_table", "label_table", "parameters"],
            outputs="train_set",
            name="build_train_dataset",
        ),
        Node(
            build_dataset,
            inputs=["train_dev_keys", "feature_table", "label_table", "parameters"],
            outputs="train_dev_set",
            name="build_train_dev_dataset",
        ),
        Node(
            build_dataset,
            inputs=["val_keys", "feature_table", "label_table", "parameters"],
            outputs="val_set",
            name="build_val_dataset",
        ),
        Node(
            build_dataset,
            inputs=["test_keys", "feature_table", "label_table", "parameters"],
            outputs="test_set",
            name="build_test_dataset",
        ),
        # --- Fit preprocessor from train only ---
        Node(
            fit_preprocessor_metadata,
            inputs=["train_set", "parameters"],
            outputs=["preprocessor", "category_mappings"],
            name="fit_preprocessor_metadata",
        ),
        # --- Transform each split to model_input ---
        Node(
            transform_to_model_input,
            inputs=["train_set", "preprocessor", "parameters"],
            outputs="train_model_input",
            name="transform_train_to_model_input",
        ),
        Node(
            transform_to_model_input,
            inputs=["train_dev_set", "preprocessor", "parameters"],
            outputs="train_dev_model_input",
            name="transform_train_dev_to_model_input",
        ),
        Node(
            transform_to_model_input,
            inputs=["val_set", "preprocessor", "parameters"],
            outputs="val_model_input",
            name="transform_val_to_model_input",
        ),
        Node(
            transform_to_model_input,
            inputs=["test_set", "preprocessor", "parameters"],
            outputs="test_model_input",
            name="transform_test_to_model_input",
        ),
    ]

    if enable_calibration:
        nodes.extend([
            Node(
                select_calibration_keys,
                inputs=["sample_pool", "parameters"],
                outputs="calibration_keys",
            ),
            Node(
                build_dataset,
                inputs=["calibration_keys", "feature_table", "label_table", "parameters"],
                outputs="calibration_set",
                name="build_calibration_dataset",
            ),
            Node(
                transform_to_model_input,
                inputs=["calibration_set", "preprocessor", "parameters"],
                outputs="calibration_model_input",
                name="transform_calibration_to_model_input",
            ),
        ])

    return Pipeline(nodes)
