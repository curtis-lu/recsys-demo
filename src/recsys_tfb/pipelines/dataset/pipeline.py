"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(backend: str = "pandas", enable_calibration: bool = False) -> Pipeline:
    if backend == "spark":
        from recsys_tfb.pipelines.dataset.nodes_spark import (
            build_dataset,
            prepare_model_input,
            prepare_model_input_with_calibration,
            select_calibration_keys,
            select_sample_keys,
            select_test_keys,
            select_val_keys,
            split_train_keys,
        )
    else:
        from recsys_tfb.pipelines.dataset.nodes_pandas import (
            build_dataset,
            prepare_model_input,
            prepare_model_input_with_calibration,
            select_calibration_keys,
            select_sample_keys,
            select_test_keys,
            select_val_keys,
            split_train_keys,
        )

    nodes = [
        Node(
            select_sample_keys,
            inputs=["sample_pool", "parameters"],
            outputs="sample_keys",
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
    ]

    if enable_calibration:
        nodes.append(
            Node(
                select_calibration_keys,
                inputs=["sample_pool", "label_table", "parameters"],
                outputs="calibration_keys",
            ),
        )
        nodes.append(
            Node(
                build_dataset,
                inputs=["calibration_keys", "feature_table", "label_table", "parameters"],
                outputs="calibration_set",
                name="build_calibration_dataset",
            ),
        )
        nodes.append(
            Node(
                prepare_model_input_with_calibration,
                inputs=[
                    "train_set", "train_dev_set", "calibration_set",
                    "val_set", "test_set", "parameters",
                ],
                outputs=[
                    "X_train", "y_train",
                    "X_train_dev", "y_train_dev",
                    "X_calibration", "y_calibration",
                    "X_val", "y_val",
                    "X_test", "y_test",
                    "preprocessor", "category_mappings",
                ],
            ),
        )
    else:
        nodes.append(
            Node(
                prepare_model_input,
                inputs=["train_set", "train_dev_set", "val_set", "test_set", "parameters"],
                outputs=[
                    "X_train", "y_train",
                    "X_train_dev", "y_train_dev",
                    "X_val", "y_val",
                    "X_test", "y_test",
                    "preprocessor", "category_mappings",
                ],
            ),
        )

    return Pipeline(nodes)
