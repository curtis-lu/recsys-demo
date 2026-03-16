"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(backend: str = "pandas") -> Pipeline:
    if backend == "spark":
        from recsys_tfb.pipelines.dataset.nodes_spark import (
            build_dataset,
            prepare_model_input,
            select_sample_keys,
            split_keys,
        )
    else:
        from recsys_tfb.pipelines.dataset.nodes_pandas import (
            build_dataset,
            prepare_model_input,
            select_sample_keys,
            split_keys,
        )

    return Pipeline(
        [
            Node(
                select_sample_keys,
                inputs=["label_table", "parameters"],
                outputs="sample_keys",
            ),
            Node(
                split_keys,
                inputs=["sample_keys", "label_table", "parameters"],
                outputs=["train_keys", "train_dev_keys", "val_keys"],
            ),
            Node(
                build_dataset,
                inputs=["train_keys", "feature_table", "label_table"],
                outputs="train_set",
                name="build_train_dataset",
            ),
            Node(
                build_dataset,
                inputs=["train_dev_keys", "feature_table", "label_table"],
                outputs="train_dev_set",
                name="build_train_dev_dataset",
            ),
            Node(
                build_dataset,
                inputs=["val_keys", "feature_table", "label_table"],
                outputs="val_set",
                name="build_val_dataset",
            ),
            Node(
                prepare_model_input,
                inputs=["train_set", "train_dev_set", "val_set", "parameters"],
                outputs=[
                    "X_train", "y_train",
                    "X_train_dev", "y_train_dev",
                    "X_val", "y_val",
                    "preprocessor", "category_mappings",
                ],
            ),
        ]
    )
