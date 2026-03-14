"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.dataset.nodes import (
    build_dataset,
    prepare_model_input,
    select_sample_keys,
    split_keys,
)


def create_pipeline() -> Pipeline:
    return Pipeline(
        [
            Node(
                select_sample_keys,
                inputs=["label_table", "parameters"],
                outputs="sample_keys",
            ),
            Node(
                split_keys,
                inputs=["sample_keys", "parameters"],
                outputs=["train_keys", "val_keys"],
            ),
            Node(
                build_dataset,
                inputs=["train_keys", "feature_table", "label_table"],
                outputs="train_set",
                name="build_train_dataset",
            ),
            Node(
                build_dataset,
                inputs=["val_keys", "feature_table", "label_table"],
                outputs="val_set",
                name="build_val_dataset",
            ),
            Node(
                prepare_model_input,
                inputs=["train_set", "val_set", "parameters"],
                outputs=["X_train", "y_train", "X_val", "y_val", "preprocessor"],
            ),
        ]
    )
