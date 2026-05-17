"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    from recsys_tfb.pipelines.dataset.nodes_spark import (
        apply_preprocessor_to_features,
        build_model_input,
        fit_preprocessor_metadata,
        select_calibration_keys,
        select_test_keys,
        select_train_keys,
        select_val_keys,
        split_train_keys,
        validate_data_consistency,
    )

    nodes = [
        # --- Layer-2 B1 data gate: runs first (insertion-order Kahn seed),
        # side-effect only (outputs=None), fail-fast before any sampling ---
        Node(
            validate_data_consistency,
            inputs=["sample_pool", "label_table", "parameters"],
            outputs=None,
            name="validate_data_consistency",
        ),
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
            inputs=["sample_pool", "parameters"],
            outputs="val_keys",
        ),
        Node(
            select_test_keys,
            inputs=["sample_pool", "parameters"],
            outputs="test_keys",
        ),
        # --- Fit preprocessor on train date-range feature_table, decoupled from sampling ---
        Node(
            fit_preprocessor_metadata,
            inputs=["feature_table", "parameters"],
            outputs=["preprocessor", "category_mappings"],
            name="fit_preprocessor_metadata",
        ),
        # --- Encode non-identity categoricals once; all splits reuse this ---
        Node(
            apply_preprocessor_to_features,
            inputs=["feature_table", "preprocessor", "parameters"],
            outputs="preprocessed_feature_table",
            name="apply_preprocessor_to_features",
        ),
        # --- Build model_input per split (join keys + labels + encoded features) ---
        Node(
            build_model_input,
            inputs=[
                "train_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_model_input",
            name="build_train_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "train_dev_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="train_dev_model_input",
            name="build_train_dev_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "val_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="val_model_input",
            name="build_val_model_input",
        ),
        Node(
            build_model_input,
            inputs=[
                "test_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "parameters",
            ],
            outputs="test_model_input",
            name="build_test_model_input",
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
                build_model_input,
                inputs=[
                    "calibration_keys", "preprocessed_feature_table", "label_table",
                    "preprocessor", "parameters",
                ],
                outputs="calibration_model_input",
                name="build_calibration_model_input",
            ),
        ])

    return Pipeline(nodes)
