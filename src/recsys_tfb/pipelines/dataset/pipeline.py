"""Dataset building pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    from recsys_tfb.pipelines.dataset.nodes_spark import (
        apply_preprocessor_to_features,
        build_model_input,
        build_test_model_input,
        filter_groups_with_positives,
        fit_preprocessor_metadata,
        select_calibration_keys,
        select_test_keys,
        select_train_keys,
        select_val_keys,
        split_train_keys,
        validate_data_consistency,
    )

    nodes = [
        # --- Layer-2 data gate (B1 item coverage + B5 categorical dtype
        #     + B6 non-numeric feature column):
        # runs first (insertion-order Kahn seed), side-effect only
        # (outputs=None), fail-fast before any sampling / preprocessing ---
        Node(
            validate_data_consistency,
            inputs=["sample_pool", "label_table", "feature_table", "parameters"],
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
        # A `*_month_plan` input marks an incremental node: it processes only
        # the months in that plan (ADR-0002/0007). The plans are built once by
        # the caller and injected into the catalog, so nodes that share an
        # artifact cannot disagree about which months this run covers — and the
        # nodes that have no plan are exactly the ones that rebuild in full.
        Node(
            select_test_keys,
            inputs=["sample_pool", "test_keys_month_plan", "parameters"],
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
            inputs=[
                "feature_table", "preprocessor",
                "preprocessed_feature_table_month_plan", "parameters",
            ],
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
            outputs="val_model_input_unfiltered",
            name="build_val_model_input",
        ),
        # test uses its own wrapper: `test_keys` is a persistent Hive table
        # holding every month, so reading it back has to be re-scoped to this
        # run's months. train/val/calibration read keys written by this run and
        # need no such wrapper.
        Node(
            build_test_model_input,
            inputs=[
                "test_keys", "preprocessed_feature_table", "label_table",
                "preprocessor", "test_model_input_month_plan", "parameters",
            ],
            outputs="test_model_input_unfiltered",
            name="build_test_model_input",
        ),
        # --- Drop (time, *entity) groups with no positives. Applied to val/test
        # only — these are evaluated by ranking metrics (mAP/NDCG) that exclude
        # zero-positive groups anyway, so retaining them just wastes Hive
        # storage and downstream predict / extract memory. train / train_dev /
        # calibration are NOT filtered: their losses use every row. ---
        Node(
            filter_groups_with_positives,
            inputs=["val_model_input_unfiltered", "parameters"],
            outputs="val_model_input",
            name="filter_val_model_input",
        ),
        # No month plan here: its input is produced by build_test_model_input,
        # which is already scoped. Re-filtering would only re-state the line
        # above. See ADR-0007 for the slicing scenario that was considered.
        Node(
            filter_groups_with_positives,
            inputs=["test_model_input_unfiltered", "parameters"],
            outputs="test_model_input",
            name="filter_test_model_input",
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
