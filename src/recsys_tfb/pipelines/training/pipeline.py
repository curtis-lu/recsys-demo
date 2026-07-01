"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.pipelines.training.diagnostics import (
    compute_feature_importance,
    compute_feature_statistics,
    compute_quadrant_profiles,
    compute_shap_diagnostics,
)
from recsys_tfb.pipelines.training.diagnostics_spark import select_shap_population
from recsys_tfb.pipelines.training.nodes import (
    cache_calibration_model_input,
    cache_test_model_input,
    cache_train_dev_model_input,
    cache_train_model_input,
    cache_val_model_input,
    calibrate_model,
    compute_test_mAP_spark,
    finalize_model,
    log_experiment,
    persist_sample_weight_report,
    predict_and_write_test_predictions,
    prepare_lgb_train_inputs,
    select_features,
    tune_hyperparameters,
)


def create_pipeline(enable_calibration: bool = False) -> Pipeline:
    # finalize_model produces the trained model; under calibration it lands in
    # `trained_model` so calibrate_model can wrap it. Strategy
    # (hpo_best / refit_on_full) is read from parameters at runtime — not a
    # DAG-shape concern.
    final_model_output = "trained_model" if enable_calibration else "model"

    nodes = [
        # Training-stage feature selection chokepoint: emit a (possibly subset)
        # preprocessor view that every model-touching node below consumes, so
        # `training.feature_selection.exclude` is applied once and stays
        # consistent. Empty selection -> view is the raw preprocessor unchanged.
        Node(
            select_features,
            inputs=["preprocessor", "parameters"],
            outputs="preprocessor_view",
        ),
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
                "preprocessor_view", "parameters",
            ],
            outputs=["train_lgb_handle", "train_dev_lgb_handle"],
        ),
    )

    nodes.append(
        Node(
            persist_sample_weight_report,
            inputs=["train_parquet_handle", "preprocessor_view", "parameters"],
            outputs="sample_weight_report",
        ),
    )

    nodes.append(
        Node(
            tune_hyperparameters,
            inputs=[
                "train_lgb_handle", "train_dev_lgb_handle",
                "val_parquet_handle", "preprocessor_view", "parameters",
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
                "preprocessor_view", "parameters",
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
                    "preprocessor_view", "parameters",
                ],
                outputs="model",
            ),
        )

    nodes.extend([
        Node(
            predict_and_write_test_predictions,
            inputs=[
                "model", "test_parquet_handle",
                "preprocessor_view", "parameters",
                "@training_eval_predictions",  # catalog handle for chunked save
            ],
            outputs="predict_manifest",
        ),
        Node(
            compute_test_mAP_spark,
            inputs=["training_eval_predictions", "predict_manifest", "parameters"],
            outputs="evaluation_results",
        ),
        Node(
            compute_feature_statistics,
            inputs=["train_parquet_handle", "preprocessor_view", "parameters"],
            outputs="feature_statistics",
        ),
        Node(
            compute_feature_importance,
            inputs=["model", "parameters"],
            outputs="feature_importance",
        ),
        Node(
            compute_shap_diagnostics,
            inputs=["model", "test_parquet_handle", "preprocessor_view", "parameters"],
            outputs="shap_diagnostics",
        ),
        # P2b 象限診斷:Spark 選樣(top@1 象限 + 每格抽樣)→ pandas per-(item×象限)
        # signed profile,獨立寫 per_quadrant.json。compute_shap_diagnostics 不動。
        Node(
            select_shap_population,
            # predict_manifest is an ordering-only dependency (same convention as
            # compute_test_mAP_spark): it forces this node to run AFTER
            # predict_and_write_test_predictions has written training_eval_predictions.
            # Without it, all three data inputs lack a node producer and Kahn's sort
            # would float this node ahead of the predict node (stale predictions).
            inputs=[
                "training_eval_predictions", "test_model_input",
                "parameters", "predict_manifest",
            ],
            outputs="shap_population",
        ),
        Node(
            compute_quadrant_profiles,
            inputs=["model", "shap_population", "preprocessor_view", "parameters"],
            outputs="quadrant_profiles",
        ),
        Node(
            log_experiment,
            # quadrant_profiles 置末:log_experiment 簽名新參數有 default None（在
            # parameters 之後），Runner 以 node.inputs 位置對應傳參,故此處順序須與簽名
            # 一致。此依賴也保證 per_quadrant.json 已由 catalog 寫入後才 log_artifacts。
            inputs=[
                "model", "best_params", "best_iteration", "evaluation_results",
                "feature_statistics", "feature_importance", "shap_diagnostics",
                "parameters", "quadrant_profiles",
            ],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
