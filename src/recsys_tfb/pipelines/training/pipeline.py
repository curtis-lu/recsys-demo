"""Training pipeline definition."""

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline
from recsys_tfb.diagnosis.model import (
    compute_feature_importance,
    compute_feature_statistics,
    compute_quadrant_cases,
    compute_quadrant_profiles,
    compute_shap_diagnostics,
)
from recsys_tfb.diagnosis.model.gain_ledger import compute_gain_ledger
from recsys_tfb.diagnosis.model.population_spark import select_shap_population
from recsys_tfb.diagnosis.model.staged import (
    compute_stage1_overview,
    compute_staged_group_diagnostics,
)
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
    log_staged_experiment,
    persist_sample_weight_report,
    predict_and_write_test_predictions,
    prepare_lgb_train_inputs,
    select_features,
    tune_hyperparameters,
)
from recsys_tfb.pipelines.training.staged import train_staged_model
from recsys_tfb.pipelines.training.staged_stage2 import train_stage2_model


def create_pipeline(
    enable_calibration: bool = False, model_structure: str = "shared",
    stage2_mode: str = "none",
) -> Pipeline:
    if model_structure == "staged":
        if enable_calibration:
            raise ValueError(
                "staged model_structure requires calibration disabled "
                "(A21 blocks this at CLI entry; direct callers get the "
                "same contract here)"
            )
        return _create_staged_pipeline(stage2_mode)
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
            compute_gain_ledger,
            inputs=["model", "preprocessor_view", "parameters"],
            outputs="gain_ledger",
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
            outputs=["shap_population", "case_rows"],
        ),
        Node(
            compute_quadrant_profiles,
            inputs=["model", "shap_population", "preprocessor_view", "parameters"],
            outputs="quadrant_profiles",
        ),
        # P2b-2 象限案例:每 (item×象限) 全格極值案例的單列 signed SHAP 圖 + manifest。
        # 與 profile 依目的解耦(讀 case_rows,自己一次小 SHAP);compute_quadrant_profiles 不動。
        Node(
            compute_quadrant_cases,
            inputs=["model", "case_rows", "preprocessor_view", "parameters"],
            outputs="cases_manifest",
        ),
        Node(
            log_experiment,
            # quadrant_profiles 置末:log_experiment 簽名新參數有 default None（在
            # parameters 之後），Runner 以 node.inputs 位置對應傳參,故此處順序須與簽名
            # 一致。此依賴也保證 per_quadrant.json 已由 catalog 寫入後才 log_artifacts。
            # cases_manifest 亦置末(default None),保證 cases PNG/manifest 於 log_artifacts 前寫好。
            inputs=[
                "model", "best_params", "best_iteration", "evaluation_results",
                "feature_statistics", "feature_importance", "shap_diagnostics",
                "parameters", "quadrant_profiles", "cases_manifest",
            ],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)


def _create_staged_pipeline(stage2_mode: str = "none") -> Pipeline:
    """Staged training DAG.

    stage2=none（PR-A 形狀）: train_staged_model 直接產出 "model"。
    stage2 in {binary, lambdarank}（PR-B）: train_staged_model 產出
    "stage1_model"，train_stage2_model 做 OOF＋stage-2 後產出 "model"；
    cache_val_model_input 拉回 DAG（val＝stage-2 early stop＋HPO 評分集，
    spec §2.2/§3.1）。predict/mAP 節點兩種形狀共用（吃 "model"）。

    診斷（PR-C Task 9）：兩形狀共通 compute_feature_statistics／
    compute_stage1_overview；stage2=none 另跑 compute_staged_group_diagnostics
    （per-group 核心四件，範圍裁決見該函式 docstring）；stage2 存在則沿用
    booster 診斷全套（compute_feature_importance／compute_gain_ledger／
    SHAP／象限 profile／cases，經 resolve_attribution_inputs 分派吃
    staged 矩陣，見 diagnosis/model/shap_per_item.py、shap_cases.py）。
    兩形狀都收斂到 log_staged_experiment（單一 MLflow run，非 log_experiment）。
    Excluded：calibrate（A21 已在 CLI entry 擋，見上方 create_pipeline）；
    shared 的 prepare_lgb/tune/finalize 由 staged_stage2.tune_stage2 取代。
    """
    with_stage2 = stage2_mode != "none"
    nodes = [
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
    ]
    if with_stage2:
        nodes.append(
            Node(
                cache_val_model_input,
                inputs=["val_model_input", "parameters"],
                outputs="val_parquet_handle",
            ),
        )
    nodes.extend([
        Node(
            cache_test_model_input,
            inputs=["test_model_input", "parameters"],
            outputs="test_parquet_handle",
        ),
        Node(
            persist_sample_weight_report,
            inputs=["train_parquet_handle", "preprocessor_view", "parameters"],
            outputs="sample_weight_report",
        ),
        Node(
            train_staged_model,
            inputs=[
                "train_parquet_handle", "train_dev_parquet_handle",
                "preprocessor_view", "parameters",
            ],
            outputs=(["stage1_model", "stage1_groups_report"] if with_stage2
                     else ["model", "stage1_groups_report"]),
            name="train_staged_model",
        ),
    ])
    if with_stage2:
        nodes.append(
            Node(
                train_stage2_model,
                inputs=[
                    "stage1_model", "stage1_groups_report",
                    "train_parquet_handle", "train_dev_parquet_handle",
                    "val_parquet_handle", "preprocessor_view", "parameters",
                ],
                outputs=["model", "stage2_report"],
                name="train_stage2_model",
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
    ])

    # ---- PR-C 診斷（兩形狀共通）----
    nodes.append(
        Node(
            compute_feature_statistics,
            inputs=["train_parquet_handle", "preprocessor_view", "parameters"],
            outputs="feature_statistics",
        ),
    )
    nodes.append(
        Node(
            compute_stage1_overview,
            # stage2_report 走 trailing-default（log_experiment 同慣例）
            inputs=(["stage1_groups_report", "parameters", "stage2_report"]
                    if with_stage2 else ["stage1_groups_report", "parameters"]),
            outputs="stage1_overview",
        ),
    )
    if with_stage2:
        nodes.extend([
            Node(
                compute_feature_importance,
                inputs=["model", "parameters"],
                outputs="feature_importance",
            ),
            Node(
                compute_gain_ledger,
                inputs=["model", "preprocessor_view", "parameters"],
                outputs="gain_ledger",
            ),
            Node(
                compute_shap_diagnostics,
                inputs=["model", "test_parquet_handle", "preprocessor_view",
                        "parameters"],
                outputs="shap_diagnostics",
            ),
            Node(
                select_shap_population,
                # predict_manifest：ordering-only（shared DAG 同註解）
                inputs=["training_eval_predictions", "test_model_input",
                        "parameters", "predict_manifest"],
                outputs=["shap_population", "case_rows"],
            ),
            Node(
                compute_quadrant_profiles,
                inputs=["model", "shap_population", "preprocessor_view",
                        "parameters"],
                outputs="quadrant_profiles",
            ),
            Node(
                compute_quadrant_cases,
                inputs=["model", "case_rows", "preprocessor_view", "parameters"],
                outputs="cases_manifest",
            ),
            Node(
                log_staged_experiment,
                # 第 6 位起=*diag_deps（ordering-only：保證 catalog 先落檔）
                inputs=["model", "stage1_groups_report", "evaluation_results",
                        "stage1_overview", "parameters", "feature_statistics",
                        "feature_importance", "gain_ledger", "shap_diagnostics",
                        "quadrant_profiles", "cases_manifest"],
                outputs=None,
            ),
        ])
    else:
        nodes.extend([
            Node(
                compute_staged_group_diagnostics,
                inputs=["model", "train_parquet_handle", "test_parquet_handle",
                        "preprocessor_view", "parameters"],
                outputs="staged_group_diagnostics",
            ),
            Node(
                log_staged_experiment,
                inputs=["model", "stage1_groups_report", "evaluation_results",
                        "stage1_overview", "parameters", "feature_statistics",
                        "staged_group_diagnostics"],
                outputs=None,
            ),
        ])
    return Pipeline(nodes)
