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
        # preprocessor view that every node *training* a model consumes, so
        # `training.feature_selection.exclude` is applied once and stays
        # consistent. Empty selection -> view is the raw preprocessor unchanged.
        # The diagnosis nodes deliberately do NOT read it: they run after the
        # model exists and ask the model itself, so they do not depend on a
        # memory-only artifact (ADR-0014 decision 7).
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
            ],
            # Chunked save: this node writes training_eval_predictions itself,
            # one partition per .save(). Registered in R1 of
            # docs/agents/architecture-constraints.md. The Runner binds write
            # targets BY KEYWORD, so the signature's parameter name must stay
            # `training_eval_predictions` -- and unlike the `quadrant_profiles`
            # case below, a new optional input must NOT be appended after it.
            writes=["training_eval_predictions"],
            outputs="predict_manifest",
        ),
        Node(
            compute_test_mAP_spark,
            inputs=["training_eval_predictions", "predict_manifest", "parameters"],
            outputs="evaluation_results",
        ),
        # The diagnosis group below reads `preprocessor` (the landed dataset-built
        # artifact) rather than the memory-only `preprocessor_view`, and asks
        # `model` which columns it actually used. Both of those have catalog
        # entries, so this group no longer drags `select_features` — and the whole
        # training chain feeding it — into a diagnosis-only rerun. The
        # `*_parquet_handle` inputs are still memory-only and deliberately left
        # that way, so this is one edge removed, not a clean cut (ADR-0014
        # decision 7 records the remaining blocker).
        #
        # `model` is a new input for compute_feature_statistics specifically — a
        # deliberate coupling, argued in its docstring. It also moves that node
        # after calibrate_model in the topological order; see the calibrate_model
        # entry in tests/test_pipelines/test_resume_contracts.py for what that
        # costs a resume.
        Node(
            compute_feature_statistics,
            inputs=["train_parquet_handle", "model", "preprocessor", "parameters"],
            outputs="feature_statistics",
        ),
        Node(
            compute_feature_importance,
            inputs=["model", "parameters"],
            outputs="feature_importance",
        ),
        Node(
            compute_gain_ledger,
            inputs=["model", "preprocessor", "parameters"],
            outputs="gain_ledger",
        ),
        Node(
            compute_shap_diagnostics,
            inputs=["model", "test_parquet_handle", "preprocessor", "parameters"],
            outputs="shap_diagnostics",
        ),
        # P2b quadrant diagnostics: Spark picks the population (top@1 quadrant
        # plus a draw from every cell), then pandas builds a per-(item x
        # quadrant) signed profile and writes per_quadrant.json on its own.
        # compute_shap_diagnostics is untouched.
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
            inputs=["model", "shap_population", "preprocessor", "parameters"],
            outputs="quadrant_profiles",
        ),
        # P2b-2 quadrant cases: for each (item x quadrant), a single-row signed
        # SHAP plot of that cell's extreme case, plus a manifest. Kept separate
        # from the profile by purpose — it reads case_rows and runs its own small
        # SHAP. compute_quadrant_profiles is untouched.
        Node(
            compute_quadrant_cases,
            inputs=["model", "case_rows", "preprocessor", "parameters"],
            outputs="cases_manifest",
        ),
        Node(
            log_experiment,
            # quadrant_profiles goes last: the parameters log_experiment gained
            # default to None and sit after `parameters` in the signature, and
            # the Runner passes arguments by position from node.inputs — so the
            # order here has to match the signature. That dependency is also
            # what guarantees the catalog has written per_quadrant.json before
            # log_artifacts runs. cases_manifest goes last for the same reason
            # (default None), which is what gets the cases PNGs and manifest
            # written before log_artifacts.
            inputs=[
                "model", "best_params", "best_iteration", "evaluation_results",
                "feature_statistics", "feature_importance", "shap_diagnostics",
                "parameters", "quadrant_profiles", "cases_manifest",
            ],
            outputs=None,
        ),
    ])

    return Pipeline(nodes)
