"""Tests for training pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.metrics import compute_ap
from recsys_tfb.io.extract import extract_Xy
from recsys_tfb.io.handles import LgbDatasetHandle, ParquetHandle
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.models.calibrated_adapter import CalibratedModelAdapter
from recsys_tfb.pipelines.training.nodes import (
    calibrate_model,
    evaluate_model,
    finalize_model,
    log_experiment,
    tune_hyperparameters,
)


def _quick_train_adapter(lgb_handles, training_parameters):
    """Build a quick-trained LightGBMAdapter for downstream-node tests.

    Replaces the legacy `train_model` node helper; mirrors its lgb.Dataset
    wiring (train + train_dev as val) so tests get the same model artefact.
    """
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    train_lgb_h, train_dev_lgb_h = lgb_handles
    best_params = {
        "learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
        "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8,
    }
    seed = training_parameters.get("random_seed", 42)
    tp = training_parameters["training"]
    params = {
        **tp.get("algorithm_params", {}),
        "seed": seed,
        **best_params,
        "num_iterations": tp.get("num_iterations", 50),
        "early_stopping_rounds": tp.get("early_stopping_rounds", 10),
    }

    adapter = LightGBMAdapter()
    ds_train = train_lgb_h.load()
    ds_dev = train_dev_lgb_h.load(reference=ds_train)
    adapter.train(
        X_train=None, y_train=None, X_val=None, y_val=None,
        params=params,
        train_dataset=ds_train, val_dataset=ds_dev,
    )
    return adapter


# ---- Fixtures ----


@pytest.fixture
def training_parameters():
    return {
        "random_seed": 42,
        "training": {
            "algorithm": "lightgbm",
            "algorithm_params": {
                "objective": "binary",
                "metric": "binary_logloss",
                "verbosity": -1,
            },
            "n_trials": 3,
            "num_iterations": 50,
            "early_stopping_rounds": 10,
            "search_space": {
                "learning_rate": {"low": 0.01, "high": 0.3},
                "num_leaves": {"low": 16, "high": 64},
                "max_depth": {"low": 3, "high": 8},
                "min_child_samples": {"low": 5, "high": 50},
                "subsample": {"low": 0.6, "high": 1.0},
                "colsample_bytree": {"low": 0.6, "high": 1.0},
            },
        },
        "mlflow": {
            "experiment_name": "test_recsys",
            "tracking_uri": "mlruns",
        },
    }


@pytest.fixture
def preprocessor_metadata():
    return {
        "feature_columns": [
            "prod_name", "total_aum", "fund_aum", "in_amt_sum_l1m", "out_amt_sum_l1m",
        ],
        "categorical_columns": ["prod_name"],
        "category_mappings": {
            "prod_name": ["exchange_fx", "exchange_usd", "fund_bond", "fund_stock"],
        },
        "drop_columns": [
            "snap_date", "cust_id", "label",
            "apply_start_date", "apply_end_date", "cust_segment_typ",
        ],
    }


@pytest.fixture
def synthetic_model_inputs(tmp_path):
    """Create synthetic model_input DataFrames mimicking dataset pipeline output.

    Returns (train_handle, train_dev_handle, val_handle, train_df, train_dev_df, val_df)
    so tests can use both handles (for node calls) and raw DataFrames (for metrics checks).
    """
    rng = np.random.RandomState(42)
    products = ["exchange_fx", "exchange_usd", "fund_bond", "fund_stock"]

    def make_model_input(snap_date, n_customers):
        rows = []
        for i in range(n_customers):
            for prod in products:
                rows.append({
                    "snap_date": pd.Timestamp(snap_date),
                    "cust_id": f"C{i:03d}",
                    "prod_name": prod,
                    "label": float(rng.binomial(1, 0.15)),
                    "total_aum": rng.uniform(100, 1000),
                    "fund_aum": rng.uniform(10, 100),
                    "in_amt_sum_l1m": rng.uniform(0, 50),
                    "out_amt_sum_l1m": rng.uniform(0, 30),
                })
        return pd.DataFrame(rows)

    train_df = make_model_input("2024-01-31", 30)      # 120 rows
    train_dev_df = make_model_input("2024-02-29", 10)   # 40 rows
    val_df = make_model_input("2024-04-30", 10)          # 40 rows

    train_path = tmp_path / "train_mi.parquet"
    train_dev_path = tmp_path / "train_dev_mi.parquet"
    val_path = tmp_path / "val_mi.parquet"

    train_df.to_parquet(train_path)
    train_dev_df.to_parquet(train_dev_path)
    val_df.to_parquet(val_path)

    train_h = ParquetHandle(str(train_path))
    train_dev_h = ParquetHandle(str(train_dev_path))
    val_h = ParquetHandle(str(val_path))

    return train_h, train_dev_h, val_h, train_df, train_dev_df, val_df


@pytest.fixture
def lgb_handles(synthetic_model_inputs, preprocessor_metadata, training_parameters, tmp_path):
    """Build LgbDatasetHandle pair from train/train_dev parquet handles."""
    from recsys_tfb.pipelines.training.nodes import prepare_lgb_train_inputs

    train_h, train_dev_h, val_h, *_ = synthetic_model_inputs
    params = {
        **training_parameters,
        "cache": {"root": str(tmp_path / "cache")},
        "base_dataset_version": "v1",
        "train_variant_id": "tv1",
        "schema": {
            "label": "label",
            "identity_columns": ["cust_id", "snap_date", "prod_name"],
        },
    }
    train_lgb_h, train_dev_lgb_h = prepare_lgb_train_inputs(
        train_h, train_dev_h, preprocessor_metadata, params
    )
    return train_lgb_h, train_dev_lgb_h


# ---- Tests: _compute_ap ----


class TestComputeAP:
    def test_perfect_ranking(self):
        y_true = np.array([1, 1, 0, 0, 0])
        y_score = np.array([0.9, 0.8, 0.3, 0.2, 0.1])
        assert compute_ap(y_true, y_score) == 1.0

    def test_worst_ranking(self):
        y_true = np.array([0, 0, 0, 1, 1])
        y_score = np.array([0.9, 0.8, 0.7, 0.2, 0.1])
        ap = compute_ap(y_true, y_score)
        assert ap < 0.5

    def test_all_zero_labels(self):
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.5, 0.3, 0.1])
        assert compute_ap(y_true, y_score) is None

    def test_single_positive(self):
        y_true = np.array([0, 0, 1, 0])
        y_score = np.array([0.1, 0.2, 0.9, 0.3])
        # Positive is ranked first → AP = 1.0
        assert compute_ap(y_true, y_score) == 1.0

    def test_all_positive(self):
        y_true = np.array([1, 1, 1])
        y_score = np.array([0.9, 0.5, 0.1])
        assert compute_ap(y_true, y_score) == 1.0


# ---- Tests: tune_hyperparameters ----


class TestTuneHyperparameters:
    def test_returns_valid_params_and_model(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        best_params, best_iteration, best_model = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )

        assert isinstance(best_params, dict)
        assert "learning_rate" in best_params
        assert "num_leaves" in best_params
        assert "max_depth" in best_params
        assert isinstance(best_model, ModelAdapter)
        assert isinstance(best_iteration, int) and best_iteration > 0

    def test_params_in_search_space(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        space = training_parameters["training"]["search_space"]
        best_params, _, _ = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )

        assert space["num_leaves"]["low"] <= best_params["num_leaves"] <= space["num_leaves"]["high"]
        assert space["max_depth"]["low"] <= best_params["max_depth"] <= space["max_depth"]["high"]

    def test_reproducible(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        p1, i1, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters)
        p2, i2, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters)
        assert p1 == p2
        assert i1 == i2

    def test_best_model_predictions_are_probabilities(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """Best-trial model returned by HPO produces valid probability scores."""
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        _, _, best_model = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )
        X_val, _ = extract_Xy(val_h, preprocessor_metadata, training_parameters)
        preds = best_model.predict(X_val)
        assert np.all(preds >= 0) and np.all(preds <= 1)


# ---- Tests: finalize_model ----


class TestFinalizeModel:
    def _hpo_outputs(self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        return tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )

    def test_hpo_best_passthrough(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """Default strategy returns the HPO best-trial adapter unchanged (identity)."""
        train_h, train_dev_h, *_ = synthetic_model_inputs
        best_params, best_iteration, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )

        params = {**training_parameters,
                  "training": {**training_parameters["training"],
                               "final_model_strategy": "hpo_best"}}

        final = finalize_model(
            train_h, train_dev_h, hpo_best_model, best_params, best_iteration,
            preprocessor_metadata, params,
        )
        assert final is hpo_best_model

    def test_default_strategy_is_hpo_best(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """When `final_model_strategy` key is absent, default to hpo_best."""
        train_h, train_dev_h, *_ = synthetic_model_inputs
        best_params, best_iteration, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )

        # training_parameters has no `final_model_strategy` set
        final = finalize_model(
            train_h, train_dev_h, hpo_best_model, best_params, best_iteration,
            preprocessor_metadata, training_parameters,
        )
        assert final is hpo_best_model

    def test_refit_on_full_returns_new_adapter(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """refit_on_full produces a fresh adapter (not the HPO best one)."""
        train_h, train_dev_h, *_ = synthetic_model_inputs
        best_params, best_iteration, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )

        params = {**training_parameters,
                  "training": {**training_parameters["training"],
                               "final_model_strategy": "refit_on_full"}}

        final = finalize_model(
            train_h, train_dev_h, hpo_best_model, best_params, best_iteration,
            preprocessor_metadata, params,
        )
        assert isinstance(final, ModelAdapter)
        assert final is not hpo_best_model

    def test_refit_on_full_uses_best_iteration(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """Refitted booster runs exactly best_iteration rounds (no early stopping)."""
        from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

        train_h, train_dev_h, *_ = synthetic_model_inputs
        best_params, best_iteration, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )

        params = {**training_parameters,
                  "training": {**training_parameters["training"],
                               "final_model_strategy": "refit_on_full"}}

        final = finalize_model(
            train_h, train_dev_h, hpo_best_model, best_params, best_iteration,
            preprocessor_metadata, params,
        )
        assert isinstance(final, LightGBMAdapter)
        assert final.booster.current_iteration() == best_iteration

    def test_unknown_strategy_raises(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_h, train_dev_h, *_ = synthetic_model_inputs
        best_params, best_iteration, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )

        params = {**training_parameters,
                  "training": {**training_parameters["training"],
                               "final_model_strategy": "bogus"}}

        with pytest.raises(ValueError, match="final_model_strategy"):
            finalize_model(
                train_h, train_dev_h, hpo_best_model, best_params, best_iteration,
                preprocessor_metadata, params,
            )


# ---- Tests: evaluate_model ----


@pytest.fixture
def trained_model_after_finalize(lgb_handles, preprocessor_metadata, training_parameters):
    """Quick-trained LightGBMAdapter used as the post-finalize model under test."""
    return _quick_train_adapter(lgb_handles, training_parameters)


@pytest.fixture
def val_h(synthetic_model_inputs):
    """ParquetHandle pointing at the synthetic validation/eval split."""
    _, _, _val_h, *_ = synthetic_model_inputs
    return _val_h


@pytest.fixture
def calibrated_model(
    trained_model_after_finalize,
    synthetic_model_inputs,
    preprocessor_metadata,
    training_parameters,
):
    """A CalibratedModelAdapter wrapping the post-finalize quick-trained model.

    Uses the train parquet split as the calibration set (mirrors
    `TestCalibrateModel` usage). Available for tests that need to compare
    calibrated vs raw predictions / metrics.
    """
    train_h, *_ = synthetic_model_inputs
    return calibrate_model(
        trained_model_after_finalize, train_h, preprocessor_metadata, training_parameters,
    )


class TestEvaluateModel:
    """evaluate_model returns (predictions_pdf, labels_pdf) tuple after refactor."""

    def test_returns_tuple_of_two_dataframes(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        model = trained_model_after_finalize
        result = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        assert isinstance(result, tuple)
        assert len(result) == 2
        predictions_pdf, labels_pdf = result
        assert isinstance(predictions_pdf, pd.DataFrame)
        assert isinstance(labels_pdf, pd.DataFrame)

    def test_predictions_has_required_columns(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        predictions_pdf, _ = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        for col in schema["identity_columns"]:
            assert col in predictions_pdf.columns
        assert schema["score"] in predictions_pdf.columns
        assert schema["rank"] in predictions_pdf.columns

    def test_labels_has_required_columns(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        _, labels_pdf = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        for col in schema["identity_columns"]:
            assert col in labels_pdf.columns
        assert schema["label"] in labels_pdf.columns

    def test_rank_starts_from_one_per_query(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        predictions_pdf, _ = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        group_cols = [schema["time"]] + schema["entity"]
        min_ranks = predictions_pdf.groupby(group_cols)[schema["rank"]].min()
        assert (min_ranks == 1).all()

    def test_non_calibrated_model_score_uncalibrated_equals_score(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        schema = get_schema(training_parameters)
        model = trained_model_after_finalize
        predictions_pdf, _ = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        assert "score_uncalibrated" in predictions_pdf.columns
        assert (predictions_pdf[schema["score"]] == predictions_pdf["score_uncalibrated"]).all()

    def test_calibrated_model_score_uncalibrated_differs_from_score(
        self, calibrated_model, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.core.schema import get_schema
        assert isinstance(calibrated_model, CalibratedModelAdapter)
        schema = get_schema(training_parameters)
        predictions_pdf, _ = evaluate_model(
            calibrated_model, val_h, preprocessor_metadata, training_parameters
        )
        assert "score_uncalibrated" in predictions_pdf.columns
        assert predictions_pdf["score_uncalibrated"].notna().all()
        # Calibration changes values; at least one row should differ
        assert (
            predictions_pdf[schema["score"]] != predictions_pdf["score_uncalibrated"]
        ).any()


# ---- Tests: log_experiment ----


class TestLogExperiment:
    def test_logs_to_mlflow(
        self, lgb_handles, preprocessor_metadata, training_parameters, tmp_path
    ):
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = _quick_train_adapter(lgb_handles, training_parameters)

        evaluation_results = {
            "overall_map": 0.75,
            "per_product_ap": {"exchange_fx": 0.8, "exchange_usd": 0.7},
            "n_queries": 10,
            "n_excluded_queries": 2,
        }

        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_experiment",
            "tracking_uri": str(tmp_path / "mlruns"),
        }}

        log_experiment(model, best_params, 123, evaluation_results, params)

        # Verify run was created
        import mlflow
        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        experiment = mlflow.get_experiment_by_name("test_experiment")
        assert experiment is not None

        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1
        assert runs.iloc[0]["metrics.overall_map"] == 0.75
        assert runs.iloc[0]["metrics.best_iteration"] == 123
        assert runs.iloc[0]["params.algorithm"] == "lightgbm"
        assert runs.iloc[0]["params.final_model_strategy"] == "hpo_best"
        assert runs.iloc[0]["params.calibrated"] == "False"

    def test_logs_calibration_info(
        self, lgb_handles, preprocessor_metadata, training_parameters, tmp_path
    ):
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = _quick_train_adapter(lgb_handles, training_parameters)

        evaluation_results = {
            "overall_map": 0.76,
            "per_product_ap": {"exchange_fx": 0.8, "exchange_usd": 0.7},
            "n_queries": 10,
            "n_excluded_queries": 2,
            "uncalibrated": {
                "overall_map": 0.75,
                "per_product_ap": {"exchange_fx": 0.78, "exchange_usd": 0.69},
            },
            "calibration_method": "isotonic",
        }

        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_calibrated",
            "tracking_uri": str(tmp_path / "mlruns"),
        }}

        log_experiment(model, best_params, 123, evaluation_results, params)

        import mlflow
        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        experiment = mlflow.get_experiment_by_name("test_calibrated")
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1
        assert runs.iloc[0]["params.calibrated"] == "True"
        assert runs.iloc[0]["params.calibration_method"] == "isotonic"
        assert runs.iloc[0]["metrics.uncalibrated_overall_map"] == 0.75


# ---- Tests: calibrate_model ----


class TestCalibrateModel:
    def _train_quick_model(self, lgb_handles, preprocessor_metadata, training_parameters):
        return _quick_train_adapter(lgb_handles, training_parameters)

    def test_returns_calibrated_adapter(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        train_h, *_ = synthetic_model_inputs
        calibrated = calibrate_model(model, train_h, preprocessor_metadata, training_parameters)
        assert isinstance(calibrated, CalibratedModelAdapter)

    def test_default_method_isotonic(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        train_h, *_ = synthetic_model_inputs
        calibrated = calibrate_model(model, train_h, preprocessor_metadata, training_parameters)
        assert calibrated.method == "isotonic"

    def test_sigmoid_method(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        train_h, *_ = synthetic_model_inputs
        params = {**training_parameters, "training": {
            **training_parameters["training"],
            "calibration": {"method": "sigmoid"},
        }}
        calibrated = calibrate_model(model, train_h, preprocessor_metadata, params)
        assert calibrated.method == "sigmoid"

    def test_calibrated_predict_returns_valid_scores(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        train_h, _, val_h, train_df, _, val_df = synthetic_model_inputs
        calibrated = calibrate_model(model, train_h, preprocessor_metadata, training_parameters)
        X_val, _ = extract_Xy(val_h, preprocessor_metadata, training_parameters)
        preds = calibrated.predict(X_val)
        assert len(preds) == len(val_df)
        assert np.all(np.isfinite(preds))


# ---- Tests: evaluate_model with calibrated model ----


class TestEvaluateModelCalibrated:
    def _train_and_calibrate(self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters):
        model = _quick_train_adapter(lgb_handles, training_parameters)
        train_h, *_ = synthetic_model_inputs
        return calibrate_model(model, train_h, preprocessor_metadata, training_parameters)

    def test_includes_uncalibrated_metrics(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        calibrated = self._train_and_calibrate(lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters)
        _, _, val_h, *_ = synthetic_model_inputs
        results = evaluate_model(calibrated, val_h, preprocessor_metadata, training_parameters)

        assert "uncalibrated" in results
        assert "overall_map" in results["uncalibrated"]
        assert "per_product_ap" in results["uncalibrated"]
        assert "calibration_method" in results
        assert results["calibration_method"] == "isotonic"

    def test_uncalibrated_map_is_float(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        calibrated = self._train_and_calibrate(lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters)
        _, _, val_h, *_ = synthetic_model_inputs
        results = evaluate_model(calibrated, val_h, preprocessor_metadata, training_parameters)

        assert isinstance(results["uncalibrated"]["overall_map"], float)
        assert isinstance(results["overall_map"], float)


# ---- Tests: compute_test_mAP ----


class TestComputeTestMAP:
    """compute_test_mAP computes ranking metrics from (predictions_pdf, labels_pdf)."""

    def test_returns_dict_with_required_keys(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        model = trained_model_after_finalize
        predictions_pdf, labels_pdf = evaluate_model(
            model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert isinstance(result, dict)
        assert "overall_map" in result
        assert "per_product_ap" in result
        assert "n_queries" in result
        assert "n_excluded_queries" in result

    def test_overall_map_in_valid_range(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        model = trained_model_after_finalize
        predictions_pdf, labels_pdf = evaluate_model(
            model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert 0.0 <= result["overall_map"] <= 1.0

    def test_calibrated_model_includes_uncalibrated_subdict(
        self, calibrated_model, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        predictions_pdf, labels_pdf = evaluate_model(
            calibrated_model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert "uncalibrated" in result
        assert "overall_map" in result["uncalibrated"]
        assert "per_product_ap" in result["uncalibrated"]

    def test_non_calibrated_model_no_uncalibrated_subdict(
        self, trained_model_after_finalize, val_h, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training.nodes import compute_test_mAP
        model = trained_model_after_finalize
        predictions_pdf, labels_pdf = evaluate_model(
            model, val_h, preprocessor_metadata, training_parameters
        )
        result = compute_test_mAP(predictions_pdf, labels_pdf, training_parameters)
        assert "uncalibrated" not in result


# ---- Tests: write_test_predictions ----

class TestWriteTestPredictions:
    """write_test_predictions iterates per prod_name and writes to Hive."""

    @pytest.fixture
    def predictions_pdf(self):
        """Non-calibrated run: score_uncalibrated equals score (evaluate_model contract)."""
        import pandas as pd
        return pd.DataFrame({
            "cust_id": ["c1", "c2", "c1", "c2"],
            "snap_date": ["2025-12-31"] * 4,
            "prod_name": ["fund_stock", "fund_stock", "ccard_ins", "ccard_ins"],
            "score": [0.9, 0.7, 0.6, 0.4],
            "score_uncalibrated": [0.9, 0.7, 0.6, 0.4],
            "rank": [1, 2, 1, 2],
        })

    @pytest.fixture
    def parameters_with_model_version(self):
        return {
            "schema": {
                "time_col": "snap_date",
                "entity_cols": ["cust_id"],
                "item_col": "prod_name",
                "label_col": "label",
                "score_col": "score",
                "rank_col": "rank",
            },
            "hive": {"db": "ml_recsys"},
            "model_version": "20260511_153000",
        }

    def test_calls_insertInto_once_per_prod_name(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        mock_write = MagicMock()
        mock_spark.createDataFrame.return_value.write = mock_write

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        # 2 distinct prod_names -> 2 insertInto calls
        assert mock_write.insertInto.call_count == 2

    def test_each_chunk_filtered_to_one_prod(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        captured_chunks = []

        def capture_create_df(pdf):
            captured_chunks.append(pdf.copy())
            return MagicMock()
        mock_spark.createDataFrame.side_effect = capture_create_df

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        assert len(captured_chunks) == 2
        for chunk in captured_chunks:
            assert chunk["prod_name"].nunique() == 1

    def test_ensures_table_via_create_if_not_exists(
        self, predictions_pdf, parameters_with_model_version
    ):
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        mock_spark.createDataFrame.return_value.write = MagicMock()

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        ddl_calls = [
            call_args
            for call_args in mock_spark.sql.call_args_list
            if "CREATE TABLE IF NOT EXISTS" in str(call_args)
        ]
        assert len(ddl_calls) == 1, f"expected 1 CREATE TABLE call, got {len(ddl_calls)}"
        assert "training_eval_predictions" in str(ddl_calls[0])
        # DDL must include score_uncalibrated column
        assert "score_uncalibrated" in str(ddl_calls[0])

    def test_raises_when_score_uncalibrated_column_missing(
        self, parameters_with_model_version
    ):
        """Contract violation: evaluate_model must always populate score_uncalibrated."""
        import pandas as pd
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        bad_pdf = pd.DataFrame({
            "cust_id": ["c1"],
            "snap_date": ["2025-12-31"],
            "prod_name": ["fund_stock"],
            "score": [0.9],
            "rank": [1],
        })
        with pytest.raises(RuntimeError, match="score_uncalibrated"):
            write_test_predictions(bad_pdf, parameters_with_model_version)

    def test_non_calibrated_run_preserves_equal_scores(
        self, predictions_pdf, parameters_with_model_version
    ):
        """Non-calibrated input has score == score_uncalibrated; pass through unchanged."""
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        mock_spark = MagicMock(name="SparkSession")
        captured_chunks = []

        def capture_create_df(pdf):
            captured_chunks.append(pdf.copy())
            return MagicMock()
        mock_spark.createDataFrame.side_effect = capture_create_df

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        for chunk in captured_chunks:
            assert "score_uncalibrated" in chunk.columns
            assert (chunk["score"] == chunk["score_uncalibrated"]).all()

    def test_calibrated_run_preserves_score_uncalibrated(
        self, parameters_with_model_version
    ):
        """Calibrated input pdf has score_uncalibrated differing from score; pass through."""
        import pandas as pd
        from unittest.mock import MagicMock, patch
        from recsys_tfb.pipelines.training.nodes import write_test_predictions

        predictions_pdf = pd.DataFrame({
            "cust_id": ["c1", "c2"],
            "snap_date": ["2025-12-31", "2025-12-31"],
            "prod_name": ["fund_stock", "fund_stock"],
            "score": [0.9, 0.7],
            "score_uncalibrated": [0.85, 0.65],
            "rank": [1, 2],
        })

        mock_spark = MagicMock(name="SparkSession")
        captured_chunks = []

        def capture_create_df(pdf):
            captured_chunks.append(pdf.copy())
            return MagicMock()
        mock_spark.createDataFrame.side_effect = capture_create_df

        with patch(
            "recsys_tfb.pipelines.training.nodes.get_or_create_spark_session",
            return_value=mock_spark,
        ):
            write_test_predictions(predictions_pdf, parameters_with_model_version)

        assert len(captured_chunks) == 1
        chunk = captured_chunks[0]
        assert list(chunk["score_uncalibrated"]) == [0.85, 0.65]
