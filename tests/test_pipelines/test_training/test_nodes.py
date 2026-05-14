"""Tests for training pipeline nodes."""

import logging
import re

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

    def test_emits_trial_start_and_completed_info_lines(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """Every trial emits a start INFO and a completed INFO with the
        expected `trial=N/total ...` shape. trial_idx covers 0..n_trials-1.
        """
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        n_trials = training_parameters["training"]["n_trials"]

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        messages = [r.getMessage() for r in caplog.records]
        start_lines = [
            m for m in messages
            if re.match(rf"tune_hyperparameters: trial=\d+/{n_trials} start ", m)
        ]
        completed_lines = [
            m for m in messages
            if re.match(rf"tune_hyperparameters: trial=\d+/{n_trials} completed ", m)
        ]
        assert len(start_lines) == n_trials
        assert len(completed_lines) == n_trials

        # trial_idx covers 0..n_trials-1, in order
        start_indices = [
            int(re.search(r"trial=(\d+)/", m).group(1)) for m in start_lines
        ]
        completed_indices = [
            int(re.search(r"trial=(\d+)/", m).group(1)) for m in completed_lines
        ]
        assert start_indices == list(range(n_trials))
        assert completed_indices == list(range(n_trials))

    def test_completed_line_has_correct_best_so_far(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """best_so_far in each completed INFO is monotonically non-decreasing,
        and the final value matches the study's best_value (i.e. the maximum
        ap actually achieved)."""
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        completed = [
            r.getMessage()
            for r in caplog.records
            if "completed ap=" in r.getMessage()
            and "tune_hyperparameters: trial=" in r.getMessage()
        ]
        best_so_far_values = [
            float(re.search(r"best_so_far=([\d.]+)", m).group(1))
            for m in completed
        ]
        # Monotonic non-decreasing
        for prev, curr in zip(best_so_far_values, best_so_far_values[1:]):
            assert curr >= prev, (
                f"best_so_far decreased from {prev} to {curr} across trials"
            )

        ap_values = [
            float(re.search(r"\bap=([\d.]+)", m).group(1)) for m in completed
        ]
        assert best_so_far_values[-1] == pytest.approx(max(ap_values))

    def test_start_line_params_contains_only_search_dimensions(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """The `start` line's params={...} prints the search-space dimensions
        (trial_params), NOT the expanded full params dict (which would also
        contain algorithm_params keys like 'objective' / 'metric')."""
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        start_lines = [
            r.getMessage()
            for r in caplog.records
            if "tune_hyperparameters: trial=" in r.getMessage()
            and " start " in r.getMessage()
        ]
        assert start_lines, "no trial start lines emitted"

        for m in start_lines:
            # Must contain the search-space keys
            assert "learning_rate" in m
            assert "num_leaves" in m
            assert "max_depth" in m
            # Must NOT contain algorithm_params keys
            assert "'objective'" not in m
            assert "'metric'" not in m
            assert "'verbosity'" not in m

    def test_emits_inner_step_events_per_trial(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """Each trial emits 4 inner log_step events: prepare_datasets, train,
        predict, score. Both step_started and step_completed fire for each.
        """
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        n_trials = training_parameters["training"]["n_trials"]
        expected_steps = {"prepare_datasets", "train", "predict", "score"}

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        started = [
            r.step
            for r in caplog.records
            if getattr(r, "event", None) == "step_started"
            and getattr(r, "step", None) in expected_steps
        ]
        completed = [
            r.step
            for r in caplog.records
            if getattr(r, "event", None) == "step_completed"
            and getattr(r, "step", None) in expected_steps
        ]

        # Each inner step fires once per trial → n_trials times total
        for step_name in expected_steps:
            assert started.count(step_name) == n_trials, (
                f"step_started count for {step_name!r} = "
                f"{started.count(step_name)}, expected {n_trials}"
            )
            assert completed.count(step_name) == n_trials, (
                f"step_completed count for {step_name!r} = "
                f"{completed.count(step_name)}, expected {n_trials}"
            )


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


