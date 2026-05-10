"""Tests for training pipeline nodes."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.metrics import compute_all_metrics, compute_ap
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


class TestEvaluateModel:
    def _train_quick_model(self, lgb_handles, preprocessor_metadata, training_parameters):
        return _quick_train_adapter(lgb_handles, training_parameters)

    def test_returns_evaluation_dict(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        _, _, val_h, *_ = synthetic_model_inputs
        results = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)

        assert "overall_map" in results
        assert "per_product_ap" in results
        assert "n_queries" in results
        assert "n_excluded_queries" in results
        assert isinstance(results["overall_map"], float)
        assert isinstance(results["per_product_ap"], dict)

    def test_overall_map_matches_compute_all_metrics(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """evaluate_model overall_map matches direct compute_all_metrics call."""
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        _, _, val_h, _, _, val_df = synthetic_model_inputs

        results = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)

        # Reproduce via compute_all_metrics directly
        X, _ = extract_Xy(val_h, preprocessor_metadata, training_parameters)
        y_score = model.predict(X)
        predictions = val_df[["snap_date", "cust_id", "prod_name"]].reset_index(drop=True).copy()
        predictions["score"] = y_score
        predictions["rank"] = (
            predictions.groupby(["snap_date", "cust_id"])["score"]
            .rank(method="first", ascending=False).astype(int)
        )
        labels = val_df[["snap_date", "cust_id", "prod_name", "label"]].reset_index(drop=True)

        metrics = compute_all_metrics(predictions, labels, k_values=["all"])
        n_products = predictions["prod_name"].nunique()
        map_key = f"map@{n_products}"

        assert results["overall_map"] == pytest.approx(metrics["overall"][map_key])
        assert results["n_queries"] == metrics["n_queries"]
        assert results["n_excluded_queries"] == metrics["n_excluded_queries"]

    def test_per_product_ap_matches_compute_all_metrics(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """evaluate_model per_product_ap matches compute_all_metrics per_product."""
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)
        _, _, val_h, _, _, val_df = synthetic_model_inputs

        results = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)

        X, _ = extract_Xy(val_h, preprocessor_metadata, training_parameters)
        y_score = model.predict(X)
        predictions = val_df[["snap_date", "cust_id", "prod_name"]].reset_index(drop=True).copy()
        predictions["score"] = y_score
        predictions["rank"] = (
            predictions.groupby(["snap_date", "cust_id"])["score"]
            .rank(method="first", ascending=False).astype(int)
        )
        labels = val_df[["snap_date", "cust_id", "prod_name", "label"]].reset_index(drop=True)

        metrics = compute_all_metrics(predictions, labels, k_values=["all"])
        n_products = predictions["prod_name"].nunique()
        map_key = f"map@{n_products}"

        expected_per_product = {
            prod: vals[map_key] for prod, vals in metrics["per_product"].items()
        }
        assert results["per_product_ap"] == pytest.approx(expected_per_product)

    def test_per_product_ap_values(
        self, lgb_handles, preprocessor_metadata, training_parameters, tmp_path
    ):
        """Per-product AP values match manual _compute_ap and exclude all-0-label products."""
        model = self._train_quick_model(lgb_handles, preprocessor_metadata, training_parameters)

        # Build val_model_input with controlled labels: product "zero" has all-0 labels
        # Note: preprocessor_metadata only maps known products, so we use products it knows
        products = ["exchange_fx", "exchange_usd", "fund_bond"]
        n_per_prod = 10
        rng = np.random.RandomState(99)

        val_df = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * n_per_prod * len(products)),
            "cust_id": [f"C{i:03d}" for i in range(n_per_prod)] * len(products),
            "prod_name": np.repeat(products, n_per_prod),
            "label": np.array(
                [1, 0, 1, 0, 0, 0, 1, 0, 0, 0]  # exchange_fx: 3 positives
                + [0, 1, 0, 0, 1, 0, 0, 0, 0, 0]  # exchange_usd: 2 positives
                + [0] * n_per_prod  # fund_bond: no positives
            ).astype(float),
            "total_aum": rng.uniform(100, 1000, n_per_prod * len(products)),
            "fund_aum": rng.uniform(10, 100, n_per_prod * len(products)),
            "in_amt_sum_l1m": rng.uniform(0, 50, n_per_prod * len(products)),
            "out_amt_sum_l1m": rng.uniform(0, 30, n_per_prod * len(products)),
        })

        val_path = tmp_path / "val_per_product.parquet"
        val_df.to_parquet(val_path)
        val_h = ParquetHandle(str(val_path))

        results = evaluate_model(model, val_h, preprocessor_metadata, training_parameters)
        per_product_ap = results["per_product_ap"]

        # All-0-label product must be excluded
        assert "fund_bond" not in per_product_ap

        # Each product with positives must have its own AP entry
        assert "exchange_fx" in per_product_ap
        assert "exchange_usd" in per_product_ap

        # Values must be valid AP scores
        for prod in ["exchange_fx", "exchange_usd"]:
            assert 0.0 <= per_product_ap[prod] <= 1.0


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
