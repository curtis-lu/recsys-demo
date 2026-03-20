"""Tests for training pipeline nodes."""

import json

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.evaluation.metrics import compute_ap
from recsys_tfb.pipelines.training.nodes import (
    _compute_map,
    compare_model_versions,
    evaluate_model,
    log_experiment,
    train_model,
    tune_hyperparameters,
)


# ---- Fixtures ----


@pytest.fixture
def training_parameters():
    return {
        "random_seed": 42,
        "training": {
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
def synthetic_data():
    """Create synthetic train/dev/val data mimicking dataset pipeline output."""
    rng = np.random.RandomState(42)
    n_train, n_dev, n_val = 120, 40, 40  # 10/3/3 customers x 4 products

    def make_features(n):
        return pd.DataFrame({
            "prod_name": np.tile([0, 1, 2, 3], n // 4),
            "total_aum": rng.uniform(100, 1000, n),
            "fund_aum": rng.uniform(10, 100, n),
            "in_amt_sum_l1m": rng.uniform(0, 50, n),
            "out_amt_sum_l1m": rng.uniform(0, 30, n),
        })

    def make_labels(n):
        return pd.DataFrame({"label": rng.binomial(1, 0.15, n).astype(float)})

    X_train = make_features(n_train)
    y_train = make_labels(n_train)
    X_dev = make_features(n_dev)
    y_dev = make_labels(n_dev)
    X_val = make_features(n_val)
    y_val = make_labels(n_val)

    return X_train, y_train, X_dev, y_dev, X_val, y_val


@pytest.fixture
def val_set():
    """Create val_set DataFrame with query group columns."""
    products = ["exchange_fx", "exchange_usd", "fund_stock", "fund_bond"]
    rows = []
    for snap in ["2024-02-29", "2024-03-31"]:
        snap_dt = pd.Timestamp(snap)
        for cid in ["C001", "C002", "C003", "C004", "C005"]:
            for prod in products:
                rows.append({
                    "snap_date": snap_dt,
                    "cust_id": cid,
                    "prod_name": prod,
                })
    return pd.DataFrame(rows)


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


# ---- Tests: _compute_map ----


class TestComputeMAP:
    def test_excludes_all_zero_queries(self):
        y_true = np.array([1, 0, 0, 0, 0, 0])
        y_score = np.array([0.9, 0.1, 0.5, 0.3, 0.2, 0.1])
        groups = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-31"] * 3 + ["2024-01-31"] * 3),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
        })
        # C001 has 1 positive, C002 has 0 → C002 excluded
        mean_ap, n_excluded = _compute_map(y_true, y_score, groups)
        assert n_excluded == 1
        assert mean_ap > 0

    def test_perfect_map(self):
        # Two queries, both perfectly ranked
        y_true = np.array([1, 0, 0, 1, 0, 0])
        y_score = np.array([0.9, 0.2, 0.1, 0.8, 0.3, 0.1])
        groups = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-31"] * 3 + ["2024-01-31"] * 3),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
        })
        mean_ap, _ = _compute_map(y_true, y_score, groups)
        assert mean_ap == 1.0


# ---- Tests: tune_hyperparameters ----


class TestTuneHyperparameters:
    def test_returns_valid_params(self, synthetic_data, training_parameters):
        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        best_params = tune_hyperparameters(X_train, y_train, X_dev, y_dev, training_parameters)

        assert isinstance(best_params, dict)
        assert "learning_rate" in best_params
        assert "num_leaves" in best_params
        assert "max_depth" in best_params

    def test_params_in_search_space(self, synthetic_data, training_parameters):
        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        space = training_parameters["training"]["search_space"]
        best_params = tune_hyperparameters(X_train, y_train, X_dev, y_dev, training_parameters)

        assert space["num_leaves"]["low"] <= best_params["num_leaves"] <= space["num_leaves"]["high"]
        assert space["max_depth"]["low"] <= best_params["max_depth"] <= space["max_depth"]["high"]

    def test_reproducible(self, synthetic_data, training_parameters):
        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        r1 = tune_hyperparameters(X_train, y_train, X_dev, y_dev, training_parameters)
        r2 = tune_hyperparameters(X_train, y_train, X_dev, y_dev, training_parameters)
        assert r1 == r2


# ---- Tests: train_model ----


class TestTrainModel:
    def test_returns_booster(self, synthetic_data, training_parameters):
        import lightgbm as lgb

        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = train_model(X_train, y_train, X_dev, y_dev, best_params, training_parameters)
        assert isinstance(model, lgb.Booster)

    def test_predictions_are_probabilities(self, synthetic_data, training_parameters):
        X_train, y_train, X_dev, y_dev, X_val, _ = synthetic_data
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = train_model(X_train, y_train, X_dev, y_dev, best_params, training_parameters)
        preds = model.predict(X_val)
        assert np.all(preds >= 0) and np.all(preds <= 1)

    def test_early_stopping(self, synthetic_data, training_parameters):
        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        params = {**training_parameters, "training": {**training_parameters["training"], "num_iterations": 500, "early_stopping_rounds": 5}}
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = train_model(X_train, y_train, X_dev, y_dev, best_params, params)
        # With small data and early_stopping_rounds=5, should stop before 500
        assert model.current_iteration() < 500


# ---- Tests: evaluate_model ----


class TestEvaluateModel:
    def _train_quick_model(self, synthetic_data, training_parameters):
        import lightgbm as lgb

        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        return train_model(X_train, y_train, X_dev, y_dev, best_params, training_parameters)

    def test_returns_evaluation_dict(self, synthetic_data, val_set, training_parameters):
        model = self._train_quick_model(synthetic_data, training_parameters)
        _, _, _, _, X_val, y_val = synthetic_data
        results = evaluate_model(model, X_val, y_val, val_set, training_parameters)

        assert "overall_map" in results
        assert "per_product_ap" in results
        assert "n_queries" in results
        assert "n_excluded_queries" in results
        assert isinstance(results["overall_map"], float)
        assert isinstance(results["per_product_ap"], dict)

    def test_perfect_model_high_map(self):
        """A mock model with perfect scores should yield mAP=1.0."""
        import lightgbm as lgb

        # Create simple data where we can control predictions
        y_true = np.array([1, 0, 0, 1, 0, 0])
        y_score = np.array([0.9, 0.2, 0.1, 0.8, 0.3, 0.1])

        groups = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-31"] * 3 + ["2024-01-31"] * 3),
            "cust_id": ["C001"] * 3 + ["C002"] * 3,
        })

        mean_ap, _ = _compute_map(y_true, y_score, groups)
        assert mean_ap == 1.0

    def test_all_zero_query_excluded(self):
        """Queries with no positive labels are excluded from mAP."""
        y_true = np.array([0, 0, 0])
        y_score = np.array([0.5, 0.3, 0.1])
        groups = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-01-31"] * 3),
            "cust_id": ["C001"] * 3,
        })
        _, n_excluded = _compute_map(y_true, y_score, groups)
        assert n_excluded == 1

    def test_per_product_ap_values(self, synthetic_data, training_parameters):
        """Per-product AP values match manual _compute_ap and exclude all-0-label products."""
        model = self._train_quick_model(synthetic_data, training_parameters)
        _, _, _, _, X_val, y_val = synthetic_data

        # Build val_set with controlled labels: product "zero" has all-0 labels
        products = ["exchange_fx", "exchange_usd", "zero"]
        n_per_prod = 10
        rng = np.random.RandomState(99)

        val_set = pd.DataFrame({
            "snap_date": pd.to_datetime(["2024-03-31"] * n_per_prod * len(products)),
            "cust_id": [f"C{i:03d}" for i in range(n_per_prod)] * len(products),
            "prod_name": np.repeat(products, n_per_prod),
        })
        # Match X_val shape to val_set length
        X_val_extended = pd.DataFrame({
            "prod_name": np.repeat([0, 1, 2], n_per_prod),
            "total_aum": rng.uniform(100, 1000, n_per_prod * len(products)),
            "fund_aum": rng.uniform(10, 100, n_per_prod * len(products)),
            "in_amt_sum_l1m": rng.uniform(0, 50, n_per_prod * len(products)),
            "out_amt_sum_l1m": rng.uniform(0, 30, n_per_prod * len(products)),
        })
        # Labels: exchange_fx and exchange_usd have some positives, zero has none
        y_val_extended = pd.DataFrame({"label": np.array(
            [1, 0, 1, 0, 0, 0, 1, 0, 0, 0]  # exchange_fx: 3 positives
            + [0, 1, 0, 0, 1, 0, 0, 0, 0, 0]  # exchange_usd: 2 positives
            + [0] * n_per_prod  # zero: no positives
        ).astype(float)})

        results = evaluate_model(model, X_val_extended, y_val_extended, val_set, training_parameters)
        per_product_ap = results["per_product_ap"]

        # All-0-label product must be excluded
        assert "zero" not in per_product_ap

        # Each product with positives must have its own AP entry
        assert "exchange_fx" in per_product_ap
        assert "exchange_usd" in per_product_ap

        # Values must match manual _compute_ap calculation
        y_score = model.predict(X_val_extended)
        for prod in ["exchange_fx", "exchange_usd"]:
            idx = val_set.index[val_set["prod_name"] == prod].values
            expected_ap = compute_ap(y_val_extended["label"].values[idx], y_score[idx])
            assert per_product_ap[prod] == pytest.approx(expected_ap)


# ---- Tests: log_experiment ----


class TestLogExperiment:
    def test_logs_to_mlflow(self, synthetic_data, training_parameters, tmp_path):
        import lightgbm as lgb

        X_train, y_train, X_dev, y_dev, _, _ = synthetic_data
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = train_model(X_train, y_train, X_dev, y_dev, best_params, training_parameters)

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

        log_experiment(model, best_params, evaluation_results, params)

        # Verify run was created
        import mlflow
        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        experiment = mlflow.get_experiment_by_name("test_experiment")
        assert experiment is not None

        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1
        assert runs.iloc[0]["metrics.overall_map"] == 0.75


# ---- Tests: compare_model_versions ----


def _create_version_dir(base_dir, version_name, overall_map, per_product_ap=None):
    """Helper to create a version directory with evaluation_results.json."""
    version_dir = base_dir / version_name
    version_dir.mkdir(parents=True)
    results = {"overall_map": overall_map, "per_product_ap": per_product_ap or {}}
    (version_dir / "evaluation_results.json").write_text(json.dumps(results))
    return version_dir


class TestCompareModelVersions:
    def test_multiple_versions_ranked_by_map(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_version_dir(models_dir, "20260315_100000", 0.65, {"exchange_fx": 0.7})
        _create_version_dir(models_dir, "20260316_100000", 0.80, {"exchange_fx": 0.85})
        _create_version_dir(models_dir, "20260317_100000", 0.72, {"exchange_fx": 0.75})

        result = compare_model_versions({}, {"models_dir": str(models_dir)})
        assert len(result["versions"]) == 3
        assert result["versions"][0]["version"] == "20260316_100000"
        assert result["recommended_version"] == "20260316_100000"

    def test_single_version(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_version_dir(models_dir, "20260316_100000", 0.75)

        result = compare_model_versions({}, {"models_dir": str(models_dir)})
        assert len(result["versions"]) == 1
        assert result["recommended_version"] == "20260316_100000"

    def test_ignores_non_version_dirs(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_version_dir(models_dir, "20260316_100000", 0.75)
        # These should be ignored
        (models_dir / "best").mkdir()
        (models_dir / "some_random_dir").mkdir()

        result = compare_model_versions({}, {"models_dir": str(models_dir)})
        assert len(result["versions"]) == 1

    def test_detects_current_best(self, tmp_path):
        models_dir = tmp_path / "models"
        _create_version_dir(models_dir, "20260316_100000", 0.75)
        _create_version_dir(models_dir, "20260317_100000", 0.80)
        # Create best/ with same mAP as 20260316
        _create_version_dir(models_dir, "best", 0.75)

        result = compare_model_versions({}, {"models_dir": str(models_dir)})
        assert result["current_best_version"] == "20260316_100000"

    def test_empty_models_dir(self, tmp_path):
        models_dir = tmp_path / "models"
        models_dir.mkdir()

        result = compare_model_versions({}, {"models_dir": str(models_dir)})
        assert result["versions"] == []
        assert result["recommended_version"] is None
        assert result["current_best_version"] is None
