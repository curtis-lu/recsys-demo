"""Tests for training pipeline nodes."""

import logging
import re
from pathlib import Path

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
            "search_space": [
                {"name": "learning_rate", "type": "float", "low": 0.01, "high": 0.3, "log": True},
                {"name": "num_leaves", "type": "int", "low": 16, "high": 64},
                {"name": "max_depth", "type": "int", "low": 3, "high": 8},
                {"name": "min_child_samples", "type": "int", "low": 5, "high": 50},
                {"name": "subsample", "type": "float", "low": 0.6, "high": 1.0},
                {"name": "colsample_bytree", "type": "float", "low": 0.6, "high": 1.0},
            ],
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


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """tune_hyperparameters 會寫 ./data/models/_hpo/<sid>/；隔離到各測試自己的 tmp cwd。"""
    monkeypatch.chdir(tmp_path)


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
        space = {s["name"]: s for s in training_parameters["training"]["search_space"]}
        best_params, _, _ = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )

        assert space["num_leaves"]["low"] <= best_params["num_leaves"] <= space["num_leaves"]["high"]
        assert space["max_depth"]["low"] <= best_params["max_depth"] <= space["max_depth"]["high"]

    def test_reproducible(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        params = {**training_parameters, "hpo_checkpointing": False}
        p1, i1, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params)
        p2, i2, _ = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params)
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
            if "completed score=" in r.getMessage()
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

        score_values = [
            float(re.search(r"\bscore=([\d.]+)", m).group(1)) for m in completed
        ]
        assert best_so_far_values[-1] == pytest.approx(max(score_values))

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

    def test_resume_only_runs_remaining(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        from recsys_tfb.pipelines.training import hpo_resume
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]

        def run(n):
            p = {
                **training_parameters, "search_id": "resumesid",
                "training": {**training_parameters["training"], "n_trials": n},
            }
            return tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p
            )

        run(2)
        sd = hpo_resume.hpo_study_dir("resumesid")
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "resumesid", 42)) == 2
        run(4)  # n_trials 不在 search_id 內 → 同一 study → 只補 2 個
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "resumesid", 42)) == 4

    def test_fresh_hpo_clears_and_logs_discard(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        from recsys_tfb.pipelines.training import hpo_resume
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        base = {
            **training_parameters, "search_id": "freshsid",
            "training": {**training_parameters["training"], "n_trials": 2},
        }
        tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, base)
        sd = hpo_resume.hpo_study_dir("freshsid")
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "freshsid", 42)) == 2

        with caplog.at_level(logging.WARNING):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata,
                {**base, "_fresh_hpo": True},
            )
        assert any(
            "--fresh-hpo" in r.getMessage() and "discarding 2" in r.getMessage()
            for r in caplog.records
        )
        assert hpo_resume.count_completed(hpo_resume.open_study(sd, "freshsid", 42)) == 2

    def test_checkpointing_disabled_writes_no_files(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        p = {
            **training_parameters, "search_id": "nocp", "hpo_checkpointing": False,
            "training": {**training_parameters["training"], "n_trials": 2},
        }
        _, _, bm = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p
        )
        assert bm is not None
        assert not (Path("data") / "models" / "_hpo" / "nocp").exists()

    def test_resume_recovers_best_model_without_retrain(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        # After a 2-trial run, a re-run that adds 0 trials (same n_trials) must
        # still return a usable model loaded from checkpoint (remaining==0 path).
        train_lgb_h, train_dev_lgb_h = lgb_handles
        val_h = synthetic_model_inputs[2]
        p = {
            **training_parameters, "search_id": "recoversid",
            "training": {**training_parameters["training"], "n_trials": 2},
        }
        tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p)
        bp, bi, bm = tune_hyperparameters(train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, p)
        assert bm is not None
        import numpy as np
        assert bm.predict(np.zeros((3, len(preprocessor_metadata["feature_columns"])))).shape == (3,)


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

    def test_hpo_best_booster_has_real_feature_names(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """The hpo_best model trains on the cached .bin; its booster must report
        real feature names (from feature_columns), not Column_N defaults."""
        _, _, hpo_best_model = self._hpo_outputs(
            lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters,
        )
        assert hpo_best_model.booster.feature_name() == preprocessor_metadata["feature_columns"]

    def test_refit_on_full_booster_has_real_feature_names(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        """refit_on_full builds a fresh lgb.Dataset; its booster must carry real
        feature names so feature_importance.json shows real names."""
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
        assert final.booster.feature_name() == preprocessor_metadata["feature_columns"]

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
            "per_item_map_attr": {"exchange_fx": 0.8, "exchange_usd": 0.7},
            "n_queries": 10,
            "n_excluded_queries": 2,
        }

        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_experiment",
            "tracking_uri": str(tmp_path / "mlruns"),
        }}

        log_experiment(model, best_params, 123, evaluation_results, {}, {}, {}, params)

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

    def test_mlflow_failure_does_not_raise(
        self, lgb_handles, training_parameters, tmp_path, monkeypatch, caplog
    ):
        """MLflow 寫入失敗時，node 應記 warning 後正常返回，不中斷 pipeline。"""
        import recsys_tfb.pipelines.training.nodes as nodes_mod

        model = _quick_train_adapter(lgb_handles, training_parameters)
        evaluation_results = {
            "overall_map": 0.75,
            "per_item_map_attr": {},
            "n_queries": 10,
            "n_excluded_queries": 2,
        }
        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_resilient",
            "tracking_uri": str(tmp_path / "mlruns"),
        }}

        def _boom(*_args, **_kwargs):
            raise RuntimeError(
                "API request to endpoint /api/2.0/mlflow/logged-models "
                "failed with error code 404 != 200"
            )

        monkeypatch.setattr(nodes_mod.mlflow, "start_run", _boom)

        with caplog.at_level("WARNING"):
            # 不應 raise
            log_experiment(model, {"learning_rate": 0.1}, 123, evaluation_results, {}, {}, {}, params)

        assert any("mlflow" in r.message.lower() for r in caplog.records)

    def test_mlflow_failure_strict_reraises(
        self, lgb_handles, training_parameters, tmp_path, monkeypatch
    ):
        """strict=True 時保留硬失敗行為（供 CI / 嚴格環境 opt-in）。"""
        import recsys_tfb.pipelines.training.nodes as nodes_mod

        model = _quick_train_adapter(lgb_handles, training_parameters)
        evaluation_results = {
            "overall_map": 0.75, "per_item_map_attr": {},
            "n_queries": 10, "n_excluded_queries": 2,
        }
        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_strict",
            "tracking_uri": str(tmp_path / "mlruns"),
            "strict": True,
        }}

        def _boom(*_args, **_kwargs):
            raise RuntimeError("404 logged-models")

        monkeypatch.setattr(nodes_mod.mlflow, "start_run", _boom)

        with pytest.raises(RuntimeError):
            log_experiment(model, {"learning_rate": 0.1}, 123, evaluation_results, {}, {}, {}, params)

    def test_logs_calibration_info(
        self, lgb_handles, preprocessor_metadata, training_parameters, tmp_path
    ):
        best_params = {"learning_rate": 0.1, "num_leaves": 31, "max_depth": 5,
                       "min_child_samples": 10, "subsample": 0.8, "colsample_bytree": 0.8}
        model = _quick_train_adapter(lgb_handles, training_parameters)

        evaluation_results = {
            "overall_map": 0.76,
            "per_item_map_attr": {"exchange_fx": 0.8, "exchange_usd": 0.7},
            "n_queries": 10,
            "n_excluded_queries": 2,
            "uncalibrated": {
                "overall_map": 0.75,
                "per_item_map_attr": {"exchange_fx": 0.78, "exchange_usd": 0.69},
            },
            "calibration_method": "isotonic",
        }

        params = {**training_parameters, "mlflow": {
            "experiment_name": "test_calibrated",
            "tracking_uri": str(tmp_path / "mlruns"),
        }}

        log_experiment(model, best_params, 123, evaluation_results, {}, {}, {}, params)

        import mlflow
        mlflow.set_tracking_uri(str(tmp_path / "mlruns"))
        experiment = mlflow.get_experiment_by_name("test_calibrated")
        runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id])
        assert len(runs) == 1
        assert runs.iloc[0]["params.calibrated"] == "True"
        assert runs.iloc[0]["params.calibration_method"] == "isotonic"
        assert runs.iloc[0]["metrics.uncalibrated_overall_map"] == 0.75


def test_log_experiment_logs_diagnostics(monkeypatch, tmp_path):
    import recsys_tfb.pipelines.training.nodes as nodes

    logged_metrics, logged_artifacts = {}, []
    monkeypatch.setattr(nodes.mlflow, "set_tracking_uri", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "set_experiment", lambda *a, **k: None)

    class _Run:
        def __enter__(self): return self
        def __exit__(self, *a): return False
    monkeypatch.setattr(nodes.mlflow, "start_run", lambda *a, **k: _Run())
    monkeypatch.setattr(nodes.mlflow, "log_params", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "log_param", lambda *a, **k: None)
    monkeypatch.setattr(nodes.mlflow, "log_metric", lambda k, v: logged_metrics.__setitem__(k, v))
    monkeypatch.setattr(nodes.mlflow, "log_artifacts", lambda d, *a, **k: logged_artifacts.append(d))

    monkeypatch.chdir(tmp_path)
    parameters = {"model_version": "mv1", "mlflow": {}, "training": {}}
    from recsys_tfb.diagnosis.model import diagnostics_dir
    diagnostics_dir(parameters)  # create the dir so log_artifacts has something

    class _Model:
        def log_to_mlflow(self): pass

    eval_results = {"overall_map": 0.5, "per_item_map_attr": {}, "n_queries": 10, "n_excluded_queries": 0}
    feature_statistics = {"f0": {"single_value": True, "high_null": False, "null_rate": 0.0, "n_distinct": 1}}
    feature_importance = {"ranked": [], "dead_features": ["f3", "f4"]}
    shap_diagnostics = {"global": {"top_features": []}, "per_item": {}, "item_idiosyncrasy": []}

    nodes.log_experiment(_Model(), {}, 10, eval_results, feature_statistics,
                         feature_importance, shap_diagnostics, parameters)

    assert logged_metrics["n_dead_features"] == 2
    assert logged_metrics["n_single_value_features"] == 1
    assert logged_metrics["n_high_null_features"] == 0
    assert len(logged_artifacts) == 1  # whole diagnostics dir uploaded once


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


def test_tune_defaults_ranking_metric(monkeypatch):
    """algorithm_params with a ranking objective and no metric => params
    passed to adapter.train carry metric='ndcg'."""
    import numpy as np
    from recsys_tfb.pipelines.training import nodes

    captured = {}

    class FakeAdapter:
        booster = type("B", (), {"best_iteration": 3})()

        def train(self, **kw):
            captured.update(kw["params"])

        def predict(self, X):
            return np.zeros(len(X))

    monkeypatch.setattr(nodes, "get_adapter", lambda algo: FakeAdapter())
    monkeypatch.setattr(nodes, "compute_mean_ap", lambda g, y, p: 0.5)

    def fake_extract(handle, meta, params, **kw):
        X = np.zeros((4, 2)); y = np.array([1, 0, 1, 0])
        g = np.array([0, 0, 1, 1], dtype=np.int64)
        return X, y, g

    monkeypatch.setattr(
        "recsys_tfb.io.extract.extract_Xy_with_groups", fake_extract
    )

    class FakeLgbHandle:
        def load(self, reference=None, params=None):
            class D:
                def construct(self_inner):
                    return self_inner
            return D()

    parameters = {
        # FakeAdapter has no .save(); checkpointing (default True) would call it.
        # This test only asserts metric defaulting, unrelated to persistence.
        "hpo_checkpointing": False,
        "training": {
            "n_trials": 1,
            "num_iterations": 5,
            "early_stopping_rounds": 2,
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "lambdarank"},
            "search_space": [
                {"name": "learning_rate", "type": "float", "low": 0.01, "high": 0.1, "log": True},
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8},
                {"name": "max_depth", "type": "int", "low": 3, "high": 5},
                {"name": "min_child_samples", "type": "int", "low": 5, "high": 10},
                {"name": "subsample", "type": "float", "low": 0.6, "high": 1.0},
                {"name": "colsample_bytree", "type": "float", "low": 0.6, "high": 1.0},
            ],
        },
        "random_seed": 42,
    }
    nodes.tune_hyperparameters(
        FakeLgbHandle(), FakeLgbHandle(), object(), {}, parameters
    )
    assert captured.get("metric") == "ndcg"


def test_finalize_refit_ranking_sets_group(monkeypatch):
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.pipelines.training import nodes

    captured = {}

    def fake_extract_groups(handle, meta, params, **kw):
        # train: 2 groups of 2 ; dev: 1 group of 2
        if getattr(handle, "tag", "") == "dev":
            X = np.ones((2, 2)); y = np.array([1, 0])
            g = np.array([0, 0], dtype=np.int64)
        else:
            X = np.zeros((4, 2)); y = np.array([1, 0, 0, 1])
            g = np.array([0, 0, 1, 1], dtype=np.int64)
        if kw.get("with_weights"):
            return X, y, g, np.ones(len(y), dtype=np.float64)
        return X, y, g

    monkeypatch.setattr(
        "recsys_tfb.io.extract.extract_Xy_with_groups", fake_extract_groups
    )

    real_dataset = lgb.Dataset

    def spy_dataset(*a, **kw):
        if "group" in kw and kw["group"] is not None:
            captured["group"] = np.asarray(kw["group"])
        return real_dataset(*a, **kw)

    monkeypatch.setattr(lgb, "Dataset", spy_dataset)

    class FakeAdapter:
        def train(self, **kw):
            captured["metric"] = kw["params"].get("metric")

    monkeypatch.setattr(nodes, "get_adapter", lambda algo: FakeAdapter())

    class H:
        def __init__(self, tag=""):
            self.tag = tag

    parameters = {
        "training": {
            "final_model_strategy": "refit_on_full",
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "lambdarank"},
        },
        "random_seed": 42,
    }
    prep_meta = {"feature_columns": ["a", "b"], "categorical_columns": []}
    nodes.finalize_model(
        H("train"), H("dev"), object(), {"num_leaves": 4}, 3,
        prep_meta, parameters,
    )
    # 3 groups total (2 from train + 1 from dev), all size 2
    np.testing.assert_array_equal(
        np.sort(captured["group"]), np.array([2, 2, 2])
    )
    assert int(captured["group"].sum()) == 6
    assert captured["metric"] == "ndcg"


class TestTuneHyperparametersObjective:
    def test_macro_per_item_objective_runs_and_returns_model(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters,
    ):
        import copy

        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        params = copy.deepcopy(training_parameters)
        params["training"]["hpo_objective"] = "macro_per_item_map"

        best_params, best_iteration, best_model = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params,
        )
        assert isinstance(best_params, dict)
        assert isinstance(best_model, ModelAdapter)

    def test_unknown_objective_raises_before_trials(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters,
    ):
        import copy

        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        params = copy.deepcopy(training_parameters)
        params["training"]["hpo_objective"] = "bogus"

        with pytest.raises(ValueError, match="hpo_objective"):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata,
                params,
            )


class TestHpoScore:
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_mean_ap_matches_compute_mean_ap(self):
        from recsys_tfb.evaluation.metrics import compute_mean_ap
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        expected = compute_mean_ap(self.GROUPS, self.Y, self.SCORE)
        result = _hpo_score("mean_ap", self.GROUPS, None, self.Y, self.SCORE)
        assert result == pytest.approx(expected)


    def test_macro_per_item_map_matches_primitive(self):
        from recsys_tfb.evaluation.metrics import compute_macro_per_item_map
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        expected = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        result = _hpo_score(
            "macro_per_item_map", self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(expected)

    def test_unknown_objective_raises_valueerror(self):
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        with pytest.raises(ValueError, match="hpo_objective"):
            _hpo_score("not_a_metric", self.GROUPS, self.ITEMS, self.Y, self.SCORE)


def test_resolve_weight_diagnostics_unmatched(tmp_path):
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import resolve_weight_diagnostics

    # train parquet: feature seg stored as int codes (0=mass,1=hnw), prod raw.
    pdf = pd.DataFrame({
        "cust_segment_typ_2a": [0, 1, 0],
        "prod_name": ["a", "a", "b"],
        "label": [1, 0, 1],
    })
    p = tmp_path / "train.parquet"
    pdf.to_parquet(p)
    handle = ParquetHandle(path=str(p))

    params = {
        "schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
                               "item": "prod_name", "label": "label"}},
        "training": {"sample_weight_keys": ["cust_segment_typ_2a"],
                     "sample_weights": {"mass": 2.0, "aff": 3.0}},  # aff absent
    }
    prep = {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw", "aff"]}}

    diag = resolve_weight_diagnostics(handle, params, prep)
    assert diag["enabled"] is True
    assert diag["weight_keys"] == ["cust_segment_typ_2a"]
    assert diag["n_weight_entries"] == 2
    assert diag["unmatched_keys"] == ["aff"]  # no row has segment 'aff'


def test_resolve_weight_diagnostics_disabled(tmp_path):
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import resolve_weight_diagnostics
    p = tmp_path / "t.parquet"
    pd.DataFrame({"prod_name": ["a"], "label": [1]}).to_parquet(p)
    params = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
              "item": "prod_name", "label": "label"}}, "training": {}}
    diag = resolve_weight_diagnostics(ParquetHandle(path=str(p)), params, {})
    assert diag == {"enabled": False, "weight_keys": ["prod_name"],
                    "n_weight_entries": 0, "unmatched_keys": []}


def test_persist_sample_weight_report_writes_json(tmp_path, monkeypatch):
    import json
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import persist_sample_weight_report

    p = tmp_path / "train.parquet"
    pd.DataFrame({"cust_segment_typ_2a": [0, 1], "prod_name": ["a", "a"],
                  "label": [1, 0]}).to_parquet(p)
    version_dir = tmp_path / "models" / "abc123"
    # node resolves the model version dir via diagnostics_dir(...).parent;
    # patch the SOURCE module so the node's lazy import picks up the fake.
    monkeypatch.setattr(
        "recsys_tfb.diagnosis.model.diagnostics_dir",
        lambda params: version_dir / "diagnostics",
    )

    params = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
              "item": "prod_name", "label": "label"}},
              "training": {"sample_weight_keys": ["cust_segment_typ_2a"],
                           "sample_weights": {"mass": 2.0}}}
    prep = {"category_mappings": {"cust_segment_typ_2a": ["mass", "hnw"]}}

    diag = persist_sample_weight_report(ParquetHandle(path=str(p)), prep, params)
    report = json.loads((version_dir / "sample_weight_report.json").read_text())
    assert report == diag
    assert report["enabled"] is True and report["unmatched_keys"] == []


def test_persist_sample_weight_report_writes_when_disabled(tmp_path, monkeypatch):
    # Empty sample_weights -> report still written, enabled=False (so the
    # manifest always records what sample_weight did this run).
    import json
    import pandas as pd
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.pipelines.training.nodes import persist_sample_weight_report

    p = tmp_path / "train.parquet"
    pd.DataFrame({"prod_name": ["a"], "label": [1]}).to_parquet(p)
    version_dir = tmp_path / "models" / "abc123"
    monkeypatch.setattr(
        "recsys_tfb.diagnosis.model.diagnostics_dir",
        lambda params: version_dir / "diagnostics",
    )
    params = {"schema": {"columns": {"time": "snap_date", "entity": ["cust_id"],
              "item": "prod_name", "label": "label"}}, "training": {}}

    diag = persist_sample_weight_report(ParquetHandle(path=str(p)), {}, params)
    report = json.loads((version_dir / "sample_weight_report.json").read_text())
    assert report == diag
    assert report["enabled"] is False and report["unmatched_keys"] == []


