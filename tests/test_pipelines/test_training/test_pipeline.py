"""Tests for training pipeline definition."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.training import create_pipeline


class TestTrainingPipeline:
    def test_pipeline_has_four_nodes(self):
        pipeline = create_pipeline()
        assert len(pipeline.nodes) == 4

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        expected = {
            "X_train", "y_train", "X_train_dev", "y_train_dev",
            "X_val", "y_val", "val_set", "parameters",
        }
        assert pipeline.inputs == expected

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {"best_params", "model", "evaluation_results"}
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "tune_hyperparameters" in names
        assert "train_model" in names
        assert "evaluate_model" in names
        assert "log_experiment" in names

    def test_topological_order(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        # tune must come before train (train depends on best_params)
        assert names.index("tune_hyperparameters") < names.index("train_model")
        # train must come before evaluate (evaluate depends on model)
        assert names.index("train_model") < names.index("evaluate_model")
        # evaluate must come before log (log depends on evaluation_results)
        assert names.index("evaluate_model") < names.index("log_experiment")


class TestTrainingPipelineE2E:
    """End-to-end: dataset pipeline → training pipeline → artifact validation."""

    def test_dataset_then_training(self, tmp_path):
        import lightgbm as lgb

        from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
        from recsys_tfb.core.runner import Runner
        from recsys_tfb.pipelines.dataset import create_pipeline as create_dataset_pipeline

        # -- Synthetic source tables --
        products = ["exchange_fx", "exchange_usd", "fund_stock"]
        customers = ["C001", "C002", "C003", "C004"]
        snaps = ["2024-01-31", "2024-02-29", "2024-03-31"]
        rng = np.random.RandomState(42)

        feature_rows = []
        for snap in snaps:
            for cid in customers:
                feature_rows.append({
                    "snap_date": pd.Timestamp(snap),
                    "cust_id": cid,
                    "total_aum": rng.uniform(100, 1000),
                    "fund_aum": rng.uniform(10, 100),
                    "in_amt_sum_l1m": rng.uniform(0, 50),
                    "out_amt_sum_l1m": rng.uniform(0, 30),
                    "in_amt_ratio_l1m": rng.uniform(0, 0.1),
                    "out_amt_ratio_l1m": rng.uniform(0, 0.05),
                })
        feature_table = pd.DataFrame(feature_rows)

        label_rows = []
        for snap in snaps:
            for cid in customers:
                for prod in products:
                    label_rows.append({
                        "snap_date": pd.Timestamp(snap),
                        "cust_id": cid,
                        "cust_segment_typ": "mass",
                        "apply_start_date": pd.Timestamp(snap) + pd.Timedelta(days=1),
                        "apply_end_date": pd.Timestamp(snap) + pd.Timedelta(days=30),
                        "label": float(rng.binomial(1, 0.2)),
                        "prod_name": prod,
                    })
        label_table = pd.DataFrame(label_rows)

        parameters = {
            "random_seed": 42,
            "dataset": {
                "sample_ratio": 1.0,
                "sample_group_keys": ["snap_date"],
                "train_dev_snap_dates": ["2024-02-29"],
                "val_snap_dates": ["2024-03-31"],
            },
            "training": {
                "n_trials": 2,
                "num_iterations": 30,
                "early_stopping_rounds": 10,
                "search_space": {
                    "learning_rate": {"low": 0.05, "high": 0.2},
                    "num_leaves": {"low": 8, "high": 32},
                    "max_depth": {"low": 3, "high": 6},
                    "min_child_samples": {"low": 2, "high": 10},
                    "subsample": {"low": 0.8, "high": 1.0},
                    "colsample_bytree": {"low": 0.8, "high": 1.0},
                },
            },
            "mlflow": {
                "experiment_name": "e2e_test",
                "tracking_uri": str(tmp_path / "mlruns"),
            },
        }

        # -- Build sample_pool from label_table (unique customer-month with segment) --
        sample_pool = label_table[["snap_date", "cust_id", "cust_segment_typ"]].drop_duplicates().reset_index(drop=True)

        # -- Build catalog with MemoryDatasets for source data --
        # Pre-register all datasets used across both pipelines so they won't
        # be auto-created (and thus won't be released by memory management).
        # In production, these are ParquetDataset/etc. in catalog.yaml.
        catalog = DataCatalog()
        catalog.add("feature_table", MemoryDataset(feature_table))
        catalog.add("label_table", MemoryDataset(label_table))
        catalog.add("sample_pool", MemoryDataset(sample_pool))
        catalog.add("parameters", MemoryDataset(parameters))
        for name in (
            "sample_keys", "train_keys", "train_dev_keys", "val_keys",
            "train_set", "train_dev_set", "val_set",
            "X_train", "y_train", "X_train_dev", "y_train_dev",
            "X_val", "y_val", "preprocessor", "category_mappings",
            "best_params", "model", "evaluation_results",
        ):
            catalog.add(name, MemoryDataset())

        runner = Runner()

        # -- Run dataset pipeline --
        dataset_pipeline = create_dataset_pipeline()
        runner.run(dataset_pipeline, catalog)

        # -- Run training pipeline --
        training_pipeline = create_pipeline()
        runner.run(training_pipeline, catalog)

        # -- Validate artifacts --
        model = catalog.load("model")
        assert isinstance(model, lgb.Booster)

        # Model predictions are probabilities in [0, 1]
        X_val = catalog.load("X_val")
        preds = model.predict(X_val)
        assert np.all(preds >= 0) and np.all(preds <= 1)

        best_params = catalog.load("best_params")
        assert isinstance(best_params, dict)
        assert "learning_rate" in best_params
        assert "num_leaves" in best_params

        evaluation_results = catalog.load("evaluation_results")
        assert isinstance(evaluation_results["overall_map"], float)
        assert isinstance(evaluation_results["per_product_ap"], dict)
