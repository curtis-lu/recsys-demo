"""Tests for training pipeline definition."""

import numpy as np
import pandas as pd
import pytest

from recsys_tfb.pipelines.training import create_pipeline


class TestTrainingPipeline:
    def test_pipeline_has_eight_nodes(self):
        pipeline = create_pipeline()
        # 3 cache nodes (train, train_dev, val) + prepare_lgb + tune + train + evaluate + log
        assert len(pipeline.nodes) == 8

    def test_pipeline_inputs(self):
        pipeline = create_pipeline()
        expected = {
            "train_model_input", "train_dev_model_input",
            "val_model_input", "preprocessor", "parameters",
        }
        assert pipeline.inputs == expected

    def test_pipeline_outputs(self):
        pipeline = create_pipeline()
        expected = {
            "best_params", "model", "evaluation_results",
            "train_parquet_handle", "train_dev_parquet_handle", "val_parquet_handle",
            "train_lgb_handle", "train_dev_lgb_handle",
        }
        assert pipeline.outputs == expected

    def test_node_names(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        assert "cache_train_model_input" in names
        assert "cache_train_dev_model_input" in names
        assert "cache_val_model_input" in names
        assert "prepare_lgb_train_inputs" in names
        assert "tune_hyperparameters" in names
        assert "train_model" in names
        assert "evaluate_model" in names
        assert "log_experiment" in names

    def test_topological_order(self):
        pipeline = create_pipeline()
        names = [n.name for n in pipeline.nodes]
        # cache nodes must come before prepare_lgb_train_inputs
        for cache_name in (
            "cache_train_model_input",
            "cache_train_dev_model_input",
        ):
            assert names.index(cache_name) < names.index("prepare_lgb_train_inputs")
        # val cache must come before tune (val_parquet_handle flows into tune)
        assert names.index("cache_val_model_input") < names.index("tune_hyperparameters")
        # prepare must come before tune and train
        assert names.index("prepare_lgb_train_inputs") < names.index("tune_hyperparameters")
        assert names.index("tune_hyperparameters") < names.index("train_model")
        assert names.index("train_model") < names.index("evaluate_model")
        assert names.index("evaluate_model") < names.index("log_experiment")

    # -- Calibration-enabled pipeline tests --

    def test_calibration_pipeline_has_ten_nodes(self):
        pipeline = create_pipeline(enable_calibration=True)
        # 4 cache nodes + prepare_lgb + tune + train + calibrate + evaluate + log
        assert len(pipeline.nodes) == 10

    def test_calibration_pipeline_has_calibrate_node(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert "calibrate_model" in names
        assert "cache_calibration_model_input" in names

    def test_calibration_pipeline_inputs(self):
        pipeline = create_pipeline(enable_calibration=True)
        assert "calibration_model_input" in pipeline.inputs

    def test_calibration_pipeline_trained_model_intermediate(self):
        pipeline = create_pipeline(enable_calibration=True)
        assert "trained_model" not in pipeline.inputs
        assert "trained_model" in pipeline.outputs

    def test_calibration_pipeline_topological_order(self):
        pipeline = create_pipeline(enable_calibration=True)
        names = [n.name for n in pipeline.nodes]
        assert names.index("cache_calibration_model_input") < names.index("calibrate_model")
        assert names.index("train_model") < names.index("calibrate_model")
        assert names.index("calibrate_model") < names.index("evaluate_model")


@pytest.mark.spark
class TestTrainingPipelineE2E:
    """End-to-end: dataset pipeline → training pipeline → artifact validation."""

    def test_dataset_then_training(self, tmp_path, spark):
        from recsys_tfb.core.catalog import DataCatalog, MemoryDataset
        from recsys_tfb.io.extract import extract_Xy
        from recsys_tfb.io.handles import ParquetHandle
        from recsys_tfb.models.base import ModelAdapter
        from recsys_tfb.core.runner import Runner
        from recsys_tfb.pipelines.dataset import create_pipeline as create_dataset_pipeline

        # -- Synthetic source tables --
        # Use ≥20 customers so that hash-based train/train_dev split (ratio=0.2)
        # reliably produces at least one dev customer; small N exposes the
        # statistical reality of any deterministic-hash sampler.
        products = ["exchange_fx", "exchange_usd", "fund_stock"]
        customers = [f"C{i:03d}" for i in range(1, 21)]
        snaps = ["2024-01-31", "2024-02-29", "2024-03-31", "2024-04-30", "2024-05-31"]
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
            "schema": {
                "categorical_values": {
                    "prod_name": sorted(products),
                },
            },
            "dataset": {
                "train_snap_dates": ["2024-01-31", "2024-02-29", "2024-03-31"],
                "sample_ratio": 1.0,
                "sample_group_keys": ["cust_segment_typ", "prod_name"],
                "sample_ratio_overrides": {},
                "train_dev_ratio": 0.2,
                "enable_calibration": False,
                "calibration_snap_dates": [],
                "calibration_sample_ratio": 1.0,
                "val_snap_dates": ["2024-04-30"],
                "val_sample_ratio": 1.0,
                "test_snap_dates": ["2024-05-31"],
            },
            "training": {
                "algorithm": "lightgbm",
                "algorithm_params": {
                    "objective": "binary",
                    "metric": "binary_logloss",
                    "verbosity": -1,
                },
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
            "cache": {"root": str(tmp_path / "cache")},
            "base_dataset_version": "v1",
            "train_variant_id": "tv1",
        }

        # -- Build sample_pool from label_table (customer-month-product granularity) --
        sample_pool = label_table[["snap_date", "cust_id", "cust_segment_typ", "prod_name"]].drop_duplicates().reset_index(drop=True)

        feature_table_sdf = spark.createDataFrame(feature_table)
        label_table_sdf = spark.createDataFrame(label_table)
        sample_pool_sdf = spark.createDataFrame(sample_pool)

        # -- Build catalog with MemoryDatasets for source data --
        catalog = DataCatalog()
        catalog.add("feature_table", MemoryDataset(feature_table_sdf))
        catalog.add("label_table", MemoryDataset(label_table_sdf))
        catalog.add("sample_pool", MemoryDataset(sample_pool_sdf))
        catalog.add("parameters", MemoryDataset(parameters))
        for name in (
            "sample_keys", "train_keys", "train_dev_keys", "val_keys", "test_keys",
            "train_set", "train_dev_set", "val_set", "test_set",
            "train_model_input", "train_dev_model_input",
            "val_model_input", "test_model_input",
            "preprocessor", "category_mappings",
            "best_params", "model", "evaluation_results",
        ):
            catalog.add(name, MemoryDataset())

        runner = Runner()

        # -- Run dataset pipeline --
        dataset_pipeline = create_dataset_pipeline()
        runner.run(dataset_pipeline, catalog)

        # -- Write model_inputs to parquet and inject ParquetHandles into catalog.
        # The training pipeline's cache nodes require Spark DataFrames; in the
        # test environment we bypass them by pre-populating the *_parquet_handle
        # slots directly so the training pipeline can start at prepare_lgb_train_inputs.
        for mi_name, handle_name in (
            ("train_model_input", "train_parquet_handle"),
            ("train_dev_model_input", "train_dev_parquet_handle"),
            ("val_model_input", "val_parquet_handle"),
        ):
            mi_sdf = catalog.load(mi_name)
            parquet_path = tmp_path / f"{mi_name}.parquet"
            mi_sdf.toPandas().to_parquet(parquet_path)
            catalog.add(handle_name, MemoryDataset(ParquetHandle(str(parquet_path))))

        # Also register the lgb handle slots and intermediate names the pipeline produces.
        for name in ("train_lgb_handle", "train_dev_lgb_handle"):
            catalog.add(name, MemoryDataset())

        # -- Run training pipeline (skip cache nodes whose inputs are now handles) --
        from recsys_tfb.core.pipeline import Pipeline
        from recsys_tfb.pipelines.training import create_pipeline as _create_training_pipeline

        full_training_pipeline = _create_training_pipeline()
        # Drop cache nodes — their outputs are already populated in catalog.
        cache_node_names = {
            "cache_train_model_input",
            "cache_train_dev_model_input",
            "cache_val_model_input",
        }
        training_nodes = [
            n for n in full_training_pipeline.nodes
            if n.name not in cache_node_names
        ]
        training_pipeline = Pipeline(training_nodes)
        runner.run(training_pipeline, catalog)

        # -- Validate artifacts --
        model = catalog.load("model")
        assert isinstance(model, ModelAdapter)

        # Model predictions are probabilities in [0, 1]
        val_handle = catalog.load("val_parquet_handle")
        preprocessor = catalog.load("preprocessor")
        X_val, _ = extract_Xy(val_handle, preprocessor, parameters)
        preds = model.predict(X_val)
        assert np.all(preds >= 0) and np.all(preds <= 1)

        best_params = catalog.load("best_params")
        assert isinstance(best_params, dict)
        assert "learning_rate" in best_params
        assert "num_leaves" in best_params

        evaluation_results = catalog.load("evaluation_results")
        assert isinstance(evaluation_results["overall_map"], float)
        assert isinstance(evaluation_results["per_product_ap"], dict)
