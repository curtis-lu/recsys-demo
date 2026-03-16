import json
import os
import re
from unittest.mock import patch

import yaml
from typer.testing import CliRunner

from recsys_tfb.__main__ import app

runner = CliRunner()


def _setup_conf(tmp_path, params_dataset=None, params_training=None, params_inference=None):
    """Create minimal conf dirs with catalog and optional parameter files."""
    base_dir = tmp_path / "conf" / "base"
    base_dir.mkdir(parents=True)
    local_dir = tmp_path / "conf" / "local"
    local_dir.mkdir(parents=True)

    catalog = {
        "model": {
            "type": "PickleDataset",
            "filepath": "data/models/${model_version}/model.pkl",
        },
        "preprocessor": {
            "type": "PickleDataset",
            "filepath": "data/dataset/${dataset_version}/preprocessor.pkl",
        },
        "sample_keys": {
            "type": "ParquetDataset",
            "filepath": "data/dataset/${dataset_version}/sample_keys.parquet",
        },
        "scoring_dataset": {
            "type": "ParquetDataset",
            "filepath": "data/inference/${model_version}/${snap_date}/scoring_dataset.parquet",
        },
    }
    with open(base_dir / "catalog.yaml", "w") as f:
        yaml.dump(catalog, f)

    if params_dataset:
        with open(base_dir / "parameters_dataset.yaml", "w") as f:
            yaml.dump(params_dataset, f)
    if params_training:
        with open(base_dir / "parameters_training.yaml", "w") as f:
            yaml.dump(params_training, f)
    if params_inference:
        with open(base_dir / "parameters_inference.yaml", "w") as f:
            yaml.dump(params_inference, f)


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "--pipeline" in result.output
        assert "--env" in result.output
        assert "--dataset-version" in result.output

    def test_help_shows_options(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Pipeline name to run" in result.output

    def test_unknown_pipeline(self, tmp_path):
        _setup_conf(tmp_path)
        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "nonexistent"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)

    def test_dataset_pipeline_uses_hash_version(self, tmp_path):
        """Dataset pipeline computes hash-based dataset_version."""
        _setup_conf(tmp_path, params_dataset={"sample_ratio": 0.1, "seed": 42})

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner"):
                    result = runner.invoke(app, ["--pipeline", "dataset"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["sample_keys"]["filepath"]
                    assert "${dataset_version}" not in fp
                    # Hash is 8 hex chars
                    assert re.search(r"data/dataset/[0-9a-f]{8}/", fp)
        finally:
            os.chdir(old_cwd)

    def test_training_uses_hash_model_version(self, tmp_path):
        """Training pipeline uses hash-based model_version."""
        _setup_conf(
            tmp_path,
            params_dataset={"sample_ratio": 0.1},
            params_training={"lr": 0.01},
        )

        # Create dataset latest symlink
        dataset_dir = tmp_path / "data" / "dataset" / "abc12345"
        dataset_dir.mkdir(parents=True)
        (tmp_path / "data" / "dataset" / "latest").symlink_to(dataset_dir.resolve())

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner"):
                    result = runner.invoke(app, ["--pipeline", "training"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["model"]["filepath"]
                    assert "${model_version}" not in fp
                    assert "models/best/" not in fp
                    # Model version should be 8 hex chars
                    assert re.search(r"models/[0-9a-f]{8}/", fp)
                    # dataset_version should be resolved
                    pp = call_args["preprocessor"]["filepath"]
                    assert "abc12345" in pp
        finally:
            os.chdir(old_cwd)

    def test_training_with_explicit_dataset_version(self, tmp_path):
        """Training pipeline accepts --dataset-version."""
        _setup_conf(
            tmp_path,
            params_dataset={"sample_ratio": 0.1},
            params_training={"lr": 0.01},
        )

        # Create the specified dataset version directory
        (tmp_path / "data" / "dataset" / "deadbeef").mkdir(parents=True)

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner"):
                    result = runner.invoke(
                        app, ["--pipeline", "training", "--dataset-version", "deadbeef"]
                    )
                    call_args = mock_catalog_cls.call_args[0][0]
                    pp = call_args["preprocessor"]["filepath"]
                    assert "deadbeef" in pp
        finally:
            os.chdir(old_cwd)

    def test_inference_uses_best_model_version(self, tmp_path):
        """Inference pipeline resolves model_version to 'best'."""
        _setup_conf(
            tmp_path,
            params_inference={"snap_dates": ["2024-03-31"]},
        )

        # Create best symlink pointing to a model with manifest
        models_dir = tmp_path / "data" / "models"
        version_dir = models_dir / "a1b2c3d4"
        version_dir.mkdir(parents=True)
        manifest = {"version": "a1b2c3d4", "dataset_version": "deadbeef"}
        (version_dir / "manifest.json").write_text(json.dumps(manifest))
        (models_dir / "best").symlink_to(version_dir.resolve())

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner"):
                    runner.invoke(app, ["--pipeline", "inference"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["model"]["filepath"]
                    assert fp == "data/models/best/model.pkl"
                    # dataset_version from manifest
                    pp = call_args["preprocessor"]["filepath"]
                    assert "deadbeef" in pp
                    # snap_date resolved
                    sd = call_args["scoring_dataset"]["filepath"]
                    assert "20240331" in sd
        finally:
            os.chdir(old_cwd)

    def test_training_pipeline_fails_without_inputs(self, tmp_path):
        _setup_conf(tmp_path)

        # Create dataset latest symlink so version resolves
        dataset_dir = tmp_path / "data" / "dataset" / "abc12345"
        dataset_dir.mkdir(parents=True)
        (tmp_path / "data" / "dataset" / "latest").symlink_to(dataset_dir.resolve())

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "training"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)
