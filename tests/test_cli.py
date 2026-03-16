from unittest.mock import patch

from typer.testing import CliRunner

from recsys_tfb.__main__ import app

runner = CliRunner()


class TestCLI:
    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "--pipeline" in result.output
        assert "--env" in result.output

    def test_help_shows_options(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Pipeline name to run" in result.output

    def test_unknown_pipeline(self, tmp_path):
        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)

        import os

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "nonexistent"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)

    def test_training_uses_timestamp_model_version(self, tmp_path):
        """Training pipeline passes timestamp as model_version runtime param."""
        import os

        import yaml

        # Set up minimal conf
        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)
        catalog = {
            "model": {
                "type": "PickleDataset",
                "filepath": "data/models/${model_version}/model.pkl",
            },
        }
        with open(base_dir / "catalog.yaml", "w") as f:
            yaml.dump(catalog, f)

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            # Pipeline will fail (no input data), but we can check catalog was built
            # with a timestamped path by patching DataCatalog
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner") as mock_runner:
                    runner.invoke(app, ["--pipeline", "training"])
                    # Check that DataCatalog was called with resolved filepath
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["model"]["filepath"]
                    assert "${model_version}" not in fp
                    assert "models/best/" not in fp
                    assert "models/20" in fp  # starts with year
        finally:
            os.chdir(old_cwd)

    def test_inference_uses_best_model_version(self, tmp_path):
        """Non-training pipelines resolve model_version to 'best'."""
        import os

        import yaml

        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)
        catalog = {
            "model": {
                "type": "PickleDataset",
                "filepath": "data/models/${model_version}/model.pkl",
            },
        }
        with open(base_dir / "catalog.yaml", "w") as f:
            yaml.dump(catalog, f)

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            with patch("recsys_tfb.core.catalog.DataCatalog") as mock_catalog_cls:
                mock_catalog_cls.return_value = mock_catalog_cls
                mock_catalog_cls.add = lambda *a, **kw: None
                with patch("recsys_tfb.core.runner.Runner") as mock_runner:
                    runner.invoke(app, ["--pipeline", "inference"])
                    call_args = mock_catalog_cls.call_args[0][0]
                    fp = call_args["model"]["filepath"]
                    assert fp == "data/models/best/model.pkl"
        finally:
            os.chdir(old_cwd)

    def test_training_pipeline_fails_without_inputs(self, tmp_path):
        base_dir = tmp_path / "conf" / "base"
        base_dir.mkdir(parents=True)
        local_dir = tmp_path / "conf" / "local"
        local_dir.mkdir(parents=True)

        import os

        old_cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            result = runner.invoke(app, ["--pipeline", "training"])
            assert result.exit_code == 1
        finally:
            os.chdir(old_cwd)
