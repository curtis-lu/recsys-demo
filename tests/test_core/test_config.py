import yaml

from recsys_tfb.core.config import ConfigLoader, ConfigEnvError, _deep_merge


class TestDeepMerge:
    def test_nested_dict_merge(self):
        base = {"a": {"b": 1, "c": 2}}
        override = {"a": {"b": 99}}
        assert _deep_merge(base, override) == {"a": {"b": 99, "c": 2}}

    def test_list_replacement(self):
        base = {"features": ["a", "b"]}
        override = {"features": ["x"]}
        assert _deep_merge(base, override) == {"features": ["x"]}

    def test_new_key_added(self):
        base = {"a": 1}
        override = {"b": 2}
        assert _deep_merge(base, override) == {"a": 1, "b": 2}


class TestConfigLoader:
    def _write_yaml(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_base_only(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "catalog.yaml", {"ds1": {"type": "ParquetDataset"}})
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_catalog_config() == {"ds1": {"type": "ParquetDataset"}}

    def test_env_overlay(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "catalog.yaml", {"ds1": {"type": "ParquetDataset", "filepath": "/base/path"}})
        self._write_yaml(tmp_path / "local" / "catalog.yaml", {"ds1": {"filepath": "/local/path"}})
        loader = ConfigLoader(str(tmp_path), env="local")
        catalog = loader.get_catalog_config()
        assert catalog["ds1"]["type"] == "ParquetDataset"
        assert catalog["ds1"]["filepath"] == "/local/path"

    def test_missing_env_dir(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "parameters.yaml", {"lr": 0.01})
        loader = ConfigLoader(str(tmp_path), env="production")
        assert loader.get_parameters() == {"lr": 0.01}

    def test_parameters_merge(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "parameters.yaml", {"seed": 42})
        self._write_yaml(tmp_path / "base" / "parameters_training.yaml", {"lr": 0.01, "epochs": 10})
        self._write_yaml(tmp_path / "base" / "parameters_dataset.yaml", {"sample_ratio": 0.1})
        loader = ConfigLoader(str(tmp_path), env="local")
        params = loader.get_parameters()
        assert params["seed"] == 42
        assert params["lr"] == 0.01
        assert params["sample_ratio"] == 0.1

    def test_runtime_params_substitution(self, tmp_path):
        self._write_yaml(
            tmp_path / "base" / "catalog.yaml",
            {
                "model": {
                    "type": "PickleDataset",
                    "filepath": "data/models/${model_version}/model.pkl",
                },
                "feature_table": {
                    "type": "ParquetDataset",
                    "filepath": "data/feature_table.parquet",
                },
            },
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        catalog = loader.get_catalog_config(
            runtime_params={"model_version": "20260316_120000"}
        )
        assert catalog["model"]["filepath"] == "data/models/20260316_120000/model.pkl"
        # Non-template paths unchanged
        assert catalog["feature_table"]["filepath"] == "data/feature_table.parquet"

    def test_runtime_params_none_preserves_template(self, tmp_path):
        self._write_yaml(
            tmp_path / "base" / "catalog.yaml",
            {
                "model": {
                    "type": "PickleDataset",
                    "filepath": "data/models/${model_version}/model.pkl",
                },
            },
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        catalog = loader.get_catalog_config()
        assert catalog["model"]["filepath"] == "data/models/${model_version}/model.pkl"

    def test_runtime_params_unknown_var_preserved(self, tmp_path):
        self._write_yaml(
            tmp_path / "base" / "catalog.yaml",
            {
                "model": {
                    "type": "PickleDataset",
                    "filepath": "data/${unknown}/model.pkl",
                },
            },
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        catalog = loader.get_catalog_config(
            runtime_params={"model_version": "v1"}
        )
        assert catalog["model"]["filepath"] == "data/${unknown}/model.pkl"

    def test_empty_yaml_file(self, tmp_path):
        (tmp_path / "base").mkdir(parents=True)
        (tmp_path / "base" / "empty.yaml").write_text("")
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_catalog_config() == {}


class TestGetParametersByName:
    def _write_yaml(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_returns_specific_parameters_file(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "parameters_dataset.yaml", {"sample_ratio": 0.1, "seed": 42})
        self._write_yaml(tmp_path / "base" / "parameters_training.yaml", {"lr": 0.01})
        loader = ConfigLoader(str(tmp_path), env="local")

        ds_params = loader.get_parameters_by_name("parameters_dataset")
        assert ds_params == {"sample_ratio": 0.1, "seed": 42}

        tr_params = loader.get_parameters_by_name("parameters_training")
        assert tr_params == {"lr": 0.01}

    def test_merges_base_and_env(self, tmp_path):
        self._write_yaml(tmp_path / "base" / "parameters_dataset.yaml", {"sample_ratio": 0.1, "seed": 42})
        self._write_yaml(tmp_path / "local" / "parameters_dataset.yaml", {"sample_ratio": 0.05})
        loader = ConfigLoader(str(tmp_path), env="local")

        params = loader.get_parameters_by_name("parameters_dataset")
        assert params["sample_ratio"] == 0.05
        assert params["seed"] == 42

    def test_raises_for_unknown_name(self, tmp_path):
        (tmp_path / "base").mkdir(parents=True)
        loader = ConfigLoader(str(tmp_path), env="local")
        import pytest
        with pytest.raises(KeyError, match="parameters_nonexistent"):
            loader.get_parameters_by_name("parameters_nonexistent")


class TestEnvResolution:
    def _write_yaml(self, path, data):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f)

    def test_env_var_substituted(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_TRACKING", "/srv/mlruns")
        self._write_yaml(
            tmp_path / "base" / "parameters_training.yaml",
            {"mlflow": {"tracking_uri": "${env.MY_TRACKING}"}},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["mlflow"]["tracking_uri"] == "/srv/mlruns"

    def test_default_used_when_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MY_TRACKING", raising=False)
        self._write_yaml(
            tmp_path / "base" / "parameters_training.yaml",
            {"mlflow": {"tracking_uri": "${env.MY_TRACKING|mlruns}"}},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["mlflow"]["tracking_uri"] == "mlruns"

    def test_empty_default(self, tmp_path, monkeypatch):
        monkeypatch.delenv("MY_VAR", raising=False)
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml", {"x": "${env.MY_VAR|}"}
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["x"] == ""

    def test_missing_required_raises(self, tmp_path, monkeypatch):
        import pytest
        monkeypatch.delenv("MY_VAR", raising=False)
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml", {"x": "${env.MY_VAR}"}
        )
        with pytest.raises(ConfigEnvError, match="MY_VAR"):
            ConfigLoader(str(tmp_path), env="local")

    def test_multiple_placeholders_one_string(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOST", "h1")
        monkeypatch.setenv("PORT", "9083")
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml",
            {"uri": "thrift://${env.HOST}:${env.PORT}"},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["uri"] == "thrift://h1:9083"

    def test_embedded_in_larger_string(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME_DIR", "/home/u")
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml",
            {"path": "file://${env.HOME_DIR}/mlruns"},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["path"] == "file:///home/u/mlruns"

    def test_collect_all_errors(self, tmp_path, monkeypatch):
        import pytest
        monkeypatch.delenv("VAR_A", raising=False)
        monkeypatch.delenv("VAR_B", raising=False)
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml",
            {"a": "${env.VAR_A}", "b": "${env.VAR_B}"},
        )
        with pytest.raises(ConfigEnvError) as exc_info:
            ConfigLoader(str(tmp_path), env="local")
        msg = str(exc_info.value)
        assert "VAR_A" in msg and "VAR_B" in msg

    def test_non_string_passthrough(self, tmp_path):
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml",
            {"n": 4, "flag": True, "items": [1, 2]},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters() == {"n": 4, "flag": True, "items": [1, 2]}

    def test_non_env_placeholder_untouched(self, tmp_path):
        self._write_yaml(
            tmp_path / "base" / "catalog.yaml",
            {"model": {"filepath": "data/${model_version}/model.txt"}},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert (
            loader.get_catalog_config()["model"]["filepath"]
            == "data/${model_version}/model.txt"
        )

    def test_error_message_names_location(self, tmp_path, monkeypatch):
        import pytest
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        self._write_yaml(
            tmp_path / "base" / "parameters_training.yaml",
            {"mlflow": {"tracking_uri": "${env.MLFLOW_TRACKING_URI}"}},
        )
        with pytest.raises(ConfigEnvError) as exc_info:
            ConfigLoader(str(tmp_path), env="local")
        msg = str(exc_info.value)
        assert "parameters_training.yaml" in msg
        assert "mlflow.tracking_uri" in msg
        assert "MLFLOW_TRACKING_URI" in msg

    def test_resolved_in_env_overlay(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OVERLAY_VAL", "from_env")
        self._write_yaml(tmp_path / "base" / "parameters.yaml", {"x": "base"})
        self._write_yaml(
            tmp_path / "production" / "parameters.yaml",
            {"x": "${env.OVERLAY_VAL}"},
        )
        loader = ConfigLoader(str(tmp_path), env="production")
        assert loader.get_parameters()["x"] == "from_env"

    def test_list_elements_resolved(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ITEM", "resolved")
        self._write_yaml(
            tmp_path / "base" / "parameters.yaml",
            {"xs": ["plain", "${env.ITEM}"]},
        )
        loader = ConfigLoader(str(tmp_path), env="local")
        assert loader.get_parameters()["xs"] == ["plain", "resolved"]
