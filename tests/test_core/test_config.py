import yaml

from recsys_tfb.core.config import ConfigLoader, _deep_merge


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
