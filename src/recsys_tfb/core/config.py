import os
from pathlib import Path

import yaml


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Non-dict values are replaced."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ConfigLoader:
    """Load and merge YAML config files from base and environment directories."""

    def __init__(self, conf_dir: str, env: str = "local"):
        self._conf_dir = Path(conf_dir)
        self._env = env
        self._config: dict[str, dict] = {}
        self._load()

    def _load_yaml_dir(self, directory: Path) -> dict[str, dict]:
        """Load all YAML files from a directory, keyed by stem name."""
        result = {}
        if not directory.is_dir():
            return result
        for filepath in sorted(directory.glob("*.yaml")):
            with open(filepath) as f:
                data = yaml.safe_load(f)
            if data is not None:
                result[filepath.stem] = data
        return result

    def _load(self) -> None:
        base_dir = self._conf_dir / "base"
        env_dir = self._conf_dir / self._env

        base_config = self._load_yaml_dir(base_dir)
        env_config = self._load_yaml_dir(env_dir)

        # Merge: for each stem, deep-merge env over base
        all_stems = set(base_config) | set(env_config)
        for stem in all_stems:
            base = base_config.get(stem, {})
            env = env_config.get(stem, {})
            self._config[stem] = _deep_merge(base, env)

    def get_catalog_config(self) -> dict:
        """Return catalog configuration dict."""
        return self._config.get("catalog", {})

    def get_parameters(self) -> dict:
        """Return merged dict of all parameters*.yaml files."""
        result = {}
        for stem, data in self._config.items():
            if stem == "parameters" or stem.startswith("parameters_"):
                result = _deep_merge(result, data)
        return result
