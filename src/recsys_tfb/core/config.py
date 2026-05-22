import os
import re
from pathlib import Path

import yaml


class ConfigEnvError(ValueError):
    """Raised when a required ${env.NAME} placeholder has no environment variable set."""


_ENV_PLACEHOLDER = re.compile(r"\$\{env\.([A-Za-z_]\w*)(\|([^}]*))?\}")


def _resolve_env_string(value: str, loc: str, errors: list[str]) -> str:
    """Resolve every ${env.NAME[|default]} placeholder in one string.

    Missing required variable (no ``|default``) appends an error to ``errors``
    and leaves the placeholder text in place; the caller raises once collected.
    A ``|default`` value cannot itself contain ``}`` (the regex stops at the
    first one) — defaults are for plain strings, not nested structures.
    """

    def repl(match: re.Match) -> str:
        name = match.group(1)
        has_default = match.group(2) is not None
        default = match.group(3)
        env_value = os.environ.get(name)
        if env_value is not None:
            return env_value
        if has_default:
            return default
        errors.append(
            f"  {loc} : 環境變數 '{name}' 未設定\n"
            f"      (如需預設值請改寫 ${{env.{name}|<default>}})"
        )
        return match.group(0)

    return _ENV_PLACEHOLDER.sub(repl, value)


def _resolve_env(config: dict) -> dict:
    """Resolve ${env.NAME} placeholders across the whole config tree.

    Walks every parameters/catalog file. Collects all missing-required-variable
    errors and raises ConfigEnvError once (collect-all). Non-string values pass
    through unchanged. Only the ``env.`` prefix is touched — ``${hive.db}`` and
    other placeholder families are left for their own resolvers.
    """
    errors: list[str] = []

    def walk_file(stem: str, data):
        """Walk one config file's tree; ``stem`` is fixed for the whole descent."""

        def walk(obj, keypath: str):
            if isinstance(obj, dict):
                return {
                    k: walk(v, f"{keypath}.{k}" if keypath else k)
                    for k, v in obj.items()
                }
            if isinstance(obj, list):
                return [walk(v, f"{keypath}[{i}]") for i, v in enumerate(obj)]
            if isinstance(obj, str):
                loc = f"{stem}.yaml -> {keypath}" if keypath else f"{stem}.yaml"
                return _resolve_env_string(obj, loc, errors)
            return obj

        return walk(data, "")

    resolved = {stem: walk_file(stem, data) for stem, data in config.items()}
    if errors:
        raise ConfigEnvError(
            f"{len(errors)} 個必填環境變數未設定:\n" + "\n".join(errors)
        )
    return resolved


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base. Non-dict values are replaced."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _flatten_params(params: dict, prefix: str = "") -> dict[str, str]:
    """Flatten nested dict into dotted keys, e.g. {'hive': {'db': 'x'}} → {'hive.db': 'x'}."""
    result: dict[str, str] = {}
    for key, value in params.items():
        full = f"{prefix}{key}"
        if isinstance(value, dict):
            result.update(_flatten_params(value, prefix=f"{full}."))
        else:
            result[full] = str(value)
    return result


def _substitute(obj, params: dict[str, str]):
    flat = _flatten_params(params)
    return _apply(obj, flat)


def _apply(obj, flat: dict[str, str]):
    if isinstance(obj, dict):
        return {k: _apply(v, flat) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_apply(v, flat) for v in obj]
    if isinstance(obj, str):
        out = obj
        for key, value in flat.items():
            out = out.replace(f"${{{key}}}", value)
        return out
    return obj


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
        self._config = _resolve_env(self._config)

    def get_catalog_config(
        self, runtime_params: dict[str, str] | None = None
    ) -> dict:
        """Return catalog configuration dict.

        If runtime_params is provided, substitute ``${key}`` placeholders
        recursively in every string value (supports nested keys like
        ``${hive.db}`` resolved from nested dicts in parameters).
        """
        catalog = self._config.get("catalog", {})
        if not runtime_params:
            return catalog
        return _substitute(catalog, runtime_params)

    def get_parameters(self) -> dict:
        """Return merged dict of all parameters*.yaml files."""
        result = {}
        for stem, data in self._config.items():
            if stem == "parameters" or stem.startswith("parameters_"):
                result = _deep_merge(result, data)
        return result

    def get_parameters_by_name(self, name: str) -> dict:
        """Return the merged content of a specific parameters file.

        Args:
            name: The stem name of the parameters file,
                  e.g. ``"parameters_dataset"`` for ``parameters_dataset.yaml``.

        Returns:
            The merged (base + env overlay) dict for that file.

        Raises:
            KeyError: If no file with that stem name was loaded.
        """
        if name not in self._config:
            raise KeyError(
                f"No config file '{name}.yaml' found in base or {self._env} directories."
            )
        return self._config[name]
