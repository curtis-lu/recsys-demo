# 環境變數設定解析（`${env.X}`）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 YAML 設定值可用 `${env.NAME}` / `${env.NAME|default}` 語法引用環境變數,由 `ConfigLoader` 全域統一解析。

**Architecture:** 在 `ConfigLoader._load()` 末端新增一個遞迴 env 解析 pass,涵蓋所有 `parameters_*` 與 `catalog`(含 spark 區塊)。必填變數未設即 collect-all 後 raise `ConfigEnvError`。`utils/vdclient_resolver.py` 內重複的 `resolve_env_placeholders` 因而成為死碼,移除。

**Tech Stack:** Python 3.10、PyYAML、Typer、pytest 7.3.1。

**Spec:** `docs/superpowers/specs/2026-05-22-env-var-config-resolution-design.md`

**前置:** 所有指令在 worktree 內執行。測試一律:
`PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/env-var-config-resolution/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
(下文以 `<PYTEST>` 代表此前綴)

---

## Task 1: `core/config.py` 的 env 解析核心

**Files:**
- Modify: `src/recsys_tfb/core/config.py`
- Test: `tests/test_core/test_config.py`

- [ ] **Step 1: 寫失敗測試**

在 `tests/test_core/test_config.py` 末端新增(同時把第 3 行 import 改為
`from recsys_tfb.core.config import ConfigLoader, ConfigEnvError, _deep_merge`):

```python
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
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `<PYTEST> tests/test_core/test_config.py::TestEnvResolution -q`
Expected: FAIL — `ImportError: cannot import name 'ConfigEnvError'`

- [ ] **Step 3: 實作 env 解析**

在 `src/recsys_tfb/core/config.py`,把第 1 行 `import os` 下方加入 `import re`
(最終 import 區為 `import os` / `import re` / `from pathlib import Path` / `import yaml`)。

在 `import yaml` 之後、`_deep_merge` 之前,加入:

```python
class ConfigEnvError(ValueError):
    """Raised when a required ${env.NAME} placeholder has no environment variable set."""


_ENV_PLACEHOLDER = re.compile(r"\$\{env\.([A-Za-z_]\w*)(\|([^}]*))?\}")


def _resolve_env_string(value: str, loc: str, errors: list[str]) -> str:
    """Resolve every ${env.NAME[|default]} placeholder in one string.

    Missing required variable (no ``|default``) appends an error to ``errors``
    and leaves the placeholder text in place; the caller raises once collected.
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

    def walk(obj, stem: str, keypath: str):
        if isinstance(obj, dict):
            return {
                k: walk(v, stem, f"{keypath}.{k}" if keypath else k)
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [walk(v, stem, f"{keypath}[{i}]") for i, v in enumerate(obj)]
        if isinstance(obj, str):
            loc = f"{stem}.yaml -> {keypath}" if keypath else f"{stem}.yaml"
            return _resolve_env_string(obj, loc, errors)
        return obj

    resolved = {stem: walk(data, stem, "") for stem, data in config.items()}
    if errors:
        raise ConfigEnvError(
            f"{len(errors)} 個必填環境變數未設定:\n" + "\n".join(errors)
        )
    return resolved
```

在 `ConfigLoader._load()` 的最後(`self._config[stem] = _deep_merge(base, env)`
迴圈結束後)加入一行:

```python
        self._config = _resolve_env(self._config)
```

- [ ] **Step 4: 跑測試確認通過**

Run: `<PYTEST> tests/test_core/test_config.py -q`
Expected: PASS — 全部 `TestDeepMerge` / `TestConfigLoader` / `TestGetParametersByName` / `TestEnvResolution` 通過(原 14 + 新 12 = 26 項)

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/config.py tests/test_core/test_config.py
git commit -m "feat(config): resolve \${env.X} placeholders in ConfigLoader"
```

---

## Task 2: 設定檔改用 `${env.X}`

**Files:**
- Modify: `conf/base/parameters_training.yaml:90`
- Modify: `conf/base/parameters.yaml:6-14`

- [ ] **Step 1: 改 `parameters_training.yaml`**

把第 90 行
```yaml
  tracking_uri: mlruns
```
改為(加引號避免 YAML 對 `|` 的任何歧義):
```yaml
  tracking_uri: "${env.MLFLOW_TRACKING_URI|mlruns}"
```

- [ ] **Step 2: 改 `parameters.yaml` 註解**

第 6-9 行的註解區塊原文:
```yaml
# Dev-cluster (Docker Spark Standalone) 覆蓋：base 的 spark.master=yarn 是給
# 真實 CDP/YARN 用，本機 dev-cluster 走 Standalone。其餘 driver.host/port、
# vdclient placeholder 等在 dev 端會由 resolve_*_placeholders 自動 drop，
# 由 ~/dev-cluster/client-template/spark/spark-defaults.conf 補齊。
```
改為:
```yaml
# Dev-cluster (Docker Spark Standalone) 覆蓋：base 的 spark.master=yarn 是給
# 真實 CDP/YARN 用，本機 dev-cluster 走 Standalone。${vdclient.*} placeholder
# 在 dev 端會由 resolve_vdclient_placeholders 自動 drop、${env.*} 由 ConfigLoader
# 解析，由 ~/dev-cluster/client-template/spark/spark-defaults.conf 補齊。
```

第 14 行原文:
```yaml
  # spark.driver.host: ${env.NODE_IP}
```
改為(反映新文法;空 default 表示未設時退回空字串、不 raise):
```yaml
  # spark.driver.host: ${env.NODE_IP|}
```

- [ ] **Step 3: 驗證 YAML 仍可載入**

Run:
```bash
<PYTEST> tests/test_core/test_config.py -q
```
另確認設定檔語法:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "import yaml; yaml.safe_load(open('conf/base/parameters_training.yaml')); yaml.safe_load(open('conf/base/parameters.yaml')); print('yaml ok')"
```
Expected: 測試 PASS;印出 `yaml ok`

- [ ] **Step 4: Commit**

```bash
git add conf/base/parameters_training.yaml conf/base/parameters.yaml
git commit -m "feat(config): mlflow tracking_uri reads MLFLOW_TRACKING_URI env var"
```

---

## Task 3: `__main__.py` 接住 `ConfigEnvError`、移除 spark env resolver 呼叫

**Files:**
- Modify: `src/recsys_tfb/__main__.py` — `_load_config_and_setup`、`_load_spark_config`
- Test: `tests/test_core/test_env_config_cli_wiring.py`(新建)

- [ ] **Step 1: 寫失敗測試**

新建 `tests/test_core/test_env_config_cli_wiring.py`:

```python
"""ConfigEnvError 必須在 _load_config_and_setup 內被接住;
spark config 不再呼叫 resolve_env_placeholders。"""

import inspect

from recsys_tfb import __main__ as m


def test_config_loader_construction_inside_try():
    src = inspect.getsource(m._load_config_and_setup)
    # ConfigLoader 在 try 區塊內建構,使 ConfigEnvError(ValueError 子類)
    # 被捕捉並轉成乾淨的 CLI exit。
    before_loader = src.split("ConfigLoader(")[0]
    assert "try:" in before_loader


def test_load_spark_config_no_env_resolver():
    src = inspect.getsource(m._load_spark_config)
    assert "resolve_env_placeholders" not in src
    assert "resolve_vdclient_placeholders" in src
```

- [ ] **Step 2: 跑測試確認失敗**

Run: `<PYTEST> tests/test_core/test_env_config_cli_wiring.py -q`
Expected: FAIL — `test_config_loader_construction_inside_try`(`ConfigLoader` 建構目前在 try 外)與 `test_load_spark_config_no_env_resolver`(`resolve_env_placeholders` 仍存在)兩項皆失敗

- [ ] **Step 3: 改 `_load_config_and_setup`**

`src/recsys_tfb/__main__.py` 的 `_load_config_and_setup` 現況:
```python
def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, RunContext]:
    conf_dir = _find_conf_dir()
    config = ConfigLoader(str(conf_dir), env=env)
    params = config.get_parameters()

    run_context = RunContext(pipeline=pipeline, env=env)
    setup_logging(params, run_context)

    try:
        validate_schema_config(params)
        validate_config_consistency(params)
    except ValueError as exc:
        logger.error("Config validation failed: %s", exc)
        raise typer.Exit(code=1)

    return config, params, run_context
```
改為(把 `ConfigLoader` 建構包進 try,使 `ConfigEnvError` 被接住):
```python
def _load_config_and_setup(pipeline: str, env: str) -> tuple[ConfigLoader, dict, RunContext]:
    conf_dir = _find_conf_dir()
    try:
        config = ConfigLoader(str(conf_dir), env=env)
        params = config.get_parameters()
    except ValueError as exc:
        logger.error("Config loading failed: %s", exc)
        raise typer.Exit(code=1)

    run_context = RunContext(pipeline=pipeline, env=env)
    setup_logging(params, run_context)

    try:
        validate_schema_config(params)
        validate_config_consistency(params)
    except ValueError as exc:
        logger.error("Config validation failed: %s", exc)
        raise typer.Exit(code=1)

    return config, params, run_context
```

- [ ] **Step 4: 改 `_load_spark_config`**

現況:
```python
    from recsys_tfb.utils.vdclient_resolver import (
        resolve_env_placeholders,
        resolve_vdclient_placeholders,
    )
```
改為:
```python
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders
```
並把這兩行:
```python
    base_spark.update(pipe_spark)
    base_spark = resolve_env_placeholders(base_spark)
    return resolve_vdclient_placeholders(base_spark)
```
改為:
```python
    base_spark.update(pipe_spark)
    return resolve_vdclient_placeholders(base_spark)
```

- [ ] **Step 5: 跑測試確認通過**

Run: `<PYTEST> tests/test_core/test_env_config_cli_wiring.py tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS — 全數通過

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/__main__.py tests/test_core/test_env_config_cli_wiring.py
git commit -m "feat(config): catch ConfigEnvError in CLI; drop spark env resolver call"
```

---

## Task 4: `utils/spark.py` fallback 路徑移除 env resolver 呼叫

**Files:**
- Modify: `src/recsys_tfb/utils/spark.py` — `_fallback_create`

- [ ] **Step 1: 改 `_fallback_create`**

`src/recsys_tfb/utils/spark.py` 內 `_fallback_create` 現況:
```python
    spark_configs = base_params.get("spark", {})

    # Match the entrypoint path (__main__._load_spark_config): resolve
    # ${env.*} and ${vdclient.*.*} placeholders before handing dict to the
    # builder. Otherwise yaml values like ${vdclient.cdp.driver_port} reach
    # SparkConf as literal strings → "spark.driver.port should be int".
    from recsys_tfb.utils.vdclient_resolver import (
        resolve_env_placeholders,
        resolve_vdclient_placeholders,
    )
    spark_configs = resolve_env_placeholders(spark_configs)
    spark_configs = resolve_vdclient_placeholders(spark_configs)
```
改為:
```python
    spark_configs = base_params.get("spark", {})

    # Match the entrypoint path (__main__._load_spark_config): resolve
    # ${vdclient.*.*} placeholders before handing dict to the builder.
    # Otherwise yaml values like ${vdclient.cdp.driver_port} reach SparkConf
    # as literal strings → "spark.driver.port should be int".
    # ${env.*} placeholders are already resolved by ConfigLoader.
    from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders
    spark_configs = resolve_vdclient_placeholders(spark_configs)
```

- [ ] **Step 2: 驗證**

Run:
```bash
grep -n "resolve_env_placeholders" src/recsys_tfb/utils/spark.py; echo "exit=$?"
<PYTEST> tests/test_utils/test_spark.py -q
```
Expected: grep 印出 `exit=1`(無殘留);`test_spark.py` PASS

- [ ] **Step 3: Commit**

```bash
git add src/recsys_tfb/utils/spark.py
git commit -m "refactor(spark): drop redundant env resolver call in _fallback_create"
```

---

## Task 5: 刪除 `vdclient_resolver.py` 的 `resolve_env_placeholders`

**Files:**
- Modify: `src/recsys_tfb/utils/vdclient_resolver.py`
- Test: `tests/test_utils/test_vdclient_resolver.py`

- [ ] **Step 1: 改測試(移除 `TestResolveEnv`)**

`tests/test_utils/test_vdclient_resolver.py` 第 7-10 行 import:
```python
from recsys_tfb.utils.vdclient_resolver import (
    resolve_env_placeholders,
    resolve_vdclient_placeholders,
)
```
改為:
```python
from recsys_tfb.utils.vdclient_resolver import resolve_vdclient_placeholders
```
並刪除整個 `class TestResolveEnv:`(第 122 行到檔末第 168 行)。

- [ ] **Step 2: 跑 trimmed 測試確認仍綠**

本任務是刪除式重構,無「先寫失敗測試」階段。Step 1 後測試檔已不引用
`resolve_env_placeholders`,而 `vdclient_resolver.py` 尚未刪函式,故測試應通過。

Run: `<PYTEST> tests/test_utils/test_vdclient_resolver.py -q`
Expected: PASS — `TestResolve` 8 項(`TestResolveEnv` 已移除)。若 FAIL 表示 Step 1 刪除範圍有誤,修正後再續。

- [ ] **Step 3: 刪除 `resolve_env_placeholders` 與相關物**

`src/recsys_tfb/utils/vdclient_resolver.py`:

(a) module docstring(第 1-18 行)整段改為:
```python
"""Resolve ${vdclient.<cluster>.<field>} placeholders in spark config values.

  ``${vdclient.<cluster>.<field>}`` → ``vdclient_magic.spark_ports("<cluster>")``
      Returns the named field from the port tuple. Supported fields:
      ``driver_port`` (index 0), ``blockManager_port`` (index 1).
      ``vdclient_magic`` is production-only; unavailable on laptops / CI.
      Lazy-imported once; result cached per cluster per call.

Drop-on-missing semantics: if the lookup fails (import error, missing getter,
unknown field), the affected spark config key is dropped from the returned
dict and a warning is logged, so PySpark falls back to its default.

``${env.<NAME>}`` placeholders are resolved earlier and globally by
``recsys_tfb.core.config.ConfigLoader`` — not here.
"""
```

(b) 刪除第 2 行 `import os`(移除 `resolve_env_placeholders` 後不再使用;保留 `import logging`、`import re`)。

(c) 刪除 `_ENV_PATTERN = re.compile(r"\$\{env\.(\w+)\}")` 這一行。

(d) 刪除整個 `def resolve_env_placeholders(spark_configs: dict) -> dict:` 函式
(原第 120-155 行,連同其 docstring 與函式體)。

- [ ] **Step 4: 跑測試確認通過**

Run: `<PYTEST> tests/test_utils/test_vdclient_resolver.py tests/test_utils/test_spark.py -q`
Expected: PASS — `TestResolve` 8 項 + `test_spark.py` 全數通過

- [ ] **Step 5: 確認無殘留引用**

Run:
```bash
grep -rn "resolve_env_placeholders" src/ tests/; echo "exit=$?"
```
Expected: 印出 `exit=1`(全 repo 無任何殘留引用)

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/utils/vdclient_resolver.py tests/test_utils/test_vdclient_resolver.py
git commit -m "refactor(spark): remove dead resolve_env_placeholders from vdclient_resolver"
```

---

## Task 6: 全域回歸驗證

**Files:** 無(僅執行驗證)

- [ ] **Step 1: 跑全部受影響測試**

Run:
```bash
<PYTEST> tests/test_core/test_config.py tests/test_core/test_env_config_cli_wiring.py tests/test_core/test_consistency_cli_wiring.py tests/test_utils/test_vdclient_resolver.py tests/test_utils/test_spark.py -q
```
Expected: PASS — 全數通過,0 failures

- [ ] **Step 2: 確認 diff 範圍**

Run:
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/env-var-config-resolution diff --stat be8cbd2..HEAD
```
Expected: 僅異動 `src/recsys_tfb/core/config.py`、`src/recsys_tfb/__main__.py`、
`src/recsys_tfb/utils/spark.py`、`src/recsys_tfb/utils/vdclient_resolver.py`、
`conf/base/parameters_training.yaml`、`conf/base/parameters.yaml`、
`tests/test_core/test_config.py`、`tests/test_core/test_env_config_cli_wiring.py`、
`tests/test_utils/test_vdclient_resolver.py`、以及兩份 `docs/superpowers/` 文件。
`src/recsys_tfb/pipelines/training/nodes.py` **不應**出現在清單中。

---

## 驗收標準

- `${env.NAME}` / `${env.NAME|default}` / `${env.NAME|}` 三種文法在所有 `parameters_*`
  與 `catalog` 設定值生效。
- 必填變數未設 → `ConfigEnvError`,訊息點名檔案、key 路徑、變數名,collect-all 一次回報。
- `mlflow.tracking_uri` 設了 `MLFLOW_TRACKING_URI` 則用之,未設退回 `mlruns`。
- `vdclient_resolver.py` 僅保留 `resolve_vdclient_placeholders`;全 repo 無
  `resolve_env_placeholders` 殘留引用。
- `nodes.py` 不被修改(`log_experiment` 的 `.get("tracking_uri", "mlruns")` 為無害雙保險)。
