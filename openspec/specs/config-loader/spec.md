## ADDED Requirements

### Requirement: Load YAML config files from environment directory

ConfigLoader SHALL load all `.yaml` files from `conf/base/` and `conf/{env}/` directories, where `env` defaults to `"local"`.

#### Scenario: Load base config only
- **WHEN** ConfigLoader is initialized with `env="local"` and only `conf/base/` contains YAML files
- **THEN** all YAML files in `conf/base/` are loaded and accessible

#### Scenario: Load with environment overlay
- **WHEN** ConfigLoader is initialized with `env="local"` and both `conf/base/` and `conf/local/` contain YAML files
- **THEN** environment-specific values override base values via deep merge

### Requirement: Deep merge environment config over base config

ConfigLoader SHALL perform recursive deep merge where environment values override base values. For nested dicts, merging is recursive. For non-dict values, the environment value replaces the base value entirely.

#### Scenario: Nested dict merge
- **WHEN** base config has `{a: {b: 1, c: 2}}` and env config has `{a: {b: 99}}`
- **THEN** merged result is `{a: {b: 99, c: 2}}`

#### Scenario: List replacement (no merge)
- **WHEN** base config has `{features: [a, b]}` and env config has `{features: [x]}`
- **THEN** merged result is `{features: [x]}` (env replaces entirely)

### Requirement: Provide typed access methods for catalog and parameters

ConfigLoader SHALL provide `get_catalog_config(runtime_params=None)` returning the catalog configuration dict, and `get_parameters()` returning a merged dict of all `parameters*.yaml` files.

`get_catalog_config()` SHALL accept an optional `runtime_params: dict[str, str]` parameter. When provided, all `${key}` placeholders in `filepath` values SHALL be replaced with the corresponding value. Unmatched placeholders SHALL be preserved as-is (no error).

#### Scenario: Get catalog config
- **WHEN** `conf/base/catalog.yaml` exists with dataset definitions
- **THEN** `get_catalog_config()` returns the parsed dict from catalog YAML files

#### Scenario: Get merged parameters
- **WHEN** `conf/base/parameters.yaml` and `conf/base/parameters_training.yaml` both exist
- **THEN** `get_parameters()` returns a single dict with all parameter files merged

#### Scenario: Missing config directory
- **WHEN** the specified `conf/{env}/` directory does not exist
- **THEN** ConfigLoader uses only base config without raising an error

#### Scenario: Get catalog config with runtime_params
- **WHEN** catalog.yaml contains `filepath: data/models/${model_version}/model.pkl` and `get_catalog_config(runtime_params={"model_version": "20260316_120000"})` is called
- **THEN** the returned dict SHALL have `filepath: data/models/20260316_120000/model.pkl`

#### Scenario: 多個 template variables 同時替換
- **WHEN** runtime_params 包含 `{"model_version": "best", "dataset_version": "a1b2c3d4", "snap_date": "20240331"}`
- **THEN** catalog 中所有 filepath 的 `${model_version}`、`${dataset_version}`、`${snap_date}` SHALL 被替換為對應值

#### Scenario: Get catalog config without runtime_params
- **WHEN** `get_catalog_config()` is called without runtime_params
- **THEN** `${model_version}` placeholders SHALL be preserved as-is in the returned dict

#### Scenario: Unknown template variable preserved
- **WHEN** catalog contains `${unknown}` and runtime_params does not include `unknown`
- **THEN** `${unknown}` SHALL remain in the filepath unchanged

### Requirement: 取得特定參數檔內容
ConfigLoader SHALL 提供 `get_parameters_by_name(name: str) -> dict` 方法，回傳指定 parameters 檔的合併後內容（base + env overlay）。

#### Scenario: 取得 dataset 參數
- **WHEN** 呼叫 `get_parameters_by_name("parameters_dataset")`
- **THEN** SHALL 回傳 parameters_dataset.yaml 的合併後完整內容（用於 hash 計算）

#### Scenario: 取得 training 參數
- **WHEN** 呼叫 `get_parameters_by_name("parameters_training")`
- **THEN** SHALL 回傳 parameters_training.yaml 的合併後完整內容


## MODIFIED Requirements

### Requirement: parameters.yaml supports schema and logging sections
The ConfigLoader SHALL pass through `schema` and `logging` sections from `parameters.yaml` without modification. No special handling is required — these are consumed by `get_schema()` and `setup_logging()` respectively.

#### Scenario: Schema section loaded
- **WHEN** `parameters.yaml` contains a `schema` section
- **THEN** `get_parameters()` SHALL include it in the returned dict

#### Scenario: Logging section loaded
- **WHEN** `parameters.yaml` contains a `logging` section
- **THEN** `get_parameters()` SHALL include it in the returned dict
