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

ConfigLoader SHALL provide `get_catalog_config()` returning the catalog configuration dict, and `get_parameters()` returning a merged dict of all `parameters*.yaml` files.

#### Scenario: Get catalog config
- **WHEN** `conf/base/catalog.yaml` exists with dataset definitions
- **THEN** `get_catalog_config()` returns the parsed dict from catalog YAML files

#### Scenario: Get merged parameters
- **WHEN** `conf/base/parameters.yaml` and `conf/base/parameters_training.yaml` both exist
- **THEN** `get_parameters()` returns a single dict with all parameter files merged

#### Scenario: Missing config directory
- **WHEN** the specified `conf/{env}/` directory does not exist
- **THEN** ConfigLoader uses only base config without raising an error
