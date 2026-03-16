## MODIFIED Requirements

### Requirement: Provide typed access methods for catalog and parameters
ConfigLoader SHALL provide `get_catalog_config(runtime_params=None)` returning the catalog configuration dict, and `get_parameters()` returning a merged dict of all `parameters*.yaml` files.

`get_catalog_config()` SHALL accept an optional `runtime_params: dict[str, str]` parameter. When provided, all `${key}` placeholders in `filepath` values SHALL be replaced with the corresponding value. Unmatched placeholders SHALL be preserved as-is (no error).

#### Scenario: Get catalog config with runtime_params
- **WHEN** catalog.yaml contains `filepath: data/models/${model_version}/model.pkl` and `get_catalog_config(runtime_params={"model_version": "20260316_120000"})` is called
- **THEN** the returned dict SHALL have `filepath: data/models/20260316_120000/model.pkl`

#### Scenario: Get catalog config without runtime_params
- **WHEN** `get_catalog_config()` is called without runtime_params
- **THEN** `${model_version}` placeholders SHALL be preserved as-is in the returned dict

#### Scenario: Unknown template variable preserved
- **WHEN** catalog contains `${unknown}` and runtime_params does not include `unknown`
- **THEN** `${unknown}` SHALL remain in the filepath unchanged
