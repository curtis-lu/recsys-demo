## MODIFIED Requirements

### Requirement: Training functions accept DataFrame labels

`tune_hyperparameters()`, `train_model()`, and `evaluate_model()` SHALL accept `y_train`, `y_train_dev`, `y_val` as `pd.DataFrame` (single column `"label"`) and extract `.values` when passing to LightGBM or numpy computation functions.

#### Scenario: tune_hyperparameters accepts DataFrame y
- **WHEN** `tune_hyperparameters()` is called with `y_train` and `y_train_dev` as `pd.DataFrame`
- **THEN** it SHALL extract `y_train["label"].values` for `lgb.Dataset` and `_compute_ap` calls
- **AND** return valid hyperparameter dict

#### Scenario: train_model accepts DataFrame y
- **WHEN** `train_model()` is called with `y_train` and `y_train_dev` as `pd.DataFrame`
- **THEN** it SHALL extract `["label"].values` for `lgb.Dataset` construction
- **AND** return a valid `lgb.Booster`

#### Scenario: evaluate_model accepts DataFrame y
- **WHEN** `evaluate_model()` is called with `y_val` as `pd.DataFrame`
- **THEN** it SHALL extract `y_val["label"].values` for `_compute_map` and per-product AP calculations
- **AND** return valid evaluation results dict
