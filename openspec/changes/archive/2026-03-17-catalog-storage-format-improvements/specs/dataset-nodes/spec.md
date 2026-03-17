## MODIFIED Requirements

### Requirement: prepare_model_input returns DataFrame for labels

`prepare_model_input()` SHALL return `y_train`, `y_train_dev`, `y_val` as `pd.DataFrame` with a single column `"label"` (instead of `np.ndarray`). The `X_train`, `X_train_dev`, `X_val` return type SHALL remain `pd.DataFrame`.

#### Scenario: y_train is a single-column DataFrame
- **WHEN** `prepare_model_input()` is called
- **THEN** `y_train` SHALL be a `pd.DataFrame` with columns `["label"]`
- **AND** `len(y_train)` SHALL equal `len(X_train)`

#### Scenario: y_train_dev is a single-column DataFrame
- **WHEN** `prepare_model_input()` is called
- **THEN** `y_train_dev` SHALL be a `pd.DataFrame` with columns `["label"]`
- **AND** `len(y_train_dev)` SHALL equal `len(X_train_dev)`

#### Scenario: y_val is a single-column DataFrame
- **WHEN** `prepare_model_input()` is called
- **THEN** `y_val` SHALL be a `pd.DataFrame` with columns `["label"]`
- **AND** `len(y_val)` SHALL equal `len(X_val)`

#### Scenario: Label values are preserved
- **WHEN** `prepare_model_input()` converts labels
- **THEN** `y_train["label"].values` SHALL be identical to the original `train_set["label"].values`
