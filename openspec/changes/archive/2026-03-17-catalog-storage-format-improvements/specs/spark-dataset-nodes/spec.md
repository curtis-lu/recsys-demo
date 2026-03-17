## MODIFIED Requirements

### Requirement: Spark prepare_model_input returns DataFrame for labels

The Spark backend `prepare_model_input()` SHALL return `y_train`, `y_train_dev`, `y_val` as `pd.DataFrame` with a single column `"label"` (instead of `np.ndarray`), consistent with the pandas backend.

#### Scenario: Spark y_train is a single-column DataFrame
- **WHEN** Spark backend `prepare_model_input()` is called
- **THEN** `y_train` SHALL be a `pd.DataFrame` with columns `["label"]`

#### Scenario: Spark y outputs match pandas backend contract
- **WHEN** Spark backend `prepare_model_input()` is called
- **THEN** all y outputs SHALL have the same type and structure as the pandas backend (`pd.DataFrame` with `"label"` column)
