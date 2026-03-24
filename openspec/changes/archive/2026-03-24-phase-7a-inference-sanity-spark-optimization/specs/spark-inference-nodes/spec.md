## MODIFIED Requirements

### Requirement: predict_scores chunks by snap_date and product
The `predict_scores` function SHALL process data in chunks of `(snap_date, prod_name)` pairs to control memory usage. Each chunk SHALL separately select feature columns and identity columns via `toPandas()`, predict scores, then concatenate results.

#### Scenario: Chunked prediction with multiple snap_dates and products
- **WHEN** X_score contains multiple snap_dates and products
- **THEN** the function iterates over each (snap_date, prod_name) pair, converting only one pair at a time to pandas for prediction

#### Scenario: Model version injection for partitioned output
- **WHEN** parameters contains a "model_version" key
- **THEN** the function adds a "model_version" column with the version string to the result DataFrame

### Requirement: build_scoring_dataset does not trigger unnecessary actions
The `build_scoring_dataset` function SHALL NOT call `.count()` on Spark DataFrames for logging purposes.

#### Scenario: No count actions in scoring dataset build
- **WHEN** build_scoring_dataset completes
- **THEN** no `.count()` action is triggered; log message uses only locally available values (product count, snap_date count)

### Requirement: rank_predictions does not trigger unnecessary actions
The `rank_predictions` function SHALL NOT call `.count()` or `.dropDuplicates().count()` for logging purposes.

#### Scenario: No count actions in rank predictions
- **WHEN** rank_predictions completes
- **THEN** no `.count()` action is triggered; log message uses only the group column names
