### Requirement: Build scoring dataset from feature table and product list
`build_scoring_dataset` SHALL accept a feature_table DataFrame and parameters dict, filter to target snap_dates, and produce a cartesian product of unique (snap_date, cust_id) combinations with the configured product list. The output SHALL include all feature columns from feature_table plus a prod_name column.

#### Scenario: Cross-join with configured products
- **WHEN** feature_table contains 100 unique (snap_date, cust_id) pairs for the target snap_date and parameters lists 22 products
- **THEN** the output SHALL contain 2200 rows (100 x 22), each with the customer's features and a prod_name value

#### Scenario: Filter by snap_dates
- **WHEN** feature_table contains data for snap_dates 2024-01-31, 2024-02-29, 2024-03-31 and parameters["inference"]["snap_dates"] is ["2024-03-31"]
- **THEN** only customers from 2024-03-31 SHALL appear in the output

#### Scenario: Empty feature table for target date
- **WHEN** feature_table contains no rows matching the target snap_dates
- **THEN** the output SHALL be an empty DataFrame with the expected columns

### Requirement: Apply preprocessor to scoring dataset
`apply_preprocessor` SHALL accept a scoring_dataset DataFrame and a preprocessor dict (from training), then apply the same transformations: drop specified columns, encode categorical columns using saved category_mappings, and ensure output columns match preprocessor["feature_columns"] in exact order.

#### Scenario: Consistent encoding with training
- **WHEN** scoring_dataset contains prod_name values that exist in preprocessor["category_mappings"]["prod_name"]
- **THEN** the encoded values SHALL match what the training pipeline would produce for the same inputs

#### Scenario: Drop columns absent in inference
- **WHEN** preprocessor["drop_columns"] includes "label" but scoring_dataset has no "label" column
- **THEN** the function SHALL proceed without error (errors="ignore")

#### Scenario: Feature column order matches model expectation
- **WHEN** apply_preprocessor completes
- **THEN** the output columns SHALL be exactly preprocessor["feature_columns"] in the same order

#### Scenario: Unknown categorical value
- **WHEN** scoring_dataset contains a prod_name not in preprocessor["category_mappings"]["prod_name"]
- **THEN** the value SHALL be encoded as -1 and a warning SHALL be logged

### Requirement: Predict scores using trained model
`predict_scores` SHALL accept a ModelAdapter instance, X_score DataFrame, and scoring_dataset DataFrame, then return a DataFrame with snap_date, cust_id, prod_code, and score columns. SHALL 呼叫 `model.predict(X_score.values)` 取得預測分數。

#### Scenario: Score output format
- **WHEN** predict_scores completes with 2200 input rows
- **THEN** the output SHALL contain 2200 rows with columns [snap_date, cust_id, prod_code, score]

#### Scenario: Score range
- **WHEN** the model is a binary classifier
- **THEN** all score values SHALL be in the range [0, 1]

#### Scenario: Product code mapping
- **WHEN** Strategy 1 is used (prod_name is the product identifier)
- **THEN** prod_code SHALL equal prod_name directly

#### Scenario: model 參數型別
- **WHEN** 查看 predict_scores 函數簽名
- **THEN** model 參數型別 SHALL 為 ModelAdapter（非 lgb.Booster）

### Requirement: Rank predictions per customer
`rank_predictions` SHALL accept a score_table DataFrame and parameters dict, group by (snap_date, cust_id), and assign a 1-based rank by descending score within each group.

#### Scenario: Rank output format
- **WHEN** rank_predictions completes
- **THEN** the output SHALL contain columns [snap_date, cust_id, prod_code, score, rank]

#### Scenario: Rank ordering
- **WHEN** a customer has scores [0.9, 0.7, 0.3] for products [A, B, C]
- **THEN** ranks SHALL be A=1, B=2, C=3

#### Scenario: Rank uniqueness per customer
- **WHEN** a customer has 22 products scored
- **THEN** ranks SHALL be 1 through 22 with no duplicates (method="first" for tie-breaking)

#### Scenario: Output sorting
- **WHEN** rank_predictions completes
- **THEN** the output SHALL be sorted by snap_date, cust_id, rank


## MODIFIED Requirements

### Requirement: Inference nodes use schema for column names
All pandas inference nodes (build_scoring_dataset, apply_preprocessor, predict_scores, rank_predictions) SHALL obtain column names from `get_schema(parameters)`.

#### Scenario: Default column names
- **WHEN** called with parameters without `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Custom item column in ranking
- **WHEN** `schema.columns.item` is set to `"channel_type"`
- **THEN** ranking SHALL group by the custom item column instead of `prod_name`
