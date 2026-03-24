## ADDED Requirements

### Requirement: Spark build_scoring_dataset node
The system SHALL provide a function `build_scoring_dataset(feature_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that builds the scoring dataset using PySpark operations.

#### Scenario: Cross-join with products
- **WHEN** feature_table is a Spark DataFrame containing 100 unique (snap_date, cust_id) for the target snap_date and parameters lists 22 products
- **THEN** the function SHALL use `crossJoin` to produce 2200 rows, each with customer features and a prod_name

#### Scenario: Filter by snap_dates
- **WHEN** parameters["inference"]["snap_dates"] specifies target dates
- **THEN** the function SHALL use `F.col("snap_date").isin(snap_dates)` to filter before cross-joining

### Requirement: Spark apply_preprocessor node
The system SHALL provide a function `apply_preprocessor(scoring_dataset: pyspark.sql.DataFrame, preprocessor: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that applies preprocessing using PySpark operations.

#### Scenario: Category encoding via broadcast join
- **WHEN** preprocessor contains category_mappings for prod_name
- **THEN** the function SHALL create a mapping DataFrame, broadcast-join it, and replace the original column with integer codes

#### Scenario: Identity columns preserved
- **WHEN** apply_preprocessor completes
- **THEN** the output SHALL retain snap_date, cust_id, prod_name columns alongside feature_columns to enable downstream alignment in predict_scores

#### Scenario: Feature column order matches training
- **WHEN** apply_preprocessor completes
- **THEN** the output SHALL contain all preprocessor["feature_columns"] in the exact same order as training

### Requirement: Spark predict_scores node with chunked conversion
The system SHALL provide a function `predict_scores(model: ModelAdapter, X_score: pyspark.sql.DataFrame, scoring_dataset: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame` in `nodes_spark.py` that predicts scores by chunking data through pandas for model prediction.

#### Scenario: Chunked prediction by snap_date and product
- **WHEN** X_score contains multiple snap_dates and products
- **THEN** the function iterates over each (snap_date, prod_name) pair, converting only one pair at a time to pandas, calling `model.predict()` for prediction

#### Scenario: model 參數型別
- **WHEN** 查看 predict_scores 函數簽名
- **THEN** model 參數型別 SHALL 為 ModelAdapter（非 lgb.Booster）

#### Scenario: Output is Spark DataFrame
- **WHEN** predict_scores completes
- **THEN** the output SHALL be a Spark DataFrame with columns [snap_date, cust_id, prod_code, score]

#### Scenario: Model version injection for partitioned output
- **WHEN** parameters contains a "model_version" key
- **THEN** the function adds a "model_version" column with the version string to the result DataFrame

### Requirement: Spark rank_predictions node
The system SHALL provide a function `rank_predictions(score_table: pyspark.sql.DataFrame, parameters: dict) -> pyspark.sql.DataFrame` in `nodes_spark.py` that ranks products using PySpark Window functions.

#### Scenario: Window-based ranking
- **WHEN** score_table is a Spark DataFrame
- **THEN** the function SHALL use `Window.partitionBy("snap_date", "cust_id").orderBy(F.desc("score"))` with `F.row_number()` to assign ranks

#### Scenario: Rank output format
- **WHEN** rank_predictions completes
- **THEN** the output SHALL contain columns [snap_date, cust_id, prod_code, score, rank]


## MODIFIED Requirements

### Requirement: Spark inference nodes use schema for column names
All Spark inference nodes SHALL obtain column names from `get_schema(parameters)`. Changes SHALL mirror the pandas backend modifications.

#### Scenario: Default column names
- **WHEN** called with parameters without `schema` section
- **THEN** behavior SHALL be identical to the current hard-coded implementation

#### Scenario: Identity columns in Spark predict_scores
- **WHEN** `schema.columns.entity` is `["branch_id", "cust_id"]`
- **THEN** identity columns preserved during scoring SHALL be `["snap_date", "branch_id", "cust_id", "prod_name"]`

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
