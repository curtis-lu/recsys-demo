## MODIFIED Requirements

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
