## MODIFIED Requirements

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
