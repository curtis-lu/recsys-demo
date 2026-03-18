## ADDED Requirements

### Requirement: Global popularity baseline
evaluation/baselines.py SHALL provide `generate_global_popularity_baseline(label_table, snap_date, customer_ids)` that:
1. Computes overall positive rate per product from label_table WHERE snap_date < target snap_date
2. Uses these rates as scores for every customer
3. Ranks products by descending rate (same ranking for all customers)
4. Returns DataFrame with columns: snap_date, cust_id, prod_code, score, rank (same schema as ranked_predictions)

#### Scenario: Same ranking for all customers
- **WHEN** global baseline is generated for 100 customers
- **THEN** all 100 customers have identical product ranking order

#### Scenario: Scores match positive rates
- **WHEN** product "fx" has 15% positive rate globally
- **THEN** every customer's "fx" score is 0.15

#### Scenario: Leakage prevention
- **WHEN** target snap_date is 2024-03-31
- **THEN** only label_table rows with snap_date < 2024-03-31 are used to compute rates

#### Scenario: No historical data
- **WHEN** target snap_date is the earliest in label_table (no prior data)
- **THEN** all available data is used with a logged warning about potential leakage

### Requirement: Segment popularity baseline
evaluation/baselines.py SHALL provide `generate_segment_popularity_baseline(label_table, snap_date, customer_ids, segment_column="cust_segment_typ")` that:
1. Computes positive rate per (segment, product) from historical label_table
2. Assigns segment-specific scores to each customer based on their segment
3. Ranks products within each customer by descending segment-specific rate
4. Returns DataFrame with same schema as ranked_predictions

#### Scenario: Different rankings per segment
- **WHEN** "mass" segment has fx as most popular and "hnw" has bond as most popular
- **THEN** mass customers have fx at rank 1 while hnw customers have bond at rank 1

#### Scenario: Output schema matches ranked_predictions
- **WHEN** baseline is generated
- **THEN** output has exactly columns: snap_date, cust_id, prod_code, score, rank
