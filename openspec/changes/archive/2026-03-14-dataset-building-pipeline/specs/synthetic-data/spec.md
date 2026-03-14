## ADDED Requirements

### Requirement: Generate synthetic feature table
The system SHALL provide a script or function that generates a synthetic `feature_table.parquet` file with the following columns: `snap_date` (date), `cust_id` (string), `total_aum` (float), `fund_aum` (float), `in_amt_sum_l1m` (float), `out_amt_sum_l1m` (float), `in_amt_ratio_l1m` (float), `out_amt_ratio_l1m` (float).

#### Scenario: Feature table schema matches ETL output
- **WHEN** synthetic feature table is generated
- **THEN** it SHALL contain exactly the columns listed above with correct dtypes

#### Scenario: Multiple snap_dates
- **WHEN** synthetic data is generated with 3 snap_dates
- **THEN** each snap_date SHALL have at least 100 unique cust_ids

#### Scenario: Deterministic output
- **WHEN** synthetic data is generated twice with the same random seed
- **THEN** both outputs SHALL be identical

### Requirement: Generate synthetic label table
The system SHALL provide a script or function that generates a synthetic `label_table.parquet` file with the following columns: `snap_date` (date), `cust_id` (string), `apply_start_date` (date), `apply_end_date` (date), `label` (int 0/1), `prod_name` (string).

#### Scenario: Label table schema matches ETL output
- **WHEN** synthetic label table is generated
- **THEN** it SHALL contain exactly the columns listed above with correct dtypes

#### Scenario: Label table covers all products
- **WHEN** synthetic label table is generated
- **THEN** prod_name SHALL include at least: fx, usd, stock, bond, mix

#### Scenario: Cross join structure
- **WHEN** synthetic label table is generated for a given snap_date
- **THEN** every cust_id SHALL have one row per prod_name (cross join pattern matching SQL ETL)

#### Scenario: Realistic label distribution
- **WHEN** synthetic label table is generated
- **THEN** label=1 SHALL represent approximately 5-15% of all rows (sparse positive labels)
