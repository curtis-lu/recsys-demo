## MODIFIED Requirements

### Requirement: Generate synthetic label table
The system SHALL provide a script or function that generates a synthetic `label_table.parquet` file with the following columns: `snap_date` (date), `cust_id` (string), `cust_segment_typ` (string), `apply_start_date` (date), `apply_end_date` (date), `label` (int 0/1), `prod_name` (string).

#### Scenario: Label table schema matches ETL output
- **WHEN** synthetic label table is generated
- **THEN** it SHALL contain exactly the columns listed above with correct dtypes, including cust_segment_typ

#### Scenario: cust_segment_typ has multiple values
- **WHEN** synthetic label table is generated
- **THEN** cust_segment_typ SHALL contain at least 3 distinct segment values (e.g., mass, affluent, hnw)

#### Scenario: cust_segment_typ is deterministic per customer
- **WHEN** synthetic label table is generated
- **THEN** each cust_id SHALL have the same cust_segment_typ value across all snap_dates and prod_names

#### Scenario: Label table covers all products
- **WHEN** synthetic label table is generated
- **THEN** prod_name SHALL include at least: fx, usd, stock, bond, mix

#### Scenario: Cross join structure
- **WHEN** synthetic label table is generated for a given snap_date
- **THEN** every cust_id SHALL have one row per prod_name (cross join pattern matching SQL ETL)

#### Scenario: Realistic label distribution
- **WHEN** synthetic label table is generated
- **THEN** label=1 SHALL represent approximately 5-15% of all rows (sparse positive labels)
