## ADDED Requirements

### Requirement: Per-product dataset statistics

evaluation/statistics.py SHALL provide `compute_product_statistics(labels: pd.DataFrame) -> pd.DataFrame` that computes per-product label statistics.

The returned DataFrame SHALL be indexed by `prod_name` with columns:

- `positive_count`: number of rows with label=1 for that product
- `negative_count`: number of rows with label=0 for that product
- `positive_rate`: positive_count / (positive_count + negative_count)
- `unique_customers`: number of distinct cust_id values for that product
- `avg_positive_products_per_customer`: global average — across all customers, mean number of products with label=1 per customer

#### Scenario: Known counts

- **WHEN** labels contain product "A" with 10 positive and 90 negative rows
- **THEN** row "A" has positive_count=10, negative_count=90, positive_rate=0.1

#### Scenario: Multiple products

- **WHEN** labels contain 3 products
- **THEN** returned DataFrame has 3 rows, one per product

#### Scenario: avg_positive_products_per_customer is global

- **WHEN** customer C0 has 2 positive products and customer C1 has 4 positive products
- **THEN** avg_positive_products_per_customer = 3.0 for all product rows

### Requirement: Per-segment dataset statistics

evaluation/statistics.py SHALL provide `compute_segment_statistics(labels: pd.DataFrame, segment_column: str = "cust_segment_typ") -> pd.DataFrame` that computes per-segment label statistics.

The returned DataFrame SHALL be indexed by segment value with the same columns as per-product statistics, except `avg_positive_products_per_customer` is computed within each segment's customers.

#### Scenario: Two segments

- **WHEN** labels contain segment_column with values ["mass", "hnw"]
- **THEN** returned DataFrame has 2 rows with correct statistics per segment

#### Scenario: Segment-scoped average

- **WHEN** segment "mass" has 10 customers averaging 2.5 positive products each
- **THEN** row "mass" has avg_positive_products_per_customer=2.5

#### Scenario: Missing segment column

- **WHEN** labels DataFrame does not contain the specified segment_column
- **THEN** function returns an empty DataFrame

