## ADDED Requirements

### Requirement: Single-query metric functions
evaluation/metrics.py SHALL provide the following pure functions, each operating on a single query's numpy arrays (y_true, y_score):

- `compute_ap(y_true, y_score) -> float | None` — Average Precision. Returns None if no positive labels.
- `compute_ndcg(y_true, y_score, k=None) -> float` — Normalized Discounted Cumulative Gain, optionally truncated at K.
- `compute_precision_at_k(y_true, y_score, k) -> float` — Precision at K.
- `compute_recall_at_k(y_true, y_score, k) -> float` — Recall at K.
- `compute_mrr(y_true, y_score) -> float` — Mean Reciprocal Rank. Returns 0 if no positive labels.

All functions SHALL sort by descending y_score before computing.

#### Scenario: AP with known values
- **WHEN** y_true=[1,0,1,0], y_score=[0.9,0.8,0.7,0.6]
- **THEN** compute_ap returns the correct AP value (precision at each positive position averaged)

#### Scenario: AP with no positives
- **WHEN** y_true=[0,0,0], y_score=[0.9,0.8,0.7]
- **THEN** compute_ap returns None

#### Scenario: nDCG perfect ranking
- **WHEN** all positive labels are ranked at the top positions
- **THEN** compute_ndcg returns 1.0

#### Scenario: nDCG at K
- **WHEN** k=3 is specified
- **THEN** compute_ndcg only considers the top 3 ranked items

#### Scenario: precision at K
- **WHEN** y_true=[1,0,1,0,0], y_score=[0.9,0.8,0.7,0.6,0.5], k=3
- **THEN** compute_precision_at_k returns 2/3

#### Scenario: recall at K
- **WHEN** y_true=[1,0,1,0,0], y_score=[0.9,0.8,0.7,0.6,0.5], k=3
- **THEN** compute_recall_at_k returns 2/2 = 1.0 (both positives found in top 3)

#### Scenario: MRR first positive at rank 3
- **WHEN** y_true=[0,0,1,0], y_score=[0.9,0.8,0.7,0.6]
- **THEN** compute_mrr returns 1/3

#### Scenario: MRR no positives
- **WHEN** y_true=[0,0,0], y_score=[0.9,0.8,0.7]
- **THEN** compute_mrr returns 0.0

### Requirement: Aggregate metrics computation
evaluation/metrics.py SHALL provide `compute_all_metrics(predictions, labels, k_values=[5, "all"])` that:
1. Joins predictions (snap_date, cust_id, prod_code, score, rank) with labels (snap_date, cust_id, prod_name, label) on (snap_date, cust_id) with prod_code=prod_name
2. Resolves `"all"` in k_values to the total number of unique products (N) in the merged data
3. Groups by (snap_date, cust_id) as query groups
4. Computes all 5 metrics for each query
5. Aggregates results into overall, per_product, per_segment (if segment column present), and per_product_segment dimensions
6. Computes macro average and micro average for each dimension

#### Scenario: Overall metrics with new defaults
- **WHEN** predictions and labels are provided with 22 products and k_values is not specified
- **THEN** result["overall"] contains keys: map, ndcg, ndcg@5, ndcg@22, precision@5, precision@22, recall@5, recall@22, mrr

#### Scenario: "all" resolved to product count
- **WHEN** k_values=[5, "all"] and merged data contains 22 unique products
- **THEN** "all" is resolved to 22, and metrics include @5 and @22 variants

#### Scenario: k_values with only integers
- **WHEN** k_values=[3, 10] (no "all")
- **THEN** metrics are computed for @3 and @10 only, no resolution needed

#### Scenario: Per-product metrics
- **WHEN** predictions contain products [fx, bond, stock]
- **THEN** result["per_product"] contains one entry per product, each with same metric keys as overall

#### Scenario: Macro average by product
- **WHEN** per_product metrics are computed for N products
- **THEN** result["macro_avg"]["by_product"] equals the unweighted mean of per_product metrics across all N products

#### Scenario: Micro average by product
- **WHEN** per_product queries have different counts (e.g., fx has 100 queries, bond has 50)
- **THEN** result["micro_avg"]["by_product"] equals the query-count-weighted average

#### Scenario: Per-segment metrics
- **WHEN** labels contain cust_segment_typ column with values [mass, affluent, hnw]
- **THEN** result["per_segment"] contains one entry per segment with all metrics

#### Scenario: Per-product-segment cross metrics
- **WHEN** both product and segment info are available
- **THEN** result["per_product_segment"] contains entries keyed by "{product}_{segment}" with all metrics

#### Scenario: Macro and micro average by segment
- **WHEN** per_segment metrics are computed
- **THEN** result["macro_avg"]["by_segment"] and result["micro_avg"]["by_segment"] are provided

#### Scenario: Excluded queries tracked
- **WHEN** some queries have no positive labels
- **THEN** result contains n_queries (total) and n_excluded_queries (no positives, excluded from AP/nDCG)

### Requirement: Query group definition
A query group SHALL be defined as all rows sharing the same (snap_date, cust_id). Within each query, items (products) are ranked by descending score.

#### Scenario: Multi-snap-date data
- **WHEN** predictions contain multiple snap_dates
- **THEN** each (snap_date, cust_id) pair is treated as a separate query


## MODIFIED Requirements

### Requirement: Metric computation uses schema for groupby keys
`compute_all_metrics()` and related functions SHALL accept schema-driven column names for groupby operations (time + entity columns for per-query grouping, item column for per-product metrics).

#### Scenario: Default groupby keys
- **WHEN** called without schema overrides
- **THEN** SHALL group by `["snap_date", "cust_id"]` for per-query metrics (identical to current behavior)

#### Scenario: Custom entity columns in metric groupby
- **WHEN** schema specifies `entity: ["branch_id", "cust_id"]`
- **THEN** SHALL group by `["snap_date", "branch_id", "cust_id"]` for per-query metrics
