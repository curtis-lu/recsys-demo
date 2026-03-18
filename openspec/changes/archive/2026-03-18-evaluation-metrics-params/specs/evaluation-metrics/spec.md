## MODIFIED Requirements

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

#### Scenario: Excluded queries tracked
- **WHEN** some queries have no positive labels
- **THEN** result contains n_queries (total) and n_excluded_queries (no positives, excluded from AP/nDCG)
