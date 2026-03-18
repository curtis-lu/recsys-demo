## ADDED Requirements

### Requirement: Segment-level metrics
evaluation/segments.py SHALL provide `compute_segment_metrics(predictions, labels, segment_column="cust_segment_typ", k_values=[3,5])` that computes all ranking metrics grouped by segment.

Returns a dict keyed by segment value, each containing overall and per_product metrics.

#### Scenario: Three segments
- **WHEN** labels contain cust_segment_typ with values [mass, affluent, hnw]
- **THEN** result has three keys, each with complete metrics (map, ndcg, precision@K, recall@K, mrr)

#### Scenario: Segment with no positive labels
- **WHEN** a segment has zero positive labels across all queries
- **THEN** metrics for that segment report 0.0 and the segment is included with n_excluded_queries noted

### Requirement: Product holding combo metrics
evaluation/segments.py SHALL provide `compute_holding_combo_metrics(predictions, labels, k_values=[3,5], top_n=10)` that:
1. Derives each customer's product holding set from label_table where label=1 (historical positive labels)
2. Creates a segment label per customer based on their set of held products (e.g., "fx,bond")
3. Only keeps the top N most frequent combos; others grouped as "其他"
4. Computes metrics per holding-combo segment

#### Scenario: Customer with multiple products
- **WHEN** customer C001 has label=1 for fx and bond in historical data
- **THEN** C001 is assigned to holding combo "bond,fx" (sorted alphabetically)

#### Scenario: Top N filtering
- **WHEN** top_n=10 and there are 50 unique combos
- **THEN** only 10 most frequent combos are named; remaining queries are grouped under "其他"

### Requirement: Segment charts
evaluation/segments.py SHALL provide `plot_segment_charts(segment_metrics, title_prefix="")` and `plot_holding_combo_charts(combo_metrics, title_prefix="")` that return lists of Plotly Figure objects:
1. Grouped bar charts: segment on x-axis, metric value on y-axis
2. One chart per metric (map, ndcg, precision@K, recall@K, mrr)

#### Scenario: Grouped bar chart structure
- **WHEN** segment_metrics has 3 segments and per_product metrics
- **THEN** each bar chart shows segments on x-axis with product-colored bars grouped together
