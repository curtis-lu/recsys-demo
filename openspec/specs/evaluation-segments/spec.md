### Requirement: Segment-level metrics
evaluation/segments.py SHALL provide `compute_segment_metrics(predictions, labels, segment_column="cust_segment_typ", k_values=[5, "all"])` that computes all ranking metrics grouped by segment.

Returns a dict keyed by segment value, each containing overall and per_product metrics.

#### Scenario: Three segments
- **WHEN** labels contain cust_segment_typ with values [mass, affluent, hnw]
- **THEN** result has three keys, each with complete metrics (map, ndcg, precision@K, recall@K, mrr)

#### Scenario: Segment with no positive labels
- **WHEN** a segment has zero positive labels across all queries
- **THEN** metrics for that segment report 0.0 and the segment is included with n_excluded_queries noted

#### Scenario: External segment column (holding_combo)
- **WHEN** labels have a `holding_combo` column joined from external source
- **THEN** `compute_segment_metrics(predictions, labels, segment_column="holding_combo")` returns metrics per holding combo value

### Requirement: External segment source loading
evaluation/segments.py SHALL provide `load_and_join_segment_sources(labels, segment_sources)` that:
1. Iterates over `segment_sources` dict (keyed by segment name)
2. For each source, reads the Parquet file at `filepath`
3. Left-joins the segment column to labels on the specified `key_columns`
4. Returns the enriched labels DataFrame with all external segment columns added

#### Scenario: Single external source
- **WHEN** segment_sources has one entry `holding_combo` with filepath pointing to a valid Parquet
- **THEN** returned labels DataFrame has a `holding_combo` column populated from the external file

#### Scenario: Multiple external sources
- **WHEN** segment_sources has two entries (`holding_combo` and `risk_level`)
- **THEN** returned labels DataFrame has both `holding_combo` and `risk_level` columns

#### Scenario: Missing external file
- **WHEN** segment_sources specifies a filepath that does not exist
- **THEN** a warning is logged and that segment source is skipped (analysis continues without it)

#### Scenario: Partial join coverage
- **WHEN** external Parquet has holding_combo for only 80% of customers in labels
- **THEN** unmatched customers have NaN in the holding_combo column and are excluded from that segment's analysis

### Requirement: Segment charts
evaluation/segments.py SHALL provide `plot_segment_charts(segment_metrics, title_prefix="")` that returns lists of Plotly Figure objects:
1. Grouped bar charts: segment on x-axis, metric value on y-axis
2. One chart per metric (map, ndcg, precision@K, recall@K, mrr)

#### Scenario: Grouped bar chart structure
- **WHEN** segment_metrics has 3 segments and per_product metrics
- **THEN** each bar chart shows segments on x-axis with product-colored bars grouped together
