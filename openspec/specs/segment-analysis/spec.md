## MODIFIED Requirements

### Requirement: Segment metrics table builder
evaluation/segments.py SHALL provide `build_segment_metrics_table(segment_metrics: dict) -> pd.DataFrame` that converts the output of `compute_segment_metrics` into a table.

The returned DataFrame SHALL have:
- Index: segment values (sorted)
- Columns: metric names from the "overall" dict of each segment's metrics result

#### Scenario: Three segments
- **WHEN** segment_metrics has keys ["mass", "affluent", "hnw"], each with "overall" containing map@5, ndcg@5, etc.
- **THEN** returned DataFrame has 3 rows indexed by segment, columns matching metric keys

#### Scenario: Empty metrics
- **WHEN** segment_metrics is an empty dict
- **THEN** returned DataFrame is empty

### Requirement: Segment charts
evaluation/segments.py SHALL retain `plot_segment_charts(segment_metrics, title_prefix="")` for backward compatibility, but `_run_analysis` in evaluate_model.py SHALL use `build_segment_metrics_table` instead of `plot_segment_charts` for report generation.

The Segment Analysis section in the evaluation report SHALL present metrics as a table (similar to Per-Product Metrics), not as bar charts.

#### Scenario: Report uses table not charts
- **WHEN** _run_analysis builds the Segment Analysis section
- **THEN** the section contains tables (from build_segment_metrics_table) and no figures from plot_segment_charts
