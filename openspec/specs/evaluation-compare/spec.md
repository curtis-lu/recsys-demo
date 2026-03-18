## ADDED Requirements

### Requirement: Comparison result computation
evaluation/compare.py SHALL provide `build_comparison_result(result_a, result_b, label_a, label_b)` that:
1. Computes delta (a - b) for all metrics at all levels (overall, per_product, per_segment, macro/micro avg)
2. Returns a dict containing both original results, labels, and all deltas

#### Scenario: Positive delta means A is better
- **WHEN** model A has mAP=0.5 and model B has mAP=0.3
- **THEN** overall_delta["map"] = 0.2

#### Scenario: Identical models
- **WHEN** two identical model versions are compared
- **THEN** all deltas are 0.0

#### Scenario: Delta at all levels
- **WHEN** comparison is computed
- **THEN** result contains overall_delta, per_product_delta, per_segment_delta, macro_avg_delta, micro_avg_delta

### Requirement: Comparison visualizations
evaluation/compare.py SHALL provide:
- `plot_comparison_metrics(comparison) -> list[Figure]` — side-by-side bar charts for each metric, with products on x-axis and two bars (A vs B) per product
- `plot_comparison_score_distributions(predictions_a, predictions_b, label_a, label_b) -> list[Figure]` — overlay histograms and side-by-side boxplots

#### Scenario: Side-by-side bar chart
- **WHEN** comparing two models across 5 products
- **THEN** each metric chart shows 5 product groups, each with 2 bars (model A in blue, model B in orange)

#### Scenario: Overlay score histograms
- **WHEN** comparing score distributions
- **THEN** histogram shows both models' distributions overlaid with transparency for each product
