## MODIFIED Requirements

### Requirement: Metric computation delegation
training/nodes.py SHALL import `compute_ap` and `compute_map` (previously `_compute_ap` and `_compute_map`) from `recsys_tfb.evaluation.metrics` instead of defining them locally.

The `evaluate_model` function's behavior SHALL remain unchanged — same inputs, same outputs, same metric values.

#### Scenario: Backward compatible evaluation
- **WHEN** training pipeline runs evaluate_model node
- **THEN** output dict has same structure: overall_map, per_product_ap, n_queries, n_excluded_queries

#### Scenario: Identical metric values
- **WHEN** same data is evaluated before and after refactoring
- **THEN** all metric values are numerically identical

#### Scenario: Import from evaluation module
- **WHEN** training/nodes.py is inspected
- **THEN** it contains `from recsys_tfb.evaluation.metrics import compute_ap` (no local `_compute_ap` definition)
