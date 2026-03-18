## ADDED Requirements

### Requirement: Calibration curve plots
evaluation/calibration.py SHALL provide `plot_calibration_curves(predictions, labels, n_bins=10, title_prefix="")` that returns a Plotly Figure:
1. One trace per product showing predicted probability vs actual positive rate
2. A diagonal reference line representing perfect calibration
3. Uses sklearn.calibration.calibration_curve internally

#### Scenario: Well-calibrated model
- **WHEN** model predictions align with actual positive rates
- **THEN** calibration curve traces are close to the diagonal line

#### Scenario: Overconfident model
- **WHEN** model predicts high probabilities but actual positive rate is low
- **THEN** calibration curve falls below the diagonal

#### Scenario: All products on one figure
- **WHEN** predictions contain multiple products
- **THEN** all product calibration curves appear on a single figure with legend

#### Scenario: Insufficient data for a bin
- **WHEN** a product has too few samples for some bins
- **THEN** those bins are skipped without error (handled by sklearn)
