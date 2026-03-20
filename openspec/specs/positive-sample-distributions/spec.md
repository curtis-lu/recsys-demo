## ADDED Requirements

### Requirement: Positive sample rank count heatmap

evaluation/distributions.py SHALL provide `plot_positive_rank_heatmap(predictions, labels, title_prefix="") -> go.Figure` that plots a heatmap counting only positive-label samples at each (product, rank) position.

The function SHALL merge predictions with labels on [snap_date, cust_id, prod_name], filter to label=1, then produce the same heatmap format as `plot_rank_heatmap` (rows=products, columns=rank positions, colorscale=Blues).

#### Scenario: Only counts positives

- **WHEN** predictions have 100 rows but only 20 have label=1
- **THEN** total count across all heatmap cells equals 20

#### Scenario: Product with no positives

- **WHEN** product "X" has zero positive labels across all customers
- **THEN** product "X" row shows all zeros in the heatmap

#### Scenario: Same dimensions as full heatmap

- **WHEN** there are 5 products
- **THEN** heatmap has shape (5, 5) — same as plot_rank_heatmap

### Requirement: Positive rate rank heatmap

evaluation/distributions.py SHALL provide `plot_positive_rate_rank_heatmap(predictions, labels, title_prefix="") -> go.Figure` that plots a heatmap showing the positive label rate at each (product, rank) position.

Each cell value SHALL equal count(label=1) / count(total) at that (product, rank). Colorscale SHALL be RdYlGn. Text SHALL display percentage format.

Division by zero (no samples at a position) SHALL result in 0.0.

#### Scenario: Values between 0 and 1

- **WHEN** heatmap is computed
- **THEN** all cell values are in range [0.0, 1.0]

#### Scenario: Known rate

- **WHEN** product "A" at rank 1 has 8 out of 10 customers with label=1
- **THEN** cell (A, rank 1) = 0.8

#### Scenario: Empty position

- **WHEN** no customer has product "B" at rank 3
- **THEN** cell (B, rank 3) = 0.0

### Requirement: Score distributions by label

evaluation/distributions.py SHALL provide `plot_score_distributions_by_label(predictions, labels, title_prefix="") -> list[go.Figure]` that returns a list containing one grouped boxplot figure.

The boxplot SHALL show:

- x-axis: product name
- y-axis: prediction score
- color: label (Positive in green, Negative in grey)
- boxmode: "group"

The function SHALL merge predictions with labels on [snap_date, cust_id, prod_name].

#### Scenario: Both labels present

- **WHEN** predictions are merged with labels containing both 0 and 1
- **THEN** the figure has exactly 2 traces: one for Positive and one for Negative

#### Scenario: All products shown

- **WHEN** there are 5 products
- **THEN** each trace covers all 5 products on the x-axis

