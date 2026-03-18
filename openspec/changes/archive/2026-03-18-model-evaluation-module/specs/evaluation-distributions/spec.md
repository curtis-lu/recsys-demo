## ADDED Requirements

### Requirement: Score distribution plots
evaluation/distributions.py SHALL provide `plot_score_distributions(predictions, title_prefix="")` that returns a list of Plotly Figure objects:
1. A histogram figure with one trace per product showing score distribution
2. A boxplot figure with one box per product

Both figures SHALL allow interactive toggling of individual product traces.

#### Scenario: All products shown
- **WHEN** predictions contain 5 products
- **THEN** histogram has 5 traces and boxplot has 5 boxes

#### Scenario: Score range validation
- **WHEN** scores are probabilities in [0, 1]
- **THEN** histogram x-axis ranges from 0 to 1

### Requirement: Rank distribution heatmap
evaluation/distributions.py SHALL provide `plot_rank_heatmap(predictions, title_prefix="")` that returns a Plotly Figure:
1. Heatmap with rows = products, columns = rank positions (1 to N_products)
2. Cell values = count of how many times each product appears at each rank position
3. Color intensity proportional to count

#### Scenario: Uniform ranking
- **WHEN** model assigns roughly equal probability to all products
- **THEN** heatmap shows relatively uniform values across rank positions for each product

#### Scenario: Biased ranking
- **WHEN** model always ranks product "fx" as rank 1
- **THEN** heatmap shows high value at (fx, rank=1) and zero at (fx, rank>1)

#### Scenario: Row sums equal total queries
- **WHEN** heatmap is generated for N customers
- **THEN** each row sums to N (each product appears exactly once per customer)
