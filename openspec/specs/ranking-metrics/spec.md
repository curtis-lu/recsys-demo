## MODIFIED Requirements

### Requirement: Single-query metric functions

evaluation/metrics.py SHALL provide the following pure functions, each operating on a single query's numpy arrays (y_true, y_score):

- `compute_ap(y_true, y_score) -> float | None` — Average Precision (full list). Returns None if no positive labels.
- `compute_ap_at_k(y_true, y_score, k) -> float | None` — Average Precision considering only the top K items. Returns None if no positive labels.
- `compute_ndcg(y_true, y_score, k=None) -> float` — Normalized Discounted Cumulative Gain, optionally truncated at K.
- `compute_precision_at_k(y_true, y_score, k) -> float` — Precision at K.
- `compute_recall_at_k(y_true, y_score, k) -> float` — Recall at K.
- `compute_mrr(y_true, y_score) -> float` — Mean Reciprocal Rank (full list). Returns 0 if no positive labels.
- `compute_mrr_at_k(y_true, y_score, k) -> float` — Mean Reciprocal Rank considering only top K. Returns 0 if first positive rank > K.

All functions SHALL sort by descending y_score before computing.

#### Scenario: AP@K with positives beyond K

- **WHEN** y_true=[0,0,1], y_score=[0.9,0.8,0.7], k=2
- **THEN** compute_ap_at_k returns 0.0 (no positives in top 2)

#### Scenario: AP@K with positives within K

- **WHEN** y_true=[1,0,1,0], y_score=[0.9,0.8,0.7,0.6], k=3
- **THEN** compute_ap_at_k considers top 3 items [1,0,1] and returns AP over those

#### Scenario: MRR@K hit

- **WHEN** y_true=[0,1,0], y_score=[0.9,0.8,0.7], k=3
- **THEN** compute_mrr_at_k returns 1/2

#### Scenario: MRR@K miss

- **WHEN** y_true=[0,0,1], y_score=[0.9,0.8,0.7], k=2
- **THEN** compute_mrr_at_k returns 0.0

### Requirement: Aggregate metric computation

evaluation/metrics.py SHALL provide `compute_all_metrics(predictions, labels, k_values=[5, "all"])` that computes ranking metrics across multiple dimensions.

All metrics in the returned dict SHALL use @K suffix format. For each K in the resolved k_values list, the following metrics SHALL be computed:

- `map@{K}`, `ndcg@{K}`, `mrr@{K}`, `precision@{K}`, `recall@{K}`

There SHALL be no metrics without @K suffix (i.e., no bare `map`, `ndcg`, or `mrr` keys).

The return dict structure SHALL remain: overall, per_product, per_segment, per_product_segment, macro_avg, micro_avg, n_queries, n_excluded_queries.

#### Scenario: Default k_values with 3 products

- **WHEN** k_values=[5, "all"] and there are 3 unique products
- **THEN** overall contains keys: map@3, map@5, ndcg@3, ndcg@5, mrr@3, mrr@5, precision@3, precision@5, recall@3, recall@5
- **THEN** overall does NOT contain keys: map, ndcg, mrr

#### Scenario: Single K value

- **WHEN** k_values=[5] and there are 10 products
- **THEN** overall contains exactly: map@5, ndcg@5, mrr@5, precision@5, recall@5

#### Scenario: Per-product metrics use same @K keys

- **WHEN** k_values=[5, "all"] resolved to [5, 22]
- **THEN** each entry in per_product has keys map@5, map@22, ndcg@5, ndcg@22, mrr@5, mrr@22, precision@5, precision@22, recall@5, recall@22

#### Scenario: k_values "all" resolves to product count

- **WHEN** k_values=[5, "all"] and merged data contains 22 unique products
- **THEN** "all" is resolved to 22, and metrics include @5 and @22 variants

