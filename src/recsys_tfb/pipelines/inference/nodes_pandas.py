"""Pure functions for the inference pipeline."""

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.base import ModelAdapter
from recsys_tfb.pipelines.inference.validation import ValidationError

logger = logging.getLogger(__name__)


def build_scoring_dataset(
    feature_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Build scoring dataset by cross-joining customers with all products."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    join_key = [time_col] + entity_cols

    snap_dates = [pd.Timestamp(d) for d in parameters["inference"]["snap_dates"]]
    products = parameters["inference"]["products"]

    # Filter to target snap_dates and get unique identity keys
    mask = feature_table[time_col].isin(snap_dates)
    customers = feature_table.loc[mask, join_key].drop_duplicates()

    # Cross-join with products
    products_df = pd.DataFrame({item_col: products})
    customers["_key"] = 1
    products_df["_key"] = 1
    scoring = customers.merge(products_df, on="_key").drop(columns="_key")

    # Left-join features
    scoring = scoring.merge(feature_table, on=join_key, how="left")

    logger.info(
        "Scoring dataset: %d rows (%d customers x %d products x %d snap_dates)",
        len(scoring),
        len(customers),
        len(products),
        len(snap_dates),
    )
    return scoring


def apply_preprocessor(
    scoring_dataset: pd.DataFrame,
    preprocessor: dict,
    parameters: dict,
) -> pd.DataFrame:
    """Apply training preprocessor to scoring dataset."""
    drop_cols = preprocessor["drop_columns"]
    category_mappings = preprocessor["category_mappings"]
    categorical_cols = preprocessor["categorical_columns"]
    feature_columns = preprocessor["feature_columns"]

    result = scoring_dataset.drop(columns=drop_cols, errors="ignore").copy()

    for col in categorical_cols:
        known = category_mappings[col]
        result[col] = pd.Categorical(result[col], categories=known).codes

    # Validate all expected features are present
    missing = set(feature_columns) - set(result.columns)
    if missing:
        raise ValueError(f"Missing feature columns in scoring dataset: {sorted(missing)}")

    # Ensure column order matches training
    result = result[feature_columns]

    logger.info("Preprocessed scoring data: %s", result.shape)
    return result


def predict_scores(
    model: ModelAdapter,
    X_score: pd.DataFrame,
    scoring_dataset: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Predict probability scores for each customer-product pair."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    score_col = schema["score"]
    identity_cols = schema["identity_columns"]

    scores = model.predict(X_score)

    score_table = pd.DataFrame({
        col: scoring_dataset[col].values for col in identity_cols
    })
    score_table[score_col] = scores

    logger.info("Predicted %d scores, mean=%.4f", len(score_table), score_table[score_col].mean())
    return score_table


def rank_predictions(
    score_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Rank products by score within each query group."""
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    group_cols = [time_col] + entity_cols

    ranked = score_table.copy()
    ranked[rank_col] = (
        ranked.groupby(group_cols)[score_col]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    logger.info(
        "Ranked predictions: %d rows, %d groups",
        len(ranked),
        ranked.groupby(group_cols).ngroups,
    )
    return ranked


def validate_predictions(
    ranked_predictions: pd.DataFrame,
    scoring_dataset: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Validate inference output with sanity checks. Raises ValidationError on failure."""
    schema = get_schema(parameters)
    identity_cols = schema["identity_columns"]
    time_col = schema["time"]
    entity_cols = schema["entity"]
    score_col = schema["score"]
    rank_col = schema["rank"]
    products = parameters["inference"]["products"]
    n_products = len(products)
    group_cols = [time_col] + entity_cols

    failures = []

    # 1. row_count_match
    n_ranked = len(ranked_predictions)
    n_scoring = len(scoring_dataset)
    if n_ranked != n_scoring:
        failures.append({
            "check": "row_count_match",
            "detail": f"ranked_predictions has {n_ranked} rows, scoring_dataset has {n_scoring} rows",
        })

    # 2. score_range
    scores = ranked_predictions[score_col]
    out_of_range = ((scores < 0.0) | (scores > 1.0)).sum()
    if out_of_range > 0:
        failures.append({
            "check": "score_range",
            "detail": f"{out_of_range} scores outside [0, 1], min={scores.min():.6f}, max={scores.max():.6f}",
        })

    # 3. no_missing
    check_cols = identity_cols + [score_col, rank_col]
    missing_counts = ranked_predictions[check_cols].isnull().sum()
    cols_with_missing = missing_counts[missing_counts > 0]
    if len(cols_with_missing) > 0:
        failures.append({
            "check": "no_missing",
            "detail": f"NaN values found: {cols_with_missing.to_dict()}",
        })

    # 4. completeness
    group_sizes = ranked_predictions.groupby(group_cols).size()
    incomplete = group_sizes[group_sizes != n_products]
    if len(incomplete) > 0:
        failures.append({
            "check": "completeness",
            "detail": (
                f"{len(incomplete)} groups do not have exactly {n_products} products, "
                f"sizes: min={group_sizes.min()}, max={group_sizes.max()}"
            ),
        })

    # 5. rank_consistency
    def _check_rank_consistency(group: pd.DataFrame) -> bool:
        expected_ranks = set(range(1, n_products + 1))
        actual_ranks = set(group[rank_col].values)
        if actual_ranks != expected_ranks:
            return False
        sorted_by_rank = group.sort_values(rank_col)
        scores_sorted = sorted_by_rank[score_col].values
        return bool(np.all(scores_sorted[:-1] >= scores_sorted[1:]))

    rank_checks = ranked_predictions.groupby(group_cols).apply(_check_rank_consistency)
    inconsistent = rank_checks[~rank_checks]
    if len(inconsistent) > 0:
        failures.append({
            "check": "rank_consistency",
            "detail": f"{len(inconsistent)} groups have inconsistent ranks",
        })

    # 6. no_duplicates
    n_dupes = ranked_predictions.duplicated(subset=identity_cols).sum()
    if n_dupes > 0:
        failures.append({
            "check": "no_duplicates",
            "detail": f"{n_dupes} duplicate rows on {identity_cols}",
        })

    if failures:
        logger.error("Validation failed: %s", failures)
        raise ValidationError(failures)

    logger.info("All %d sanity checks passed (%d rows)", 6, n_ranked)
    return ranked_predictions
