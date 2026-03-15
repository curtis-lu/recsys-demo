"""Pure functions for the inference pipeline."""

import logging

import lightgbm as lgb
import pandas as pd

logger = logging.getLogger(__name__)


def build_scoring_dataset(
    feature_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Build scoring dataset by cross-joining customers with all products."""
    snap_dates = [pd.Timestamp(d) for d in parameters["inference"]["snap_dates"]]
    products = parameters["inference"]["products"]

    # Filter to target snap_dates and get unique (snap_date, cust_id)
    mask = feature_table["snap_date"].isin(snap_dates)
    customers = feature_table.loc[mask, ["snap_date", "cust_id"]].drop_duplicates()

    # Cross-join with products
    products_df = pd.DataFrame({"prod_name": products})
    customers["_key"] = 1
    products_df["_key"] = 1
    scoring = customers.merge(products_df, on="_key").drop(columns="_key")

    # Left-join features
    scoring = scoring.merge(feature_table, on=["snap_date", "cust_id"], how="left")

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
    model: lgb.Booster,
    X_score: pd.DataFrame,
    scoring_dataset: pd.DataFrame,
) -> pd.DataFrame:
    """Predict probability scores for each customer-product pair."""
    scores = model.predict(X_score)

    score_table = pd.DataFrame({
        "snap_date": scoring_dataset["snap_date"].values,
        "cust_id": scoring_dataset["cust_id"].values,
        "prod_code": scoring_dataset["prod_name"].values,
        "score": scores,
    })

    logger.info("Predicted %d scores, mean=%.4f", len(score_table), score_table["score"].mean())
    return score_table


def rank_predictions(
    score_table: pd.DataFrame,
    parameters: dict,
) -> pd.DataFrame:
    """Rank products by score within each (snap_date, cust_id) group."""
    ranked = score_table.copy()
    ranked["rank"] = (
        ranked.groupby(["snap_date", "cust_id"])["score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    logger.info(
        "Ranked predictions: %d rows, %d groups",
        len(ranked),
        ranked.groupby(["snap_date", "cust_id"]).ngroups,
    )
    return ranked
