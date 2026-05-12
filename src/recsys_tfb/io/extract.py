"""Convert a ParquetHandle into algorithm-agnostic numpy (X, y) arrays.

Encapsulates deferred categorical encoding (e.g. prod_name) that the dataset
pipeline keeps as raw string values; downstream training code expects fully
numeric numpy arrays.

Moved out of pipelines/training/nodes.py so that ModelAdapter implementations
(e.g. LightGBMAdapter.prepare_train_inputs) can reuse it without circular
imports.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle

logger = logging.getLogger(__name__)


def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.

    Emits sub-step ``log_step`` events (``read_parquet`` → ``slice_features`` →
    ``encode_categoricals`` (skipped when no deferred cats) → ``to_numpy``) and
    per-step INFO size summaries so OOM-killed runs can be diagnosed from log.
    """
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]

    logger.info(
        "extract_Xy start path=%s n_feature_cols=%d label=%s identity_cols=%s",
        getattr(handle, "path", "<unknown>"),
        len(feature_cols),
        label_col,
        identity_cols,
    )

    with log_step(logger, "read_parquet"):
        pdf = handle.to_pandas()
    logger.info(
        "extract_Xy: parquet loaded rows=%d cols=%d",
        len(pdf), len(pdf.columns),
    )

    with log_step(logger, "slice_features"):
        X_df = pdf[feature_cols].copy()
    logger.info(
        "extract_Xy: X_df rows=%d n_features=%d mem=%.1fMB",
        len(X_df), X_df.shape[1],
        X_df.memory_usage(deep=False).sum() / 1024**2,
    )

    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    if deferred_cats:
        with log_step(logger, "encode_categoricals"):
            for col in deferred_cats:
                known = category_mappings[col]
                X_df[col] = pd.Categorical(X_df[col], categories=known).codes
        logger.info(
            "extract_Xy: encoded deferred_cats=%s count=%d",
            deferred_cats, len(deferred_cats),
        )

    with log_step(logger, "to_numpy"):
        X = X_df.values
        y = pdf[label_col].values
    logger.info(
        "extract_Xy: X shape=%s dtype=%s nbytes=%.1fMB; y len=%d dtype=%s",
        X.shape, X.dtype, X.nbytes / 1024**2,
        len(y), y.dtype,
    )

    return X, y
