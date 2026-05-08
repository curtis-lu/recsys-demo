"""Convert a ParquetHandle into algorithm-agnostic numpy (X, y) arrays.

Encapsulates deferred categorical encoding (e.g. prod_name) that the dataset
pipeline keeps as raw string values; downstream training code expects fully
numeric numpy arrays.

Moved out of pipelines/training/nodes.py so that ModelAdapter implementations
(e.g. LightGBMAdapter.prepare_train_inputs) can reuse it without circular
imports.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from recsys_tfb.core.schema import get_schema
from recsys_tfb.io.handles import ParquetHandle


def extract_Xy(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Read the parquet at ``handle.path`` and return (X, y) as numpy arrays.

    Categorical identity columns (e.g. prod_name) are int-coded via the
    preprocessor's ``category_mappings``.
    """
    pdf = handle.to_pandas()
    feature_cols = preprocessor_metadata["feature_columns"]
    schema = get_schema(parameters)
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]

    X_df = pdf[feature_cols].copy()

    categorical_cols = preprocessor_metadata["categorical_columns"]
    category_mappings = preprocessor_metadata["category_mappings"]
    deferred_cats = [
        c for c in categorical_cols if c in identity_cols and c in X_df.columns
    ]
    for col in deferred_cats:
        known = category_mappings[col]
        X_df[col] = pd.Categorical(X_df[col], categories=known).codes

    X = X_df.values
    y = pdf[label_col].values
    return X, y
