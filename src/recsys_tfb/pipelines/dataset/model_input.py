"""Assembling a split's ``model_input``: the two joins, the output column rule,
and the column-existence guards that back them.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from recsys_tfb.core.logging import log_step
from recsys_tfb.core.schema import get_schema
from recsys_tfb.preprocessing import _cast_feature_floats_to_float32

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def _validate_columns(
    columns: list[str],
    required: list[str],
    context: str,
) -> None:
    """Check that all required columns exist. Raises ValueError if missing."""
    missing = set(required) - set(columns)
    if missing:
        raise ValueError(f"Missing columns in {context}: {sorted(missing)}")


def build_model_input(
    keys: DataFrame,
    preprocessed_feature_table: DataFrame,
    label_table: DataFrame,
    preprocessor_metadata: dict,
    parameters: dict,
) -> DataFrame:
    """Merge Spark keys + labels + pre-encoded features into model_input.

    Equivalent to (build_dataset + transform_to_model_input) but encoding is
    already applied to feature_table once, so splits share the work.
    """
    schema = get_schema(parameters)
    time_col = schema["time"]
    entity_cols = schema["entity"]
    item_col = schema["item"]
    label_col = schema["label"]
    identity_cols = schema["identity_columns"]
    base_key = [time_col] + entity_cols

    feature_columns = preprocessor_metadata["feature_columns"]

    # keys' grain IS model_input's grain (ADR-0005). This used to fall back to a
    # base-key-only label join when item was absent, which silently multiplied
    # every (time, entity) by label_table's item count and took `item`'s values
    # from label_table. No caller can reach that branch — identity_columns is
    # derived ([time] + entity + [item], core/schema.py) and both wrappers in
    # pipelines/dataset/nodes.py feed it, across all five pipeline nodes
    # they register — but its failure mode is a silently N-times-too-large
    # dataset, so the missing column is an error rather than a mode.
    label_join_key = base_key + [item_col]
    _validate_columns(keys.columns, label_join_key, "build_model_input keys")
    with log_step(logger, "merge_labels"):
        dataset = keys.join(label_table, on=label_join_key, how="left")
        # sample_pool is dense (cust × prod fully expanded); label_table is
        # sparse (only customers with category transactions). Join misses are
        # treated as negatives.
        dataset = dataset.withColumn(label_col, F.coalesce(F.col(label_col), F.lit(0)))
    with log_step(logger, "merge_features"):
        dataset = dataset.join(preprocessed_feature_table, on=base_key, how="left")

    with log_step(logger, "select_output_columns"):
        required = list(set(identity_cols + [label_col] + feature_columns))
        _validate_columns(dataset.columns, required, "build_model_input")

        carry_present = [
            c for c in keys.columns
            if c not in identity_cols and c not in feature_columns
            and c != label_col and c in dataset.columns
        ]
        output_cols = list(dict.fromkeys(
            identity_cols + [label_col] + feature_columns + carry_present
        ))
        result = dataset.select(*output_cols)

    with log_step(logger, "cast_features_to_float32"):
        result, casted = _cast_feature_floats_to_float32(result, feature_columns)
    logger.info(
        "build_model_input: %d features, cast %d float-like feature columns to float32",
        len(feature_columns), len(casted),
    )
    if casted:
        logger.debug("build_model_input: casted columns = %s", casted)
    return result
