"""Backend-agnostic helpers for preprocessing."""

from __future__ import annotations

import logging

from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def _get_preprocessing_config(parameters: dict) -> tuple[list[str], list[str]]:
    """Extract drop_columns and categorical_columns from parameters.

    Returns:
        (drop_columns, categorical_columns)
    """
    schema = get_schema(parameters)
    pmi_config = parameters.get("dataset", {}).get("prepare_model_input", {})

    drop_cols = pmi_config.get("drop_columns", [
        schema["time"], *schema["entity"], schema["label"],
        "apply_start_date", "apply_end_date", "cust_segment_typ",
    ])
    categorical_cols = pmi_config.get("categorical_columns", [schema["item"]])

    return drop_cols, categorical_cols


def _compute_feature_columns(
    feature_table_cols: list[str],
    identity_cols: list[str],
    categorical_cols: list[str],
    drop_cols: list[str],
    label_col: str,
) -> list[str]:
    """Compute feature_columns list preserving original post-join column order.

    Order: identity categoricals first (in identity_cols order), then
    feature_table columns minus drops / non-categorical identity / label.

    Lives here rather than in ``_spark`` because it is pure column-name
    bookkeeping with no Spark in it, and it has two callers on opposite sides
    of the package boundary: ``_spark.fit_preprocessor_metadata``, which turns
    the answer into ``preprocessor_metadata``, and the Layer-2 data gate
    (``pipelines/dataset/nodes_data_gate.py``), which must classify *the same*
    prospective feature set for B6. A second implementation of this rule would
    let the gate pass a column the preprocessor then treats as a feature.
    """
    non_feature = set(drop_cols) | (set(identity_cols) - set(categorical_cols)) | {label_col}
    feature_columns: list[str] = []
    for c in identity_cols:
        if c in categorical_cols and c not in feature_columns:
            feature_columns.append(c)
    for c in feature_table_cols:
        if c in non_feature or c in feature_columns:
            continue
        feature_columns.append(c)
    return feature_columns


def _validate_columns(
    columns: list[str],
    required: list[str],
    context: str,
) -> None:
    """Check that all required columns exist. Raises ValueError if missing."""
    missing = set(required) - set(columns)
    if missing:
        raise ValueError(f"Missing columns in {context}: {sorted(missing)}")


def _warn_missing_drop_columns(
    columns: list[str],
    drop_cols: list[str],
    context: str,
) -> None:
    """Log warning for drop_columns that don't exist in the DataFrame."""
    missing = [c for c in drop_cols if c not in columns]
    if missing:
        logger.warning(
            "drop_columns not found in %s (will be ignored): %s",
            context, missing,
        )


def feature_selection_exclude(parameters: dict) -> list[str]:
    """Return the training-stage feature exclusion list (empty if unset).

    Single reader for ``training.feature_selection.exclude`` so the cache-key
    hash, manifest provenance, and the metadata view all agree on the same
    normalized list.
    """
    fs = parameters.get("training", {}).get("feature_selection") or {}
    return list(fs.get("exclude") or [])


def apply_feature_selection(preprocessor_metadata: dict, parameters: dict) -> dict:
    """Return a training-only view of ``preprocessor_metadata`` with features dropped.

    Drops every column in ``training.feature_selection.exclude`` from
    ``feature_columns`` (and from ``categorical_columns``), preserving the
    original feature order so the numpy column layout, the ``feature_name``
    baked into the lgb ``.bin``, and the booster's reported names all stay
    aligned. ``category_mappings`` / ``drop_columns`` pass through untouched.

    This is a *training-stage* subset: the dataset-built ``preprocessor.json``
    keeps the full feature set (``base_dataset_version`` unchanged). Selection
    lives in the ``training:`` block, so it bumps ``model_version`` only.

    Empty / absent selection returns the input object unchanged, so non-selection
    runs are byte-identical to before. The input dict is never mutated.
    """
    exclude = feature_selection_exclude(parameters)
    if not exclude:
        return preprocessor_metadata

    exclude_set = set(exclude)
    view = dict(preprocessor_metadata)
    view["feature_columns"] = [
        c for c in preprocessor_metadata["feature_columns"] if c not in exclude_set
    ]
    view["categorical_columns"] = [
        c for c in preprocessor_metadata["categorical_columns"] if c not in exclude_set
    ]
    return view
