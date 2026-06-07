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
