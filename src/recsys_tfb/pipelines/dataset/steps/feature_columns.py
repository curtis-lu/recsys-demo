"""Pure column-name derivation for the dataset pipeline, and the guards over it.

No Spark, and no frame: everything here is arithmetic over column-name lists and
the config that decides which names are features — plus the guards that raise or
warn when that arithmetic comes out wrong. Two callers on opposite ends of the
pipeline ask these questions — the preprocessor fit node, which bakes the answer
into ``preprocessor.json``, and the Layer-2 data gate, which must get the same
answer from the same config.

Every function here is one mechanism or one guard; the ML decisions they serve
are named at the call sites in ``nodes.py`` (ADR-0008 §2).
"""

from __future__ import annotations

import logging

from recsys_tfb.core.consistency import DataConsistencyError
from recsys_tfb.core.schema import get_schema

logger = logging.getLogger(__name__)


def prepare_model_input_config(parameters: dict) -> tuple[list[str], list[str]]:
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


def compute_feature_columns(
    feature_table_cols: list[str],
    identity_cols: list[str],
    categorical_cols: list[str],
    drop_cols: list[str],
    label_col: str,
) -> list[str]:
    """Compute feature_columns list preserving original post-join column order.

    Order: identity categoricals first (in identity_cols order), then
    feature_table columns minus drops / non-categorical identity / label.

    Pure column-name arithmetic — no Spark — with two callers: the
    ``fit_preprocessor_metadata`` node (which bakes the result into
    ``preprocessor.json``) and the Layer-2 data gate's B6 check (which must
    ask the same question of the same config). One definition here is what makes
    "the gate passed it" and "the preprocessor treats it as a feature" the same
    answer; two copies could drift into letting a column through the gate that
    the preprocessor then turns into a feature.
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


def split_categorical_sources(
    categorical_cols: list[str],
    feature_table_cols: list[str],
) -> tuple[list[str], list[str]]:
    """Partition the categoricals by where their vocabulary can come from.

    A categorical that exists as a ``feature_table`` column has its values in
    the data; an identity categorical (``schema.item`` and friends) does not
    appear in ``feature_table`` at all, so the only place its full domain can be
    read from is the schema declaration.

    Returns:
        (from_data, from_schema_declaration), each in ``categorical_cols`` order.
    """
    ft_cols = set(feature_table_cols)
    from_data = [c for c in categorical_cols if c in ft_cols]
    from_schema = [c for c in categorical_cols if c not in ft_cols]
    return from_data, from_schema


def encoded_frame_columns(
    base_key: list[str],
    feature_columns: list[str],
    frame_cols: list[str],
) -> list[str]:
    """The columns a pre-encoded ``feature_table`` keeps: base key, then features.

    Only the features that live in this frame — the identity-sourced ones
    (``schema.item`` and friends) arrive later, from ``keys``, so selecting them
    here would select a column this frame does not have.
    """
    ft_feature_cols = [c for c in feature_columns if c in frame_cols]
    return list(dict.fromkeys(list(base_key) + ft_feature_cols))


def require_item_is_a_feature(
    item_col: str | None,
    feature_columns: list[str],
    categorical_cols: list[str],
) -> None:
    """Registered backstop: for a ranking task the item column must be a feature.

    The most common way to lose it is omitting it from
    ``dataset.prepare_model_input.categorical_columns`` in yaml — which silently
    makes X miss the item dimension, collapses predictions to a constant within
    each query group, and produces a flat mAP across every HPO trial. Nothing
    upstream raises, so this is the only place that failure becomes visible.
    """
    if item_col and item_col not in feature_columns:
        raise DataConsistencyError(
            f"schema.item='{item_col}' is missing from derived feature_columns. "
            f"For a ranking task the item column must be a model feature; "
            f"otherwise the booster cannot differentiate items within a query "
            f"group and HPO mAP collapses to a constant across trials. "
            f"Fix: add '{item_col}' to "
            f"dataset.prepare_model_input.categorical_columns in "
            f"parameters_dataset.yaml. "
            f"(current categorical_columns={categorical_cols})"
        )


def require_base_key_columns(columns: list[str], base_key: list[str]) -> None:
    """Pre-check: ``feature_table`` must carry (time + entity).

    That pair is the join key every split's ``model_input`` is assembled on, so
    a frame missing it cannot be joined at all — better to say so before the
    encoding work than to fail inside a downstream join.
    """
    missing = [c for c in base_key if c not in columns]
    if missing:
        raise ValueError(f"feature_table missing base-key columns: {missing}")


def warn_missing_drop_columns(
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
