"""Pure column-name derivation for the dataset pipeline.

No Spark: everything here is arithmetic over column-name lists and the config
that decides which names are features. Two callers on opposite ends of the
pipeline ask these questions — the preprocessor fit node, which bakes the answer
into ``preprocessor.json``, and the Layer-2 data gate, which must get the same
answer from the same config.
"""

from __future__ import annotations

from recsys_tfb.core.schema import get_schema


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
