"""Centralized column schema for all pipelines.

Provides get_schema() to retrieve column names from parameters with defaults
that are fully backward-compatible with the existing hard-coded values.
"""

import copy


_DEFAULTS = {
    "time": "snap_date",
    "entity": ["cust_id"],
    "item": "prod_name",
    "label": "label",
    "score": "score",
    "rank": "rank",
}


def get_schema(parameters: dict) -> dict:
    """Return column schema from parameters with defaults.

    Reads ``parameters["schema"]["columns"]`` when present; falls back to
    hard-coded defaults that match the current codebase conventions.

    The ``entity`` field is always normalised to a list.  An automatically
    derived ``identity_columns`` field is appended as ``[time] + entity + [item]``.

    Args:
        parameters: The full parameters dict (may or may not contain a
            ``schema`` key).

    Returns:
        A new dict with keys: time, entity, item, label, score, rank,
        identity_columns.
    """
    columns = (
        parameters.get("schema", {}).get("columns", {})
    )

    schema = copy.deepcopy(_DEFAULTS)
    schema.update({k: v for k, v in columns.items() if k in _DEFAULTS})

    # Normalise entity to list
    if isinstance(schema["entity"], str):
        schema["entity"] = [schema["entity"]]

    # Derive identity_columns
    schema["identity_columns"] = (
        [schema["time"]] + schema["entity"] + [schema["item"]]
    )

    return schema
