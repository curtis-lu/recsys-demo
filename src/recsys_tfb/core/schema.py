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


_SCALAR_KEYS = ("time", "item", "label", "score", "rank")


def get_schema(parameters: dict) -> dict:
    """Return column schema from parameters with defaults.

    Reads ``parameters["schema"]["columns"]`` when present; falls back to
    hard-coded defaults that match the current codebase conventions.

    The ``entity`` field is always normalised to a list.  An automatically
    derived ``identity_columns`` field is appended as ``[time] + entity + [item]``.
    ``categorical_values`` is sourced from ``parameters["schema"]["categorical_values"]``
    (default ``{}``) and provides explicit category declarations for columns
    whose distinct values cannot be discovered from ``feature_table`` alone
    (e.g. ``prod_name``, which only appears in keys tables).

    Args:
        parameters: The full parameters dict (may or may not contain a
            ``schema`` key).

    Returns:
        A new dict with keys: time, entity, item, label, score, rank,
        identity_columns, categorical_values.
    """
    schema_section = parameters.get("schema", {}) or {}
    columns = schema_section.get("columns", {}) or {}

    schema = copy.deepcopy(_DEFAULTS)
    schema.update({k: v for k, v in columns.items() if k in _DEFAULTS})

    # Normalise entity to list
    if isinstance(schema["entity"], str):
        schema["entity"] = [schema["entity"]]

    # Derive identity_columns
    schema["identity_columns"] = (
        [schema["time"]] + schema["entity"] + [schema["item"]]
    )

    schema["categorical_values"] = copy.deepcopy(
        schema_section.get("categorical_values", {}) or {}
    )

    return schema


def get_schema_for_hash(parameters: dict) -> dict:
    """Return canonical schema dict intended for version hashing.

    Same resolution logic as :func:`get_schema` but excludes the derived
    ``identity_columns`` field. ``categorical_values`` IS included so
    changes to declared category lists (e.g. adding a new product) bust
    the base dataset version.
    """
    schema = get_schema(parameters)
    keys = list(_DEFAULTS) + ["categorical_values"]
    return {k: schema[k] for k in keys}


def validate_schema_config(parameters: dict) -> None:
    """Validate the shape of ``parameters["schema"]``.

    Enforces:
    - Scalar keys (time, item, label, score, rank) must be non-empty strings.
    - ``entity`` must be a non-empty string or a non-empty list of non-empty
      strings.
    - ``identity_columns`` ([time] + entity + [item]) must not contain
      duplicates.
    - ``categorical_values`` must be a mapping of non-empty str -> list.
    - The item column (``schema.item``) — when declared in
      ``dataset.prepare_model_input.categorical_columns`` — must have a
      non-empty entry in ``schema.categorical_values``. This invariant (A3)
      is delegated to :func:`recsys_tfb.core.consistency.resolved_item_values`
      so config-time and runtime guards share one definition; see that
      function for the precise rule.
    - Missing keys are allowed (they fall back to :data:`_DEFAULTS` in
      :func:`get_schema`).

    Args:
        parameters: The full parameters dict.

    Raises:
        ValueError: If the schema config is malformed.
    """
    schema_section = parameters.get("schema", {}) or {}
    raw_columns = schema_section.get("columns", {})
    if not isinstance(raw_columns, dict):
        raise ValueError(
            "Invalid schema.columns in parameters.yaml: expected mapping, got "
            f"{type(raw_columns).__name__}"
        )

    # Scalar string keys
    for key in _SCALAR_KEYS:
        if key not in raw_columns:
            continue
        value = raw_columns[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"Invalid schema.columns in parameters.yaml: '{key}' must be a "
                f"non-empty string, got {value!r}"
            )

    # Entity key: str or list[str]
    if "entity" in raw_columns:
        entity = raw_columns["entity"]
        if isinstance(entity, str):
            if not entity.strip():
                raise ValueError(
                    "Invalid schema.columns in parameters.yaml: 'entity' string "
                    "must not be empty"
                )
        elif isinstance(entity, list):
            if not entity:
                raise ValueError(
                    "Invalid schema.columns in parameters.yaml: 'entity' list "
                    "must not be empty"
                )
            for idx, item in enumerate(entity):
                if not isinstance(item, str) or not item.strip():
                    raise ValueError(
                        "Invalid schema.columns in parameters.yaml: 'entity' "
                        f"element at index {idx} must be a non-empty string, "
                        f"got {item!r}"
                    )
        else:
            raise ValueError(
                "Invalid schema.columns in parameters.yaml: 'entity' must be a "
                f"string or list of strings, got {type(entity).__name__}"
            )

    # identity_columns uniqueness
    schema = get_schema(parameters)
    identity = schema["identity_columns"]
    if len(identity) != len(set(identity)):
        raise ValueError(
            "Invalid schema.columns in parameters.yaml: identity_columns "
            f"contain duplicates: {identity}"
        )

    # categorical_values shape
    raw_cat_values = schema_section.get("categorical_values", {})
    if not isinstance(raw_cat_values, dict):
        raise ValueError(
            "Invalid schema.categorical_values in parameters.yaml: expected "
            f"mapping, got {type(raw_cat_values).__name__}"
        )
    for col, values in raw_cat_values.items():
        if not isinstance(col, str) or not col.strip():
            raise ValueError(
                "Invalid schema.categorical_values in parameters.yaml: keys "
                f"must be non-empty strings, got {col!r}"
            )
        if not isinstance(values, list) or not values:
            raise ValueError(
                "Invalid schema.categorical_values in parameters.yaml: values "
                f"for '{col}' must be a non-empty list, got {values!r}"
            )

    # Identity categorical columns must declare category lists (invariant A3).
    # Single definition lives in core.consistency; call it so config-time and
    # runtime guards never drift. Import locally to avoid an import cycle
    # (consistency imports get_schema from this module).
    from recsys_tfb.core.consistency import resolved_item_values

    resolved_item_values(parameters)
