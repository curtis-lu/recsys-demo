"""Centralized column schema for all pipelines.

Provides get_schema() to retrieve column names from parameters. The three
roles that name columns in the user's own tables (time / entity / item) have
no defaults and must be declared; the three this framework produces itself
(label / score / rank) do.
"""

import copy


#: Every role :func:`get_schema` resolves, in the order
#: :func:`get_schema_for_hash` emits them.
#:
#: Kept separate from :data:`_DEFAULTS` since #328: three of these roles have
#: no default any more, so a dict of defaults can no longer double as the list
#: of settable keys. Using ``_DEFAULTS`` for the merge filter would silently
#: drop a user's declared ``time`` / ``entity`` / ``item``; using it for the
#: hash key list would change ``base_dataset_version`` for every existing user.
_ROLE_KEYS = ("time", "entity", "item", "label", "score", "rank")


#: Defaults for the roles this framework produces. The other three are
#: deliberately absent -- see :data:`_REQUIRED_ROLES`.
_DEFAULTS = {
    "label": "label",
    "score": "score",
    "rank": "rank",
}


_SCALAR_KEYS = ("time", "item", "label", "score", "rank")


#: Roles the user has to declare, because they name columns that already exist
#: in the user's own tables. The framework has no basis for guessing them: a
#: default here is a silent assumption that the deployment is the example one
#: (bank product recommendation), and a wrong guess produces a run that
#: finishes and computes the wrong thing rather than one that fails.
#:
#: ``label`` / ``score`` / ``rank`` are deliberately NOT here — those columns
#: are produced by this framework, so it is entitled to name them, and making
#: users declare three names they never chose buys nothing.
#:
#: Enforced twice, on purpose (#328). :func:`validate_schema_config` runs at
#: the CLI entry and collects every omission into one message before a Spark
#: session exists, so a real run fails in seconds rather than after a cold
#: start. :func:`get_schema` then refuses again for anything that never went
#: through the CLI -- which in practice means tests. Leaving only the CLI gate
#: would let an undeclared test config keep resolving to the example
#: deployment's column names and stay green against code that cannot handle
#: any other spelling (#274).
_REQUIRED_ROLES = ("time", "entity", "item")


def _missing_roles_message(missing: list) -> str:
    """The one message both gates raise, so their wording cannot drift."""
    return (
        "Missing schema.columns in parameters.yaml: "
        f"{', '.join(missing)}. These name columns in your own tables, so "
        "this framework cannot guess them. Declare each one under "
        "'schema:' -> 'columns:' in conf/base/parameters.yaml, e.g.\n"
        "  schema:\n"
        "    columns:\n"
        "      time: <the column one ranking request is scoped to>\n"
        "      entity: [<the column(s) naming who is being ranked for>]\n"
        "      item: <the column naming what is being ranked>\n"
        "('label', 'score' and 'rank' are produced by this framework and "
        "keep their defaults.)"
    )


def get_schema(parameters: dict) -> dict:
    """Return column schema from parameters.

    Reads ``parameters["schema"]["columns"]``. The three roles in
    :data:`_REQUIRED_ROLES` must be declared there; ``label`` / ``score`` /
    ``rank`` fall back to :data:`_DEFAULTS`.

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

    Raises:
        ValueError: If any of :data:`_REQUIRED_ROLES` is not declared.
    """
    schema_section = parameters.get("schema", {}) or {}
    columns = schema_section.get("columns", {}) or {}

    schema = copy.deepcopy(_DEFAULTS)
    schema.update({k: v for k, v in columns.items() if k in _ROLE_KEYS})

    missing = [role for role in _REQUIRED_ROLES if role not in schema]
    if missing:
        raise ValueError(_missing_roles_message(missing))

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
    keys = list(_ROLE_KEYS) + ["categorical_values"]
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
    - ``time``, ``entity`` and ``item`` must be declared (:data:`_REQUIRED_ROLES`).
    - The item column (``schema.item``) — when declared in
      ``dataset.prepare_model_input.categorical_columns`` — must have a
      non-empty entry in ``schema.categorical_values``. This invariant (A3)
      is delegated to :func:`recsys_tfb.core.consistency.resolved_item_values`
      so config-time and runtime guards share one definition; see that
      function for the precise rule.
    - The other keys (``label``, ``score``, ``rank``) may be omitted; they
      fall back to :data:`_DEFAULTS` in :func:`get_schema`.

    Runs at the CLI entry so every omission is reported at once. It is the
    fast, complete gate, not the only one: :func:`get_schema` refuses the same
    three roles for callers that never went through the CLI.

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

    # Required roles — see _REQUIRED_ROLES for why these three and not the
    # other three. Reported together rather than one per run: a user who
    # declared none of them should not have to re-run three times to find out.
    missing = [role for role in _REQUIRED_ROLES if role not in raw_columns]
    if missing:
        raise ValueError(_missing_roles_message(missing))

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

    # NOTE: this checks only schema.item (the single identity categorical in
    # the current schema). If entity/time columns are ever declared categorical,
    # extend resolved_item_values to cover them; until then the broader
    # post-feature_table case is caught by the _spark.py identity-cat guard.
    resolved_item_values(parameters)


#: ``dataset`` keys that declare the unit a per-entity operation groups on.
#: Both default to the full ``schema.entity``; both are validated by invariant
#: A29 (:func:`recsys_tfb.core.consistency.entity_grouping_key_errors`).
#: Deliberately NOT part of :data:`_ROLE_KEYS`: every key in that tuple flows
#: into :func:`get_schema_for_hash`, so adding one there would move
#: ``base_dataset_version`` for every existing user who has not asked for any
#: of this. See docs/adr/0016-split-unit-declared-by-two-keys.md.
ENTITY_GROUPING_KEYS: tuple[str, ...] = ("train_split_keys", "val_sample_keys")


def get_entity_grouping(parameters: dict, dataset_key: str) -> list[str]:
    """Columns a per-entity operation groups on, for one ``dataset`` key.

    Returns the declared ``parameters["dataset"][dataset_key]`` when present,
    otherwise the whole ``schema.entity``. The full entity is the default
    because it is what the surrounding config already says a ranking request
    belongs to; defaulting to ``entity[0]`` would promote a historical
    implementation detail into the spec.

    Shape is NOT re-checked here. A value that is not a non-empty subset of
    ``schema.entity`` is rejected at the CLI entry by A29, seconds into the
    run, so this resolver stays a plain lookup rather than a second, silently
    diverging copy of that rule.
    """
    if dataset_key not in ENTITY_GROUPING_KEYS:
        # A mistyped key name would resolve to "nothing declared" and hand back
        # the whole entity — a plausible-looking answer that silently ignores
        # whatever the user configured, and that A29 never sees because it
        # validates the real keys, not this call. Programmer error, so it
        # raises here rather than joining the config gate.
        raise ValueError(
            f"{dataset_key!r} is not an entity-grouping key. "
            f"Expected one of {list(ENTITY_GROUPING_KEYS)}."
        )
    ds = parameters.get("dataset") or {}
    declared = ds.get(dataset_key)
    columns = declared if declared else get_schema(parameters)["entity"]
    # Deduplicated so a repeated column name cannot reach a Spark join key list
    # (where it is at best redundant) — and so A29 can say it does not police
    # duplicates without that being a claim about Spark's behaviour.
    return list(dict.fromkeys(columns))
