"""Centralized column schema for all pipelines.

Provides get_schema() to retrieve column names from parameters. The three
roles that name columns in the user's own tables (time / entity / item) have
no defaults and must be declared; the three this framework produces itself
(label / score / rank) do. One role is optional (event) — absent unless the
deployment declares it, and absent is what every existing deployment is.
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


#: Optional roles: present in the resolved schema only when the user declared
#: them (ADR-0025 decision 1). Unlike every key in :data:`_ROLE_KEYS` they have
#: no default and no placeholder — an undeclared one is simply not a key of the
#: returned dict, so ``schema.get("event", [])`` is how callers ask.
#:
#: **Why not in ``_ROLE_KEYS``.** Every key there flows into
#: :func:`get_schema_for_hash` unconditionally, so a permanent ``event: None``
#: entry would move ``base_dataset_version`` for every deployment that never
#: asked for any of this. Declared ones DO enter the hash (see
#: :func:`get_schema_for_hash`) — a deployment that starts naming one row per
#: impression is building a different dataset and must not read back the old
#: artifacts. Precedent: :data:`ENTITY_GROUPING_KEYS`, deliberately outside
#: ``_ROLE_KEYS`` for the same reason.
#:
#: ``event`` — **which row, when a query group holds the same item more than
#: once.** One column or several (an impression id, or a second-resolution
#: timestamp). It joins ``identity_columns`` and NOT ``query_group_columns``:
#: two impressions of one item compete for rank inside the same ranking, they
#: do not form two rankings (ADR-0025 decision 1). ``occasion``, the role that
#: widens the query group, is the next ticket and lands here beside it.
_OPTIONAL_ROLE_KEYS = ("event",)


#: Every key ``schema.columns`` may carry. Anything else is a typo, and
#: :func:`_unknown_column_keys_message` says so rather than letting the merge
#: filter drop it — a dropped ``evnet:`` changes no version ID and produces a
#: run that finishes and ranks the wrong thing.
_SETTABLE_COLUMN_KEYS = _ROLE_KEYS + _OPTIONAL_ROLE_KEYS


#: Defaults for the roles this framework produces. The other three are
#: deliberately absent -- see :data:`_REQUIRED_ROLES`.
_DEFAULTS = {
    "label": "label",
    "score": "score",
    "rank": "rank",
}


_SCALAR_KEYS = ("time", "item", "label", "score", "rank")


#: The column lists :func:`get_schema` derives. None of them is settable: under
#: ``schema`` they miss the ``columns`` lookup, and under ``schema.columns`` the
#: ``if k in _ROLE_KEYS`` filter drops them (S5 in
#: docs/agents/architecture-constraints.md refuses both spellings out loud).
#: They are excluded from :func:`get_schema_for_hash`, so adding them moved no
#: version ID.
#:
#: Three keys rather than one because they are three different questions, and
#: the answers only coincide under today's roles (ADR-0025 decision 2):
#:
#: - ``query_group_columns`` -- **the scope ranks are compared within.** Widens
#:   to ``time + entity + occasion`` once the optional ``occasion`` role lands.
#: - ``base_key_columns`` -- **an entity at a time.** What an entity-level table
#:   (a feature table, a segment source) joins onto candidate rows by. Never
#:   widens with ``occasion``: those tables have no column for one. The name
#:   follows what the code already called it (``base_key``,
#:   ``require_base_key_columns`` in the dataset pipeline).
#: - ``identity_columns`` -- **one candidate row.** ``time + entity + item``,
#:   plus ``event`` when declared; ``occasion`` joins it too once that role
#:   lands.
#:
#: ``identity_columns``' **order is a rule, not a spelling**: deterministic
#: sampling buckets by hashing its columns joined in order, so reordering it
#: draws a different sample from the same data. Nothing may reorder the columns
#: already in it; new roles are appended at the positions ADR-0025 fixes.
#: Pinned by ``tests/test_core/test_schema.py``.
_DERIVED_KEYS = ("query_group_columns", "base_key_columns", "identity_columns")


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


def _missing_roles_message(missing: list[str]) -> str:
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


def _unknown_column_keys(columns) -> list[str]:
    """Keys of ``schema.columns`` that name no role, sorted.

    Sorted so two runs of the same config report them in the same order, and
    so a config with three typos is fixed in one pass rather than three.
    """
    return sorted(k for k in columns if k not in _SETTABLE_COLUMN_KEYS)


def _unknown_column_keys_message(unknown: list[str]) -> str:
    """The one message both gates raise, so their wording cannot drift.

    Every unknown key at once, for :func:`_missing_roles_message`'s reason.

    **Why this is an error and not a warning.** Until #378 an unrecognised key
    under ``schema.columns`` was dropped by the merge filter: no message, and —
    because the dropped key never reaches :func:`get_schema_for_hash` — no
    change to any version ID either. A deployment that meant to declare
    ``event`` and typed ``evnet`` got a run that finished, read back the
    artifacts of the undeclared shape, and ranked one row per item where the
    operator believed it was ranking one row per impression. Verified before
    the change: every key in this framework's own ``conf/`` and in
    ``examples/*/conf/`` names a real role, so this gate is a no-op for them.

    The derived lists (``identity_columns`` / ``query_group_columns`` /
    ``base_key_columns``) land here too, which is what S5 in
    docs/agents/architecture-constraints.md refuses statically: declaring one
    is meaningless — they are computed from the roles — and the static scan
    only sees Python literals, so a YAML conf needs this runtime half.
    """
    return (
        "Unknown key(s) in schema.columns in parameters.yaml: "
        f"{', '.join(unknown)}. Recognised roles are "
        f"{', '.join(_SETTABLE_COLUMN_KEYS)}. An unrecognised key names no "
        "column role: it would be ignored and would move no version ID, so a "
        "typo'd role would silently resolve to the undeclared shape. "
        "(identity_columns / query_group_columns / base_key_columns are "
        "derived from the roles above and cannot be declared.)"
    )


def _check_multi_column_role(role: str, value) -> None:
    """A role that may name several columns holds a str or a non-empty list of
    non-empty strs.

    Shared by ``entity`` and by every role in :data:`_OPTIONAL_ROLE_KEYS`; the
    messages name ``role`` so the user reads about the key they actually wrote.
    Raises on the first problem rather than collecting: one role's value is one
    thing to fix, unlike the across-roles checks above.
    """
    if isinstance(value, str):
        if not value.strip():
            raise ValueError(
                f"Invalid schema.columns in parameters.yaml: '{role}' string "
                "must not be empty"
            )
        return
    if isinstance(value, list):
        if not value:
            raise ValueError(
                f"Invalid schema.columns in parameters.yaml: '{role}' list "
                "must not be empty"
            )
        for idx, item in enumerate(value):
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    f"Invalid schema.columns in parameters.yaml: '{role}' "
                    f"element at index {idx} must be a non-empty string, "
                    f"got {item!r}"
                )
        return
    raise ValueError(
        f"Invalid schema.columns in parameters.yaml: '{role}' must be a "
        f"string or list of strings, got {type(value).__name__}"
    )


def get_schema(parameters: dict) -> dict:
    """Return column schema from parameters.

    Reads ``parameters["schema"]["columns"]``. The three roles in
    :data:`_REQUIRED_ROLES` must be declared there; ``label`` / ``score`` /
    ``rank`` fall back to :data:`_DEFAULTS`.

    The ``entity`` field is always normalised to a list. Three derived column
    lists are appended -- see :data:`_DERIVED_KEYS` for what each one means and
    why they are separate keys rather than one.
    ``categorical_values`` is sourced from ``parameters["schema"]["categorical_values"]``
    (default ``{}``) and provides explicit category declarations for columns
    whose distinct values cannot be discovered from ``feature_table`` alone
    (e.g. ``prod_name``, which only appears in keys tables).

    Args:
        parameters: The full parameters dict (may or may not contain a
            ``schema`` key).

    Returns:
        A new dict with keys: time, entity, item, label, score, rank,
        query_group_columns, base_key_columns, identity_columns,
        categorical_values — plus ``event`` (normalised to a list) when the
        config declares it, and no ``event`` key at all when it does not.

    Raises:
        ValueError: If any of :data:`_REQUIRED_ROLES` is not declared.
    """
    schema_section = parameters.get("schema", {}) or {}
    columns = schema_section.get("columns", {}) or {}

    unknown = _unknown_column_keys(columns)
    if unknown:
        raise ValueError(_unknown_column_keys_message(unknown))

    schema = copy.deepcopy(_DEFAULTS)
    schema.update({k: v for k, v in columns.items() if k in _SETTABLE_COLUMN_KEYS})

    missing = [role for role in _REQUIRED_ROLES if role not in schema]
    if missing:
        raise ValueError(_missing_roles_message(missing))

    # Normalise entity to list
    if isinstance(schema["entity"], str):
        schema["entity"] = [schema["entity"]]

    # Same normalisation for every optional role, but only when declared: an
    # undeclared one must stay absent, not become [] — `"event" in schema` is
    # what the pipelines branch on, and an empty list is a third state that
    # would read as "declared, no columns".
    for role in _OPTIONAL_ROLE_KEYS:
        if role in schema:
            value = schema[role]
            schema[role] = [value] if isinstance(value, str) else list(value)

    # Derive the three column lists. Each builds its own list object, so a
    # caller that mutates one cannot reach the others.
    schema["query_group_columns"] = [schema["time"]] + schema["entity"]
    schema["base_key_columns"] = [schema["time"]] + schema["entity"]
    # Order is a rule, not a spelling -- see _DERIVED_KEYS. `event` is appended
    # AFTER `item` (ADR-0025 decision 1), so an undeclared `event` leaves the
    # list byte-for-byte what it was and every existing deployment's
    # deterministic sample is unmoved.
    schema["identity_columns"] = (
        [schema["time"]]
        + schema["entity"]
        + [schema["item"]]
        + list(schema.get("event", []))
    )

    schema["categorical_values"] = copy.deepcopy(
        schema_section.get("categorical_values", {}) or {}
    )

    return schema


def get_schema_for_hash(parameters: dict) -> dict:
    """Return canonical schema dict intended for version hashing.

    Same resolution logic as :func:`get_schema` but excludes every field in
    :data:`_DERIVED_KEYS` -- they are functions of the roles already hashed, so
    hashing them would change nothing but the digest.
    ``categorical_values`` IS included so
    changes to declared category lists (e.g. adding a new product) bust
    the base dataset version.

    An optional role (:data:`_OPTIONAL_ROLE_KEYS`) enters the payload **only
    when declared**, and is emitted after the six fixed roles. Both halves
    matter: an undeclared one must add no key at all, or every existing
    deployment's ``base_dataset_version`` moves for a role it never asked for;
    a declared one must be in, or a deployment switching to one row per
    impression would silently read back the artifacts of the old shape.
    """
    schema = get_schema(parameters)
    keys = (
        list(_ROLE_KEYS)
        + [r for r in _OPTIONAL_ROLE_KEYS if r in schema]
        + ["categorical_values"]
    )
    return {k: schema[k] for k in keys}


def validate_schema_config(parameters: dict) -> None:
    """Validate the shape of ``parameters["schema"]``.

    Enforces:
    - Scalar keys (time, item, label, score, rank) must be non-empty strings.
    - ``entity`` — and ``event`` when declared — must be a non-empty string or
      a non-empty list of non-empty strings.
    - No key of ``schema.columns`` outside :data:`_SETTABLE_COLUMN_KEYS`
      (every unknown one reported at once).
    - ``identity_columns`` ([time] + entity + [item] + event) must not contain
      duplicates. This is also what stops two roles overlapping — a column
      declared as both ``item`` and ``event`` shows up twice in that list —
      so there is no second overlap rule to keep in step with this one.
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

    # Unknown keys first: every check below asks "is this role's value the
    # right shape", which has no answer for a key that names no role, and a
    # config with a typo'd role is also missing that role — reporting the
    # omission first would send the user to add a key they already wrote.
    unknown = _unknown_column_keys(raw_columns)
    if unknown:
        raise ValueError(_unknown_column_keys_message(unknown))

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

    # Multi-column roles: str or non-empty list[str]. One loop rather than one
    # block per role — `event` has exactly `entity`'s shape, and two copies of
    # this would be two messages to keep in step for no gain. The role name is
    # interpolated, so a user still reads about the key they wrote.
    for role in ("entity", *_OPTIONAL_ROLE_KEYS):
        if role not in raw_columns:
            continue
        _check_multi_column_role(role, raw_columns[role])

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
