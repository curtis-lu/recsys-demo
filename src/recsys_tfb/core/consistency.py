"""Single source of truth for config / data consistency invariants.

Every invariant is defined ONCE here as a pure predicate. Layer-1 config-static
validation, Layer-2 preprocessing guards, and the test_product_consistency lint
all call these predicates — no duplicated definitions, no message drift.

All errors subclass ValueError so existing ``except ValueError`` call sites
(__main__._load_config_and_setup) and existing tests keep working unchanged.

Invariant legend
----------------
Code comments across this module, ``core/schema.py`` and
``preprocessing/_spark.py`` reference invariants by ID. This docstring is the
canonical legend (the full design rationale lives in
``docs/superpowers/plans/2026-05-17-config-consistency-validation.md``).

Layer 1 — config-static (implemented here; aggregated by
``validate_config_consistency``, run at CLI entry):

* A1 — a column declared in BOTH ``drop_columns`` and ``categorical_columns``
  (contradictory role). Predicate: ``config_role_conflicts``.
* A2 — ``categorical_columns`` omits ``schema.item``. Predicate:
  ``item_missing_from_categorical`` (runtime backstop: ``_spark.py`` item
  guard).
* A3 — an identity categorical (``schema.item``) is declared in
  ``categorical_columns`` but absent from ``schema.categorical_values``.
  Predicate: ``resolved_item_values`` (also delegated to by
  ``schema.validate_schema_config``; runtime backstop: ``_spark.py``
  identity-cat guard, which raises ``DataConsistencyError``).
* A4 — ``inference.products`` ≠ ``schema.categorical_values[item]``.
  Predicate: ``inference_products_mismatch``.
* A5 — a ``sample_ratio_overrides`` key references an item value absent from
  ``schema.categorical_values[item]``. Predicate: ``override_unknown_items``.
* A6 — the hardcoded item lists across YAML/SQL/synthetic-data disagree.
  Enforced by the ``tests/test_pipelines/test_source_etl/
  test_product_consistency.py`` lint (consumes ``resolved_item_values``),
  not a predicate here.

Layer 2/3 — specified but DEFERRED (NOT implemented in this module yet); see
the plan doc for the full table:

* B1 — train-data item value ∉ ``categorical_values[item]`` (silent ``-1``
  training corruption). B2 — label-window leakage columns reach features.
  B3 — a declared item has zero positives over the train window.
* C1 — produced sample_pool/label distinct item ≠ config (source_etl
  runtime pre-flight).
"""

from __future__ import annotations

from recsys_tfb.core.schema import get_schema


class ConsistencyError(ValueError):
    """Base for all consistency failures (subclasses ValueError by design)."""


class ConfigConsistencyError(ConsistencyError):
    """Config self-contradiction detectable without data (Layer 1)."""


class DataConsistencyError(ConsistencyError):
    """Config disagrees with the actual data (Layer 2)."""


def _prepare_model_input(parameters: dict) -> dict:
    return (parameters.get("dataset", {}) or {}).get("prepare_model_input", {}) or {}


def resolved_item_values(parameters: dict) -> list[str]:
    """Canonical sorted list of valid item values (the single source).

    Reads ``schema.categorical_values[schema.item]``. Raises
    ``ConfigConsistencyError`` when the item column is a declared categorical
    (in prepare_model_input.categorical_columns) but has no category list —
    this is invariant A3, defined here once.

    Returns ``[]`` when the item column is not a declared categorical (or
    ``categorical_columns`` is absent). Callers relying on this as the single
    source of valid item values must ensure ``item_missing_from_categorical``
    (invariant A2) is validated upstream — ``validate_config_consistency``
    does this.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    cat_values = schema.get("categorical_values", {}) or {}
    declared_cats = _prepare_model_input(parameters).get("categorical_columns")
    if declared_cats is not None and item in declared_cats and item not in cat_values:
        raise ConfigConsistencyError(
            f"schema.item={item!r} is in dataset.prepare_model_input."
            f"categorical_columns but has no schema.categorical_values[{item!r}] "
            f"declaration. Add the full value list under "
            f"schema.categorical_values.{item} in parameters.yaml."
        )
    return sorted(cat_values.get(item, []))


def config_role_conflicts(parameters: dict) -> list[str]:
    """Columns declared in BOTH drop_columns and categorical_columns (A1).

    A column in both lists is an illegal, environment-divergent config state
    (silent 'drop wins' in prod, misleading fail-loud in dev). Returned sorted;
    empty list means OK.
    """
    pmi = _prepare_model_input(parameters)
    drop = set(pmi.get("drop_columns", []) or [])
    cat = set(pmi.get("categorical_columns", []) or [])
    return sorted(drop & cat)


def inference_products_mismatch(parameters: dict) -> dict:
    """Symmetric diff between inference.products and resolved_item_values (A4).

    Empty 'inference' section → no mismatch (inference not configured here).
    """
    declared = set(resolved_item_values(parameters))
    inf = parameters.get("inference") or {}
    if "products" not in inf:
        return {"only_in_inference": [], "only_in_categorical": []}
    products = set(inf.get("products") or [])
    return {
        "only_in_inference": sorted(products - declared),
        "only_in_categorical": sorted(declared - products),
    }


def override_unknown_items(parameters: dict) -> list[str]:
    """sample_ratio_overrides keys whose item component ∉ resolved_item_values (A5).

    Override keys are '|'-joined sample_group_keys values. If schema.item is not
    a sample_group_key there is no item component → nothing to check.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    ds = parameters.get("dataset", {}) or {}
    group_keys = ds.get("sample_group_keys", [])
    if item not in group_keys:
        return []
    idx = group_keys.index(item)
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in (ds.get("sample_ratio_overrides") or {}):
        parts = str(key).split("|")
        if idx < len(parts) and parts[idx] not in declared:
            bad.add(parts[idx])
    return sorted(bad)


def item_missing_from_categorical(parameters: dict) -> bool:
    """True if schema.item is absent from an explicitly-set categorical_columns (A2).

    When the key is absent, the codebase default ([schema.item]) includes it,
    so that case is OK.
    """
    schema = get_schema(parameters)
    item = schema["item"]
    declared = _prepare_model_input(parameters).get("categorical_columns")
    if declared is None:
        return False
    return item not in declared


def validate_config_consistency(parameters: dict) -> None:
    """Layer-1 config-static gate. Collects ALL failures, raises once.

    Collect-all (not fail-on-first) so a user fixes every problem in one pass.
    """
    errors: list[str] = []

    for col in config_role_conflicts(parameters):
        errors.append(
            f"{col!r} is declared in BOTH "
            f"dataset.prepare_model_input.drop_columns and categorical_columns "
            f"— contradictory intent. Resolve by choosing one:\n"
            f"    - want it as a feature  -> remove from drop_columns\n"
            f"    - want it excluded      -> remove from categorical_columns"
        )

    if item_missing_from_categorical(parameters):
        item = get_schema(parameters)["item"]
        errors.append(
            f"schema.item={item!r} is missing from "
            f"dataset.prepare_model_input.categorical_columns. For a ranking "
            f"task the item must be a model feature; add {item!r} back."
        )

    mm = inference_products_mismatch(parameters)
    if mm["only_in_inference"] or mm["only_in_categorical"]:
        errors.append(
            f"inference.products disagrees with schema.categorical_values"
            f"[item]: only_in_inference={mm['only_in_inference']}, "
            f"only_in_categorical={mm['only_in_categorical']}. They must be "
            f"identical sets."
        )

    unknown = override_unknown_items(parameters)
    if unknown:
        errors.append(
            f"sample_ratio_overrides references item value(s) {unknown} "
            f"absent from schema.categorical_values[item] — the override "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )

    if errors:
        raise ConfigConsistencyError(
            "Config consistency check failed (" + str(len(errors))
            + " issue(s)):\n- " + "\n- ".join(errors)
        )
