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
* A7 — a ranking ``training.algorithm_params.objective``
  (``lambdarank``/``rank_xendcg``) paired with a non-ranking ``metric`` or an
  undefined query group (empty ``schema.entity``). Predicate:
  ``ranking_objective_conflicts``.
* A8 — ``training.search_space`` declarative schema validity: must be an
  ordered list of ParamSpec maps; each needs ``name`` (unique) + ``type`` ∈
  {int,float,categorical}; numeric ``low < high``; positive ``step``;
  ``log: true`` ⟹ ``low > 0`` and no ``step``; categorical needs non-empty
  ``choices``. ``when`` / string-expression bounds are rejected until
  Phase 3. Predicate: ``search_space_errors``.
* A9 — a ``training.sample_weights`` key references a product value absent
  from ``schema.categorical_values[item]``. Predicate:
  ``weight_unknown_items`` (product-only check, mirrors A5).
* A10 — an ``evaluation.segment_columns`` entry has no ``evaluation.
  segment_sources`` entry providing it (matching ``segment_column``); the
  per-segment report section would silently never render. Predicate:
  ``segment_columns_without_source``.
* A11 — every ``evaluation.compare_sources[*]`` is well-formed:
  ``kind`` ∈ {model_version, external_hive}; ``label`` required; ranked
  by-kind required fields (``model_version`` for model_version, optional
  ``source`` ∈ {enriched_eval_predictions, ranked_predictions,
  training_eval_predictions} — default ``enriched_eval_predictions``;
  ``table`` + ``columns`` (cust_id/snap_date/prod_name/
  score) + ``prod_mapping`` + ``unmapped_policy`` ∈ {fail, drop} for
  external_hive); ``model_version`` kind must NOT declare
  ``columns``/``prod_mapping`` (config leak guard). Predicate:
  ``compare_source_well_formed_errors``.
* A12 — ``--compare X`` / ``--compare-only X`` resolves to a key in
  ``compare_sources``. Predicate: ``compare_source_key_exists`` (raises
  ``ConfigConsistencyError`` directly; not aggregated by validate).
* A13 — ``--compare`` and ``--compare-only`` are mutually exclusive (only
  one or neither). Predicate: ``compare_mutual_exclusive_errors``.

Layer 2 — data-stage validation (B1 implemented and wired; B2/B3/B4 implemented for compare feature):

* B1 — sample_pool items ↔ declared items must be equal; label items ⊆
  declared items (unknown item values corrupt training or violate invariants).
  Predicate: ``item_coverage_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` (``preprocessing/_spark.py``) as the first
  node of the dataset pipeline. B3 — a declared item has zero positives over
  the train window — intentionally NOT reported by ``item_coverage_errors``
  (deferred).
* B2 — label-window leakage columns reach features (specified but DEFERRED).

Layer 3 — specified but DEFERRED (NOT implemented in this module yet); see
the plan doc for the full table:

* C1 — produced sample_pool/label distinct item ≠ config (source_etl
  runtime pre-flight).
"""

from __future__ import annotations

from recsys_tfb.core.group_utils import RANKING_OBJECTIVES
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


# Eval metrics LightGBM accepts for a learning-to-rank objective. Anything
# else (e.g. binary_logloss) makes ranking early-stopping silently
# meaningless. Kept here (not in group_utils) because it is a config-policy
# fact owned by the consistency layer.
RANKING_METRICS: frozenset[str] = frozenset({"ndcg", "map", "lambdarank"})


def ranking_objective_conflicts(parameters: dict) -> list[str]:
    """A7 — a ranking objective requires a ranking metric and a query group.

    ``lambdarank``/``rank_xendcg`` cannot early-stop on a binary metric
    (silently meaningless) and need a per-query group. The query group is
    ``schema['time'] + schema['entity']``; ``entity`` must be non-empty. An
    *unset* metric is allowed — it is defaulted to ``ndcg`` at train time by
    ``group_utils.default_metric_for_objective``. Returns collect-all error
    strings; empty list means OK.
    """
    training = parameters.get("training", {}) or {}
    ap = training.get("algorithm_params", {}) or {}
    objective = ap.get("objective")
    if objective not in RANKING_OBJECTIVES:
        return []

    errors: list[str] = []

    metric = ap.get("metric")
    if metric is not None and str(metric) not in RANKING_METRICS:
        errors.append(
            f"training.algorithm_params.objective={objective!r} is a ranking "
            f"objective but metric={metric!r} is not a ranking metric. Set "
            f"training.algorithm_params.metric to one of "
            f"{sorted(RANKING_METRICS)} (e.g. 'ndcg'), or remove it to default "
            f"to 'ndcg'."
        )

    schema = get_schema(parameters)
    if not schema.get("entity"):
        errors.append(
            f"training.algorithm_params.objective={objective!r} is a ranking "
            f"objective but the query group (schema.columns.time + entity) is "
            f"undefined: schema 'entity' is empty. A ranking objective needs a "
            f"per-query group."
        )

    return errors


_SS_TYPES = frozenset({"int", "float", "categorical"})


def _is_number(v) -> bool:
    """True for a real int/float bound; bool excluded (``low: true`` in YAML
    is a typo, not the integer 1 — fail loud, never silently accept)."""
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def search_space_errors(parameters: dict) -> list[str]:
    """A8 — declarative ``training.search_space`` schema validity (collect-all).

    Phase 2 supports literal numeric int/float bounds and categorical
    ``choices``. ``when`` and string (expression) bounds are parsed by the
    search_space module but **rejected here fail-loud** until Phase 3 — never
    silently ignored. Empty/absent search_space is OK. Returns error strings.
    """
    training = parameters.get("training", {}) or {}
    if "search_space" not in training:
        return []
    space = training["search_space"]
    errors: list[str] = []

    if not isinstance(space, list):
        return [
            "training.search_space must be a list of ParamSpec maps "
            f"(got {type(space).__name__}). Migrate the old dict form to an "
            "ordered list: [{name, type, low, high, ...}, ...]."
        ]

    seen: set = set()
    for i, item in enumerate(space):
        if not isinstance(item, dict):
            errors.append(f"search_space[{i}] must be a map, got {type(item).__name__}.")
            continue
        name = item.get("name")
        ptype = item.get("type")
        tag = f"search_space[{i}]" + (f" ({name})" if name else "")

        if not name or not isinstance(name, str):
            errors.append(f"{tag}: missing/invalid required 'name' (string).")
        elif name in seen:
            errors.append(f"{tag}: duplicate name {name!r}.")
        else:
            seen.add(name)

        if ptype not in _SS_TYPES:
            errors.append(
                f"{tag}: type={ptype!r} invalid; must be one of "
                f"{sorted(_SS_TYPES)}."
            )

        if "when" in item:
            errors.append(
                f"{tag}: 'when' (conditional search space) is implemented in "
                f"Phase 3; not yet supported."
            )

        if ptype in ("int", "float"):
            low, high, step = item.get("low"), item.get("high"), item.get("step")
            for k, v in (("low", low), ("high", high)):
                if isinstance(v, str):
                    errors.append(
                        f"{tag}: expression-valued '{k}' is implemented in "
                        f"Phase 3; not yet supported (use a number)."
                    )
            if isinstance(step, str):
                errors.append(
                    f"{tag}: expression-valued 'step' is implemented in "
                    f"Phase 3; not yet supported (use a number)."
                )
            for k, v in (("low", low), ("high", high)):
                if not isinstance(v, str) and not _is_number(v):
                    errors.append(
                        f"{tag}: '{k}' must be a number (got "
                        f"{type(v).__name__}: {v!r})."
                    )
            if step is not None and not isinstance(step, str) and not _is_number(step):
                errors.append(
                    f"{tag}: 'step' must be a number (got "
                    f"{type(step).__name__}: {step!r})."
                )
            if _is_number(low) and _is_number(high) and not (low < high):
                errors.append(f"{tag}: low ({low}) must be < high ({high}).")
            if _is_number(step) and step <= 0:
                errors.append(f"{tag}: step must be positive (got {step}).")
            log = bool(item.get("log", False))
            if log and _is_number(low) and low <= 0:
                errors.append(
                    f"{tag}: log: true requires a positive low (got {low})."
                )
            if log and step is not None:
                errors.append(
                    f"{tag}: log: true and step are mutually exclusive "
                    f"(Optuna forbids it)."
                )
        elif ptype == "categorical":
            choices = item.get("choices")
            if not isinstance(choices, list) or len(choices) == 0:
                errors.append(f"{tag}: categorical requires a non-empty 'choices' list.")

    return errors


def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A9).

    Weight-table keys are fixed-format ``"<cust_segment_typ>|<prod_name>"``
    (2 parts, '|'-joined). Only the product component (index 1) is validated;
    the segment component has no config-declared value list (mirrors A5's
    item-only check in :func:`override_unknown_items`). Keys without a '|'
    (no product component) are skipped.
    """
    training = parameters.get("training", {}) or {}
    weights = training.get("sample_weights") or {}
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) >= 2 and parts[1] not in declared:
            bad.add(parts[1])
    return sorted(bad)


def segment_columns_without_source(parameters: dict) -> list[str]:
    """evaluation.segment_columns entries with no providing segment_source (A10).

    Every column in ``evaluation.segment_columns`` must be delivered by some
    ``evaluation.segment_sources`` entry's ``segment_column``. Otherwise the
    metric layer silently produces no per_segment results and the report
    drops the per-segment section without warning. Returns sorted offending
    columns; empty list means OK.
    """
    ev = parameters.get("evaluation", {}) or {}
    seg_cols = ev.get("segment_columns", []) or []
    sources = (ev.get("segment_sources", {}) or {}).values()
    provided = {(cfg or {}).get("segment_column") for cfg in sources}
    return sorted(c for c in seg_cols if c not in provided)


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

    for msg in ranking_objective_conflicts(parameters):
        errors.append(msg)

    unknown_w = weight_unknown_items(parameters)
    if unknown_w:
        errors.append(
            f"training.sample_weights references product value(s) {unknown_w} "
            f"absent from schema.categorical_values[item] — the weight "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )
        
    for msg in search_space_errors(parameters):
        errors.append(msg)

    errors.extend(compare_source_well_formed_errors(parameters))

    seg_no_src = segment_columns_without_source(parameters)
    if seg_no_src:
        errors.append(
            f"evaluation.segment_columns entries {seg_no_src} have no "
            f"evaluation.segment_sources entry providing them (no "
            f"segment_source has a matching segment_column). The per-segment "
            f"report section would silently never render. Add a "
            f"segment_sources entry for each, or remove them from "
            f"segment_columns."
        )

    if errors:
        raise ConfigConsistencyError(
            "Config consistency check failed (" + str(len(errors))
            + " issue(s)):\n- " + "\n- ".join(errors)
        )


def item_coverage_errors(
    item: str,
    declared: list[str],
    sample_pool_items: set[str],
    label_items: set[str],
) -> list[str]:
    """B1 invariant — the single definition.

    sample_pool ↔ declared must be EQUAL (both directions are hard errors):
    a value the data has but config does not encodes to -1 (same code as
    null) and corrupts training/scoring; a value config declares but
    sample_pool never produces can never be scored.

    label_table: only ``label_items - declared`` is an error (label business
    logic produced an unknown item). ``declared - label_items`` is B3
    (zero-positive), deferred — intentionally NOT reported here.

    Keys off the passed ``item`` only; never hardcodes 'prod_name'. Returns
    collect-all error strings; empty list means OK.
    """
    declared_set = set(declared)
    errors: list[str] = []

    sp_unknown = sorted(sample_pool_items - declared_set)
    if sp_unknown:
        errors.append(
            f"sample_pool has item value(s) {sp_unknown} not in "
            f"schema.categorical_values[{item!r}] — these encode to -1 "
            f"(same code as null) and silently corrupt training/scoring. Add "
            f"them to schema.categorical_values.{item} in parameters.yaml, or "
            f"fix sample_pool.sql."
        )

    sp_missing = sorted(declared_set - sample_pool_items)
    if sp_missing:
        errors.append(
            f"schema.categorical_values[{item!r}] declares value(s) "
            f"{sp_missing} that sample_pool never produces — they can never "
            f"be scored/recommended (silent). Remove them from config, or fix "
            f"sample_pool.sql to emit them."
        )

    lb_unknown = sorted(label_items - declared_set)
    if lb_unknown:
        errors.append(
            f"label_table has item value(s) {lb_unknown} not in "
            f"schema.categorical_values[{item!r}] — label business logic "
            f"(label_*.sql) produced an item the model config does not know. "
            f"Reconcile label_*.sql with schema.categorical_values.{item}."
        )

    return errors


# ---------------------------------------------------------------------------
# A11/A12/A13 — compare-source predicates (multi-model comparison feature)
# ---------------------------------------------------------------------------

_COMPARE_KINDS = {"model_version", "external_hive"}
_REQUIRED_COLUMNS = {"cust_id", "snap_date", "prod_name", "score"}
_VALID_UNMAPPED = {"fail", "drop"}
# Same-stack Hive tables a model_version compare source may read from.
# Mirror of evaluation.comparison.sources.MODEL_VERSION_SOURCES; A11 is the
# config-static gate, the loader checks again at read time.
_VALID_MODEL_VERSION_SOURCES = {
    "enriched_eval_predictions",
    "ranked_predictions",
    "training_eval_predictions",
}


def compare_source_well_formed_errors(parameters: dict) -> list[str]:
    """(A11) Each evaluation.compare_sources[*] is well-formed.

    Returns list of error messages (empty when all sources valid).
    """
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    errs: list[str] = []
    for key, src in sources.items():
        if not isinstance(src, dict):
            errs.append(f"(A11) compare_sources[{key!r}] must be a dict, got {type(src).__name__}")
            continue
        if "kind" not in src:
            errs.append(f"(A11) compare_sources[{key!r}] missing 'kind'")
            continue
        kind = src["kind"]
        if kind not in _COMPARE_KINDS:
            errs.append(
                f"(A11) compare_sources[{key!r}].kind={kind!r} not in {sorted(_COMPARE_KINDS)}"
            )
            continue
        if "label" not in src:
            errs.append(f"(A11) compare_sources[{key!r}] missing 'label'")
        if kind == "model_version":
            if "model_version" not in src:
                errs.append(f"(A11) compare_sources[{key!r}] kind=model_version missing 'model_version'")
            if "columns" in src:
                errs.append(
                    f"(A11) compare_sources[{key!r}] kind=model_version must not declare 'columns' "
                    "(same-stack source uses ranked_predictions schema)"
                )
            if "prod_mapping" in src:
                errs.append(
                    f"(A11) compare_sources[{key!r}] kind=model_version must not declare 'prod_mapping' "
                    "(same-stack source uses identical prod universe)"
                )
            if "source" in src and src["source"] not in _VALID_MODEL_VERSION_SOURCES:
                errs.append(
                    f"(A11) compare_sources[{key!r}].source={src['source']!r} "
                    f"not in {sorted(_VALID_MODEL_VERSION_SOURCES)}"
                )
        elif kind == "external_hive":
            if "table" not in src:
                errs.append(f"(A11) compare_sources[{key!r}] kind=external_hive missing 'table'")
            cols = src.get("columns", {}) or {}
            missing = _REQUIRED_COLUMNS - set(cols.keys())
            if missing:
                errs.append(
                    f"(A11) compare_sources[{key!r}].columns missing required keys: {sorted(missing)}"
                )
            if not src.get("prod_mapping"):
                errs.append(f"(A11) compare_sources[{key!r}] kind=external_hive missing 'prod_mapping'")
            policy = src.get("unmapped_policy", "fail")
            if policy not in _VALID_UNMAPPED:
                errs.append(
                    f"(A11) compare_sources[{key!r}].unmapped_policy={policy!r} "
                    f"not in {sorted(_VALID_UNMAPPED)}"
                )
    return errs


def compare_source_key_exists(parameters: dict, key: str | None) -> dict | None:
    """(A12) Resolve `key` against evaluation.compare_sources or raise.

    Returns the source dict, or None when `key` is None.
    """
    if key is None:
        return None
    sources = (
        (parameters.get("evaluation", {}) or {}).get("compare_sources", {}) or {}
    )
    if key not in sources:
        available = sorted(sources.keys())
        raise ConfigConsistencyError(
            f"(A12) --compare/--compare-only key {key!r} not in "
            f"evaluation.compare_sources. Available: {available}"
        )
    return sources[key]


def compare_mutual_exclusive_errors(compare: str | None, compare_only: str | None) -> list[str]:
    """(A13) --compare and --compare-only must not be passed together."""
    if compare is not None and compare_only is not None:
        return [
            f"(A13) --compare={compare!r} and --compare-only={compare_only!r} "
            "are mutually exclusive — pass at most one"
        ]
    return []
