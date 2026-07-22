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
canonical legend.

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
* A9 — ``training.sample_weights`` integrity (keys are '|'-joined
  ``training.sample_weight_keys`` values), split into:
    - A9a — a ``sample_weight_keys`` column ∉ identity ∪ {label} ∪
      ``dataset.carry_columns`` (cross-file: the column would be absent from
      or int-encoded in the train model_input parquet, so the weight silently
      no-ops). Predicate: ``weight_key_columns_unavailable``.
    - A9b — a ``sample_weights`` key whose '|'-segment count ≠
      ``len(sample_weight_keys)`` (silently never matches). Predicate:
      ``weight_key_arity_mismatch``.
    - A9c — a ``sample_weights`` key whose product component (when
      ``schema.item`` is a weight key) ∉ ``resolved_item_values`` (mirrors A5).
      Predicate: ``weight_unknown_items``.
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
* A14 — ``schema.item`` appears in ``training.feature_selection.exclude``.
  Training-stage feature selection must never drop the item column (for a
  ranking task the item must stay a model feature; mirrors A2/A7). Predicate:
  ``feature_selection_excludes_item``.
* A15 — ``evaluation.metric`` / ``evaluation.diagnosis`` parameter domains:
  ``weight_alpha`` ∈ [0,1]; ``k`` null or int ≥ 1; ``min_positives`` ≥ 0;
  ``shrinkage_k`` ≥ 0; ``diagnosis.sample.max_queries`` ≥ 1;
  ``diagnosis.sample.min_pos_queries_per_item`` ≥ 1;
  ``diagnosis.ci.n_boot`` ≥ 1; ``diagnosis.ci.enabled`` and every
  ``diagnosis.<name>.enabled`` for ``name`` in
  ``diagnosis.metric.contract.DIAGNOSES`` must be a real bool (a quoted YAML
  ``"false"`` is truthy and would silently enable the node); and
  ``evaluation.segment_columns`` must not use the sampler's reserved names
  ``stratum`` / ``inclusion_weight``. Predicate:
  ``diagnosis_metric_param_errors``. Registry diagnoses stay in A15 rather
  than getting their own code: their ``enabled`` flag is what decides whether
  the shared diagnosis sample is drawn, i.e. the same invariant family as
  ``ci.enabled``.
* A16 — retired 2026-07-17 with the reconciliation layer. The code is NOT
  renumbered: existing docs and plans cite invariants by number, so reusing
  A16 or shifting A17+ would silently repoint those references.
* A17 — retired 2026-07-19 with the quadrant diagnosis layer (threshold-based
  bucketing discarded continuous information; superseded by a scatter view).
  The code is NOT renumbered: existing docs and plans cite invariants by
  number, so reusing A17 or shifting A18+ would silently repoint those
  references.
* A18 — retired 2026-07-22 with the offset_sweep diagnosis layer. The code is
  NOT renumbered: existing docs and plans cite invariants by number, so reusing
  A18 or shifting A19+ would silently repoint those references.
* A19 — evaluation.diagnosis.suppression.top_examples must be a non-negative
  int (enabled is covered by A15). Predicate: ``suppression_param_errors``.
* A20 — training-side ``diagnostics.*`` parameter domains:
  ``diagnostics.shap.background`` ∈ {global, per_item};
  ``diagnostics.gain_ledger.enabled`` and ``diagnostics.shap.
  quadrant_enabled`` are bool; ``diagnostics.shap.quadrant_top_k_decision`` /
  ``quadrant_sample_per_cell`` / ``quadrant_min_rows`` are integers >= 1.
  Predicate: ``training_diagnostics_param_errors``.

Layer 2 — data-stage validation (B1 + B5 + B6 implemented and wired):

* B1 — sample_pool items ↔ declared items must be equal; label items ⊆
  declared items (unknown item values corrupt training or violate invariants).
  Predicate: ``item_coverage_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` (``preprocessing/_spark.py``) as the first
  node of the dataset pipeline. B3 — a declared item has zero positives over
  the train window — intentionally NOT reported by ``item_coverage_errors``
  (deferred).
* B2 — label-window leakage columns reach features (specified but DEFERRED).
* B5 — a column declared in ``dataset.prepare_model_input.categorical_columns``
  is a continuous-numeric type (decimal/double/float) in feature_table. decimal
  collects to Python ``decimal.Decimal`` (not JSON-serializable → the opaque
  ``fit_preprocessor_metadata`` save crash this gate front-runs); double/float
  serialize but are near-certain mis-tags. Predicate:
  ``categorical_dtype_errors`` (pure, no Spark); wired via
  ``validate_data_consistency`` alongside B1 (reads ``feature_table.dtypes``,
  metastore metadata only — no scan).
* B6 — a feature column that is non-numeric (string / binary / date / timestamp /
  complex) and is NOT declared categorical (so never integer-encoded): it becomes
  an ``object``-dtype model feature → driver OOM at ``_pdf_to_X`` ``to_numpy`` and
  a downstream LightGBM float-cast error. Predicate: ``nonnumeric_feature_errors``
  (with the ``spark_dtype_is_numeric`` classifier). Wired at TWO call sites — the
  dataset gate ``validate_data_consistency`` (prevents a rebuilt dataset baking it
  in) and a training-read backstop in ``io/extract.py`` (fails fast on an
  already-built parquet, before the expensive pandas read). B4 is unused.

Layer 3 — specified but DEFERRED (NOT implemented in this module yet); see
the plan doc for the full table:

* C1 — produced sample_pool/label distinct item ≠ config (source_etl
  runtime pre-flight).
"""

from __future__ import annotations

import math

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


def feature_selection_excludes_item(parameters: dict) -> bool:
    """schema.item is listed in training.feature_selection.exclude (A14).

    Training-stage feature selection drops features at model build time without
    rebuilding the dataset. It must never drop the item column: for a ranking
    task the item must remain a model feature (mirrors A2/A7). Returns True when
    the item is in the exclude list.
    """
    item = get_schema(parameters)["item"]
    fs = (parameters.get("training", {}) or {}).get("feature_selection") or {}
    return item in (fs.get("exclude") or [])


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


def weight_key_columns_unavailable(parameters: dict) -> list[str]:
    """training.sample_weight_keys columns absent from train model_input (A9a).

    The train/train_dev model_input parquet physically contains only identity
    columns, the label, dataset.carry_columns, and *encoded* features. A weight
    key must therefore be one of identity ∪ {label} ∪ carry_columns ∪ declared
    categorical columns — the raw-valued columns (encode-aware lookup translates
    declared categorical columns at runtime). Anything else is either physically
    absent (weight silently no-ops at 1.0) or int-encoded (key never matches).
    This is a cross-file dependency: sample_weight_keys lives in
    parameters_training.yaml but carry_columns lives in parameters_dataset.yaml.
    Returns sorted offending columns; empty means OK.
    """
    schema = get_schema(parameters)
    dataset_cfg = parameters.get("dataset", {}) or {}
    # Route through the file's own _prepare_model_input helper (as sibling
    # predicates do) and default only when the key is absent — matching
    # _get_preprocessing_config, so an explicit `categorical_columns: []`
    # is honoured rather than silently coerced to [schema["item"]].
    declared_cats = _prepare_model_input(parameters).get("categorical_columns")
    categorical_cols = declared_cats if declared_cats is not None else [schema["item"]]
    available = (
        set(schema["identity_columns"])
        | {schema["label"]}
        | set(dataset_cfg.get("carry_columns") or [])
        | set(categorical_cols)
    )
    keys = (parameters.get("training", {}) or {}).get("sample_weight_keys") or []
    return sorted(k for k in keys if k not in available)


def weight_key_arity_mismatch(parameters: dict) -> list[str]:
    """training.sample_weights keys whose '|'-segment count != key arity (A9b).

    Each weight-table key is sample_weight_keys values joined with '|', so it
    must have exactly len(sample_weight_keys) segments. A miscounted key
    silently never matches any row. Returns sorted offending keys; empty
    means OK. No keys configured (arity 0) → nothing to check.
    """
    training = parameters.get("training", {}) or {}
    n = len(training.get("sample_weight_keys") or [])
    if n == 0:
        return []
    weights = training.get("sample_weights") or {}
    return sorted(str(k) for k in weights if len(str(k).split("|")) != n)


def weight_unknown_items(parameters: dict) -> list[str]:
    """training.sample_weights keys whose product component ∉ resolved_item_values (A9c).

    Weight-table keys are '|'-joined sample_weight_keys values. If schema.item
    is not a weight key there is no product component → nothing to check
    (mirrors A5's item-only check in override_unknown_items). Only keys whose
    segment count matches the key arity are inspected; arity errors are
    reported separately by weight_key_arity_mismatch.
    """
    training = parameters.get("training", {}) or {}
    keys = training.get("sample_weight_keys") or []
    item = get_schema(parameters)["item"]
    if item not in keys:
        return []
    idx = keys.index(item)
    weights = training.get("sample_weights") or {}
    declared = set(resolved_item_values(parameters))
    bad: set[str] = set()
    for key in weights:
        parts = str(key).split("|")
        if len(parts) == len(keys) and parts[idx] not in declared:
            bad.add(parts[idx])
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


def diagnosis_metric_param_errors(parameters: dict) -> list[str]:
    """evaluation.metric / evaluation.diagnosis parameter domains (A15).

    Absent blocks are fine (all keys have behavior-preserving defaults);
    present values must be in-domain, else the metric family silently
    degenerates (e.g. alpha>1 over-concentrates on hot items) or the
    bootstrap is undefined (n_boot<1).
    """
    errors: list[str] = []
    ev = parameters.get("evaluation", {}) or {}
    metric = ev.get("metric", {}) or {}
    diag = ev.get("diagnosis", {}) or {}
    sample = diag.get("sample", {}) or {}
    ci = diag.get("ci", {}) or {}

    alpha = metric.get("weight_alpha", 0.0)
    if not (_is_number(alpha) and 0.0 <= float(alpha) <= 1.0):
        errors.append(
            f"evaluation.metric.weight_alpha={alpha!r} must be a number in "
            f"[0, 1] (0 = equal-weight macro, 1 = positive-count weighting)."
        )
    k = metric.get("k", None)
    if k is not None and not (
        isinstance(k, int) and not isinstance(k, bool) and k >= 1
    ):
        errors.append(
            f"evaluation.metric.k={k!r} must be null (no truncation) or an "
            f"int >= 1."
        )
    mp = metric.get("min_positives", 0)
    if not (isinstance(mp, int) and not isinstance(mp, bool) and mp >= 0):
        errors.append(
            f"evaluation.metric.min_positives={mp!r} must be an int >= 0."
        )
    sk = metric.get("shrinkage_k", 0)
    if not (_is_number(sk) and float(sk) >= 0.0):
        errors.append(
            f"evaluation.metric.shrinkage_k={sk!r} must be a number >= 0."
        )

    for key, val, floor in (
        ("evaluation.diagnosis.sample.max_queries",
         sample.get("max_queries", 200000), 1),
        ("evaluation.diagnosis.sample.min_pos_queries_per_item",
         sample.get("min_pos_queries_per_item", 50), 1),
        ("evaluation.diagnosis.ci.n_boot", ci.get("n_boot", 200), 1),
    ):
        if not (isinstance(val, int) and not isinstance(val, bool)
                and val >= floor):
            errors.append(f"{key}={val!r} must be an int >= {floor}.")

    en = ci.get("enabled", True)
    if not isinstance(en, bool):
        errors.append(
            f"evaluation.diagnosis.ci.enabled={en!r} must be a boolean "
            f"(YAML true/false; a quoted string like \"false\" is truthy and "
            f"would silently enable the node)."
        )

    # Same YAML trap for every registry diagnosis (diagnosis.metric.contract.
    # DIAGNOSES). These belong to A15 rather than a new code: their `enabled`
    # is what decides whether the SHARED diagnosis sample gets drawn at all,
    # so it is another member of the same invariant as ci.enabled — not a
    # separate concern.
    #
    # Imported lazily: core/ must not gain an import-time dependency on
    # diagnosis/ (the layering claim is that diagnosis sits above the core
    # config layer, not beside it).
    from recsys_tfb.diagnosis.metric.contract import DIAGNOSES

    for name in DIAGNOSES:
        val = (diag.get(name, {}) or {}).get("enabled", True)
        if not isinstance(val, bool):
            errors.append(
                f"evaluation.diagnosis.{name}.enabled={val!r} must be a "
                f"boolean (YAML true/false; a quoted string like \"false\" is "
                f"truthy and would silently enable the node)."
            )

    # ``draw_diagnosis_sample`` adds its own 'stratum' / 'inclusion_weight'
    # columns to the sample, then joins them onto the predictions by query
    # key. A segment column of the same name would be duplicated by that join
    # and blow up far downstream with an opaque pandas error ("Grouper for
    # 'stratum' not 1-dimensional"). Catching it here means the CLI rejects
    # the config in ~1s instead of 2-4 minutes into a Spark job.
    reserved = {"stratum", "inclusion_weight"}
    bad_seg = sorted(
        set(ev.get("segment_columns", []) or []) & reserved
    )
    for col in bad_seg:
        errors.append(
            f"evaluation.segment_columns entry {col!r} is a reserved column "
            f"name: the diagnosis sampler creates its own 'stratum' and "
            f"'inclusion_weight' columns, so a segment column of the same "
            f"name would collide in the sample join. Rename it in the source "
            f"table or drop it from evaluation.segment_columns."
        )
    return errors


def suppression_param_errors(parameters: dict) -> list[str]:
    """evaluation.diagnosis.suppression.top_examples parameter domain (A19).

    ``enabled`` is intentionally NOT re-validated here: A15
    (``diagnosis_metric_param_errors``) already walks every name in
    ``diagnosis.metric.contract.DIAGNOSES`` — including ``suppression`` once
    it is registered — and validates its ``enabled`` flag there. Checking it
    again here would raise two error messages for the same bad value.
    """
    errors: list[str] = []
    diag = ((parameters.get("evaluation", {}) or {})
            .get("diagnosis", {}) or {})
    cfg = diag.get("suppression", {}) or {}
    top_examples = cfg.get("top_examples", 50)
    if not (
        isinstance(top_examples, int) and not isinstance(top_examples, bool)
        and top_examples >= 0
    ):
        errors.append(
            f"evaluation.diagnosis.suppression.top_examples={top_examples!r} "
            "must be a non-negative int."
        )
    return errors


def training_diagnostics_param_errors(parameters: dict) -> list[str]:
    """A20 — training-side ``diagnostics.*`` parameter domains.

    Covers every ``diagnostics.*`` key consumed via bare truthiness/int
    comparison by the training-side diagnostics nodes, where a wrong YAML
    type is silently accepted rather than raising:
    ``diagnostics.shap.background`` must be ``global`` or ``per_item``;
    ``diagnostics.gain_ledger.enabled`` and ``diagnostics.shap.
    quadrant_enabled`` must be bool (a quoted YAML string like "false" is
    truthy in Python and would silently enable the node —
    ``shap_cases.py``/``population_spark.py`` both read
    ``cfg.get("quadrant_enabled", True)`` bare); ``diagnostics.shap.
    quadrant_top_k_decision`` / ``quadrant_sample_per_cell`` /
    ``quadrant_min_rows`` must be integers >= 1. Absent keys use
    behavior-preserving defaults. Returns collect-all error strings; empty
    means OK.
    """
    errors: list[str] = []
    diag = parameters.get("diagnostics", {}) or {}
    shap_cfg = diag.get("shap", {}) or {}
    bg = shap_cfg.get("background", "global")
    if bg not in ("global", "per_item"):
        errors.append(
            f"A20: diagnostics.shap.background must be 'global' or 'per_item' "
            f"(got {bg!r})."
        )
    gl_en = (diag.get("gain_ledger", {}) or {}).get("enabled", True)
    if not isinstance(gl_en, bool):
        errors.append(
            f"A20: diagnostics.gain_ledger.enabled={gl_en!r} must be a bool "
            f"(true/false without quotes in YAML)."
        )
    q_en = shap_cfg.get("quadrant_enabled", True)
    if not isinstance(q_en, bool):
        errors.append(
            f"A20: diagnostics.shap.quadrant_enabled={q_en!r} must be a bool "
            f"(true/false without quotes in YAML)."
        )
    for key, default in (
        ("quadrant_top_k_decision", 1),
        ("quadrant_sample_per_cell", 30),
        ("quadrant_min_rows", 10),
    ):
        v = shap_cfg.get(key, default)
        if not (isinstance(v, int) and not isinstance(v, bool) and v >= 1):
            errors.append(
                f"A20: diagnostics.shap.{key}={v!r} must be an integer >= 1."
            )
    return errors


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

    cols_bad = weight_key_columns_unavailable(parameters)
    if cols_bad:
        errors.append(
            f"training.sample_weight_keys column(s) {cols_bad} are not in the "
            f"train model_input parquet (identity ∪ {{label}} ∪ "
            f"dataset.carry_columns) — the weight would silently never match. "
            f"Add them to dataset.carry_columns and re-run the dataset "
            f"pipeline (this busts base_dataset_version)."
        )

    arity_bad = weight_key_arity_mismatch(parameters)
    if arity_bad:
        n = len((parameters.get("training", {}) or {}).get("sample_weight_keys") or [])
        errors.append(
            f"training.sample_weights key(s) {arity_bad} do not have "
            f"{n} '|'-separated segment(s) to match "
            f"sample_weight_keys — the weight silently never matches. "
            f"Fix the key(s) or sample_weight_keys."
        )

    unknown_w = weight_unknown_items(parameters)
    if unknown_w:
        errors.append(
            f"training.sample_weights references product value(s) {unknown_w} "
            f"absent from schema.categorical_values[item] — the weight "
            f"silently never matches. Fix the key(s) or declare the value(s)."
        )

    for msg in search_space_errors(parameters):
        errors.append(msg)

    if feature_selection_excludes_item(parameters):
        item = get_schema(parameters)["item"]
        errors.append(
            f"schema.item={item!r} is listed in "
            f"training.feature_selection.exclude. The item column must remain a "
            f"model feature (ranking invariant); remove it from the exclude list."
        )

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

    errors.extend(diagnosis_metric_param_errors(parameters))

    errors.extend(suppression_param_errors(parameters))

    errors.extend(training_diagnostics_param_errors(parameters))

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


# Spark DataFrame.dtypes simpleString forms that are continuous-numeric and
# therefore an illegal type for a declared categorical (B5). decimal carries a
# precision/scale suffix ("decimal(15,0)"), so it is matched by prefix below.
_CONTINUOUS_NUMERIC_DTYPES = {"double", "float"}


def categorical_dtype_errors(
    categorical_cols: list[str],
    feature_table_dtypes: dict[str, str],
) -> list[str]:
    """B5 invariant — the single definition.

    A column declared in ``dataset.prepare_model_input.categorical_columns``
    must not be a continuous-numeric type (``decimal`` / ``double`` / ``float``)
    in ``feature_table``:

    - ``decimal`` collects to Python ``decimal.Decimal``, which is not
      JSON-serializable — ``fit_preprocessor_metadata`` crashes when saving the
      preprocessor metadata, but only after the full per-column ``distinct()``
      pass (the opaque, expensive failure this gate replaces).
    - ``double`` / ``float`` serialize fine but a continuous value used as a
      category is almost always a mis-tag, and float-equality lookup in the
      ``F.create_map`` encoding is fragile.

    ``feature_table_dtypes`` maps a feature_table column name to its Spark
    ``DataFrame.dtypes`` simpleString (e.g. ``"decimal(15,0)"``, ``"double"``,
    ``"string"``). Identity categoricals (``schema.item``) come from
    ``schema.categorical_values`` rather than feature_table, so they are absent
    from this mapping and correctly skipped. Pure (no Spark): the Layer-2 gate
    passes ``dict(feature_table.dtypes)`` in. Returns collect-all error strings
    sorted by column; empty list means OK.
    """
    errors: list[str] = []
    for col in sorted(categorical_cols):
        dt = feature_table_dtypes.get(col)
        if dt is None:
            continue  # identity categorical / not a feature_table column
        if dt.startswith("decimal") or dt in _CONTINUOUS_NUMERIC_DTYPES:
            errors.append(
                f"categorical column {col!r} is a continuous-numeric type "
                f"(type={dt}) in feature_table — a decimal categorical is not "
                f"JSON-serializable (fit_preprocessor_metadata save crashes) "
                f"and a double/float categorical is almost always a mis-tag. "
                f"If {col!r} is a numeric feature, remove it from "
                f"dataset.prepare_model_input.categorical_columns; if it is not "
                f"a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
            )
    return errors


# ---------------------------------------------------------------------------
# B6 — non-numeric feature column that will not be encoded (object-dtype OOM)
# ---------------------------------------------------------------------------

# Spark ``DataFrame.dtypes`` simpleStrings for the numeric/boolean types that
# survive ``DataFrame.values`` into a numeric numpy matrix. ``decimal(p,s)`` is
# the only parametric one (special-cased below). Whitelist, NOT blacklist: an
# unknown type (char/varchar/void/null/…) must be treated as non-numeric so it
# is never silently passed by the B6 gate (fail-safe).
_NUMERIC_SPARK_TYPES = frozenset(
    {"tinyint", "smallint", "int", "bigint", "float", "double", "boolean"}
)


def spark_dtype_is_numeric(simple_string: str) -> bool:
    """True iff a Spark ``DataFrame.dtypes`` simpleString denotes a type that
    survives ``DataFrame.values`` into a numeric numpy matrix (int / float /
    decimal / boolean). Every other type — string / binary / date / timestamp /
    char / varchar / void / null / complex — forces ``object`` dtype (the B6
    footgun) and returns False. Pure string classification (no Spark import).
    """
    dt = simple_string.strip().lower()
    return dt.startswith("decimal") or dt in _NUMERIC_SPARK_TYPES


def nonnumeric_feature_errors(
    feature_kinds: dict[str, str],
    will_be_encoded: set[str],
) -> list[str]:
    """B6 invariant — the single definition.

    A *feature* column that is non-numeric AND will not be encoded to numeric
    downstream forces ``DataFrame.values`` into ``object`` dtype: every cell
    becomes a boxed Python object (~34 B/cell vs 8 B for float64), exploding
    driver memory (OOM at ``_pdf_to_X`` ``to_numpy``) and later failing
    LightGBM's float cast. Prevented by declaring the column categorical (so it
    is integer-encoded) or dropping it.

    ``feature_kinds`` maps each *feature* column to ``"numeric"`` or
    ``"nonnumeric"``; the caller classifies using its own dtype vocabulary
    (Spark simpleString via :func:`spark_dtype_is_numeric` at the dataset gate,
    or pyarrow types at the training-read backstop). ``will_be_encoded`` is the
    set of feature columns that are non-numeric now but become numeric
    downstream (declared categoricals, incl. deferred identity categoricals).
    Returns collect-all error strings sorted by column; empty means OK.
    """
    errors: list[str] = []
    for col in sorted(feature_kinds):
        if feature_kinds[col] != "numeric" and col not in will_be_encoded:
            errors.append(
                f"feature column {col!r} is non-numeric and is not declared "
                f"categorical, so it would become an un-encoded object-dtype "
                f"model feature (OOM at _pdf_to_X.to_numpy, then a LightGBM "
                f"float-cast error). If {col!r} is a categorical feature, add it "
                f"to dataset.prepare_model_input.categorical_columns (it is then "
                f"integer-encoded); if it is not a model feature, add it to "
                f"dataset.prepare_model_input.drop_columns."
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
