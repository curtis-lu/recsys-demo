# Config / Data Consistency — Phase 2 (Layer-2 data gate, B1) — Design

**Status:** Approved (brainstorming complete, locked 2026-05-17). Next: implementation plan.

**Predecessor:** Phase 1 (`docs/superpowers/plans/2026-05-17-config-consistency-validation.md`,
merged `91b15e6`, PR #19). This spec implements the **Phase 2** slice deferred there.

---

## 1. Goal

Eliminate the **B1** silent failure: when the item-value set actually present in
training/scoring data disagrees with what config declares, the encoder
(`pd.Categorical(values, categories=known).codes` in `io/extract.py`) maps every
undeclared value to `-1` — the *same code as null* — silently corrupting training
and scoring with no error, no log, no signal. Phase 2 makes that disagreement a
hard, actionable failure **before any node runs**.

## 2. Problem context (why Phase 1's stated hook point is invalid)

Phase 1's plan assumed B1 would hook into `fit_preprocessor_metadata`
"immediately after `category_mappings` is built — it already collects
train-window distinct values cheaply." Code inspection disproved this:

- `fit_preprocessor_metadata(feature_table, parameters)` sees **only**
  `feature_table`.
- The item column (`schema["item"]`, production: `prod_name`) is **not in
  `feature_table`**. It is an *identity categorical* (`identity_cat_cols`):
  `category_mappings[item]` is taken straight from `schema.categorical_values`
  (config), never observed from data.
- The item's real values live in **`sample_pool`** (dense cust×prod scoring
  universe, produced by hand-written `sample_pool.sql`) and **`label_table`**
  (sparse, produced by hand-written `label_*.sql` CASE-WHEN business logic).
  They are joined in `build_model_input`, downstream of preprocessor fit.

Therefore B1 cannot be checked in `fit_preprocessor_metadata`. It must run where
the item is actually present: `sample_pool` and `label_table`. This also means
Phase 2's B1 absorbs what Phase 1's plan had filed as Phase-3 "C1" (the
`label_table` distinct-vs-config check) — `label_table`'s item values come from
irreducibly-manual SQL no generator can validate.

## 3. Scope

**In scope:** B1 only — item-value set coverage on `sample_pool` + `label_table`.

**Out of scope (deferred, unchanged from Phase 1 plan):**

- **B2** label-window leakage guard (`apply_start_date`/`apply_end_date`).
- **B3** zero-positive-per-item severity switch. Note: `label_table`'s
  `declared − actual` direction *is* B3 ("declared item has zero labels"); it is
  explicitly **not** treated as an error in Phase 2.
- The `inference` pipeline. Inference also scores `sample_pool`; the same
  `validate_data_consistency` call is trivially reusable there later, but Phase 2
  guards only the `dataset` pipeline (where training data — the costliest
  corruption — is built). No code is written for inference now.

## 4. Locked design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Phase 2 = **B1 only** | YAGNI; B1 is the #1 silent-corruption hole. |
| D2 | Validate **both** `sample_pool` and `label_table` | Item lives in both; they come from two independently hand-written SQL sources that can drift apart from config and from each other. |
| D3 | `sample_pool`: **set equality** vs `resolved_item_values` — both directions hard error | `actual − declared` → silent `-1` corruption. `declared − actual` → product can never be scored/recommended (the mirror bug; equally silent). Both were the user's original bidirectional concern. |
| D4 | `label_table`: **only `actual − declared`** is a hard error | `actual − declared` → label business-logic drift (label maps to an item config doesn't know). `declared − actual` = "declared item has zero labels" = **B3**, deferred — must **not** error in Phase 2. |
| D5 | Scan **only configured windows** | Validate exactly the snap_dates that become the model matrix/eval: `∪(train_snap_dates, val_snap_dates, test_snap_dates, [calibration_snap_dates if enable_calibration])`. Avoids false-positive hard errors from stale historical partitions containing retired products that never enter any split. |
| D6 | **Collect-all, raise once** `DataConsistencyError` | Consistent with Phase 1's `validate_config_consistency`; user fixes every problem in one pass. `DataConsistencyError` already exists (Phase-1 hierarchy, subclasses `ValueError`). |
| D7 | Hook as a **dedicated side-effect node, listed first** in the `dataset` pipeline — no passthrough, `__main__.py` untouched | See §6. Verified Runner semantics guarantee first, unbypassable, fail-fast execution. Data checks are nodes (consistent with the rest of the dataset pipeline); config checks (Phase 1) are CLI-entry. Clean split by *what each needs*. |
| D8 | Fold in the §5 cleanup (`_spark.py` A2 backstop type) | One-line, zero-behaviour, taxonomy-completing; Phase-1 plan explicitly earmarked it for Phase 2. |
| D9 | All predicates key off `schema["item"]`, never literal `"prod_name"` | Channel-name generalization requirement (future rename to `channel_name` needs only schema config). |

## 5. Architecture — components & interfaces

### 5.1 `core/consistency.py` — new pure predicate (single source of truth)

No Spark, no I/O. Pure-set logic, fast to unit-test, the canonical definition of
the B1 invariant.

```python
def item_coverage_errors(
    item: str,
    declared: list[str],          # = resolved_item_values(parameters)
    sample_pool_items: set[str],  # actual distinct item values, configured windows
    label_items: set[str],        # actual distinct item values, configured windows
) -> list[str]:
    """B1 invariant. Returns human-readable error strings (collect-all);
    empty list means OK.

    sample_pool ↔ declared: SET EQUALITY (both directions are errors) [D3].
    label_table: only (label_items - declared) is an error [D4];
                 (declared - label_items) is B3, deferred — NOT reported here.

    Keys off the passed `item` only; never hardcodes 'prod_name' [D9].
    """
```

Error-string content (each names value(s), table, direction, silent
consequence):

- `sample_pool_items − declared` non-empty →
  `"sample_pool has item value(s) {sorted(...)} not in
  schema.categorical_values[{item}] — these encode to -1 (same code as null)
  and silently corrupt training/scoring. Add them to
  schema.categorical_values.{item} in parameters.yaml, or fix sample_pool.sql."`
- `declared − sample_pool_items` non-empty →
  `"schema.categorical_values[{item}] declares value(s) {sorted(...)} that
  sample_pool never produces — they can never be scored/recommended (silent).
  Remove them from config, or fix sample_pool.sql to emit them."`
- `label_items − declared` non-empty →
  `"label_table has item value(s) {sorted(...)} not in
  schema.categorical_values[{item}] — label business logic (label_*.sql)
  produced an item the model config does not know. Reconcile label_*.sql with
  schema.categorical_values.{item}."`

### 5.2 `preprocessing/_spark.py` — new Spark caller

```python
def validate_data_consistency(
    sample_pool: DataFrame,
    label_table: DataFrame,
    parameters: dict,
) -> None:
    """Layer-2 B1 data gate. Side-effect only: raises DataConsistencyError on
    violation, returns None on success. Called as the first node of the
    dataset pipeline [D7]."""
```

Behaviour:

1. `item = get_schema(parameters)["item"]`; `time_col = get_schema(...)["time"]`.
2. `windows = _configured_snap_dates(parameters)` (§5.3).
3. For each of `sample_pool`, `label_table`:
   `{ r[item] for r in df.filter(F.col(time_col).isin(windows))
                          .select(item).distinct().collect()
              if r[item] is not None }`
   (low-cardinality column; map-side partial-distinct collapses to ~tens of
   values per partition before the exchange — one cheap shuffle per table).
4. `errs = item_coverage_errors(item, resolved_item_values(parameters),
   sample_pool_items, label_items)`.
5. If `errs`: `raise DataConsistencyError("Data consistency check failed ("
   + str(len(errs)) + " issue(s)):\n- " + "\n- ".join(errs))`.
6. Else return `None`.

Imports `DataConsistencyError` / `resolved_item_values` locally (lazy) to keep
the existing `_spark → core.schema → core.consistency` cycle-avoidance pattern
already established in Phase 1 (see existing local import at `_spark.py:155`).

### 5.3 `_configured_snap_dates(parameters) -> list` helper

Location: `preprocessing/_spark.py` (private, alongside `validate_data_consistency`).

Returns the de-duplicated union of `pd.Timestamp`-cast dates from:
`dataset.train_snap_dates` + `dataset.val_snap_dates` (default `[]`) +
`dataset.test_snap_dates` (default `[]`) +
(`dataset.calibration_snap_dates` only if `dataset.enable_calibration` is truthy).

Mirrors how `select_train_keys` / `select_val_keys` / `select_test_keys` /
`select_calibration_keys` already read these keys today (verified in
`pipelines/dataset/nodes_spark.py`).

### 5.4 `_spark.py` A2 backstop type unification [D8 / spec §5]

`preprocessing/_spark.py` line ~196: the ranking-task invariant guard
`if item_col and item_col not in feature_columns: raise ValueError(...)` →
`raise DataConsistencyError(...)`. Message text **unchanged**. The local import
of `DataConsistencyError` already exists at `_spark.py:155` (Phase 1), so no new
import. Pure exception-type narrowing: `DataConsistencyError` subclasses
`ValueError`, so `__main__.py`'s `except ValueError`, `pytest.raises(ValueError)`,
and all callers behave identically. This completes the `_spark.py` taxonomy
(`:160 missing_cats`, `:196 item guard`, and the new
`validate_data_consistency` all raise `DataConsistencyError`). Empirically
low-risk: Phase-1 Task 8 did the identical conversion on the sibling `:160`
guard and the Spark suite stayed green.

## 6. DAG wiring (D7) — verified Runner semantics

The check is a **dedicated side-effect node prepended to the `dataset`
pipeline node list**, with no outputs:

```python
# pipelines/dataset/pipeline.py — first element of `nodes`
Node(
    validate_data_consistency,
    inputs=["sample_pool", "label_table", "parameters"],
    outputs=None,                       # Node._normalize(None) -> []
    name="validate_data_consistency",
),
```

No other node changes; no passthrough/renaming; `__main__.py` untouched.

Guarantees, each verified against this repo's own framework (not generic
kedro):

- **Runs first, deterministically.** `core/pipeline.py::_topological_sort`
  (Kahn) seeds `queue = deque(n for n in nodes if in_degree[n] == 0)` by
  iterating `nodes` in insertion order. The validator's inputs are all catalog
  source datasets (`sample_pool`, `label_table`, `parameters`) → `in_degree==0`;
  prepended → popped first.
- **Never pruned.** `__main__._execute_pipeline` runs the *full* pipeline; it
  does not call `Pipeline.only_nodes_with_outputs`. A node whose output nobody
  consumes (here: no output at all) still executes.
- **No-output node is first-class.** `core/runner.py::Runner.run` saves outputs
  only for `len(node.outputs) == 1` / `> 1`; with `0` it executes and saves
  nothing. `Node.__init__` accepts `outputs=None` → `[]`.
- **Fail-fast.** As the first node, `raise DataConsistencyError` propagates
  through `Runner.run` (re-raises) to `_execute_pipeline`'s
  `except Exception: ... raise typer.Exit(code=1)` — before any sampling,
  preprocessing, or join cost.
- **Unbypassable.** Guards both the CLI path and any programmatic
  `Runner().run(pipe, catalog)` — strictly stronger than an `__main__.py`
  pre-flight (which would also have added a third pipeline-specific `if
  pipeline_name ==` branch to `_execute_pipeline`, an orchestration-layer
  bolt-on for what is a data check).
- **No premature dataset release.** `sample_pool`/`label_table` are catalog
  source datasets (not in `catalog._auto_created`) and are re-consumed by later
  `select_*`/`build_model_input` nodes, so `Runner`'s intermediate-release
  logic does not free them after the validator.

## 7. Testing strategy

Per CLAUDE.md 測試效能: run only `tests/test_core` +
`tests/test_pipelines/test_dataset`; never the full ~33-min suite.

**Fast, no Spark — `tests/test_core/test_consistency.py` (extend):**

- `item_coverage_errors`: equal sets → `[]`.
- `sample_pool_items − declared` → error string naming the value + "-1".
- `declared − sample_pool_items` → error string ("can never be scored").
- `label_items − declared` → error string (label business-logic drift).
- `declared − label_items` (B3 case) → **NOT** in result (asserts deferral).
- Channel-name: `item="channel_name"` path produces equivalent behaviour.
- Collect-all: multiple simultaneous violations all present in the list.

**Spark fixture — `tests/test_pipelines/test_dataset/` (new or extend):**

- Tiny `sample_pool`/`label_table` fixtures across several snap_dates;
  `validate_data_consistency` returns `None` when consistent.
- Item value present only in a **non-configured** snap_date → **not** flagged
  (asserts D5 window filter).
- Item value in a configured window but undeclared → raises
  `DataConsistencyError` with the offending value in the message.
- A2 backstop: characterization test that the `_spark.py:196` guard now raises
  `DataConsistencyError` (still an `isinstance(_, ValueError)`), message
  substring preserved.

**Pipeline wiring — `tests/test_pipelines/test_dataset/`:**

- `create_pipeline()` first node is `validate_data_consistency` with
  `inputs == ["sample_pool", "label_table", "parameters"]` and `outputs == []`;
  `Pipeline(nodes).nodes[0]` is that node (verifies first-execution ordering).

## 8. File map

| File | Change |
|------|--------|
| `src/recsys_tfb/core/consistency.py` | **Add** pure `item_coverage_errors`. |
| `src/recsys_tfb/preprocessing/_spark.py` | **Add** `validate_data_consistency` + `_configured_snap_dates`; **modify** `:196` `ValueError` → `DataConsistencyError`. |
| `src/recsys_tfb/pipelines/dataset/pipeline.py` | **Prepend** the `validate_data_consistency` node. |
| `tests/test_core/test_consistency.py` | **Extend** with `item_coverage_errors` cases. |
| `tests/test_pipelines/test_dataset/…` | **Add** Spark + wiring tests. |
| `docs/superpowers/plans/2026-05-17-config-consistency-validation.md` | **Update** "Future Phases": mark Phase 2 done; record the hook-point correction (node, not `fit_preprocessor_metadata`) and that B1 absorbed C1's label-table check. |
| `CLAUDE.md` | **Extend** the `## Config consistency gate` section: add the Layer-2 data gate (node, dataset pipeline). |

No new dependencies (production constraint: no network, no extra packages).

## 9. Open environment blocker (not a design issue)

`/Users/curtislu/projects/recsys_tfb/.venv` is currently a **self-referential
symlink** (`readlink .venv` → itself), so all `python`/`pytest` invocations
loop. The real virtualenv directory is gone (no backup found). This does **not**
affect this spec, but **the implementation/test phase cannot start until the
venv is repaired** (recreate the editable-install venv with the pinned
toolchain in CLAUDE.md / pyproject). To be resolved with the user before
writing-plans → subagent-driven-development.
