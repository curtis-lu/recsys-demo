# Configurable HPO Search Space — Design

**Status:** Approved (design discussion complete; phased implementation pending)
**Worktree / branch:** `.worktrees/configurable-hpo-search-space` / `feat/configurable-hpo-search-space`
**Author:** brainstorming session 2026-05-18

---

## Problem Statement

`tune_hyperparameters()` in `src/recsys_tfb/pipelines/training/nodes.py` hard-codes
the Optuna search space: exactly six hyperparameters, each with a fixed
`suggest_*` call and fixed kwargs (`learning_rate` is `suggest_float(..., log=True)`;
`num_leaves`/`max_depth`/`min_child_samples` are bare `suggest_int`;
`subsample`/`colsample_bytree` are bare `suggest_float`). The YAML
`training.search_space` only carries `{low, high}` per param — it cannot express
`step`, `log`, categorical choices, inter-parameter dependencies, or a different
algorithm. The training objective is also effectively pinned to `binary`
(early-stopping is wired to `binary_logloss` semantics via the val
`lgb.Dataset`), so a learning-to-rank objective is not reachable even though the
business target (per-customer mAP over a query group) is inherently a ranking
problem.

Four capabilities are required, prioritised by the user:

1. **(P1, first)** Switch the training objective to `lambdarank` / `rank_xendcg`
   with the LightGBM query-`group` plumbed through training/early-stopping.
2. Declarative search space in YAML: per-parameter `type`, and for int/float the
   finer Optuna knobs (`step`, `log`); categorical `choices`.
3. User-configured **dependencies** between hyperparameters (conditional
   presence *and* bounds derived from earlier-sampled params).
4. **XGBoost** usable as an alternative algorithm (last; gated on the prod env
   having the package — see Non-Goals).

The user explicitly asked for a **phased** plan, with lambdarank first, and
accepted a **one-time `model_version` bump** for the schema restructure.

---

## Goals / Non-Goals

**Goals**

- `tune_hyperparameters` builds its trial params *entirely* from
  `training.search_space` YAML — no hard-coded parameter names or `suggest_*`
  kwargs left in the node.
- `lambdarank`/`rank_xendcg` selectable via `training.algorithm_params.objective`
  with the query group correctly applied to train, train_dev, and the
  early-stopping val dataset; ranking-appropriate early-stopping metric.
- Every new config invariant added as a pure predicate in
  `src/recsys_tfb/core/consistency.py` and surfaced at CLI entry through
  `validate_config_consistency` (collect-all → `ConfigConsistencyError`). No
  ad-hoc validation scattered in pipelines (project rule).
- No new third-party packages (production constraint: no network, no extra
  installs). Expression evaluation uses the stdlib `ast` module only.
- Evaluation semantics unchanged: the business metric stays per-customer mAP
  (`compute_mean_ap`); only the *training* objective and *HPO selection
  early-stopping metric* change.

**Non-Goals**

- Changing the offline evaluation pipeline, `compute_mean_ap`, or the test/eval
  Hive write path. HPO trial selection still maximises val mAP.
- Auto-tuning the *number* of trials, samplers, or pruners (out of scope; can be
  a later spec).
- Shipping XGBoost to production. Phase 4 only lands the adapter + handle
  generalisation + YAML wiring; **adding the `xgboost` dependency and pinning
  its version is the user's action** (they will record the pin in CLAUDE.md /
  `pyproject.toml` later). Phase 4 is gated on that and on the prod Hadoop env
  actually having the package — the "no extra installs" constraint makes this a
  hard, user-owned prerequisite, not something this design assumes away.

---

## Approach (selected: Approach A)

**Approach A — declarative ordered ParamSpec list + restricted stdlib-`ast`
expression evaluator.** Selected over (B) a thin per-param dict still
interpreted by hard-coded dispatch, and (C) pulling in an expression library
(`asteval`/`numexpr` — rejected outright by the no-extra-packages constraint).

- The search space is an **ordered list** of `ParamSpec` maps. Order is
  meaningful: Optuna define-by-run samples in list order, so a later param's
  `when`/bounds may reference any earlier param by name.
- A small `build_trial_params(trial, search_space, context)` function replaces
  the hard-coded `objective()` body: it walks the list, evaluates any `when`
  guard and expression-valued bounds via `safe_eval`, and dispatches to the
  matching `trial.suggest_*`.
- `safe_eval` is a **restricted stdlib `ast`** evaluator: parse → walk an
  allow-listed node set (literals, arithmetic, comparison, boolean ops, names
  bound to the context dict, a tiny function allowlist e.g. `min`/`max`). No
  `eval`/`exec`, no imports, no attribute access, no arbitrary calls. This is
  the only viable path under the production constraints and is small enough to
  unit-test exhaustively.

This keeps the *interpreter* (generic, tested once) separate from the *policy*
(YAML, user-owned), which is the isolation the brainstorming guidance asks for.

---

## Architecture

### New / changed units

| Unit | Location | Responsibility |
|---|---|---|
| `safe_eval(expr, context)` | new `src/recsys_tfb/core/safe_eval.py` | Evaluate a restricted arithmetic/boolean expression string against a name→value dict. Pure, no I/O, no Spark. |
| `build_trial_params(trial, search_space, context)` | new `src/recsys_tfb/pipelines/training/search_space.py` | Turn the ParamSpec list + an Optuna trial into the trial param dict. Uses `safe_eval` for `when`/expression bounds. A dedicated module (not inside `nodes.py`) so it is unit-testable without importing the heavy training-node graph. |
| `group_utils` | new `src/recsys_tfb/core/group_utils.py` | Algorithm-agnostic: given the per-row int64 group-id array from `extract_Xy_with_groups`, produce a stable sort permutation making each group contiguous and the LightGBM/XGBoost run-length `group` counts. |
| ranking-objective plumbing | `models/lightgbm_adapter.py`, `pipelines/training/nodes.py` | Build `lgb.Dataset` with `.set_group(counts)` for train / train_dev / trial-val; default a ranking metric for early stopping. |
| new consistency predicates | `src/recsys_tfb/core/consistency.py` | Search-space schema validity; ranking-objective coherence; expression static-reference checks. Aggregated by `validate_config_consistency`. |
| `XGBoostAdapter` | new `src/recsys_tfb/models/xgboost_adapter.py` (Phase 4) | `ModelAdapter` impl registered as `"xgboost"`; reuses `group_utils`. |
| `ModelAdapter.prepare_train_inputs` return type | `models/base.py` | Generalise `tuple[LgbDatasetHandle, LgbDatasetHandle]` → a generic train-input handle pair (Phase 4). |

### Data flow (HPO trial, post-design)

```
val parquet ──▶ extract_Xy_with_groups ──▶ (X_v, y_v, group_ids_v)   [unchanged; mAP scoring]
train.bin / train_dev.bin ──▶ lgb.Dataset(.set_group?) ──▶ adapter.train(params)
                                              ▲
build_trial_params(trial, search_space, ctx) ─┘  (params merged with algorithm_params)
                                              │
            ctx = {already-sampled params} + static config (e.g. objective)
adapter.predict(X_v) ──▶ compute_mean_ap(group_ids_v, y_v, y_pred)  [unchanged]
```

The val scoring path keeps the **per-row group-id** form (`compute_mean_ap`
consumes ids, not run-length counts). Only the *Dataset-building* path needs
`group_utils` to (a) stably reorder rows so each group is contiguous and
(b) emit run-length counts for `.set_group`.

---

## Phased Plan

Phases are independent enough to land and review separately; order is fixed.

### Phase 1 — lambdarank / rank_xendcg objective + group plumbing  *(user priority; independent of the search-space refactor)*

The hard-coded six-param block stays as-is in Phase 1. Scope:

1. **`group_utils`** (algorithm-agnostic): `sort_perm, counts = to_contiguous_groups(group_ids)`.
   `group_ids` is the existing `extract_Xy_with_groups` `ngroup()` int64 array
   (query group = `schema["time"]` + `schema["entity"]` = `(snap_date, cust_id)`).
   Stable sort by group id; `counts` = run-length per group in sorted order.
2. **Train/dev Dataset carries group.** `LightGBMAdapter.prepare_train_inputs`
   currently uses `extract_Xy` (no groups) and `save_binary`. For a ranking
   objective it must instead read groups, reorder X/y, build the `lgb.Dataset`
   with `categorical_feature` + `params={"feature_pre_filter": False}` (kept),
   call `.set_group(counts)`, then `save_binary` (group is persisted in the
   `.bin`). `train_dev` keeps `reference=ds_train` for aligned binning.
3. **Trial-val dataset carries group.** In `tune_hyperparameters`, the
   early-stopping dataset (`ds_dev`) must also have group set; for lambdarank,
   early stopping must use a **ranking metric** (`ndcg`) — `binary_logloss` is
   invalid for the ranking objective. When `objective ∈ {lambdarank,
   rank_xendcg}` and no `metric` is set, default `metric: ndcg` (with a
   configurable `ndcg_eval_at`).
4. **lgb-binary cache key — decision.** The lgb `.bin` cache dir is
   `cache_root/<base_dataset_version>/train_variants/<train_variant_id>/lgb/`
   and is **not** keyed by `model_version`; `objective` lives in
   `algorithm_params` which feeds `model_version`, not the binary path. A
   binary built for `binary` and silently reused for `lambdarank` would lack
   group → wrong/again-silent failure. **Resolution: add a ranking-vs-not
   discriminator to the binary sub-path** (e.g. `lgb/binary/` vs
   `lgb/ranking/`), derived from the objective family. This preserves the exact
   current binary-objective behaviour and row order (lower blast radius than
   "always sort + always set group", which would change bagging row selection
   for the default objective). The discriminator is the objective *family*, not
   the full objective string, so `lambdarank`↔`rank_xendcg` share a binary
   (group-bearing, identical layout).
5. **Consistency predicate(s)** in `core/consistency.py`: ranking objective ⟹
   metric is a ranking metric (no contradictory `binary_logloss`); the schema
   actually defines a query group (`time` + non-empty `entity`). Wire into
   `validate_config_consistency`.
6. **Evaluation untouched.** `compute_mean_ap` and the eval/test pipeline do not
   change; HPO still selects the trial maximising val mAP.

**Config delta (Phase 1):** `training.algorithm_params.objective: lambdarank`
(already free-form) + optional `metric: ndcg`, `ndcg_eval_at: [...]`. No
search-space schema change yet.

### Phase 2 — declarative search-space schema + `safe_eval` + `build_trial_params` + YAML migration

1. **YAML schema:** `training.search_space` becomes an **ordered list** of
   ParamSpec maps:
   - `name` (required) — Optuna suggest name **and** the algorithm param key
     (must map 1:1 to a native LightGBM/XGBoost param so `finalize_model`'s
     `best_params` merge stays correct).
   - `type` ∈ `int | float | categorical`.
   - int/float: `low`, `high`, optional `step`, optional `log` (bool).
   - categorical: `choices` (non-empty list).
   - (`when` and expression-valued `low/high/step` added in Phase 3 — schema
     accepts the keys now, evaluator wired in Phase 3.)
2. **`safe_eval` module** built here as the foundation (exercised in Phase 3).
   Fully unit-tested in isolation: accepts allow-listed nodes only; rejects
   imports/attribute/`__`/calls outside the allowlist with a clear error.
3. **`build_trial_params`** (new `pipelines/training/search_space.py`) replaces
   the hard-coded `objective()` block: iterate the list in order, dispatch to
   `suggest_int`/`suggest_float`/`suggest_categorical` with configured kwargs.
   `tune_hyperparameters` imports and calls it; the module imports nothing from
   `nodes.py` (testable in isolation).
4. **Consistency predicates** (collect-all, CLI entry): required keys per
   `type`; `type` in the allowed set; no duplicate `name`; int `step` positive
   int; `log: true` requires `low > 0`; categorical `choices` non-empty.
5. **YAML migration:** rewrite `conf/base/parameters_training.yaml`
   `search_space:` to the list form preserving the **exact** current six params
   and bounds (`learning_rate` float `log: true` 0.001–0.1; `num_leaves` int
   4–64; `max_depth` int 3–8; `min_child_samples` int 5–100; `subsample` float
   0.6–1.0; `colsample_bytree` float 0.6–1.0). This restructures the hashed
   `training:` block → **one-time `model_version` bump** (accepted by user;
   `_model_version_payload` hashes the whole `training:` block, so structural
   change is expected and safe — over-invalidation, never a silent collision).

### Phase 3 — hyperparameter dependencies (`when` + expression-valued bounds)

Both forms (user chose "both"):

- `when: "<expr>"` — param is suggested only when `safe_eval(expr, ctx)` is
  truthy; skipped params are absent from trial params (algorithm default
  applies). Optuna define-by-run supports conditional spaces natively.
- `low` / `high` / `step` may be a number **or** an expression string evaluated
  by `safe_eval` against `ctx` = already-sampled params (+ static config).
  List order guarantees referenced params were already sampled.
- **Consistency predicates extended:** every `when`/bound expression must parse
  under the restricted grammar and reference only (a) names of params *earlier*
  in the list and (b) an allow-listed static-context key set. Forward/unknown
  references fail at CLI entry with an actionable message.

### Phase 4 — XGBoost adapter  *(last; gated on user pinning the dependency)*

1. Generalise `ModelAdapter.prepare_train_inputs` return type from
   `tuple[LgbDatasetHandle, LgbDatasetHandle]` to a generic train-input handle
   pair (introduce a neutral handle abstraction; `LgbDatasetHandle` becomes one
   implementation).
2. `XGBoostAdapter(ModelAdapter)` registered as `"xgboost"` in
   `ADAPTER_REGISTRY`. Ranking via `rank:pairwise` / `rank:ndcg` reusing
   `group_utils` run-length counts on `DMatrix.set_group` (this is *why*
   `group_utils` is algorithm-agnostic in Phase 1).
3. `tune_hyperparameters` already dispatches through `get_adapter(algorithm)`
   and the search space is declarative, so the remaining work is the adapter,
   the handle generalisation, and XGBoost-named params in YAML.
4. **Prerequisite (user-owned):** add and pin `xgboost` in `pyproject.toml` and
   record the version in CLAUDE.md, and confirm the production Hadoop env has
   it. Phase 4 does not proceed until this is done.

---

## Consistency Invariants (single source of truth)

All new invariants are pure predicates in `core/consistency.py`, consumed by
`validate_config_consistency` (Layer-1, CLI entry, collect-all →
`ConfigConsistencyError`), consistent with the existing A1–A6 / B1 design. New
IDs to be assigned in the implementation plan; conceptually:

- Ranking-objective coherence (Phase 1): ranking objective ⟺ ranking
  early-stopping metric; schema defines a non-empty query group.
- Search-space schema validity (Phase 2): per-`type` required keys; unique
  `name`; positive int `step`; `log` ⟹ `low > 0`; non-empty `choices`.
- Expression safety/reference (Phase 3): every expression parses under the
  restricted grammar and references only earlier params + allow-listed context.

No predicate hardcodes parameter names or `"prod_name"`; the query group keys
off `schema["time"]`/`schema["entity"]`, consistent with the channel-name
generalisation rule already in the codebase.

---

## `model_version` Impact

- Phase 2's `search_space` restructure changes the hashed `training:` block →
  **one explicit, accepted bump**. Documented here so the bump is expected, not
  a surprise.
- Phase 1 objective/metric changes already flow through `algorithm_params` into
  `model_version` (correctly — a different objective is a different model).
- The lgb-binary cache (Phase 1, item 4) is keyed independently of
  `model_version`; the ranking discriminator in the sub-path is the mechanism
  that prevents a stale group-less binary from being reused — this is the one
  place where relying on the `model_version` bump alone is **insufficient** and
  is handled explicitly.

---

## Testing Strategy

Per the project test-performance rule (make tests fast; run targeted; no full
slow suite, background long commands):

- `safe_eval`, `build_trial_params`, `group_utils`, and the new consistency
  predicates are **pure Python** → fast non-Spark unit tests covering the
  allow-list boundary (rejection cases), define-by-run ordering, conditional
  skips, expression bounds, and every schema-validation failure message.
- Phase 1 needs one **small** LightGBM integration test (tiny synthetic data,
  few groups) asserting: group counts sum to row count, ranking objective
  trains, early stopping uses the ndcg metric, and the binary-objective path is
  byte-for-byte unchanged (cache discriminator).
- Run only the touched test files per change; verify with SHA-based
  `git diff <base>..<head>` + targeted grep; background anything that could
  exceed ~2 min.

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Stale group-less lgb binary reused after objective switch (silent) | Ranking-vs-not discriminator in the `.bin` sub-path (Phase 1, item 4). |
| `safe_eval` sandbox escape | Allow-list AST node/func set; reject everything else; exhaustive rejection unit tests; no `eval`/`exec`/import/attr. |
| Define-by-run order vs dependency references | List is ordered; predicate rejects forward/unknown references at CLI entry. |
| Optuna `best_params` ↔ algorithm param-name drift | `name` is contractually both the suggest name and the native param key; documented + validated. |
| XGBoost not in prod env | Phase 4 gated on user pinning the dep + confirming prod availability; spec states this as a hard prerequisite, not an assumption. |
| Unexpected `model_version` churn | Single, documented, accepted bump at Phase 2; phases 1/3/4 changes flow through the existing hash correctly. |

---

## Resolved Decisions (from design discussion)

- Phased, **lambdarank first** (Phase 1), independent of the search-space
  refactor.
- Both dependency forms required (`when` + expression-valued bounds) — Phase 3.
- XGBoost is the **last** phase; version pin is the user's later action.
- One-time `model_version` bump for the schema restructure is **accepted**.
- Approach A (declarative ordered ParamSpec list + restricted stdlib-`ast`
  `safe_eval`) selected; no new packages.
