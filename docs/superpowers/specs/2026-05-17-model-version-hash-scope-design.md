# Design: scope the `model_version` hash to model-defining config

- **Date:** 2026-05-17
- **Status:** Approved (design)
- **Area:** `src/recsys_tfb/core/versioning.py`

## Problem

`compute_model_version` (`src/recsys_tfb/core/versioning.py:106`) hashes the
**entire** merged `parameters_training.yaml` dict:

```python
canonical = yaml.dump(params, sort_keys=True, default_flow_style=False)
```

The caller (`src/recsys_tfb/__main__.py:435`) passes
`config.get_parameters_by_name("parameters_training")`, which is the full
file (base + env overlay) with no filtering. Therefore **every key** in
`parameters_training.yaml` busts `model_version`, including keys that have
zero effect on the trained artifact:

| Key | Affects trained model? | Currently busts `model_version`? |
|---|---|---|
| `training.algorithm_params.verbosity` | no (pure logging) | yes (wrong) |
| `training.algorithm_params.log_period` | no (pure logging) | yes (wrong) |
| `training.algorithm_params.num_threads` | treated as no (see Decision 1) | yes (wrong) |
| top-level `mlflow.*` | no (tracking only) | yes (wrong) |
| top-level `cache.root` | no (env-relative driver-local path) | yes (wrong) |
| top-level `spark.*` | no (executor config) | yes (wrong) |
| `training.algorithm` / `objective` / `metric` / `n_trials` / `num_iterations` / `early_stopping_rounds` / `search_space` / `calibration` / `final_model_strategy` | yes | yes (correct) |

`cache.root` is the most acute: it is an environment-relative local path
(dev vs production differ), so running the same logical model on a different
machine produces a different `model_version` for an identical model.

`compute_base_dataset_version` is **not** affected: `parameters_dataset.yaml`
carries only a top-level `dataset:` key and that function already strips
`ALL_SAMPLING_KEYS`. The defect is specific to the training layer because
`parameters_training.yaml` uniquely carries `spark` / `mlflow` / `cache`
top-level blocks plus runtime knobs under `algorithm_params`.

## Decisions

1. **`num_threads` is treated as model-irrelevant** and excluded from the
   hash, alongside `verbosity` and `log_period`. (LightGBM is not guaranteed
   bitwise-identical across thread counts without `deterministic=true` +
   `force_*_wise`, which this config does not set, so this is a pragmatic
   choice accepting a small silent-collision risk in exchange for stability.
   Per `CLAUDE.md`, `num_threads` is pinned to the production core count and
   rarely changes.)
2. **Mechanism: section-scoped denylist (Approach C).** Hash only the
   `training:` subtree (top-level ops blocks excluded structurally), then
   apply a small inner denylist for the three logging/threading knobs.
3. **Clean break for backward compatibility.** All existing `model_version`
   hashes change. No migration tooling. Old `models/<hash>/` dirs are left
   as-is; the next training run plus a manual re-promote produces the new
   hash. This fits the ephemeral dev-cluster (`--env production`).

### Why Approach C over a flat denylist or an allowlist

- **Flat denylist (rejected):** consistent with the existing
  `compute_base_dataset_version` style and low test churn, but a new ops/log
  key added later and forgotten silently busts the version (safe direction,
  but needs ongoing maintenance with no structural guarantee).
- **Allowlist (rejected):** noise-proof, but **dangerous failure
  direction** — a new genuinely model-affecting hyperparam that is forgotten
  causes two different models to share one `model_version` (silent
  collision → wrong model promoted/cached).
- **Section-scoped denylist (chosen):** matches the real invariant — *the
  `training:` block is the sole model-defining config; everything else is
  ops*. Safe failure direction at every level: a new top-level ops block is
  auto-excluded; a new key under `training:` defaults to busting the version
  (safe over-invalidation, never collision); only a new pure-logging knob
  needs a one-line denylist addition.

## Design

### 1. Core change — `src/recsys_tfb/core/versioning.py`

Add a public constant (parallel to the existing `TRAIN_SAMPLING_KEYS` /
`CALIBRATION_SAMPLING_KEYS` style in the same module):

```python
MODEL_VERSION_IRRELEVANT_PARAMS: frozenset[str] = frozenset({
    "verbosity", "log_period", "num_threads",
})
```

Add a private payload extractor:

```python
def _model_version_payload(params: dict) -> dict:
    """Hash-relevant view: only the training: block, minus pure-ops knobs.

    Top-level spark/mlflow/cache (and any future ops block) are excluded
    structurally by narrowing to training:. Within algorithm_params, the
    logging/threading knobs in MODEL_VERSION_IRRELEVANT_PARAMS are dropped.
    Deep-copies so the caller's params dict is never mutated.
    """
    training = params.get("training")
    if not isinstance(training, dict):
        return {}
    training = copy.deepcopy(training)
    ap = training.get("algorithm_params")
    if isinstance(ap, dict):
        for k in MODEL_VERSION_IRRELEVANT_PARAMS:
            ap.pop(k, None)
    return {"training": training}
```

`compute_model_version` changes one line — it builds `canonical` from
`_model_version_payload(params)` instead of `params`. Everything else is
unchanged: `yaml.dump(..., sort_keys=True, default_flow_style=False)`,
`sha256(...).hexdigest()[:8]`, and concatenation with
`base_dataset_version` / `train_variant_id` / `calibration_variant_id`
(the existing `calibration_variant_id is None` ⇔ omitted behavior is
preserved).

### 2. Provenance is preserved

`src/recsys_tfb/__main__.py:459` writes the **full** `params_training` into
`manifest.json` for audit. The deep-copy in `_model_version_payload`
guarantees the manifest still records complete params — only the *hash
input* is scoped, not what is stored. No change at the call site.

### 3. Tests — `tests/test_core/test_versioning.py` (`TestComputeModelVersion`)

- **Update** `test_different_params_different_hash`: the current flat
  `{"lr": 0.01}` shape is unrealistic and would now hash to an empty
  payload. Rewrite to a nested realistic shape, e.g.
  `{"training": {"algorithm_params": {"learning_rate": 0.01}}}` vs `0.05`.
- **Add:** mutating `verbosity` / `log_period` / `num_threads` under
  `training.algorithm_params` → hash unchanged.
- **Add:** mutating a real hyperparam (`learning_rate`, `num_iterations`,
  `search_space`, `calibration`) → hash changes.
- **Add:** adding or changing a top-level `spark` / `mlflow` / `cache`
  block → hash unchanged.
- **Add:** the caller's `params` dict is not mutated by
  `compute_model_version`.
- **Keep unchanged:** `test_returns_8_char_hex`,
  `test_same_inputs_same_hash`, `test_different_base_different_hash`,
  `test_different_train_variant_different_hash`,
  `test_calibration_variant_affects_hash`,
  `test_calibration_none_equivalent_to_omitted` (independent of the noise
  keys).

All pure dict/hashlib unit tests — sub-second, no Spark, consistent with
the project's "make tests fast" guidance.

### 4. Documentation

- `compute_model_version` docstring + `versioning.py` module docstring:
  state the scoping rule (`training:` block minus pure-ops knobs) and the
  safe-failure-direction rationale.
- `conf/base/parameters_training.yaml`: short comments near the
  `spark` / `mlflow` / `cache` blocks and the three knobs noting they do
  **not** affect `model_version` (matches the file's existing
  richly-commented style).

## Out of scope (explicit)

- No change to `compute_base_dataset_version` / `compute_train_variant_id` /
  `compute_calibration_variant_id` — their input is already clean.
- No migration or hash-aliasing tooling — clean break (Decision 3).
- No change to `src/recsys_tfb/core/consistency.py` — this is a
  hashing-scope concern, not an A1–A6 config-consistency invariant.

## Risks and mitigations

- **Risk:** a future pure-ops knob added under `training.algorithm_params`
  busts `model_version` until added to `MODEL_VERSION_IRRELEVANT_PARAMS`.
  **Mitigation:** safe failure direction (over-invalidation, never
  collision); one-line fix; documented in the constant's vicinity.
- **Risk:** the `num_threads` exclusion can let two numerically-different
  models share a `model_version`. **Mitigation:** accepted per Decision 1;
  `num_threads` is pinned in production and the divergence is small.
- **Risk:** clean break orphans existing `models/<hash>/` dirs.
  **Mitigation:** accepted per Decision 3; dev-cluster is ephemeral and
  promotion is already a manual step.
