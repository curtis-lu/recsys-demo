# Scope `model_version` Hash to Model-Defining Config — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `compute_model_version` hash only the model-defining `training:` config so changing ops-only knobs (`spark`/`mlflow`/`cache`, `verbosity`/`log_period`/`num_threads`) no longer orphans an otherwise-identical model.

**Architecture:** Section-scoped denylist (Approach C). A new private `_model_version_payload(params)` narrows to the `training:` subtree (top-level ops blocks excluded structurally) and pops the three pure logging/threading knobs from `algorithm_params`, deep-copying so the caller's dict (still written full to `manifest.json`) is untouched. `compute_model_version` hashes that payload instead of raw `params`; everything else (yaml.dump/sha256/variant concat) is unchanged.

**Tech Stack:** Python 3.10, pytest 7.3.1. Pure dict/hashlib unit tests — no Spark.

**Spec:** `docs/superpowers/specs/2026-05-17-model-version-hash-scope-design.md`

---

## Conventions (worktree SOP — use verbatim)

All commands run with this exact prefix (the project's worktree/venv SOP — bare `python`/`pytest` silently targets main's `src`):

```
PYRUN = PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/model-version-hash-scope/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python
WT    = /Users/curtislu/projects/recsys_tfb/.worktrees/model-version-hash-scope
```

Git is always `git -C $WT …`. Commits use an explicit pathspec (`-- <files>`) so the graphify post-commit hook's `GRAPH_REPORT.md` churn is never co-committed.

## File Structure

- **Modify** `src/recsys_tfb/core/versioning.py` — add `MODEL_VERSION_IRRELEVANT_PARAMS` constant, add `_model_version_payload` helper, rewire `compute_model_version`, update module + function docstrings.
- **Modify** `tests/test_core/test_versioning.py` — add `import copy`; rewrite `test_different_params_different_hash`; add 4 tests to `TestComputeModelVersion`.
- **Modify** `conf/base/parameters_training.yaml` — single authoritative header note (DRY; avoids scattering 4 comments, consistent with the project's single-source-of-truth ethos).

---

## Task 1: Scope the hash to the `training:` block (TDD)

**Files:**
- Test: `tests/test_core/test_versioning.py` (class `TestComputeModelVersion`, lines 252-285; imports lines 3-7)
- Modify: `src/recsys_tfb/core/versioning.py` (constants after line 42; replace `compute_model_version` lines 106-118)

- [ ] **Step 1: Write the failing tests**

In `tests/test_core/test_versioning.py`, add `import copy` to the import block. Replace:

```python
import json
import re
from unittest.mock import patch
```

with:

```python
import copy
import json
import re
from unittest.mock import patch
```

Replace the existing `test_different_params_different_hash` (lines 272-275):

```python
    def test_different_params_different_hash(self):
        a = compute_model_version({"lr": 0.01}, "base1234", "trai1234")
        b = compute_model_version({"lr": 0.05}, "base1234", "trai1234")
        assert a != b
```

with (the old flat `{"lr": ...}` shape is unrealistic and now hashes to an empty payload):

```python
    def test_different_params_different_hash(self):
        a = compute_model_version(
            {"training": {"algorithm_params": {"learning_rate": 0.01}}},
            "base1234", "trai1234",
        )
        b = compute_model_version(
            {"training": {"algorithm_params": {"learning_rate": 0.05}}},
            "base1234", "trai1234",
        )
        assert a != b
```

Then add these four tests at the end of `class TestComputeModelVersion`, immediately after `test_calibration_none_equivalent_to_omitted` (after line 285):

```python
    def test_logging_threading_knobs_do_not_affect_hash(self):
        base = {"training": {"algorithm_params": {"learning_rate": 0.01}}}
        noisy = {
            "training": {
                "algorithm_params": {
                    "learning_rate": 0.01,
                    "verbosity": -1,
                    "log_period": 100,
                    "num_threads": 4,
                }
            }
        }
        assert compute_model_version(base, "b", "t") == compute_model_version(
            noisy, "b", "t"
        )

    def test_relevant_hyperparam_changes_hash(self):
        a = {"training": {"num_iterations": 500}}
        b = {"training": {"num_iterations": 800}}
        assert compute_model_version(a, "b", "t") != compute_model_version(
            b, "b", "t"
        )

    def test_top_level_ops_blocks_do_not_affect_hash(self):
        bare = {"training": {"algorithm_params": {"learning_rate": 0.01}}}
        with_ops = {
            "training": {"algorithm_params": {"learning_rate": 0.01}},
            "spark": {"app_name": "x"},
            "mlflow": {"experiment_name": "y", "tracking_uri": "z"},
            "cache": {"root": "/some/local/path"},
        }
        assert compute_model_version(bare, "b", "t") == compute_model_version(
            with_ops, "b", "t"
        )

    def test_caller_params_not_mutated(self):
        params = {
            "training": {
                "algorithm_params": {"learning_rate": 0.01, "verbosity": -1}
            },
            "spark": {"app_name": "x"},
        }
        snapshot = copy.deepcopy(params)
        compute_model_version(params, "b", "t")
        assert params == snapshot
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
PYTHONPATH=$WT/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  $WT/tests/test_core/test_versioning.py::TestComputeModelVersion -q
```

Expected: FAIL. `test_logging_threading_knobs_do_not_affect_hash`, `test_top_level_ops_blocks_do_not_affect_hash`, `test_caller_params_not_mutated` fail (current code hashes the full dict, so equal-expected pairs differ and the caller dict is the same object but not stripped — actually unmodified, so that one may pass; the two equality tests definitely fail). `test_different_params_different_hash` and `test_relevant_hyperparam_changes_hash` pass against current code.

- [ ] **Step 3: Write the implementation**

In `src/recsys_tfb/core/versioning.py`, add the constant directly after the `ALL_SAMPLING_KEYS` line (line 42), before `def _hash8`:

```python


# Keys under training.algorithm_params that do NOT affect the trained model
# (pure logging / threading). Excluded from the model_version hash so changing
# them does not orphan an otherwise-identical model. num_threads is treated as
# irrelevant by decision: LightGBM is not guaranteed bitwise-identical across
# thread counts, but it is pinned to the production core count and rarely
# changes (see the design spec, Decision 1).
MODEL_VERSION_IRRELEVANT_PARAMS: frozenset[str] = frozenset({
    "verbosity",
    "log_period",
    "num_threads",
})
```

Replace the whole `compute_model_version` function (lines 106-118):

```python
def compute_model_version(
    params: dict,
    base_dataset_version: str,
    train_variant_id: str,
    calibration_variant_id: str | None = None,
) -> str:
    """Compute model version ID from training params and dataset variant IDs."""
    canonical = yaml.dump(params, sort_keys=True, default_flow_style=False)
    parts = [canonical, base_dataset_version, train_variant_id]
    if calibration_variant_id is not None:
        parts.append(calibration_variant_id)
    combined = "".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:8]
```

with:

```python
def _model_version_payload(params: dict) -> dict:
    """Return the model-defining view of training params for hashing.

    Only the ``training:`` block defines the trained artifact; top-level
    ``spark`` / ``mlflow`` / ``cache`` (and any future ops block) are excluded
    structurally by narrowing here. Within ``training.algorithm_params`` the
    pure logging/threading knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS`` are
    dropped. A new key *under* ``training:`` defaults to being included — safe
    over-invalidation, never a silent ``model_version`` collision.

    Deep-copies so the caller's params dict is never mutated: the full,
    unscoped params are still written to ``manifest.json`` for provenance.
    """
    training = params.get("training")
    if not isinstance(training, dict):
        return {}
    training = copy.deepcopy(training)
    ap = training.get("algorithm_params")
    if isinstance(ap, dict):
        for key in MODEL_VERSION_IRRELEVANT_PARAMS:
            ap.pop(key, None)
    return {"training": training}


def compute_model_version(
    params: dict,
    base_dataset_version: str,
    train_variant_id: str,
    calibration_variant_id: str | None = None,
) -> str:
    """Compute model version ID from model-defining training params + variants.

    Only the model-defining subset of ``params`` is hashed (see
    :func:`_model_version_payload`): the ``training:`` block minus the pure
    logging/threading knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS``. Changing
    ops-only config (``spark`` / ``mlflow`` / ``cache``, ``verbosity``,
    ``log_period``, ``num_threads``) therefore does not change the version.
    """
    canonical = yaml.dump(
        _model_version_payload(params),
        sort_keys=True,
        default_flow_style=False,
    )
    parts = [canonical, base_dataset_version, train_variant_id]
    if calibration_variant_id is not None:
        parts.append(calibration_variant_id)
    combined = "".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:8]
```

(`import copy` is already present at line 17 of `versioning.py` — no import change needed there.)

- [ ] **Step 4: Run the full test file to verify it passes**

Run:

```bash
PYTHONPATH=$WT/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  $WT/tests/test_core/test_versioning.py -q
```

Expected: PASS — 70 passed (66 baseline + 4 new; `test_different_params_different_hash` rewritten, not added). Confirm `test_returns_8_char_hex`, `test_same_inputs_same_hash`, `test_different_base_different_hash`, `test_different_train_variant_different_hash`, `test_calibration_variant_affects_hash`, `test_calibration_none_equivalent_to_omitted` still pass (they use `{"lr": 0.01}` → empty payload but compare base/train/cal variance, which is unaffected).

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git -C $WT commit -m "fix(versioning): scope model_version hash to model-defining training config" -- \
  src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git -C $WT log -1 --oneline
```

---

## Task 2: Documentation — module docstring + config note

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py` (module docstring, lines 1-15)
- Modify: `conf/base/parameters_training.yaml` (header note before `spark:`, lines 1-2)

- [ ] **Step 1: Update the module docstring**

In `src/recsys_tfb/core/versioning.py`, replace:

```python
- ``calibration_variant_id``: derived from calibration-sampling params only.
  Keys calibration model_input under the base dataset directory.

Also provides manifest generation, symlink management, and version resolution
for dataset, training, and inference pipelines.
"""
```

with:

```python
- ``calibration_variant_id``: derived from calibration-sampling params only.
  Keys calibration model_input under the base dataset directory.
- ``model_version``: derived from the *model-defining* subset of training
  params only — the ``training:`` block minus the pure logging/threading
  knobs in ``MODEL_VERSION_IRRELEVANT_PARAMS``. Ops-only config
  (``spark`` / ``mlflow`` / ``cache``) is excluded structurally so changing
  it does not orphan an otherwise-identical model.

Also provides manifest generation, symlink management, and version resolution
for dataset, training, and inference pipelines.
"""
```

- [ ] **Step 2: Add the authoritative config note**

In `conf/base/parameters_training.yaml`, replace:

```yaml
spark:
  app_name: recsys_tfb-training
```

with:

```yaml
# model_version scope: only the model-defining subset of this file is hashed
# into model_version — the `training:` block MINUS the pure logging/threading
# knobs `verbosity`, `log_period`, `num_threads`. The `spark:`, `mlflow:`, and
# `cache:` blocks below do NOT affect model_version. Single source of truth:
# src/recsys_tfb/core/versioning.py :: _model_version_payload.
spark:
  app_name: recsys_tfb-training
```

(One authoritative header note rather than four scattered comments — DRY, and consistent with the project's single-source-of-truth ethos for config invariants.)

- [ ] **Step 3: Verify docs are consistent and nothing regressed**

Run:

```bash
grep -n "_model_version_payload" $WT/conf/base/parameters_training.yaml $WT/src/recsys_tfb/core/versioning.py
PYTHONPATH=$WT/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  $WT/tests/test_core/test_versioning.py -q
```

Expected: `grep` shows the reference in both the yaml note and the function definition; pytest still 70 passed (docstring/comment-only changes — no behavior change).

- [ ] **Step 4: Commit**

```bash
git -C $WT add src/recsys_tfb/core/versioning.py conf/base/parameters_training.yaml
git -C $WT commit -m "docs(versioning): document model_version scoping in docstring + config" -- \
  src/recsys_tfb/core/versioning.py conf/base/parameters_training.yaml
git -C $WT log -1 --oneline
```

---

## Task 3: Sync graphify + final verification

**Files:** none (maintenance only)

- [ ] **Step 1: Rebuild the graphify code graph (project rule after code changes)**

Run from the worktree root:

```bash
cd $WT && python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

Expected: prints a `[graphify watch] Rebuilt: …` line. (The post-commit hook may have already done this; this is the explicit, documented form.)

- [ ] **Step 2: Final diff review against the spec**

Run:

```bash
git -C $WT diff main..feat/model-version-hash-scope -- \
  src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py conf/base/parameters_training.yaml
```

Expected: only the constant, `_model_version_payload`, the rewired `compute_model_version`, the docstrings, the 5 test changes, and the one yaml header note. No changes to `compute_base_dataset_version`, variant functions, `__main__.py`, or `core/consistency.py` (out-of-scope per spec).

- [ ] **Step 3: Commit the graphify sync if it produced changes**

```bash
git -C $WT status --porcelain graphify-out
git -C $WT add graphify-out && git -C $WT commit -m "chore(graphify): sync graph after model_version scoping" -- graphify-out || echo "no graphify changes to commit"
git -C $WT log --oneline -4
```

---

## Self-Review (performed during plan authoring)

- **Spec coverage:** Design §1 (constant + `_model_version_payload` + rewire) → Task 1 Step 3. §2 (provenance / deep-copy / no call-site change) → covered by deep-copy in helper + `test_caller_params_not_mutated`; no `__main__.py` edit (verified Task 3 Step 2). §3 (all six test categories) → Task 1 Steps 1/4. §4 (docstrings + yaml note) → Task 2. "Out of scope" (no base/variant/consistency change) → Task 3 Step 2 assertion. Decision 1/2/3 all reflected.
- **Placeholder scan:** none — every code/command step shows literal content.
- **Type/name consistency:** `MODEL_VERSION_IRRELEVANT_PARAMS` and `_model_version_payload` spelled identically in versioning.py edits, module docstring, test references, and the yaml note. `compute_model_version` signature unchanged (4 params, `calibration_variant_id` optional) — preserves existing passing tests.
