# tune_hyperparameters Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-trial observability to `tune_hyperparameters` so a long Optuna study no longer goes silent for hours at `optuna_optimize`.

**Architecture:** Inside the `objective(trial)` closure of `tune_hyperparameters`, emit an INFO "start" summary, wrap the existing operations in four `log_step` blocks (`prepare_datasets` / `train` / `predict` / `score`), and emit an INFO "completed" summary with `ap`, `best_iteration`, `duration`, `best_so_far`. Optuna's own verbosity stays at WARNING so we don't double-log. No external contract changes.

**Tech Stack:** Python 3.10+, Optuna 4.5.0, LightGBM 4.6.0, pytest 7.3.1. Uses existing `log_step` context manager from `recsys_tfb.core.logging` (signature: `log_step(logger, step_name)`; emits records with `event="step_started"|"step_completed"` and `step=<name>`).

**Spec:** `docs/superpowers/specs/2026-05-12-tune-hyperparameters-observability-design.md`

**Branch:** `feat/tune-hyperparameters-observability` (already created off main; spec already committed as `500f7b7`).

**Pre-existing untracked changes (DO NOT TOUCH):** `PRD.md`, `conf/base/catalog.yaml`, `conf/base/parameters_training.yaml`, `graphify-out/GRAPH_REPORT.md`. They belong to other in-progress work. Never `git add -A` or `git add .` — always stage explicit paths.

**Venv:** All `pytest` invocations use `.venv/bin/pytest` (project memory: 執行前確認虛擬環境).

---

## File Structure

| File | Role | Action |
|---|---|---|
| `src/recsys_tfb/pipelines/training/nodes.py` | Houses `tune_hyperparameters` (lines 254-368) | Modify — add `import time`; modify `objective()` |
| `tests/test_pipelines/test_training/test_nodes.py` | Houses `TestTuneHyperparameters` class (lines 207-258) with fixtures `lgb_handles` / `synthetic_model_inputs` / `preprocessor_metadata` / `training_parameters` (`n_trials=3`) | Modify — add four new test methods inside `TestTuneHyperparameters` |

No new files. No new fixtures. Decomposition: one logical change to one function; tests piggyback on existing fixtures.

---

## Task 1: Per-trial start/completed INFO summary lines

**Goal:** Add the two `logger.info(...)` lines that bookend each trial. Wraps three of the four spec verification tests (#1 start/completed lines, #3 best_so_far semantics, #4 start-line param subset).

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (imports + `tune_hyperparameters.objective`)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (append to `TestTuneHyperparameters`)

### Step 1.1: Write the failing tests

- [ ] **Step 1.1: Append three new test methods inside `TestTuneHyperparameters` in `tests/test_pipelines/test_training/test_nodes.py`**

Use the existing class indentation (4 spaces). Place these methods after the existing `test_best_model_predictions_are_probabilities` method (current end of class around line 258), before the `# ---- Tests: finalize_model ----` divider.

Also add this import at the top of the file (alongside the existing imports):

```python
import logging
import re
```

Then append the three methods to `TestTuneHyperparameters`:

```python
    def test_emits_trial_start_and_completed_info_lines(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """Every trial emits a start INFO and a completed INFO with the
        expected `trial=N/total ...` shape. trial_idx covers 0..n_trials-1.
        """
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        n_trials = training_parameters["training"]["n_trials"]

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        messages = [r.getMessage() for r in caplog.records]
        start_lines = [
            m for m in messages
            if re.match(rf"tune_hyperparameters: trial=\d+/{n_trials} start ", m)
        ]
        completed_lines = [
            m for m in messages
            if re.match(rf"tune_hyperparameters: trial=\d+/{n_trials} completed ", m)
        ]
        assert len(start_lines) == n_trials
        assert len(completed_lines) == n_trials

        # trial_idx covers 0..n_trials-1, in order
        start_indices = [
            int(re.search(r"trial=(\d+)/", m).group(1)) for m in start_lines
        ]
        completed_indices = [
            int(re.search(r"trial=(\d+)/", m).group(1)) for m in completed_lines
        ]
        assert start_indices == list(range(n_trials))
        assert completed_indices == list(range(n_trials))

    def test_completed_line_has_correct_best_so_far(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """best_so_far in each completed INFO is monotonically non-decreasing,
        and the final value matches the study's best_value (i.e. the maximum
        ap actually achieved)."""
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        completed = [
            r.getMessage()
            for r in caplog.records
            if "completed ap=" in r.getMessage()
            and "tune_hyperparameters: trial=" in r.getMessage()
        ]
        best_so_far_values = [
            float(re.search(r"best_so_far=([\d.]+)", m).group(1))
            for m in completed
        ]
        # Monotonic non-decreasing
        for prev, curr in zip(best_so_far_values, best_so_far_values[1:]):
            assert curr >= prev, (
                f"best_so_far decreased from {prev} to {curr} across trials"
            )

        ap_values = [
            float(re.search(r"\bap=([\d.]+)", m).group(1)) for m in completed
        ]
        assert best_so_far_values[-1] == pytest.approx(max(ap_values))

    def test_start_line_params_contains_only_search_dimensions(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """The `start` line's params={...} prints the search-space dimensions
        (trial_params), NOT the expanded full params dict (which would also
        contain algorithm_params keys like 'objective' / 'metric')."""
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        start_lines = [
            r.getMessage()
            for r in caplog.records
            if "tune_hyperparameters: trial=" in r.getMessage()
            and " start " in r.getMessage()
        ]
        assert start_lines, "no trial start lines emitted"

        for m in start_lines:
            # Must contain the search-space keys
            assert "learning_rate" in m
            assert "num_leaves" in m
            assert "max_depth" in m
            # Must NOT contain algorithm_params keys
            assert "'objective'" not in m
            assert "'metric'" not in m
            assert "'verbosity'" not in m
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_emits_trial_start_and_completed_info_lines tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_completed_line_has_correct_best_so_far tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_start_line_params_contains_only_search_dimensions -v
```

Expected: 3 tests FAIL — assertions `len(start_lines) == n_trials` fail because no such log lines exist yet (they're 0 in production code, expected 3).

### Step 1.3-1.5: Implementation

- [ ] **Step 1.3: Modify `src/recsys_tfb/pipelines/training/nodes.py`**

Add `import time` to the imports block at the top of the file. Current state (lines 1-21):

```python
"""Pure functions for the training pipeline."""

import logging
import shutil
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd

from recsys_tfb.core.logging import log_step
...
```

Insert `import time` so the stdlib block reads:

```python
"""Pure functions for the training pipeline."""

import logging
import shutil
import time
from pathlib import Path

import mlflow
import numpy as np
import optuna
import pandas as pd
```

Then modify `objective(trial)` inside `tune_hyperparameters`. Current state (lines 289-353):

```python
    def objective(trial: optuna.Trial) -> float:
        trial_params = {
            "learning_rate": trial.suggest_float(
                "learning_rate",
                search_space["learning_rate"]["low"],
                search_space["learning_rate"]["high"],
                log=True,
            ),
            "num_leaves": trial.suggest_int(
                "num_leaves",
                search_space["num_leaves"]["low"],
                search_space["num_leaves"]["high"],
            ),
            "max_depth": trial.suggest_int(
                "max_depth",
                search_space["max_depth"]["low"],
                search_space["max_depth"]["high"],
            ),
            "min_child_samples": trial.suggest_int(
                "min_child_samples",
                search_space["min_child_samples"]["low"],
                search_space["min_child_samples"]["high"],
            ),
            "subsample": trial.suggest_float(
                "subsample",
                search_space["subsample"]["low"],
                search_space["subsample"]["high"],
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                search_space["colsample_bytree"]["low"],
                search_space["colsample_bytree"]["high"],
            ),
        }

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
        ap = ap if ap is not None else 0.0

        if ap > best_state["ap"]:
            best_state["ap"] = ap
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        return ap
```

Insert the start INFO after the `params = {...}` block, and the completed INFO right before `return ap`. Capture `t0` after the start log. Final state of `objective`:

```python
    def objective(trial: optuna.Trial) -> float:
        trial_idx = trial.number
        trial_params = {
            "learning_rate": trial.suggest_float(
                "learning_rate",
                search_space["learning_rate"]["low"],
                search_space["learning_rate"]["high"],
                log=True,
            ),
            "num_leaves": trial.suggest_int(
                "num_leaves",
                search_space["num_leaves"]["low"],
                search_space["num_leaves"]["high"],
            ),
            "max_depth": trial.suggest_int(
                "max_depth",
                search_space["max_depth"]["low"],
                search_space["max_depth"]["high"],
            ),
            "min_child_samples": trial.suggest_int(
                "min_child_samples",
                search_space["min_child_samples"]["low"],
                search_space["min_child_samples"]["high"],
            ),
            "subsample": trial.suggest_float(
                "subsample",
                search_space["subsample"]["low"],
                search_space["subsample"]["high"],
            ),
            "colsample_bytree": trial.suggest_float(
                "colsample_bytree",
                search_space["colsample_bytree"]["low"],
                search_space["colsample_bytree"]["high"],
            ),
        }

        params = {
            **algorithm_params,
            "seed": seed,
            "feature_pre_filter": False,
            **trial_params,
            "num_iterations": num_iterations,
            "early_stopping_rounds": early_stopping_rounds,
        }

        logger.info(
            "tune_hyperparameters: trial=%d/%d start params=%s",
            trial_idx, n_trials, trial_params,
        )
        t0 = time.monotonic()

        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
        ap = ap if ap is not None else 0.0

        if ap > best_state["ap"]:
            best_state["ap"] = ap
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed ap=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, n_trials, ap,
            adapter.booster.best_iteration, duration, best_state["ap"],
        )

        return ap
```

- [ ] **Step 1.4: Run the 3 new tests to verify they pass**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_emits_trial_start_and_completed_info_lines tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_completed_line_has_correct_best_so_far tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_start_line_params_contains_only_search_dimensions -v
```

Expected: 3 PASSED.

- [ ] **Step 1.5: Run the full `TestTuneHyperparameters` class to confirm no regression on existing tests**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters -v
```

Expected: 4 existing tests + 3 new tests = 7 PASSED.

- [ ] **Step 1.6: Commit**

Stage **only** the two files modified in this task — do NOT use `git add .`/`-A` (will pick up pre-existing uncommitted changes in `PRD.md`, `conf/base/*.yaml`, `graphify-out/GRAPH_REPORT.md`):

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "$(cat <<'EOF'
feat(training): emit per-trial start/completed INFO in tune_hyperparameters

Mirrors PR #9 extract_Xy pattern. trial_idx covers 0..n_trials-1; start
line prints trial_params (search-space dims) not full params; completed
line includes ap, best_iteration, duration, best_so_far.
EOF
)"
```

---

## Task 2: Wrap inner operations in 4 `log_step` blocks

**Goal:** Wrap `dataset.load()` × 2, `adapter.train`, `adapter.predict`, `compute_ap` in `log_step` context managers so step_started/step_completed events fire per sub-step per trial. Covers spec verification test #2.

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (`objective()`)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (append to `TestTuneHyperparameters`)

### Step 2.1: Write the failing test

- [ ] **Step 2.1: Append one more test method to `TestTuneHyperparameters`**

Place after the three tests added in Task 1, before the `# ---- Tests: finalize_model ----` divider:

```python
    def test_emits_inner_step_events_per_trial(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata,
        training_parameters, caplog,
    ):
        """Each trial emits 4 inner log_step events: prepare_datasets, train,
        predict, score. Both step_started and step_completed fire for each.
        """
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        n_trials = training_parameters["training"]["n_trials"]
        expected_steps = {"prepare_datasets", "train", "predict", "score"}

        with caplog.at_level(
            logging.INFO, logger="recsys_tfb.pipelines.training.nodes"
        ):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h,
                preprocessor_metadata, training_parameters,
            )

        started = [
            r.step
            for r in caplog.records
            if getattr(r, "event", None) == "step_started"
            and getattr(r, "step", None) in expected_steps
        ]
        completed = [
            r.step
            for r in caplog.records
            if getattr(r, "event", None) == "step_completed"
            and getattr(r, "step", None) in expected_steps
        ]

        # Each inner step fires once per trial → n_trials times total
        for step_name in expected_steps:
            assert started.count(step_name) == n_trials, (
                f"step_started count for {step_name!r} = "
                f"{started.count(step_name)}, expected {n_trials}"
            )
            assert completed.count(step_name) == n_trials, (
                f"step_completed count for {step_name!r} = "
                f"{completed.count(step_name)}, expected {n_trials}"
            )
```

- [ ] **Step 2.2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_emits_inner_step_events_per_trial -v
```

Expected: FAIL — `started.count("prepare_datasets") == 0`, expected `n_trials` (3).

### Step 2.3-2.5: Implementation

- [ ] **Step 2.3: Wrap inner operations in `log_step` blocks**

In `src/recsys_tfb/pipelines/training/nodes.py`, inside `objective()` (modified shape from Task 1), change the existing block:

```python
        adapter = get_adapter(algorithm)
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )
        y_pred = adapter.predict(X_v)

        ap = compute_ap(y_v, y_pred)
        ap = ap if ap is not None else 0.0
```

To:

```python
        adapter = get_adapter(algorithm)

        with log_step(logger, "prepare_datasets"):
            ds_train = train_lgb_handle.load()
            ds_dev = train_dev_lgb_handle.load(reference=ds_train)

        with log_step(logger, "train"):
            adapter.train(
                X_train=None, y_train=None, X_val=None, y_val=None,
                params=params,
                train_dataset=ds_train, val_dataset=ds_dev,
            )

        with log_step(logger, "predict"):
            y_pred = adapter.predict(X_v)

        with log_step(logger, "score"):
            ap = compute_ap(y_v, y_pred)
            ap = ap if ap is not None else 0.0
```

`log_step` is already imported at the top of the file (line 12: `from recsys_tfb.core.logging import log_step`).

- [ ] **Step 2.4: Run the new test to verify it passes**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_emits_inner_step_events_per_trial -v
```

Expected: PASSED.

- [ ] **Step 2.5: Run the full `TestTuneHyperparameters` class to confirm no regression**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters -v
```

Expected: 4 existing + 4 new = 8 PASSED.

- [ ] **Step 2.6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "$(cat <<'EOF'
feat(training): wrap tune_hyperparameters inner ops with log_step

Emits step_started/step_completed for prepare_datasets / train /
predict / score on every trial. Stuck trials now show exactly which
sub-step is mid-flight.
EOF
)"
```

---

## Task 3: Full regression + open PR

**Goal:** Run the full test suite to catch any unintended regression elsewhere in the codebase, then open the PR.

### Step 3.1-3.3: Final regression

- [ ] **Step 3.1: Run the full training test module**

```bash
.venv/bin/pytest tests/test_pipelines/test_training/ -v
```

Expected: All tests pass. The new ones (`test_emits_trial_start_and_completed_info_lines`, `test_completed_line_has_correct_best_so_far`, `test_start_line_params_contains_only_search_dimensions`, `test_emits_inner_step_events_per_trial`) show PASSED. No prior tests regressed.

- [ ] **Step 3.2: Run the full project test suite**

```bash
.venv/bin/pytest tests/ -v
```

Expected: All tests pass. If anything new fails that we did not touch, investigate — do not skip or xfail. If a test was already broken before this branch, capture it as a finding to report alongside the PR and proceed (it is not caused by this work).

- [ ] **Step 3.3: Verify there are exactly two new commits on this branch (Task 1 + Task 2)**

```bash
git log --oneline main..HEAD
```

Expected: 3 commits visible — `500f7b7 docs(spec): ...` from spec, plus the two `feat(training): ...` commits from Tasks 1 and 2.

### Step 3.4-3.5: Open PR

- [ ] **Step 3.4: Push branch to origin**

```bash
git push -u origin feat/tune-hyperparameters-observability
```

- [ ] **Step 3.5: Open PR via `gh`**

```bash
gh pr create --title "feat(training): per-trial observability in tune_hyperparameters" --body "$(cat <<'EOF'
## Summary

Adds per-trial observability inside `tune_hyperparameters` so a long Optuna study stops going silent for hours at `optuna_optimize`. Mirrors the PR #9 `extract_Xy` pattern.

Each trial now emits:
- An INFO **start** line with `trial=N/total` and `trial_params={…}` (search-space dims only)
- Four inner `log_step` events: `prepare_datasets` → `train` → `predict` → `score`
- An INFO **completed** line with `ap`, `best_iteration`, `duration`, `best_so_far`

Optuna verbosity stays at WARNING (no double-logging). External signature/return values unchanged.

## Why

Production HPO runs sit at `Step started: optuna_optimize` for 5–8 hours with zero further output, making it impossible to tell whether the job is healthy, which trial is in flight, or which sub-step is the bottleneck. PR #11 just resolved an OOM in `extract_Xy`; this PR is the next iteration of the same observability arc.

## Test plan

- [ ] `.venv/bin/pytest tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters -v` passes locally (4 existing + 4 new)
- [ ] `.venv/bin/pytest tests/` full suite green
- [ ] After merge: rerun training in company env; verify per-trial INFO lines appear and step events bracket each trial's sub-steps
EOF
)"
```

- [ ] **Step 3.6: Print PR URL for the user**

`gh pr create` prints the URL on success — surface it to the user so they can review and merge.

---

## Self-Review

**1. Spec coverage:**
- Spec § "objective(trial) 改動" code block → Tasks 1 + 2 reproduce it line-for-line.
- Spec § "Step names 用 generic 名稱" → Task 2 uses `prepare_datasets` / `train` / `predict` / `score`.
- Spec § "`time.monotonic()`" → Task 1 imports `time` and uses `time.monotonic()`.
- Spec § "`adapter.booster.best_iteration` 從當前 trial-local adapter 拿" → Task 1 completed-line `adapter.booster.best_iteration`, not `best_state["iteration"]`.
- Spec § "start 行印 trial_params" → Task 1 uses `params=%s, trial_params`; Test #4 (Task 1.1's third method) asserts `algorithm_params` keys are absent.
- Spec § "duration 用秒、1 位小數" → Task 1 uses `duration=%.1fs`.
- Spec § "驗證 / 單元測試" lists 4 tests → Tasks 1.1 + 2.1 add all 4 inside `TestTuneHyperparameters`.
- Spec § "不做的事" → No tasks for those (correctly excluded).

**2. Placeholder scan:** No "TBD", "TODO", "fill in details" — every step shows complete code or exact command.

**3. Type consistency:** Step name strings consistent across Task 2 implementation and test (`"prepare_datasets"`, `"train"`, `"predict"`, `"score"`). Log-line format strings in implementation match the regex patterns in tests. `n_trials` accessed identically (`training_parameters["training"]["n_trials"]`) in tests and implementation.
