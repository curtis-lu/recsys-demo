# Evaluation `snap_date` Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the evaluation pipeline scope its predictions to the single configured `evaluation.snap_date` instead of evaluating every snapshot present in the table.

**Architecture:** Add one filter block to the `prepare_eval_data` node, immediately after the existing `model_version` filter. Compares the `snap_date` column (cast to string) against the configured ISO date. Fails loud — raises `ValueError` when the date is unset or matches no rows. One node serves both pipeline modes, so the change covers monitoring (`ranked_predictions`) and `--post-training` (`training_eval_predictions`) at once.

**Tech Stack:** PySpark 3.3.2, pytest 7.3.1.

**Spec:** `docs/superpowers/specs/2026-05-22-eval-snap-date-filter-design.md`

---

## File Structure

- `conf/base/parameters_evaluation.yaml` — `snap_date` value already set to the
  ISO form `"2025-12-31"`; comment already updated. Task 1 only commits it.
- `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — `prepare_eval_data`
  gains the snap_date filter block. Sole behavioural change.
- `tests/test_pipelines/test_evaluation/test_nodes_spark.py` — 3 new tests;
  3 existing tests get `evaluation.snap_date` added to their `parameters`.

## Notes for the engineer

- This work happens in the worktree `/Users/curtislu/projects/recsys_tfb/.worktrees/eval-snap-date-filter` (branch `feat/eval-snap-date-filter`).
- Run tests with the repo's single real venv and the worktree's `src` on the path:
  ```
  PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/eval-snap-date-filter/src \
    /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
  ```
  A bare `pytest` resolves `recsys_tfb` to the main tree's editable install and silently tests the wrong code.
- `prepare_eval_data` already binds `eval_params = parameters.get("evaluation", {})` near the top of the function — reuse it, do not re-fetch.

---

### Task 1: Commit the `parameters_evaluation.yaml` ISO-date prerequisite

The `snap_date` filter compares the config value as a string against the
`STRING` `snap_date` partition column, so the config must be the canonical ISO
form. The yaml value and comment are already changed in the working tree; this
task only verifies and commits them.

**Files:**
- Modify: `conf/base/parameters_evaluation.yaml` (already changed, uncommitted)

- [ ] **Step 1: Verify the working-tree change**

Run: `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/eval-snap-date-filter diff conf/base/parameters_evaluation.yaml`

Expected: `snap_date` value is `"2025-12-31"` and the comment above it explains
the ISO-date requirement (mentions baselines string comparison and the
`.replace("-", "")` path normalisation). The first lines of the file should read:

```yaml
evaluation:
  # Snap date for evaluation. Must be an ISO date (YYYY-MM-DD): the baselines
  # pipeline compares it against the DATE-typed snap_date column cast to string
  # (which yields YYYY-MM-DD). The evaluation output path normalises it via
  # .replace("-", "") so it still produces a clean YYYYMMDD/ directory.
  snap_date: "2025-12-31"
```

If the file does not match, set it to exactly the above before continuing.

- [ ] **Step 2: Commit**

```bash
git add conf/base/parameters_evaluation.yaml
git commit -m "chore(evaluation): make snap_date config an ISO date string"
```

---

### Task 2: Filter predictions to `evaluation.snap_date` in `prepare_eval_data`

**Files:**
- Modify: `src/recsys_tfb/pipelines/evaluation/nodes_spark.py` — `prepare_eval_data`, insert after the `model_version` filter (currently line 58), before the `# Filter labels to snap_dates in predictions` comment (currently line 60)
- Test: `tests/test_pipelines/test_evaluation/test_nodes_spark.py`

- [ ] **Step 1: Add the three new tests**

Append these three functions to the end of
`tests/test_pipelines/test_evaluation/test_nodes_spark.py`. `pytest` is already
imported at the top of the file.

```python
def test_prepare_eval_data_filters_to_configured_snap_date(spark):
    """prepare_eval_data keeps only rows at evaluation.snap_date, dropping the
    other snapshots that share the same model_version in the table."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"],
        "prod_name": ["A", "B", "A", "B"],
        "score": [0.9, 0.1, 0.2, 0.8],
        "rank": [1, 2, 2, 1],
        "model_version": ["v1"] * 4,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2"],
        "snap_date": ["2025-01-31", "2025-01-31", "2025-02-28", "2025-02-28"],
        "prod_name": ["A", "B", "A", "B"],
        "label": [1, 0, 0, 1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }

    result = prepare_eval_data(predictions, labels, parameters).toPandas()
    assert set(result["snap_date"]) == {"2025-01-31"}
    assert len(result) == 2


def test_prepare_eval_data_raises_when_snap_date_absent(spark):
    """When evaluation.snap_date matches no predictions row, prepare_eval_data
    raises ValueError and the message names the snap_dates actually present."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "score": [0.9, 0.1],
        "rank": [1, 2],
        "model_version": ["v1"] * 2,
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1", "c1"],
        "snap_date": ["2025-01-31", "2025-01-31"],
        "prod_name": ["A", "B"],
        "label": [1, 0],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {"snap_date": "2099-12-31"},
    }

    with pytest.raises(ValueError, match="2025-01-31"):
        prepare_eval_data(predictions, labels, parameters)


def test_prepare_eval_data_raises_when_snap_date_unset(spark):
    """When evaluation.snap_date is not configured, prepare_eval_data raises
    ValueError rather than silently evaluating the whole table."""
    from recsys_tfb.pipelines.evaluation.nodes_spark import prepare_eval_data
    import pandas as pd

    predictions_pdf = pd.DataFrame({
        "cust_id": ["c1"], "snap_date": ["2025-01-31"], "prod_name": ["A"],
        "score": [0.9], "rank": [1], "model_version": ["v1"],
    })
    labels_pdf = pd.DataFrame({
        "cust_id": ["c1"], "snap_date": ["2025-01-31"], "prod_name": ["A"],
        "label": [1],
    })
    predictions = spark.createDataFrame(predictions_pdf)
    labels = spark.createDataFrame(labels_pdf)
    parameters = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"], "item": "prod_name",
            "label": "label", "score": "score", "rank": "rank",
            "identity_columns": ["cust_id", "snap_date", "prod_name"]}},
        "model_version": "v1",
        "evaluation": {},
    }

    with pytest.raises(ValueError, match="snap_date not configured"):
        prepare_eval_data(predictions, labels, parameters)
```

- [ ] **Step 2: Update the three existing tests that omit `evaluation.snap_date`**

`test_prepare_eval_data_injects_rank_when_missing`,
`test_prepare_eval_data_preserves_existing_rank_column`, and
`test_prepare_eval_data_dedupes_label_when_predictions_carry_it` all build
`parameters` with `"evaluation": {}` and prediction data at `snap_date ==
"2025-01-31"`. Once Task 2's filter exists they would raise the "not
configured" `ValueError`. Add the matching snap_date to each.

In `tests/test_pipelines/test_evaluation/test_nodes_spark.py`, replace **all
three** occurrences of this exact block:

```python
        "model_version": "v1",
        "evaluation": {},
    }
```

with:

```python
        "model_version": "v1",
        "evaluation": {"snap_date": "2025-01-31"},
    }
```

(The `TestPrepareEvalDataModelVersionFilter` fixture uses `model_version`
`"20260511_153000"`, not `"v1"`, so this replacement does not touch it.)

- [ ] **Step 3: Run the tests to verify the new ones fail**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/eval-snap-date-filter/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py -q
```

Expected: `test_prepare_eval_data_filters_to_configured_snap_date` FAILS
(returns 4 rows / both snap_dates — no filter yet);
`test_prepare_eval_data_raises_when_snap_date_absent` FAILS (no `ValueError`
raised); `test_prepare_eval_data_raises_when_snap_date_unset` FAILS (no
`ValueError` raised). The three existing tests updated in Step 2 PASS.

- [ ] **Step 4: Implement the snap_date filter**

In `src/recsys_tfb/pipelines/evaluation/nodes_spark.py`, find this block in
`prepare_eval_data` (currently lines 49-62):

```python
    # Filter predictions to the resolved model_version (resolved upstream by
    # __main__.py via core.versioning.resolve_model_version).
    model_version = parameters.get("model_version")
    if model_version is None:
        raise RuntimeError(
            "parameters['model_version'] missing. CLI should resolve via "
            "core.versioning.resolve_model_version before pipeline run."
        )
    logger.info("Filtering predictions to model_version=%s", model_version)
    ranked_predictions = ranked_predictions.filter(F.col("model_version") == model_version)

    # Filter labels to snap_dates in predictions
    pred_snap_dates = ranked_predictions.select(time_col).distinct()
    labels = labels.join(pred_snap_dates, on=time_col, how="inner")
```

Insert the snap_date filter between the `model_version` filter line and the
`# Filter labels to snap_dates in predictions` comment, so the block becomes:

```python
    # Filter predictions to the resolved model_version (resolved upstream by
    # __main__.py via core.versioning.resolve_model_version).
    model_version = parameters.get("model_version")
    if model_version is None:
        raise RuntimeError(
            "parameters['model_version'] missing. CLI should resolve via "
            "core.versioning.resolve_model_version before pipeline run."
        )
    logger.info("Filtering predictions to model_version=%s", model_version)
    ranked_predictions = ranked_predictions.filter(F.col("model_version") == model_version)

    # Filter predictions to the configured evaluation snap_date. evaluation.
    # snap_date is an ISO date string (YYYY-MM-DD); the snap_date partition
    # column on ranked_predictions / training_eval_predictions is STRING, so
    # .cast("string") is a no-op here and stays correct if it is ever DATE.
    # Applies to both pipeline modes (this node serves monitoring and
    # --post-training). Fails loud — never silently evaluates the whole table.
    snap_date = str(eval_params.get("snap_date") or "").strip()
    if not snap_date:
        raise ValueError(
            "evaluation.snap_date not configured. Set evaluation.snap_date "
            "(ISO YYYY-MM-DD) in conf/base/parameters_evaluation.yaml."
        )
    logger.info("Filtering predictions to snap_date=%s", snap_date)
    predictions_at_snap = ranked_predictions.filter(
        F.col(time_col).cast("string") == snap_date
    )
    if predictions_at_snap.isEmpty():
        available = sorted(
            str(r[time_col])
            for r in ranked_predictions.select(time_col).distinct().collect()
        )
        raise ValueError(
            f"No predictions found for evaluation.snap_date={snap_date!r} "
            f"(model_version={model_version}). snap_dates present in "
            f"predictions: {available}"
        )
    ranked_predictions = predictions_at_snap

    # Filter labels to snap_dates in predictions
    pred_snap_dates = ranked_predictions.select(time_col).distinct()
    labels = labels.join(pred_snap_dates, on=time_col, how="inner")
```

`eval_params` and `time_col` are already bound earlier in the function — do not
re-fetch them.

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/eval-snap-date-filter/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_pipelines/test_evaluation/test_nodes_spark.py -q
```

Expected: all tests in the file PASS (3 new + 3 updated + the
`TestPrepareEvalDataModelVersionFilter` pair, which is unaffected — its
`model_version`-missing case raises before the snap_date block, and its
filter-applied case raises the "not configured" `ValueError` after the
`model_version` filter, still caught by that test's bare `except Exception`).

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/evaluation/nodes_spark.py \
        tests/test_pipelines/test_evaluation/test_nodes_spark.py
git commit -m "feat(evaluation): filter predictions to configured snap_date"
```

---

## Self-Review

**Spec coverage:**
- "Filter to single configured snap_date" → Task 2 Step 4 filter block.
- "Both pipeline modes" → covered: `prepare_eval_data` is the shared node;
  no mode branching needed.
- "Fail loud — unset" → Task 2 `if not snap_date` raise + test
  `test_prepare_eval_data_raises_when_snap_date_unset`.
- "Fail loud — no match, list available" → Task 2 `isEmpty()` raise + test
  `test_prepare_eval_data_raises_when_snap_date_absent`.
- "ISO-date config prerequisite" → Task 1.
- "`.cast("string")` defensive comparison" → Task 2 Step 4 filter.
- Tests (scoping / empty / unset) → Task 2 Steps 1, 3, 5.

**Placeholder scan:** none — every code and command step is concrete.

**Type consistency:** `snap_date` is a `str` throughout; `predictions_at_snap`
is a Spark DataFrame; `available` is a `list[str]`. `eval_params`, `time_col`,
`model_version` reuse names already bound in `prepare_eval_data`.
