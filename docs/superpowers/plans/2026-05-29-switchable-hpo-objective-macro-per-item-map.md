# Switchable HPO Objective (macro per-item mAP) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the training HPO scoring objective config-switchable and add a `macro_per_item_map` objective that reproduces evaluation's `macro_avg["by_item"]["map_attr@all"]`.

**Architecture:** A new numpy primitive in `evaluation/metrics.py` computes macro per-item mAP on the single-driver val arrays (no Spark per trial). `extract_Xy_with_groups` gains a `with_items` flag to supply per-row product ids. `tune_hyperparameters` reads `training.hpo_objective` and routes scoring through a small testable `_hpo_score` selector. Default value in the shipped config is `macro_per_item_map`; code falls back to `mean_ap` when the key is absent.

**Tech Stack:** Python 3.10, numpy 1.25, PySpark 3.3.2 (parity test only), pytest 7.3.1, Optuna 4.5.0, LightGBM 4.6.0.

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/hpo-macro-per-item-map`
**Run tests with (absolute venv python + PYTHONPATH):**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/hpo-macro-per-item-map
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

---

### Task 1: numpy primitive `compute_macro_per_item_map`

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics.py` (append after `compute_mean_ap`, currently ends line 95)
- Test: `tests/test_evaluation/test_metrics.py`

- [ ] **Step 1: Write the failing tests**

Edit the import at the top of `tests/test_evaluation/test_metrics.py`:

```python
from recsys_tfb.evaluation.metrics import (
    compute_ap,
    compute_macro_per_item_map,
    compute_mean_ap,
)
```

Append this class to the end of `tests/test_evaluation/test_metrics.py`:

```python
class TestComputeMacroPerItemMap:
    # Two customers, three products (mirrors the metrics_spark
    # _two_customer_raw fixture so the parity test shares the math):
    #   C0: A(0.9,1) B(0.5,0) C(0.1,1)  ranking A,B,C -> prec A=1.0, C=2/3
    #   C1: B(0.8,1) C(0.6,0) A(0.3,0)  ranking B,C,A -> prec B=1.0
    # per-item map_attr@all: A=1.0, B=1.0, C=2/3
    # macro = (1.0 + 1.0 + 2/3) / 3 = 8/9
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_full_map_macro_over_items(self):
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(8 / 9)

    def test_k_truncation_zeros_contrib_beyond_k(self):
        # k=1: C0 A pos1 -> 1.0, C pos3 -> 0.0 ; C1 B pos1 -> 1.0
        # per-item: A=1.0, B=1.0, C=0.0 ; macro = 2/3
        result = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE, k=1
        )
        assert result == pytest.approx(2 / 3)

    def test_skips_group_with_no_positives(self):
        # group 2 has no positives -> contributes nothing; A and C each 1.0
        groups = np.array([0, 0, 1, 1, 2, 2])
        items = np.array(["A", "C", "A", "C", "A", "C"])
        y = np.array([1, 0, 0, 1, 0, 0])
        score = np.array([0.9, 0.1, 0.4, 0.5, 0.9, 0.1])
        # C0: A(0.9,1) C(0.1,0) -> A prec 1.0 ; C1: C(0.5,1) A(0.4,0) -> C prec 1.0
        # per-item: A=1.0, C=1.0 ; macro = 1.0
        result = compute_macro_per_item_map(groups, items, y, score)
        assert result == pytest.approx(1.0)

    def test_all_no_positives_returns_zero(self):
        groups = np.array([0, 0, 1, 1])
        items = np.array(["A", "B", "A", "B"])
        y = np.array([0, 0, 0, 0])
        score = np.array([0.9, 0.8, 0.7, 0.6])
        assert compute_macro_per_item_map(groups, items, y, score) == 0.0

    def test_empty_inputs_return_zero(self):
        empty = np.array([], dtype=np.int64)
        assert (
            compute_macro_per_item_map(
                empty, np.array([]), empty, np.array([], dtype=np.float64)
            )
            == 0.0
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_metrics.py::TestComputeMacroPerItemMap -q
```
Expected: FAIL — `ImportError: cannot import name 'compute_macro_per_item_map'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/evaluation/metrics.py` (after `compute_mean_ap`):

```python
def compute_macro_per_item_map(
    groups: np.ndarray,
    items: np.ndarray,
    y_true: np.ndarray,
    y_score: np.ndarray,
    k: Optional[int] = None,
) -> float:
    """Macro average over items of per-item attributed mAP@k.

    Reproduces ``evaluation.metrics_spark`` ``macro_avg["by_item"]["map_attr@K"]``
    on numpy arrays so the HPO loop can score a trial without a Spark job.

    Ranking is *within each query* (``groups``, e.g. ``(snap_date, cust_id)``),
    exactly as in :func:`compute_mean_ap`. Each positive row contributes its
    within-query cumulative precision ``prec_at_pos`` (zeroed when its rank is
    beyond ``k``). Per item we average that contribution over the item's
    positive rows (row-equal-weight), then average across items
    (item-equal-weight). ``k=None`` means no truncation — full mAP, equivalent
    to ``k = n_products``.

    Empty input, or no positive rows anywhere, returns ``0.0``.

    Implementation mirrors :func:`compute_mean_ap`: one ``np.lexsort`` on
    ``(groups, -y_score)`` (``O(N log N)``), a per-group slice walk, then a
    vectorized per-item aggregation via ``np.unique`` + ``np.bincount``.
    """
    if len(groups) == 0:
        return 0.0

    sort_idx = np.lexsort((-y_score, groups))
    g_sorted = groups[sort_idx]
    y_sorted = y_true[sort_idx].astype(np.float64, copy=False)
    items_sorted = items[sort_idx]

    boundaries = np.concatenate([
        [0],
        np.flatnonzero(np.diff(g_sorted)) + 1,
        [len(g_sorted)],
    ])

    contribs: list[np.ndarray] = []
    pos_items: list[np.ndarray] = []
    for i in range(len(boundaries) - 1):
        s, e = boundaries[i], boundaries[i + 1]
        y = y_sorted[s:e]
        if y.sum() == 0:
            continue
        positions = np.arange(1, len(y) + 1, dtype=np.float64)
        prec = np.cumsum(y) / positions
        if k is not None:
            prec = prec * (positions <= k)
        pos_mask = y == 1
        contribs.append(prec[pos_mask])
        pos_items.append(items_sorted[s:e][pos_mask])

    if not contribs:
        return 0.0

    contrib_all = np.concatenate(contribs)
    items_all = np.concatenate(pos_items)
    _, inv = np.unique(items_all, return_inverse=True)
    sums = np.bincount(inv, weights=contrib_all)
    counts = np.bincount(inv)
    per_item = sums / counts
    return float(per_item.mean())
```

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_metrics.py -q
```
Expected: PASS (all `TestComputeMacroPerItemMap` + existing `TestComputeAP`/`TestComputeMeanAP`).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics.py tests/test_evaluation/test_metrics.py
git commit -m "feat(evaluation): add compute_macro_per_item_map numpy primitive"
```

---

### Task 2: Spark parity test (primitive == metrics_spark macro_avg.by_item)

**Files:**
- Test: `tests/test_evaluation/test_metrics_spark.py` (append; reuses the `spark` fixture and the existing `_two_customer_raw` / `_make_parameters` helpers)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_evaluation/test_metrics_spark.py`:

```python
def test_macro_per_item_map_numpy_matches_spark(spark):
    """compute_macro_per_item_map (numpy, HPO) == compute_all_metrics
    macro_avg.by_item.map_attr@all (Spark) on identical data.

    k_values=(3,) and 3 products => k=3 == n_products == 'all'.
    Scores are distinct so lexsort tie-order vs Spark row_number is moot.
    """
    import numpy as np

    from recsys_tfb.evaluation.metrics import compute_macro_per_item_map

    df = _two_customer_raw(spark)
    params = _make_parameters(k_values=(3,))
    result = ms.compute_all_metrics(df, params)
    spark_macro = result["macro_avg"]["by_item"]["map_attr@3"]

    rows = df.collect()
    group_ids = {("20240331", "C0"): 0, ("20240331", "C1"): 1}
    groups = np.array([group_ids[(r["snap_date"], r["cust_id"])] for r in rows])
    items = np.array([r["prod_name"] for r in rows])
    y = np.array([r["label"] for r in rows])
    score = np.array([r["score"] for r in rows], dtype=np.float64)

    numpy_macro = compute_macro_per_item_map(groups, items, y, score)
    assert numpy_macro == pytest.approx(spark_macro, rel=1e-12)
```

- [ ] **Step 2: Run the test to verify it passes (no new production code)**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_evaluation/test_metrics_spark.py::test_macro_per_item_map_numpy_matches_spark -q
```
Expected: PASS. (This test validates Task 1's primitive against the Spark source of truth; it has no separate implementation step. If it FAILS, the primitive is wrong — fix `compute_macro_per_item_map`, not the test.)

- [ ] **Step 3: Commit**

```bash
git add tests/test_evaluation/test_metrics_spark.py
git commit -m "test(evaluation): parity test numpy macro per-item mAP vs Spark"
```

---

### Task 3: `extract_Xy_with_groups` `with_items` flag

**Files:**
- Modify: `src/recsys_tfb/io/extract.py` (signature line 197-203; return block line 264-268)
- Test: `tests/test_io/test_extract.py` (append after `test_extract_xy_with_groups_returns_groups`, line 284; reuses `_make_handle`, `_make_grouped_df`, `_make_grouped_prep_meta`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_io/test_extract.py` (after line 284):

```python
def test_extract_xy_with_groups_with_items_returns_item_ids(tmp_path: Path) -> None:
    from recsys_tfb.io.extract import extract_Xy_with_groups

    handle = _make_handle(tmp_path, _make_grouped_df())

    X, y, groups, items = extract_Xy_with_groups(
        handle, _make_grouped_prep_meta(), {}, with_items=True
    )

    assert X.shape == (6, 2)
    assert len(items) == 6
    # items are the raw prod_name values, row-aligned with X / y / groups
    assert list(items) == ["fund", "ccard", "fund", "ccard", "fund", "ccard"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py::test_extract_xy_with_groups_with_items_returns_item_ids -q
```
Expected: FAIL — `TypeError: extract_Xy_with_groups() got an unexpected keyword argument 'with_items'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/io/extract.py`, change the signature (line 197-203) to add `with_items`:

```python
def extract_Xy_with_groups(
    handle: ParquetHandle,
    preprocessor_metadata: dict,
    parameters: dict,
    *,
    with_weights: bool = False,
    with_items: bool = False,
) -> tuple:
```

Replace the return block (currently line 264-268):

```python
    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy_with_groups.w", w)
        return X, y, groups, w
    return X, y, groups
```

with:

```python
    result: list = [X, y, groups]
    if with_weights:
        w = _row_weights_from_pdf(pdf, parameters)
        log_data_volume(logger, "extract_Xy_with_groups.w", w)
        result.append(w)
    if with_items:
        items = pdf[schema["item"]].to_numpy()
        log_data_volume(logger, "extract_Xy_with_groups.items", items)
        result.append(items)
    return tuple(result)
```

(`schema` is already bound at the top of the function via `schema = get_schema(parameters)`; `schema["item"]` is the product column, e.g. `prod_name`.)

- [ ] **Step 4: Run tests to verify they pass**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_io/test_extract.py -q
```
Expected: PASS (new test + existing `test_extract_xy_with_groups_returns_groups` and weight tests — return order for `with_weights` is unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/io/extract.py tests/test_io/test_extract.py
git commit -m "feat(io): extract_Xy_with_groups with_items returns per-row product ids"
```

---

### Task 4: `_hpo_score` selector in training nodes

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (import line 15; add module-level helper + constant near `tune_hyperparameters`, around line 252)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (append a new test class)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pipelines/test_training/test_nodes.py`:

```python
class TestHpoScore:
    GROUPS = np.array([0, 0, 0, 1, 1, 1])
    ITEMS = np.array(["A", "B", "C", "A", "B", "C"])
    Y = np.array([1, 0, 1, 0, 1, 0])
    SCORE = np.array([0.9, 0.5, 0.1, 0.3, 0.8, 0.6])

    def test_mean_ap_matches_compute_mean_ap(self):
        from recsys_tfb.evaluation.metrics import compute_mean_ap
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        expected = compute_mean_ap(self.GROUPS, self.Y, self.SCORE)
        result = _hpo_score("mean_ap", self.GROUPS, None, self.Y, self.SCORE)
        assert result == pytest.approx(expected)

    def test_macro_per_item_map_matches_primitive(self):
        from recsys_tfb.evaluation.metrics import compute_macro_per_item_map
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        expected = compute_macro_per_item_map(
            self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        result = _hpo_score(
            "macro_per_item_map", self.GROUPS, self.ITEMS, self.Y, self.SCORE
        )
        assert result == pytest.approx(expected)

    def test_unknown_objective_raises_valueerror(self):
        from recsys_tfb.pipelines.training.nodes import _hpo_score

        with pytest.raises(ValueError, match="hpo_objective"):
            _hpo_score("not_a_metric", self.GROUPS, self.ITEMS, self.Y, self.SCORE)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestHpoScore -q
```
Expected: FAIL — `ImportError: cannot import name '_hpo_score'`.

- [ ] **Step 3: Write minimal implementation**

In `src/recsys_tfb/pipelines/training/nodes.py`, change the import (line 15) from:

```python
from recsys_tfb.evaluation.metrics import compute_mean_ap
```

to:

```python
from recsys_tfb.evaluation.metrics import (
    compute_macro_per_item_map,
    compute_mean_ap,
)
```

Add immediately above `def tune_hyperparameters(` (line 254):

```python
HPO_OBJECTIVES = ("mean_ap", "macro_per_item_map")


def _hpo_score(
    objective_name: str,
    groups: np.ndarray,
    items: Optional[np.ndarray],
    y_true: np.ndarray,
    y_score: np.ndarray,
) -> float:
    """Score val predictions for one HPO trial under the chosen objective.

    ``mean_ap``            — per-query mAP (``items`` unused).
    ``macro_per_item_map`` — macro average of per-item attributed mAP.

    Unknown ``objective_name`` raises ``ValueError`` (fail-loud).
    """
    if objective_name == "mean_ap":
        return compute_mean_ap(groups, y_true, y_score)
    if objective_name == "macro_per_item_map":
        return compute_macro_per_item_map(groups, items, y_true, y_score)
    raise ValueError(
        f"unknown training.hpo_objective {objective_name!r}; "
        f"allowed: {', '.join(HPO_OBJECTIVES)}"
    )
```

Confirm `Optional` is imported at the top of the file. If not, add `from typing import Optional` with the other imports.

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py::TestHpoScore -q
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): add _hpo_score selector for switchable HPO objective"
```

---

### Task 5: Wire `tune_hyperparameters` to the config switch + ship config

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (`tune_hyperparameters` body: extract block ~line 295-300; `best_state` line 302; scoring + best-update ~line 349-367; final log ~line 378-380)
- Modify: `conf/base/parameters_training.yaml` (`training:` block)
- Test: existing `tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters` (default path must still pass) + one new macro-path test

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_training/test_nodes.py` inside (or just after) `class TestTuneHyperparameters` — it reuses the module's `lgb_handles`, `preprocessor_metadata`, `training_parameters` fixtures:

```python
    def test_macro_per_item_objective_runs_and_returns_model(
        self, lgb_handles, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h, val_h = lgb_handles
        params = dict(training_parameters)
        params["hpo_objective"] = "macro_per_item_map"

        best_params, best_iteration, best_model = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params
        )
        assert isinstance(best_params, dict)
        assert best_model is not None

    def test_unknown_objective_raises_before_trials(
        self, lgb_handles, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h, val_h = lgb_handles
        params = dict(training_parameters)
        params["hpo_objective"] = "bogus"

        with pytest.raises(ValueError, match="hpo_objective"):
            tune_hyperparameters(
                train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, params
            )
```

Note: `training_parameters` is a dict fixture; `dict(...)` gives a shallow copy so the added key does not leak to other tests.

- [ ] **Step 2: Run the new tests to verify they fail**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest "tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_macro_per_item_objective_runs_and_returns_model" "tests/test_pipelines/test_training/test_nodes.py::TestTuneHyperparameters::test_unknown_objective_raises_before_trials" -q
```
Expected: FAIL — `test_unknown_objective_raises_before_trials` does not raise (no validation yet); the macro test either errors on the unhandled key path or scores with mean_ap.

- [ ] **Step 3: Write minimal implementation**

In `tune_hyperparameters` (`src/recsys_tfb/pipelines/training/nodes.py`):

(a) After `algorithm = training_params.get("algorithm", "lightgbm")` (line 281), add objective resolution + early validation:

```python
    hpo_objective = training_params.get("hpo_objective", "mean_ap")
    if hpo_objective not in HPO_OBJECTIVES:
        raise ValueError(
            f"unknown training.hpo_objective {hpo_objective!r}; "
            f"allowed: {', '.join(HPO_OBJECTIVES)}"
        )
```

(b) Replace the extract block (currently line 297-300):

```python
    with log_step(logger, "extract_features"):
        X_v, y_v, groups_v = extract_Xy_with_groups(
            val_parquet_handle, preprocessor_metadata, parameters,
        )
```

with:

```python
    with log_step(logger, "extract_features"):
        if hpo_objective == "macro_per_item_map":
            X_v, y_v, groups_v, items_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
                with_items=True,
            )
        else:
            X_v, y_v, groups_v = extract_Xy_with_groups(
                val_parquet_handle, preprocessor_metadata, parameters,
            )
            items_v = None
```

(c) Replace `best_state` init (line 302):

```python
    best_state: dict = {"mean_ap": -1.0, "model": None, "iteration": 0}
```

with:

```python
    best_state: dict = {"score": -1.0, "model": None, "iteration": 0}
```

(d) Replace the scoring + best-update block (currently line 349-367):

```python
        with log_step(logger, "score"):
            mean_ap = compute_mean_ap(groups_v, y_v, y_pred)

        if mean_ap > best_state["mean_ap"]:
            best_state["mean_ap"] = mean_ap
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed ap=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, n_trials, mean_ap,
            adapter.booster.best_iteration, duration, best_state["mean_ap"],
        )

        return mean_ap
```

with:

```python
        with log_step(logger, "score"):
            score = _hpo_score(hpo_objective, groups_v, items_v, y_v, y_pred)

        if score > best_state["score"]:
            best_state["score"] = score
            best_state["model"] = adapter
            # `best_iteration` is set by the early_stopping callback on the
            # underlying Booster regardless of whether early stopping fired.
            best_state["iteration"] = adapter.booster.best_iteration

        duration = time.monotonic() - t0
        logger.info(
            "tune_hyperparameters: trial=%d/%d completed score=%.4f "
            "best_iteration=%d duration=%.1fs best_so_far=%.4f",
            trial_idx, n_trials, score,
            adapter.booster.best_iteration, duration, best_state["score"],
        )

        return score
```

(e) Replace the final summary log (currently line 378-381):

```python
    logger.info(
        "Best trial mAP: %.4f, best_iteration: %d, params: %s",
        study.best_value, best_iteration, best_params,
    )
```

with (adds the objective name; `study.best_value` is the best returned score — unchanged source):

```python
    logger.info(
        "Best trial score (%s): %.4f, best_iteration: %d, params: %s",
        hpo_objective, study.best_value, best_iteration, best_params,
    )
```

- [ ] **Step 4: Add the config knob**

In `conf/base/parameters_training.yaml`, inside the `training:` block, add after `n_trials: 20` (line 45):

```yaml
  # HPO scoring objective (switchable). Routed by tune_hyperparameters via
  # _hpo_score; independent of algorithm_params.objective (the LightGBM
  # learning objective).
  #   mean_ap            — per-(snap_date,cust_id) query mAP, query-equal-weight
  #                        (frequent products dominate via positive frequency).
  #   macro_per_item_map — evaluation's macro_avg.by_item.map_attr@all: per-item
  #                        attributed mAP, macro-averaged over products
  #                        (every product equal weight; cold products count).
  # NOTE: this key is inside the model_version-hashed training: block, so
  # changing it is a deliberate one-time model_version bump.
  hpo_objective: macro_per_item_map
```

- [ ] **Step 5: Run the new + existing tune tests to verify they pass**

Run:
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/test_pipelines/test_training/test_nodes.py -q
```
Expected: PASS — including `TestTuneHyperparameters` default-path tests (default `mean_ap`, log prefixes `start `/`completed ` preserved), the two new tests, and `TestHpoScore`.

- [ ] **Step 6: Commit**

```bash
git add src/recsys_tfb/pipelines/training/nodes.py conf/base/parameters_training.yaml tests/test_pipelines/test_training/test_nodes.py
git commit -m "feat(training): switchable HPO objective via training.hpo_objective (default macro_per_item_map)"
```

---

### Task 6: Full regression on touched areas + graph refresh

**Files:** none (verification only)

- [ ] **Step 1: Run the affected test modules**

Run (background if >2 min):
```bash
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/test_evaluation/test_metrics.py \
  tests/test_io/test_extract.py \
  tests/test_pipelines/test_training/test_nodes.py \
  tests/test_core/test_versioning.py \
  -q
```
Expected: all PASS. (versioning included because `hpo_objective` enters the hashed `training:` block — tests build their own param dicts, so they should be unaffected; if a golden-hash test fails, that is the expected one-time model_version bump — update the golden value and note it in the commit.)

- [ ] **Step 2: Refresh the graphify code graph**

Run:
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```

- [ ] **Step 3: Commit any graph changes**

```bash
git add -A
git commit -m "chore: refresh graphify code graph" || echo "no graph changes"
```

---

## Self-Review

**Spec coverage:**
- §架構 (a) numpy primitive → Task 1 ✓; parity to Spark def → Task 2 ✓
- §架構 (b) extract with_items → Task 3 ✓
- §架構 (c) _hpo_score selector + tune_hyperparameters wiring → Tasks 4, 5 ✓
- §架構 (d) config yaml default macro_per_item_map → Task 5 ✓
- §驗證 node-level fail-loud (not consistency.py) → Task 4 (`_hpo_score`) + Task 5 (early validation) ✓
- §驗證 one-time model_version bump → Task 5 config note + Task 6 versioning run ✓
- §測試 1–4 → Tasks 1, 2, 3, 4, 5 ✓
- §YAGNI K fixed all (k param kept, no config) → Task 1 signature ✓

**Placeholder scan:** none — all steps carry real code/commands.

**Type/name consistency:** `compute_macro_per_item_map(groups, items, y_true, y_score, k=None)` used identically in Tasks 1, 2, 4. `_hpo_score(objective_name, groups, items, y_true, y_score)` used identically in Tasks 4, 5. `HPO_OBJECTIVES` defined Task 4, reused Task 5. `with_items` flag defined Task 3, used Task 5. `best_state["score"]` consistent across Task 5 edits.
