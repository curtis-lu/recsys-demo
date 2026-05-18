# Configurable HPO — Phase 1: lambdarank/rank_xendcg + Group Plumbing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the LightGBM training objective switchable to `lambdarank` / `rank_xendcg` by plumbing the per-query group through the cached `lgb.Dataset` binaries, the HPO early-stopping path, and the `refit_on_full` finalize path — fail-fast on incoherent ranking config.

**Architecture:** A new algorithm-agnostic `core/group_utils.py` converts the existing per-row group-id array (`extract_Xy_with_groups`) into LightGBM run-length `group` counts plus a stable contiguity permutation. `LightGBMAdapter.prepare_train_inputs` gains an objective-family sub-path (`lgb/binary/` vs `lgb/ranking/`) so a group-bearing binary can never be silently reused for the wrong objective. A new Layer-1 consistency predicate (A7) rejects ranking-objective + non-ranking-metric and undefined query group at CLI entry. The offline evaluation pipeline and `compute_mean_ap` are untouched; HPO still selects on val mAP.

**Tech Stack:** Python 3.10, LightGBM 4.6.0, numpy 1.25.0, Optuna 4.5.0, pytest 7.3.1. No new dependencies (production constraint).

---

## Conventions for every command in this plan

This work happens in the worktree. Define once (every `Run:` below assumes these):

```bash
WT=/Users/curtislu/projects/recsys_tfb/.worktrees/configurable-hpo-search-space
PY=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
# All pytest/CLI invocations:
#   PYTHONPATH=$WT/src $PY -m pytest <path> -q
# All git invocations:
#   git -C $WT <args>     (venv must be on PATH so the post-commit graphify
#                           hook resolves `python3`; prepend $PY's dir)
export PATH="/Users/curtislu/projects/recsys_tfb/.venv/bin:$PATH"
```

Pre-flight (run once before starting; abort if any line is wrong):

```bash
readlink $WT/.venv          # must print /Users/curtislu/projects/recsys_tfb/.venv
$PY -V                      # must print Python 3.10.9
git -C $WT rev-parse --abbrev-ref HEAD   # must print feat/configurable-hpo-search-space
```

Phase 1 adds **no Spark tests** — all new tests are numpy / parquet / pure-Python and run in seconds. Run only the touched test files per task (project test-perf rule).

---

## File Structure

| File | Create / Modify | Responsibility |
|---|---|---|
| `src/recsys_tfb/core/group_utils.py` | **Create** | Algorithm-agnostic ranking helpers: `RANKING_OBJECTIVES`, `is_ranking_objective`, `objective_family`, `default_metric_for_objective`, `to_contiguous_groups`. Pure; imports only numpy. |
| `tests/test_core/test_group_utils.py` | **Create** | Unit tests for the above. |
| `src/recsys_tfb/core/consistency.py` | **Modify** | Add A7 predicate `ranking_objective_conflicts`; extend module legend docstring; wire into `validate_config_consistency`. |
| `tests/test_core/test_consistency.py` | **Modify** | A7 predicate tests. |
| `tests/test_core/test_consistency_cli_wiring.py` | **Modify** | A7 surfaces through `validate_config_consistency`. |
| `src/recsys_tfb/models/lightgbm_adapter.py` | **Modify** | `prepare_train_inputs`: objective-family sub-path + ranking branch (group via `extract_Xy_with_groups` + `to_contiguous_groups`). |
| `tests/test_models/test_adapter.py` | **Modify** | Ranking `prepare_train_inputs` + family sub-path tests. |
| `src/recsys_tfb/pipelines/training/nodes.py` | **Modify** | `tune_hyperparameters` + `finalize_model`: default ranking metric when unset; `refit_on_full` group plumbing. |
| `tests/test_pipelines/test_training/test_nodes.py` | **Modify** | `finalize_model` refit ranking + metric-default tests. |
| `conf/base/parameters_training.yaml` | **Modify** | Commented lambdarank usage example + `ndcg_eval_at` doc. No functional default change → **no `model_version` change in Phase 1**. |

`group_utils.py` is the shared, single-source unit reused by XGBoost in Phase 4 — that is why it is algorithm-agnostic and has no LightGBM import.

---

## Task 1: `core/group_utils.py` — ranking helpers

**Files:**
- Create: `src/recsys_tfb/core/group_utils.py`
- Test: `tests/test_core/test_group_utils.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/test_group_utils.py`:

```python
"""Tests for recsys_tfb.core.group_utils."""

import numpy as np
import pytest

from recsys_tfb.core.group_utils import (
    RANKING_OBJECTIVES,
    default_metric_for_objective,
    is_ranking_objective,
    objective_family,
    to_contiguous_groups,
)


class TestObjectiveClassification:
    def test_ranking_objectives_set(self):
        assert RANKING_OBJECTIVES == frozenset({"lambdarank", "rank_xendcg"})

    @pytest.mark.parametrize("obj", ["lambdarank", "rank_xendcg"])
    def test_is_ranking_true(self, obj):
        assert is_ranking_objective(obj) is True

    @pytest.mark.parametrize("obj", ["binary", "regression", None, ""])
    def test_is_ranking_false(self, obj):
        assert is_ranking_objective(obj) is False

    def test_objective_family(self):
        assert objective_family("lambdarank") == "ranking"
        assert objective_family("rank_xendcg") == "ranking"
        assert objective_family("binary") == "binary"
        assert objective_family(None) == "binary"


class TestDefaultMetricForObjective:
    def test_ranking_without_metric_defaults_ndcg(self):
        assert default_metric_for_objective("lambdarank", None) == "ndcg"
        assert default_metric_for_objective("rank_xendcg", "") == "ndcg"

    def test_ranking_with_metric_kept(self):
        assert default_metric_for_objective("lambdarank", "ndcg") == "ndcg"
        assert default_metric_for_objective("lambdarank", "map") == "map"

    def test_non_ranking_metric_unchanged(self):
        assert default_metric_for_objective("binary", None) is None
        assert default_metric_for_objective("binary", "binary_logloss") == "binary_logloss"


class TestToContiguousGroups:
    def test_empty_input(self):
        perm, counts = to_contiguous_groups(np.array([], dtype=np.int64))
        assert perm.shape == (0,)
        assert counts.shape == (0,)
        assert perm.dtype == np.int64
        assert counts.dtype == np.int64

    def test_already_contiguous(self):
        ids = np.array([0, 0, 1, 2, 2, 2], dtype=np.int64)
        perm, counts = to_contiguous_groups(ids)
        np.testing.assert_array_equal(perm, np.array([0, 1, 2, 3, 4, 5]))
        np.testing.assert_array_equal(counts, np.array([2, 1, 3]))
        assert int(counts.sum()) == len(ids)

    def test_interleaved_ids_made_contiguous_stably(self):
        # group 2 (rows 0,1), group 0 (rows 2,3), group 1 (row 4)
        ids = np.array([2, 2, 0, 0, 1], dtype=np.int64)
        perm, counts = to_contiguous_groups(ids)
        # stable sort by id -> rows of id 0 (orig 2,3), id 1 (orig 4), id 2 (orig 0,1)
        np.testing.assert_array_equal(perm, np.array([2, 3, 4, 0, 1]))
        np.testing.assert_array_equal(counts, np.array([2, 1, 2]))
        sorted_ids = ids[perm]
        # each group is now a single contiguous run
        np.testing.assert_array_equal(sorted_ids, np.array([0, 0, 1, 2, 2]))
        assert int(counts.sum()) == len(ids)

    def test_perm_applies_to_X_and_y(self):
        ids = np.array([1, 0, 1, 0], dtype=np.int64)
        X = np.array([[10], [20], [30], [40]], dtype=float)
        y = np.array([1, 0, 0, 1])
        perm, counts = to_contiguous_groups(ids)
        np.testing.assert_array_equal(ids[perm], np.array([0, 0, 1, 1]))
        np.testing.assert_array_equal(X[perm].ravel(), np.array([20, 40, 10, 30]))
        np.testing.assert_array_equal(y[perm], np.array([0, 1, 1, 0]))
        np.testing.assert_array_equal(counts, np.array([2, 2]))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_group_utils.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.core.group_utils'`

- [ ] **Step 3: Write the module**

Create `src/recsys_tfb/core/group_utils.py`:

```python
"""Algorithm-agnostic ranking / query-group helpers.

Single source for: which objectives are learning-to-rank, the default eval
metric for them, and converting the per-row group-id array produced by
``recsys_tfb.io.extract.extract_Xy_with_groups`` into the run-length
``group`` count vector LightGBM / XGBoost ranking require.

Pure module: imports only numpy. Reused by both LightGBM (Phase 1) and a
future XGBoost adapter (Phase 4), so it must stay framework-free.
"""

from __future__ import annotations

import numpy as np

# LightGBM learning-to-rank objectives this project supports. XGBoost's
# rank:* objectives map onto the same group plumbing in Phase 4.
RANKING_OBJECTIVES: frozenset[str] = frozenset({"lambdarank", "rank_xendcg"})


def is_ranking_objective(objective: str | None) -> bool:
    """True iff ``objective`` is a supported learning-to-rank objective."""
    return objective in RANKING_OBJECTIVES


def objective_family(objective: str | None) -> str:
    """Coarse family used to key the on-disk lgb-binary cache sub-path.

    ``"ranking"`` for any RANKING_OBJECTIVES value (so lambdarank and
    rank_xendcg share one group-bearing binary), else ``"binary"``.
    """
    return "ranking" if is_ranking_objective(objective) else "binary"


def default_metric_for_objective(objective: str | None, metric):
    """Default the eval metric to ``"ndcg"`` for a ranking objective with no
    metric set.

    LightGBM binary metrics are invalid under lambdarank/rank_xendcg and make
    early stopping silently meaningless. An explicitly-set *contradictory*
    metric is rejected upstream by the A7 consistency check
    (``ranking_objective_conflicts``); this helper only fills the
    *unset* case so behaviour is never silently wrong.
    """
    if is_ranking_objective(objective) and not metric:
        return "ndcg"
    return metric


def to_contiguous_groups(group_ids: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Make every query group contiguous and return ``(sort_perm, counts)``.

    ``group_ids``: per-row int array from ``extract_Xy_with_groups`` — rows in
    the same query share an id; ids are NOT guaranteed contiguous or sorted.
    LightGBM/XGBoost ranking need rows ordered so each group is one
    consecutive block plus a run-length ``group`` count vector.

    Returns:
        sort_perm: int64 index array. Apply as ``X[sort_perm]``,
            ``y[sort_perm]`` before building the Dataset.
        counts: int64 run-length per group, ordered to match the sorted rows;
            ``counts.sum() == len(group_ids)``.

    Empty input returns two empty int64 arrays. The sort is stable so row
    order within a group is preserved and the result is deterministic
    regardless of the integer labels chosen for the groups.
    """
    group_ids = np.asarray(group_ids)
    if group_ids.shape[0] == 0:
        return np.empty(0, dtype=np.int64), np.empty(0, dtype=np.int64)
    sort_perm = np.argsort(group_ids, kind="stable").astype(np.int64)
    sorted_ids = group_ids[sort_perm]
    _, counts = np.unique(sorted_ids, return_counts=True)
    return sort_perm, counts.astype(np.int64)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_group_utils.py -q`
Expected: PASS (all tests green)

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/core/group_utils.py tests/test_core/test_group_utils.py
git -C $WT commit -m "feat(hpo): add core.group_utils ranking helpers (Phase 1)"
git -C $WT rev-parse --short HEAD   # confirm HEAD advanced
git -C $WT status --porcelain      # must NOT show graphify-out/GRAPH_REPORT.md
```

---

## Task 2: A7 consistency predicate — reject incoherent ranking config

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (legend docstring lines 17-38; add predicate after `item_missing_from_categorical` ~line 168; wire into `validate_config_consistency` ~line 210 before the `if errors:` block)
- Test: `tests/test_core/test_consistency.py` (append), `tests/test_core/test_consistency_cli_wiring.py` (append)

- [ ] **Step 1: Write the failing predicate tests**

Append to `tests/test_core/test_consistency.py`:

```python
from recsys_tfb.core.consistency import ranking_objective_conflicts


class TestRankingObjectiveConflicts:
    def _params(self, objective=None, metric=None, entity=("cust_id",)):
        ap = {}
        if objective is not None:
            ap["objective"] = objective
        if metric is not None:
            ap["metric"] = metric
        return {
            "schema": {"columns": {
                "time": "snap_date",
                "entity": list(entity),
                "item": "prod_name",
                "label": "label",
            }},
            "training": {"algorithm_params": ap},
        }

    def test_non_ranking_objective_ok(self):
        assert ranking_objective_conflicts(
            self._params("binary", "binary_logloss")) == []

    def test_no_training_block_ok(self):
        assert ranking_objective_conflicts({"schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}}}) == []

    def test_ranking_with_ndcg_ok(self):
        assert ranking_objective_conflicts(
            self._params("lambdarank", "ndcg")) == []

    def test_ranking_without_metric_ok(self):
        # unset metric is allowed — defaulted to ndcg at train time
        assert ranking_objective_conflicts(
            self._params("rank_xendcg", None)) == []

    def test_ranking_with_binary_metric_rejected(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "binary_logloss"))
        assert len(errs) == 1
        assert "ranking metric" in errs[0]
        assert "binary_logloss" in errs[0]

    def test_ranking_with_empty_entity_rejected(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "ndcg", entity=()))
        assert len(errs) == 1
        assert "query group" in errs[0]

    def test_collect_all_both_failures(self):
        errs = ranking_objective_conflicts(
            self._params("lambdarank", "binary_logloss", entity=()))
        assert len(errs) == 2
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency.py -q -k RankingObjectiveConflicts`
Expected: FAIL — `ImportError: cannot import name 'ranking_objective_conflicts'`

- [ ] **Step 3: Add the predicate and wire it in**

In `src/recsys_tfb/core/consistency.py`, add the import near the top (after `from recsys_tfb.core.schema import get_schema`, line 59):

```python
from recsys_tfb.core.group_utils import RANKING_OBJECTIVES
```

Add the predicate after `item_missing_from_categorical` (after line 168, before `validate_config_consistency`):

```python
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
```

In `validate_config_consistency`, add this block immediately before the final `if errors:` (currently line ~212):

```python
    for msg in ranking_objective_conflicts(parameters):
        errors.append(msg)
```

In the module legend docstring, under the Layer-1 list (after the A6 bullet, before the `Layer 2` heading, ~line 38), add:

```
* A7 — a ranking ``training.algorithm_params.objective``
  (``lambdarank``/``rank_xendcg``) paired with a non-ranking ``metric`` or an
  undefined query group (empty ``schema.entity``). Predicate:
  ``ranking_objective_conflicts``.
```

- [ ] **Step 4: Run predicate tests to verify they pass**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency.py -q`
Expected: PASS (the whole file, incl. existing A1–A6 tests, stays green)

- [ ] **Step 5: Add + run the CLI-wiring test**

Append to `tests/test_core/test_consistency_cli_wiring.py`:

```python
def test_a7_ranking_conflict_surfaces_via_validate():
    from recsys_tfb.core.consistency import (
        ConfigConsistencyError,
        validate_config_consistency,
    )
    import pytest

    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"algorithm_params": {
            "objective": "lambdarank", "metric": "binary_logloss"}},
    }
    with pytest.raises(ConfigConsistencyError, match="ranking metric"):
        validate_config_consistency(params)
```

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git -C $WT add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py
git -C $WT commit -m "feat(hpo): A7 consistency — reject incoherent ranking config (Phase 1)"
git -C $WT status --porcelain   # must NOT show graphify-out/GRAPH_REPORT.md
```

---

## Task 3: `LightGBMAdapter.prepare_train_inputs` — family sub-path + group

**Files:**
- Modify: `src/recsys_tfb/models/lightgbm_adapter.py` (`prepare_train_inputs`, lines 122-216)
- Test: `tests/test_models/test_adapter.py` (append)

The binary cache is `cache_dir/lgb/...` and is **not** keyed by `model_version`
(objective lives in `algorithm_params`, which feeds `model_version`, not the
binary path). Inserting an objective-family segment (`lgb/binary/` vs
`lgb/ranking/`) makes reuse-for-wrong-objective impossible. The binary path
stays byte-identical (no group, no row reorder); only its directory changes.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_models/test_adapter.py`:

```python
def _ranking_parameters(objective):
    return {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"algorithm_params": {"objective": objective}},
    }


def _ranking_frames():
    import pandas as pd
    # 3 customers x 2 products on one snap_date => 3 query groups of size 2
    df_tr = pd.DataFrame({
        "cust_id": ["c1", "c1", "c2", "c2", "c3", "c3"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 6),
        "prod_name": ["fund", "ccard"] * 3,
        "feat_a": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
        "label": [1, 0, 0, 1, 1, 0],
    })
    df_dev = pd.DataFrame({
        "cust_id": ["c4", "c4", "c5", "c5"],
        "snap_date": pd.to_datetime(["2025-01-31"] * 4),
        "prod_name": ["fund", "ccard"] * 2,
        "feat_a": [1.5, 2.5, 3.5, 4.5],
        "label": [0, 1, 1, 0],
    })
    return df_tr, df_dev


def test_prepare_train_inputs_binary_family_subpath(tmp_path):
    import lightgbm as lgb
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("binary"), str(cache),
    )
    assert (cache / "lgb" / "binary" / "_SUCCESS").exists()
    assert not (cache / "lgb" / "ranking").exists()
    ds = lgb.Dataset(str(cache / "lgb" / "binary" / "train.bin")).construct()
    assert ds.get_group() is None  # binary path: no group set


def test_prepare_train_inputs_ranking_sets_group(tmp_path):
    import lightgbm as lgb
    import numpy as np
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    train_h, dev_h = LightGBMAdapter().prepare_train_inputs(
        ParquetHandle(str(tr)), ParquetHandle(str(dv)),
        prep_meta, _ranking_parameters("lambdarank"), str(cache),
    )
    assert (cache / "lgb" / "ranking" / "_SUCCESS").exists()
    assert "ranking" in train_h.bin_path and train_h.role == "train"
    assert "ranking" in dev_h.bin_path and dev_h.role == "train_dev"

    ds_tr = lgb.Dataset(train_h.bin_path).construct()
    g_tr = ds_tr.get_group()
    assert g_tr is not None
    np.testing.assert_array_equal(np.sort(g_tr), np.array([2, 2, 2]))
    assert int(np.sum(g_tr)) == 6  # all train rows covered

    ds_dv = lgb.Dataset(dev_h.bin_path, reference=ds_tr).construct()
    g_dv = ds_dv.get_group()
    np.testing.assert_array_equal(np.sort(g_dv), np.array([2, 2]))
    assert int(np.sum(g_dv)) == 4


def test_prepare_train_inputs_both_families_coexist(tmp_path):
    """Switching objective rebuilds in its own sub-path; never reuses the
    other family's binary."""
    from recsys_tfb.io.handles import ParquetHandle
    from recsys_tfb.models.lightgbm_adapter import LightGBMAdapter

    df_tr, df_dev = _ranking_frames()
    tr = tmp_path / "tr.parquet"; dv = tmp_path / "dv.parquet"
    df_tr.to_parquet(tr); df_dev.to_parquet(dv)
    prep_meta = {
        "feature_columns": ["feat_a", "prod_name"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": ["fund", "ccard"]},
    }
    cache = tmp_path / "variant"
    a = LightGBMAdapter()
    a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                           prep_meta, _ranking_parameters("binary"), str(cache))
    a.prepare_train_inputs(ParquetHandle(str(tr)), ParquetHandle(str(dv)),
                           prep_meta, _ranking_parameters("lambdarank"), str(cache))
    assert (cache / "lgb" / "binary" / "_SUCCESS").exists()
    assert (cache / "lgb" / "ranking" / "_SUCCESS").exists()
```

Note: the existing `test_lightgbm_prepare_train_inputs_writes_bins` /
`_cache_hit` / `_partial_cache_rebuild` / `_passes_categorical_feature` tests
use `parameters = {"schema": {...}}` with **no** `training` block — objective
resolves to the `"binary"` default, so they must keep passing with their
asserted paths updated. Update those four tests' path assertions from
`cache_dir / "lgb" / X` to `cache_dir / "lgb" / "binary" / X` (and the
`captured_cat_features` spy test needs no change — it asserts call count, not
path).

- [ ] **Step 2: Run to verify new tests fail (and see which old asserts break)**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_models/test_adapter.py -q`
Expected: FAIL — new tests fail (no `binary/` sub-path; no group); the four
existing path-asserting tests fail on the old `lgb/` path.

- [ ] **Step 3: Implement the family sub-path + ranking branch**

In `src/recsys_tfb/models/lightgbm_adapter.py`, replace the body of
`prepare_train_inputs` from the `lgb_dir = Path(cache_dir) / "lgb"` line
(line 142) through the `del X_dev, y_dev, ds_train, ds_dev` line (line 205)
with:

```python
        from recsys_tfb.core.group_utils import (
            is_ranking_objective,
            objective_family,
            to_contiguous_groups,
        )

        objective = (
            parameters.get("training", {})
            .get("algorithm_params", {})
            .get("objective")
        )
        family = objective_family(objective)
        ranking = is_ranking_objective(objective)

        # Objective-family sub-path: the lgb-binary cache is NOT keyed by
        # model_version, so a group-bearing ranking binary must never be
        # reused for a binary objective (or vice versa). lambdarank and
        # rank_xendcg share the "ranking" family (identical group layout).
        lgb_dir = Path(cache_dir) / "lgb" / family
        success = lgb_dir / "_SUCCESS"
        train_bin = lgb_dir / "train.bin"
        dev_bin = lgb_dir / "train_dev.bin"

        if success.exists():
            logger.info("lgb binary cache hit at %s", lgb_dir)
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            return (
                LgbDatasetHandle(bin_path=str(train_bin), role="train"),
                LgbDatasetHandle(bin_path=str(dev_bin), role="train_dev"),
            )

        if lgb_dir.exists():
            logger.warning(
                "Partial lgb cache at %s, clearing before rebuild", lgb_dir
            )
            shutil.rmtree(lgb_dir)
        lgb_dir.mkdir(parents=True, exist_ok=True)

        cat_idx = self._categorical_indices(preprocessor_metadata)
        construct_params = {"feature_pre_filter": False}

        if ranking:
            from recsys_tfb.io.extract import extract_Xy_with_groups

            X_tr, y_tr, gid_tr = extract_Xy_with_groups(
                train_handle, preprocessor_metadata, parameters
            )
            perm_tr, grp_tr = to_contiguous_groups(gid_tr)
            ds_train = lgb.Dataset(
                X_tr[perm_tr],
                label=y_tr[perm_tr],
                group=grp_tr,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            ds_train.save_binary(str(train_bin))
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            del X_tr, y_tr, gid_tr, perm_tr

            X_dev, y_dev, gid_dev = extract_Xy_with_groups(
                train_dev_handle, preprocessor_metadata, parameters
            )
            perm_dev, grp_dev = to_contiguous_groups(gid_dev)
            ds_dev = lgb.Dataset(
                X_dev[perm_dev],
                label=y_dev[perm_dev],
                group=grp_dev,
                reference=ds_train,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            ds_dev.save_binary(str(dev_bin))
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            del X_dev, y_dev, gid_dev, perm_dev, ds_train, ds_dev
        else:
            from recsys_tfb.io.extract import extract_Xy

            X_tr, y_tr = extract_Xy(train_handle, preprocessor_metadata, parameters)
            ds_train = lgb.Dataset(
                X_tr,
                label=y_tr,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_train", ds_train)
            ds_train.save_binary(str(train_bin))
            log_data_volume(logger, "prepare.train.bin", str(train_bin))
            del X_tr, y_tr

            X_dev, y_dev = extract_Xy(train_dev_handle, preprocessor_metadata, parameters)
            ds_dev = lgb.Dataset(
                X_dev,
                label=y_dev,
                reference=ds_train,
                categorical_feature=cat_idx,
                params=construct_params,
                free_raw_data=True,
            ).construct()
            log_data_volume(logger, "prepare.ds_dev", ds_dev)
            ds_dev.save_binary(str(dev_bin))
            log_data_volume(logger, "prepare.train_dev.bin", str(dev_bin))
            del X_dev, y_dev, ds_train, ds_dev
```

Then update the four existing path-asserting tests in
`tests/test_models/test_adapter.py` (`_writes_bins`, `_cache_hit`,
`_partial_cache_rebuild`, and the `_passes_categorical_feature` cache dir is
fine — only the three with explicit `cache_dir / "lgb" / "..."` asserts):
change `cache_dir / "lgb" / "train.bin"` → `cache_dir / "lgb" / "binary" / "train.bin"`
(and `train_dev.bin`, `_SUCCESS` similarly).

- [ ] **Step 4: Run the full adapter test file**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_models/test_adapter.py -q`
Expected: PASS (new ranking tests + the four updated binary-path tests + all others)

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/models/lightgbm_adapter.py tests/test_models/test_adapter.py
git -C $WT commit -m "feat(hpo): lgb prepare_train_inputs objective-family subpath + group (Phase 1)"
git -C $WT status --porcelain   # must NOT show graphify-out/GRAPH_REPORT.md
```

---

## Task 4: `tune_hyperparameters` — default ranking metric for early stopping

In `tune_hyperparameters` the early-stopping dataset is the `train_dev`
binary (now group-bearing for ranking via Task 3). The eval metric flows from
`algorithm_params`. If a user sets `objective: lambdarank` but omits `metric`,
LightGBM would early-stop on the wrong metric. Default it to `ndcg` when
unset (A7 already rejects an explicit *contradictory* metric).

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (`tune_hyperparameters`, line 282 area)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_training/test_nodes.py`:

```python
def test_tune_defaults_ranking_metric(monkeypatch):
    """algorithm_params with a ranking objective and no metric => params
    passed to adapter.train carry metric='ndcg'."""
    import numpy as np
    from recsys_tfb.pipelines.training import nodes

    captured = {}

    class FakeAdapter:
        booster = type("B", (), {"best_iteration": 3})()

        def train(self, **kw):
            captured.update(kw["params"])

        def predict(self, X):
            return np.zeros(len(X))

    monkeypatch.setattr(nodes, "get_adapter", lambda algo: FakeAdapter())
    monkeypatch.setattr(
        nodes, "compute_mean_ap", lambda g, y, p: 0.5
    )

    def fake_extract(handle, meta, params, **kw):
        X = np.zeros((4, 2)); y = np.array([1, 0, 1, 0])
        g = np.array([0, 0, 1, 1], dtype=np.int64)
        return X, y, g

    monkeypatch.setattr(
        "recsys_tfb.io.extract.extract_Xy_with_groups", fake_extract
    )

    class FakeLgbHandle:
        def load(self, reference=None, params=None):
            class D:
                def construct(self_inner):
                    return self_inner
            return D()

    parameters = {
        "training": {
            "n_trials": 1,
            "num_iterations": 5,
            "early_stopping_rounds": 2,
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "lambdarank"},
            "search_space": {
                "learning_rate": {"low": 0.01, "high": 0.1},
                "num_leaves": {"low": 4, "high": 8},
                "max_depth": {"low": 3, "high": 5},
                "min_child_samples": {"low": 5, "high": 10},
                "subsample": {"low": 0.6, "high": 1.0},
                "colsample_bytree": {"low": 0.6, "high": 1.0},
            },
        },
        "random_seed": 42,
    }
    nodes.tune_hyperparameters(
        FakeLgbHandle(), FakeLgbHandle(), object(), {}, parameters
    )
    assert captured.get("metric") == "ndcg"
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_nodes.py -q -k tune_defaults_ranking_metric`
Expected: FAIL — `captured.get("metric")` is `None` (no metric defaulting yet)

- [ ] **Step 3: Implement the metric default**

In `src/recsys_tfb/pipelines/training/nodes.py`, in `tune_hyperparameters`,
replace line 282:

```python
    algorithm_params = training_params.get("algorithm_params", {})
```

with:

```python
    from recsys_tfb.core.group_utils import default_metric_for_objective

    algorithm_params = dict(training_params.get("algorithm_params", {}))
    _metric = default_metric_for_objective(
        algorithm_params.get("objective"), algorithm_params.get("metric")
    )
    if _metric:
        algorithm_params["metric"] = _metric
```

(The local `dict(...)` copy is required so the defaulted metric does not
mutate the shared `parameters` dict — `parameters` is still written verbatim
to `manifest.json`.)

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_nodes.py -q -k tune_defaults_ranking_metric`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git -C $WT commit -m "feat(hpo): default ranking metric to ndcg in tune_hyperparameters (Phase 1)"
git -C $WT status --porcelain
```

---

## Task 5: `finalize_model` — `refit_on_full` group plumbing for ranking

`finalize_model` default strategy `hpo_best` returns the trial-trained adapter
unchanged → already correct for ranking (Task 3 trained it with group).
`refit_on_full` rebuilds on `train + train_dev` via `extract_Xy` (no group) →
**incorrect for a ranking objective**. Fix it; leave the binary path
unchanged. Train and train_dev are customer-disjoint by sampling design
(`train_dev_ratio` partitions customers), so a `(snap_date, cust_id)` query
never spans both splits — offsetting dev group ids past train's max is
correct.

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (`finalize_model`, lines 444-495)
- Test: `tests/test_pipelines/test_training/test_nodes.py` (append)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_pipelines/test_training/test_nodes.py`:

```python
def test_finalize_refit_ranking_sets_group(monkeypatch):
    import numpy as np
    import lightgbm as lgb
    from recsys_tfb.pipelines.training import nodes

    captured = {}

    def fake_extract_groups(handle, meta, params, **kw):
        # train: 2 groups of 2 ; dev: 1 group of 2
        if getattr(handle, "tag", "") == "dev":
            X = np.ones((2, 2)); y = np.array([1, 0])
            g = np.array([0, 0], dtype=np.int64)
        else:
            X = np.zeros((4, 2)); y = np.array([1, 0, 0, 1])
            g = np.array([0, 0, 1, 1], dtype=np.int64)
        return X, y, g

    monkeypatch.setattr(
        "recsys_tfb.io.extract.extract_Xy_with_groups", fake_extract_groups
    )

    real_dataset = lgb.Dataset

    def spy_dataset(*a, **kw):
        if "group" in kw and kw["group"] is not None:
            captured["group"] = np.asarray(kw["group"])
        return real_dataset(*a, **kw)

    monkeypatch.setattr(lgb, "Dataset", spy_dataset)

    class FakeAdapter:
        def train(self, **kw):
            captured["metric"] = kw["params"].get("metric")

    monkeypatch.setattr(nodes, "get_adapter", lambda algo: FakeAdapter())

    class H:
        def __init__(self, tag=""):
            self.tag = tag

    parameters = {
        "training": {
            "final_model_strategy": "refit_on_full",
            "algorithm": "lightgbm",
            "algorithm_params": {"objective": "lambdarank"},
        },
        "random_seed": 42,
    }
    prep_meta = {"feature_columns": ["a", "b"], "categorical_columns": []}
    nodes.finalize_model(
        H("train"), H("dev"), object(), {"num_leaves": 4}, 3,
        prep_meta, parameters,
    )
    # 3 groups total (2 from train + 1 from dev), all size 2
    np.testing.assert_array_equal(
        np.sort(captured["group"]), np.array([2, 2, 2])
    )
    assert int(captured["group"].sum()) == 6
    assert captured["metric"] == "ndcg"
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_nodes.py -q -k finalize_refit_ranking_sets_group`
Expected: FAIL — `KeyError: 'group'` (refit path uses `extract_Xy`, no group)

- [ ] **Step 3: Implement ranking refit**

In `src/recsys_tfb/pipelines/training/nodes.py`, in `finalize_model`, replace
the block from `import lightgbm as lgb` / `from recsys_tfb.io.extract import
extract_Xy` (line 444-445) down through the `ds_full = lgb.Dataset(...)`
construction (line 478) and the `params = {...}` assembly (lines 480-487)
with:

```python
    import lightgbm as lgb
    import numpy as np

    from recsys_tfb.core.group_utils import (
        default_metric_for_objective,
        is_ranking_objective,
        to_contiguous_groups,
    )

    training_params = parameters["training"]
    seed = parameters.get("random_seed", 42)
    algorithm = training_params.get("algorithm", "lightgbm")
    algorithm_params = dict(training_params.get("algorithm_params", {}))
    objective = algorithm_params.get("objective")
    _metric = default_metric_for_objective(
        objective, algorithm_params.get("metric")
    )
    if _metric:
        algorithm_params["metric"] = _metric

    logger.info(
        "final_model_strategy=refit_on_full (num_iterations=%d, no early stopping)",
        best_iteration,
    )

    feat_cols = preprocessor_metadata["feature_columns"]
    cat_cols = preprocessor_metadata.get("categorical_columns", [])
    cat_idx = [feat_cols.index(c) for c in cat_cols if c in feat_cols] or None

    if is_ranking_objective(objective):
        from recsys_tfb.io.extract import extract_Xy_with_groups

        with log_step(logger, "extract_features"):
            X_tr, y_tr, gid_tr = extract_Xy_with_groups(
                train_parquet_handle, preprocessor_metadata, parameters
            )
            X_dv, y_dv, gid_dv = extract_Xy_with_groups(
                train_dev_parquet_handle, preprocessor_metadata, parameters
            )
        # train / train_dev are customer-disjoint by sampling design, so a
        # query group never spans both splits — offset dev ids past train's
        # max to keep them distinct after concatenation.
        offset = (int(gid_tr.max()) + 1) if len(gid_tr) else 0
        X_full = np.concatenate([X_tr, X_dv], axis=0)
        y_full = np.concatenate([y_tr, y_dv], axis=0)
        gid_full = np.concatenate([gid_tr, gid_dv + offset])
        log_data_volume(logger, "finalize.X_full", X_full)
        log_data_volume(logger, "finalize.y_full", y_full)
        del X_tr, y_tr, X_dv, y_dv, gid_tr, gid_dv

        perm, grp = to_contiguous_groups(gid_full)
        ds_full = lgb.Dataset(
            X_full[perm],
            label=y_full[perm],
            group=grp,
            categorical_feature=cat_idx,
            params={"feature_pre_filter": False},
            free_raw_data=True,
        )
    else:
        from recsys_tfb.io.extract import extract_Xy

        with log_step(logger, "extract_features"):
            X_tr, y_tr = extract_Xy(
                train_parquet_handle, preprocessor_metadata, parameters
            )
            X_dv, y_dv = extract_Xy(
                train_dev_parquet_handle, preprocessor_metadata, parameters
            )
        X_full = np.concatenate([X_tr, X_dv], axis=0)
        y_full = np.concatenate([y_tr, y_dv], axis=0)
        log_data_volume(logger, "finalize.X_full", X_full)
        log_data_volume(logger, "finalize.y_full", y_full)
        del X_tr, y_tr, X_dv, y_dv

        ds_full = lgb.Dataset(
            X_full,
            label=y_full,
            categorical_feature=cat_idx,
            params={"feature_pre_filter": False},
            free_raw_data=True,
        )

    params = {
        **algorithm_params,
        "seed": seed,
        "feature_pre_filter": False,
        **best_params,
        "num_iterations": best_iteration,
        "early_stopping_rounds": 0,
    }
```

The trailing `with log_step(logger, "model_refit"):` block (lines 489-501)
stays unchanged. Delete the now-duplicated lines that originally lived between
old line 447 and 487 (the old single-path `training_params`/`seed`/
`algorithm`/`algorithm_params` reads at 447-450, the old `extract_Xy`
block 457-464, the old `feat_cols`/`cat_idx` 466-468, the old `ds_full`
472-478, and the old `params` 480-487) — they are fully replaced above.

- [ ] **Step 4: Run to verify it passes (and the rest of the file)**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_nodes.py -q`
Expected: PASS (new test + all existing finalize/tune tests)

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/pipelines/training/nodes.py tests/test_pipelines/test_training/test_nodes.py
git -C $WT commit -m "feat(hpo): refit_on_full group plumbing for ranking objective (Phase 1)"
git -C $WT status --porcelain
```

---

## Task 6: YAML usage documentation + Phase 1 verification

No functional default change (default objective stays `binary`, so
`model_version` is **unchanged** in Phase 1 — comments are not part of the
parsed dict that `_model_version_payload` hashes).

**Files:**
- Modify: `conf/base/parameters_training.yaml` (the `algorithm_params:` block, lines 14-25)

- [ ] **Step 1: Add the commented usage block**

In `conf/base/parameters_training.yaml`, immediately after the
`algorithm_params:` mapping's existing keys (after the `num_threads: 4` line,
line 25), add:

```yaml
    # --- Learning-to-rank (Phase 1) -------------------------------------
    # To train a ranking model, set objective + a ranking metric, e.g.:
    #   objective: lambdarank      # or rank_xendcg
    #   metric: ndcg               # required ranking metric; omit => ndcg
    #   ndcg_eval_at: [5, 10]      # optional NDCG truncation levels
    # The per-query group is schema.columns.time + entity
    # (default: snap_date + cust_id) and is plumbed automatically into the
    # train/train_dev binaries and HPO early stopping. A ranking objective
    # with a non-ranking metric is rejected at CLI entry (consistency A7).
    # Evaluation stays per-customer mAP regardless of training objective.
    # --------------------------------------------------------------------
```

(`ndcg_eval_at` is passed straight through `algorithm_params` to LightGBM as
the native `ndcg_eval_at`/`eval_at` param — no code change needed; documented
here for discoverability.)

- [ ] **Step 2: Verify model_version is unchanged by the comment**

Run:
```bash
PYTHONPATH=$WT/src $PY -c "
import yaml, copy
from recsys_tfb.core.versioning import _model_version_payload
p = yaml.safe_load(open('$WT/conf/base/parameters_training.yaml'))
print('payload keys:', sorted(_model_version_payload(p).get('training', {})))
print('objective:', p['training']['algorithm_params'].get('objective'))
"
```
Expected: `objective: binary` (unchanged) — comments are absent from the
parsed dict, so the hashed payload is identical to pre-Phase-1.

- [ ] **Step 3: Run the full set of Phase-1-touched test files**

Run:
```bash
PYTHONPATH=$WT/src $PY -m pytest \
  $WT/tests/test_core/test_group_utils.py \
  $WT/tests/test_core/test_consistency.py \
  $WT/tests/test_core/test_consistency_cli_wiring.py \
  $WT/tests/test_models/test_adapter.py \
  $WT/tests/test_pipelines/test_training/test_nodes.py \
  -q
```
Expected: PASS — all green, 0 failures.

- [ ] **Step 4: Refresh the graphify code graph (CLAUDE.md rule after code changes)**

Run:
```bash
cd $WT && $PY -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
git -C $WT status --porcelain   # graphify-out/GRAPH_REPORT.md must be untracked (absent here)
```

- [ ] **Step 5: Commit**

```bash
git -C $WT add conf/base/parameters_training.yaml
git -C $WT commit -m "docs(hpo): document lambdarank usage in parameters_training.yaml (Phase 1)"
git -C $WT log --oneline 61ee9ac..HEAD   # review the Phase 1 commit series
```

---

## Self-Review

**1. Spec coverage (spec §"Phase 1"):**
- group_utils algorithm-agnostic → Task 1 ✓
- train/train_dev Dataset carries group → Task 3 (ranking branch + save_binary persists group) ✓
- trial-val (train_dev binary) carries group; ranking early-stopping metric → Task 3 (dev binary group) + Task 4 (metric default to ndcg) ✓
- lgb-binary cache discriminator (objective family sub-path) → Task 3 ✓
- consistency predicate (ranking objective ⟺ ranking metric; query group defined) → Task 2 (A7) ✓
- evaluation untouched → no eval/`compute_mean_ap` files modified anywhere in this plan ✓
- config delta (objective/metric/ndcg_eval_at) → Task 6 ✓
- one-time model_version bump is Phase 2 only; Phase 1 must NOT bump → Task 6 Step 2 verifies ✓
- `refit_on_full` correctness for ranking (spec mentions train+train_dev concatenated) → Task 5 ✓ (closes a silent-failure gap, consistent with the project's no-silent-failure ethos)

**2. Placeholder scan:** No "TBD"/"add error handling"/"similar to". Every code step has complete code; every run step has an exact command + expected result. ✓

**3. Type consistency:** `to_contiguous_groups(group_ids) -> (sort_perm, counts)` — same signature used in Tasks 1, 3, 5. `objective_family`/`is_ranking_objective`/`default_metric_for_objective` signatures consistent across Tasks 1, 3, 4, 5. `RANKING_OBJECTIVES` defined in `group_utils` (Task 1), imported by `consistency` (Task 2). `LgbDatasetHandle(bin_path=..., role=...)` matches `io/handles.py`. `ranking_objective_conflicts(parameters) -> list[str]` matches the collect-all predicate contract used by `validate_config_consistency`. ✓

No gaps found.

---

## Execution Handoff

(Provided by the orchestrator after plan approval — see the writing-plans skill's handoff options.)
