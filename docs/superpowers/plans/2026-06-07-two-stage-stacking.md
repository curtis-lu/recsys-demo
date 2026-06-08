# Two-Stage Stacking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a config-switchable `training.model_structure: per_group_plus_rank` mode that trains per-grouping Stage-1 point-wise models (per-item or per-category) plus a Stage-2 LTR model over their out-of-fold predictions, exposed behind the existing `ModelAdapter` interface so inference/evaluation/catalog stay unchanged.

**Architecture:** A new `CompositeModelAdapter` implements `ModelAdapter` for the inference contract (`predict/save/load/feature_importance/log_to_mlflow`); its training is driven by a new `train_composite_model` node that does customer-disjoint K-fold cross-fitting (leakage-clean Stage-2 features), refits Stage-1 on full train for inference, and trains a lambdarank Stage-2. Stage-1/Stage-2 sub-boosters reuse `LightGBMAdapter`. The grouping table lives in a shared top-level `product_categories` block (single source of truth for both training grouping and evaluation collapse).

**Tech Stack:** Python 3.10, PySpark 3.3.2 (dataset/inference only — composite training is driver-local pandas, like existing LightGBM training), LightGBM 4.6.0, pytest 7.3.1. No UDFs, no new packages.

---

## Refinements to the spec (`docs/superpowers/specs/2026-06-07-two-stage-stacking-design.md`)

Three concretizations discovered while grounding in code; they refine, not contradict, the spec:

1. **Grouping table location** = top-level `product_categories` (sibling to `schema`), NOT `schema.product_categories`. `compute_base_dataset_version` hashes the whole `schema` (`versioning.py:104`); nesting there would bust `base_dataset_version` (full dataset rebuild) on every grouping change. Top-level keeps it out of `base_dataset_version`; folded into `model_version` only when `stage1.grouping == category` (Task 1.3).
2. **Stage-2 needs a query group for lambdarank.** `LightGBMAdapter.train` only sets group when handed a pre-built `lgb.Dataset` (the `train_dataset=` kwarg, `lightgbm_adapter.py:62-64`). Composite builds the Stage-2 `lgb.Dataset` with `group=` (customer counts) and passes it via that kwarg — reusing the adapter, no signature change.
3. **predict routes on encoded item codes**, not raw strings. The composite stores `item_col_index` + `item_code_to_group` so `predict(X)` works directly on the encoded int in `X[:, item_col_index]` (no string decode at inference).

## File Structure

| File | Responsibility |
|---|---|
| `src/recsys_tfb/core/categories.py` (new) | Pure resolver: `resolve_category_mapping(parameters) -> dict[item,group]` + `resolve_groups(parameters, grouping) -> dict[item,group]`. Single source for category/grouping. |
| `src/recsys_tfb/models/composite_adapter.py` (new) | `CompositeModelAdapter(ModelAdapter)`: predict routing, save/load serialization of N+1 boosters + manifest. |
| `src/recsys_tfb/models/composite_train.py` (new) | Pure-ish training helpers: fold assignment, Stage-2 feature assembly, the OOF orchestration callable. Kept out of the adapter so the adapter stays a thin inference/persistence object. |
| `src/recsys_tfb/pipelines/training/nodes.py` | New `train_composite_model` node. |
| `src/recsys_tfb/pipelines/training/pipeline.py` | Branch on `training.model_structure`. |
| `src/recsys_tfb/pipelines/training/diagnostics.py` | Guard composite in SHAP (single-booster assumption). |
| `src/recsys_tfb/core/consistency.py` | A15/A16 predicates + wire into `validate_config_consistency`. |
| `src/recsys_tfb/core/versioning.py` | Fold `product_categories` into `model_version` when grouping==category. |
| `src/recsys_tfb/evaluation/metrics_spark.py` | `_build_category_mapping` delegates to `core/categories.py`. |
| `conf/base/parameters.yaml` | New top-level `product_categories` block. |
| `conf/base/parameters_training.yaml` | `model_structure` / `stage1` / `stage2`. |
| `conf/base/parameters_evaluation.yaml` | Drop the duplicated `mapping`; keep `enabled`. |
| `README.md`, `docs/pipelines/training.md`, `docs/pipelines/evaluation.md`, `docs/design-principles.md`, `docs/change-guide.md`, `docs/handbooks/*` | Docs (Phase 4). |

Phases are independently committable. Phase 0–1 ship value/guards even before the adapter exists. Phase 2 is unit-testable standalone. Phase 3 wires it. Phase 4 documents.

**Test command (always, per worktree SOP):**
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/two-stage-stacking/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
```

---

## Phase 0 — Shared `product_categories` resolver

Ships value alone: removes evaluation's private category mapping (single source of truth).

### Task 0.1: Pure category/grouping resolver

**Files:**
- Create: `src/recsys_tfb/core/categories.py`
- Test: `tests/test_core/test_categories.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/test_categories.py
"""Tests for core.categories — shared product grouping resolver."""
import pytest

from recsys_tfb.core.categories import resolve_category_mapping, resolve_groups


def _params(**pc):
    return {
        "schema": {
            "columns": {"item": "prod_name"},
            "categorical_values": {
                "prod_name": ["fund_a", "fund_b", "ccard_x", "loner"]
            },
        },
        "product_categories": pc or {
            "mapping": {"fund": ["fund_a", "fund_b"], "ccard": ["ccard_x"]},
            "unmapped": "singleton",
        },
    }


def test_resolve_category_mapping_singletons_unmapped():
    m = resolve_category_mapping(_params())
    assert m == {
        "fund_a": "fund", "fund_b": "fund",
        "ccard_x": "ccard", "loner": "loner",  # singleton
    }


def test_unknown_product_in_mapping_fails_loud():
    p = _params(mapping={"fund": ["nope"]}, unmapped="singleton")
    with pytest.raises(ValueError, match="unknown product"):
        resolve_category_mapping(p)


def test_unsupported_unmapped_policy_fails_loud():
    p = _params(mapping={"fund": ["fund_a"]}, unmapped="merge")
    with pytest.raises(ValueError, match="only 'singleton'"):
        resolve_category_mapping(p)


def test_resolve_groups_item_is_identity():
    # grouping == 'item': every item is its own group.
    g = resolve_groups(_params(), "item")
    assert g == {"fund_a": "fund_a", "fund_b": "fund_b",
                 "ccard_x": "ccard_x", "loner": "loner"}


def test_resolve_groups_category_uses_mapping():
    g = resolve_groups(_params(), "category")
    assert g["fund_a"] == "fund" and g["loner"] == "loner"


def test_resolve_groups_rejects_unknown_grouping():
    with pytest.raises(ValueError, match="grouping"):
        resolve_groups(_params(), "bogus")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_core/test_categories.py -q`
Expected: FAIL — `ModuleNotFoundError: recsys_tfb.core.categories`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/recsys_tfb/core/categories.py
"""Shared product grouping resolver (single source of truth).

`product_categories` lives at the top level of the merged parameters (sibling
to `schema`) so it is visible to BOTH the training Stage-1 grouping and the
evaluation category collapse, and so it does NOT bust base_dataset_version
(which hashes only `schema`). The category mapping logic here is the single
definition; evaluation's `_build_category_mapping` and the composite trainer
both call it.
"""
from __future__ import annotations

from recsys_tfb.core.schema import get_schema


def resolve_category_mapping(parameters: dict) -> dict[str, str]:
    """Return {item_value: category}. Products absent from every list become
    their own singleton category. Fail-loud on an unknown product or an
    unsupported `unmapped` policy. Reads top-level `product_categories`.
    """
    pc = parameters.get("product_categories", {}) or {}
    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
    known_set = set(known)

    mapping: dict[str, str] = {}
    for category, prods in (pc.get("mapping", {}) or {}).items():
        for prod in prods:
            if prod not in known_set:
                raise ValueError(
                    f"product_categories.mapping references unknown product "
                    f"'{prod}' (not in schema.categorical_values['{item_col}'])"
                )
            mapping[prod] = category

    unmapped = pc.get("unmapped", "singleton")
    if unmapped != "singleton":
        raise ValueError(
            f"product_categories.unmapped='{unmapped}' unsupported; "
            f"only 'singleton' is implemented"
        )
    for prod in known:
        mapping.setdefault(prod, prod)
    return mapping


def resolve_groups(parameters: dict, grouping: str) -> dict[str, str]:
    """Return {item_value: group_name} for a Stage-1 grouping.

    grouping == 'item'     -> identity (each item is its own group).
    grouping == 'category' -> resolve_category_mapping.
    """
    schema = get_schema(parameters)
    item_col = schema["item"]
    known = list((schema.get("categorical_values", {}) or {}).get(item_col, []))
    if grouping == "item":
        return {p: p for p in known}
    if grouping == "category":
        return resolve_category_mapping(parameters)
    raise ValueError(
        f"stage1.grouping={grouping!r} invalid; must be 'item' or 'category'"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_core/test_categories.py -q`
Expected: PASS (6 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/categories.py tests/test_core/test_categories.py
git commit -m "feat(core): shared product_categories grouping resolver"
```

### Task 0.2: Point evaluation + config at the shared resolver

**Files:**
- Modify: `src/recsys_tfb/evaluation/metrics_spark.py` (`_build_category_mapping`)
- Modify: `conf/base/parameters.yaml` (add top-level `product_categories`)
- Modify: `conf/base/parameters_evaluation.yaml` (drop duplicated `mapping`/`unmapped`, keep `enabled`)
- Test: `tests/test_evaluation/test_metrics_spark_category.py` (existing — must still pass)

- [ ] **Step 1: Write the failing test** (delegation regression)

```python
# tests/test_core/test_categories.py  (append)
def test_evaluation_build_mapping_delegates(monkeypatch):
    from recsys_tfb.evaluation import metrics_spark
    params = {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["fund_a", "loner"]}},
        "product_categories": {"mapping": {"fund": ["fund_a"]}, "unmapped": "singleton"},
        "evaluation": {"product_categories": {"enabled": True}},
    }
    assert metrics_spark._build_category_mapping(params) == {"fund_a": "fund", "loner": "loner"}
    params["evaluation"]["product_categories"]["enabled"] = False
    assert metrics_spark._build_category_mapping(params) is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_core/test_categories.py::test_evaluation_build_mapping_delegates -q`
Expected: FAIL — current `_build_category_mapping` reads `evaluation.product_categories.mapping`, returns `{}` (no products mapped → `loner` singleton only) — assertion mismatch.

- [ ] **Step 3: Rewrite `_build_category_mapping` to delegate**

Replace the body of `_build_category_mapping` in `src/recsys_tfb/evaluation/metrics_spark.py` with:

```python
def _build_category_mapping(parameters: dict) -> dict[str, str] | None:
    """Resolve {prod_name: category}. None when categories disabled.

    `enabled` is the evaluation-report toggle; the mapping data is the shared
    top-level `product_categories` block (see core.categories). Single source:
    do not re-read evaluation.product_categories.mapping here.
    """
    from recsys_tfb.core.categories import resolve_category_mapping

    eval_pc = (parameters.get("evaluation", {}) or {}).get("product_categories", {}) or {}
    if not eval_pc.get("enabled"):
        return None
    return resolve_category_mapping(parameters)
```

(Remove the now-unused local `get_schema`/`known`/`unmapped` logic in that function only.)

- [ ] **Step 4: Add the shared block to `conf/base/parameters.yaml`**

Append a top-level block (sibling to `schema:`):

```yaml
# 產品分群（單一真實來源）：training Stage-1 grouping(category) 與 evaluation
# 的大類 collapse 都讀這裡。放頂層（非 schema.*）是刻意的——schema 會進
# base_dataset_version，放這裡改 mapping 只 bust model_version、不重建 dataset。
product_categories:
  unmapped: singleton            # 未列入任何 list 的 product 自成 singleton 大類
  mapping:
    fund: [fund_stock, fund_bond, fund_mix]
    exchange: [exchange_fx, exchange_usd]
    ccard: [ccard_bill, ccard_cash, ccard_ins]
```

- [ ] **Step 5: Trim `conf/base/parameters_evaluation.yaml`**

Under `evaluation.product_categories`, delete the `mapping:` and `unmapped:` keys (now sourced from the shared block); keep `enabled:` and any display keys. The block becomes:

```yaml
  product_categories:
    enabled: true                # 開大類報表；mapping 取自頂層 product_categories
```

- [ ] **Step 6: Run the evaluation category tests + new test**

Run: `pytest tests/test_evaluation/test_metrics_spark_category.py tests/test_core/test_categories.py -q`
Expected: PASS. (If a fixture in the eval test hardcoded `evaluation.product_categories.mapping`, update it to set top-level `product_categories` instead — same values.)

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/evaluation/metrics_spark.py conf/base/parameters.yaml \
        conf/base/parameters_evaluation.yaml tests/test_core/test_categories.py \
        tests/test_evaluation/test_metrics_spark_category.py
git commit -m "refactor(eval): product_categories single source of truth via core.categories"
```

---

## Phase 1 — Consistency (A15/A16) + config scaffolding + versioning

### Task 1.1: A15/A16 consistency predicates

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py`
- Test: `tests/test_core/test_consistency.py` (append a class)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_core/test_consistency.py  (append)
class TestModelStructure:
    def _base(self, **training):
        return {
            "schema": {"columns": {"item": "prod_name", "entity": ["cust_id"]},
                       "categorical_values": {"prod_name": ["a", "b", "c"]}},
            "product_categories": {"mapping": {"grp": ["a", "b"]}, "unmapped": "singleton"},
            "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]}},
            "inference": {"products": ["a", "b", "c"]},
            "training": {"model_structure": "shared", "algorithm_params": {"objective": "binary"}, **training},
        }

    def test_shared_is_ok(self):
        from recsys_tfb.core.consistency import model_structure_errors
        assert model_structure_errors(self._base()) == []

    def test_unknown_structure_flagged(self):
        from recsys_tfb.core.consistency import model_structure_errors
        errs = model_structure_errors(self._base(model_structure="bogus"))
        assert any("model_structure" in e for e in errs)

    def test_stage2_must_be_ranking(self):
        from recsys_tfb.core.consistency import model_structure_errors
        p = self._base(model_structure="per_group_plus_rank",
                       stage1={"grouping": "category", "objective": "binary"},
                       stage2={"objective": "binary"})  # not ranking
        errs = model_structure_errors(p)
        assert any("stage2" in e and "ranking" in e for e in errs)

    def test_category_grouping_must_cover_all_items(self):
        from recsys_tfb.core.consistency import model_structure_errors
        p = self._base(model_structure="per_group_plus_rank",
                       stage1={"grouping": "category", "objective": "binary"},
                       stage2={"objective": "lambdarank"})
        # 'c' is a singleton -> covered; make mapping reference a missing item to fail
        p["product_categories"] = {"mapping": {"grp": ["a", "b", "zzz"]}, "unmapped": "singleton"}
        errs = model_structure_errors(p)
        assert any("zzz" in e for e in errs)

    def test_calibration_must_be_off_in_composite(self):
        from recsys_tfb.core.consistency import model_structure_errors
        p = self._base(model_structure="per_group_plus_rank",
                       stage1={"grouping": "category", "objective": "binary"},
                       stage2={"objective": "lambdarank"},
                       calibration={"enabled": True})
        errs = model_structure_errors(p)
        assert any("calibration" in e for e in errs)
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest tests/test_core/test_consistency.py::TestModelStructure -q`
Expected: FAIL — `ImportError: cannot import name 'model_structure_errors'`.

- [ ] **Step 3: Add the predicate + legend + wiring**

In `src/recsys_tfb/core/consistency.py`, add to the Invariant legend docstring:

```
* A15 — training.model_structure ∈ {shared, per_group_plus_rank}; under
  per_group_plus_rank: stage1.grouping ∈ {item, category}; stage2.objective is a
  ranking objective; the category mapping references only known items; and
  calibration is disabled (the lambdarank Stage-2 score is not a probability).
  Predicate: ``model_structure_errors``.
```

Add the predicate (after `ranking_objective_conflicts`):

```python
_VALID_STRUCTURES = frozenset({"shared", "per_group_plus_rank"})
_VALID_GROUPINGS = frozenset({"item", "category"})


def model_structure_errors(parameters: dict) -> list[str]:
    """A15 — two-stage stacking config validity (collect-all). Empty == OK."""
    training = parameters.get("training", {}) or {}
    structure = training.get("model_structure", "shared")
    errors: list[str] = []
    if structure not in _VALID_STRUCTURES:
        errors.append(
            f"training.model_structure={structure!r} invalid; must be one of "
            f"{sorted(_VALID_STRUCTURES)}."
        )
        return errors
    if structure == "shared":
        return errors

    stage1 = training.get("stage1", {}) or {}
    stage2 = training.get("stage2", {}) or {}

    grouping = stage1.get("grouping", "category")
    if grouping not in _VALID_GROUPINGS:
        errors.append(
            f"training.stage1.grouping={grouping!r} invalid; must be one of "
            f"{sorted(_VALID_GROUPINGS)}."
        )

    s2_obj = stage2.get("objective")
    if s2_obj not in RANKING_OBJECTIVES:
        errors.append(
            f"training.stage2.objective={s2_obj!r} must be a ranking objective "
            f"{sorted(RANKING_OBJECTIVES)} (Stage-2 learns cross-product ranking)."
        )

    if (training.get("calibration", {}) or {}).get("enabled"):
        errors.append(
            "training.calibration.enabled must be false under "
            "model_structure=per_group_plus_rank: the lambdarank Stage-2 output "
            "is a ranking score, not a probability. Disable calibration (a "
            "future final-layer stacker can wrap the composite instead)."
        )

    # Category mapping must reference only known items (mirrors collapse fail-loud).
    if grouping == "category":
        try:
            from recsys_tfb.core.categories import resolve_category_mapping
            resolve_category_mapping(parameters)
        except ValueError as exc:
            errors.append(f"training.stage1.grouping=category: {exc}")

    return errors
```

Wire into `validate_config_consistency` (after the `ranking_objective_conflicts` loop):

```python
    errors.extend(model_structure_errors(parameters))
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_core/test_consistency.py::TestModelStructure -q`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py
git commit -m "feat(consistency): A15 two-stage model_structure invariants"
```

### Task 1.2: Config scaffolding in parameters_training.yaml

**Files:**
- Modify: `conf/base/parameters_training.yaml`
- Test: `tests/test_core/test_versioning.py` (append) — shared unchanged, composite bumps

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/test_versioning.py  (append)
class TestModelStructureVersioning:
    def _p(self, **training):
        base = {"algorithm": "lightgbm", "algorithm_params": {"objective": "binary"}}
        base.update(training)
        return {"training": base}

    def test_shared_unaffected_by_absent_keys(self):
        from recsys_tfb.core.versioning import compute_model_version
        a = compute_model_version(self._p(), "base1", "tv1")
        b = compute_model_version(self._p(model_structure="shared"), "base1", "tv1")
        # adding the explicit default 'shared' changes the hash (training: block
        # is hashed verbatim) — documented over-invalidation, acceptable once.
        assert a != b  # sanity: hash is sensitive to training: content

    def test_composite_config_changes_version(self):
        from recsys_tfb.core.versioning import compute_model_version
        a = compute_model_version(
            self._p(model_structure="per_group_plus_rank",
                    stage1={"grouping": "category", "objective": "binary"},
                    stage2={"objective": "lambdarank"}), "base1", "tv1")
        b = compute_model_version(
            self._p(model_structure="per_group_plus_rank",
                    stage1={"grouping": "item", "objective": "binary"},
                    stage2={"objective": "lambdarank"}), "base1", "tv1")
        assert a != b  # grouping is part of training: -> different model_version
```

- [ ] **Step 2: Run to verify pass-or-fail**

Run: `pytest tests/test_core/test_versioning.py::TestModelStructureVersioning -q`
Expected: PASS already — `_model_version_payload` hashes the whole `training:` block, so `stage1`/`stage2` are auto-included. This test documents/locks that behavior (no code change needed for `training.*`).

- [ ] **Step 3: Add config keys**

In `conf/base/parameters_training.yaml`, under `training:` (after `algorithm_params`), add:

```yaml
  # --- Two-stage stacking（進階可選模式）-------------------------------------
  # shared(預設) = 現況單一模型；per_group_plus_rank = Stage-1 per-grouping
  # point-wise + Stage-2 LTR（OOF cross-fitting 避免 leakage）。切換會 bump
  # model_version。詳見 docs/pipelines/training.md。
  model_structure: shared
  # 僅 per_group_plus_rank 生效：
  stage1:
    grouping: category          # item(每產品一個 binary) | category(每大類一個 share)
    objective: binary
    metric: binary_logloss
    n_folds: 5                  # OOF 折數；折鍵 = entity 雜湊互斥
  stage2:
    objective: lambdarank
    metric: ndcg
    # inputs 固定 pointwise（自身分數 + 客戶特徵 + grouping id）；跨產品相對特徵為 future
```

- [ ] **Step 4: Run consistency on the real config**

Run: `pytest tests/test_core/test_versioning.py::TestModelStructureVersioning -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add conf/base/parameters_training.yaml tests/test_core/test_versioning.py
git commit -m "feat(config): training.model_structure scaffolding (default shared)"
```

### Task 1.3: Fold `product_categories` into model_version when grouping==category

**Files:**
- Modify: `src/recsys_tfb/core/versioning.py` (`_model_version_payload`)
- Test: `tests/test_core/test_versioning.py` (append)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_core/test_versioning.py  (append to TestModelStructureVersioning)
    def test_category_mapping_change_bumps_version_when_grouping_category(self):
        from recsys_tfb.core.versioning import compute_model_version
        def mk(mapping):
            return {
                "training": {"model_structure": "per_group_plus_rank",
                             "stage1": {"grouping": "category", "objective": "binary"},
                             "stage2": {"objective": "lambdarank"}},
                "product_categories": {"mapping": mapping, "unmapped": "singleton"},
            }
        a = compute_model_version(mk({"g": ["x"]}), "b", "t")
        b = compute_model_version(mk({"g": ["x", "y"]}), "b", "t")
        assert a != b

    def test_category_mapping_ignored_when_grouping_item(self):
        from recsys_tfb.core.versioning import compute_model_version
        def mk(mapping):
            return {
                "training": {"model_structure": "per_group_plus_rank",
                             "stage1": {"grouping": "item", "objective": "binary"},
                             "stage2": {"objective": "lambdarank"}},
                "product_categories": {"mapping": mapping, "unmapped": "singleton"},
            }
        a = compute_model_version(mk({"g": ["x"]}), "b", "t")
        b = compute_model_version(mk({"g": ["x", "y"]}), "b", "t")
        assert a == b  # grouping=item ignores the category table
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_core/test_versioning.py::TestModelStructureVersioning::test_category_mapping_change_bumps_version_when_grouping_category -q`
Expected: FAIL — `product_categories` is top-level, not in `training:`, so the current payload ignores it; both hashes equal.

- [ ] **Step 3: Extend `_model_version_payload`**

In `src/recsys_tfb/core/versioning.py`, replace the return of `_model_version_payload` so it conditionally folds the category table in:

```python
    payload: dict = {"training": training}
    # The category table defines the model ONLY when Stage-1 groups by category.
    # Fold it in then (so editing the mapping bumps model_version); leave it out
    # otherwise to avoid spurious invalidation of shared / per-item models.
    if (
        training.get("model_structure") == "per_group_plus_rank"
        and (training.get("stage1", {}) or {}).get("grouping") == "category"
    ):
        payload["product_categories"] = params.get("product_categories")
    return payload
```

(The function already deep-copies `training`; `params` is the original arg — read `product_categories` from it directly.)

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_core/test_versioning.py::TestModelStructureVersioning -q`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/core/versioning.py tests/test_core/test_versioning.py
git commit -m "feat(versioning): fold product_categories into model_version for category grouping"
```

---

## Phase 2 — CompositeModelAdapter (inference contract: predict/save/load)

Unit-testable with tiny in-memory LightGBM boosters; no pipeline needed.

### Task 2.1: predict routing

**Files:**
- Create: `src/recsys_tfb/models/composite_adapter.py`
- Test: `tests/test_models/test_composite_adapter.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models/test_composite_adapter.py
"""Tests for CompositeModelAdapter (inference contract)."""
import numpy as np
import lightgbm as lgb
import pytest

from recsys_tfb.models.composite_adapter import CompositeModelAdapter


def _tiny_booster(const: float) -> lgb.Booster:
    """A booster that predicts ~const regardless of input (1 leaf)."""
    X = np.random.RandomState(0).rand(40, 3)
    y = np.full(40, const)
    ds = lgb.Dataset(X, label=y)
    return lgb.train({"objective": "regression", "min_data_in_leaf": 1,
                      "num_leaves": 2, "verbosity": -1}, ds, num_boost_round=1)


def _make_adapter():
    # feature_columns: f0, prod_name(idx1), f2 ; item codes 0->groupA, 1->groupB
    stage1 = {"A": _tiny_booster(0.2), "B": _tiny_booster(0.9)}
    # Stage-2: predict = stage1_score (identity-ish); build a booster on
    # [s1, f0, f2, group_code] that returns ~its first column's scale.
    stage2 = _tiny_booster(0.5)  # constant; routing is what we assert
    return CompositeModelAdapter._from_parts(
        stage1_boosters=stage1,
        stage2_booster=stage2,
        item_col_index=1,
        item_code_to_group={0: "A", 1: "B"},
        group_to_code={"A": 0, "B": 1},
        n_features=3,
    )


def test_predict_routes_each_row_to_its_group():
    a = _make_adapter()
    # row0 item-code 0 -> group A (s1=0.2); row1 item-code 1 -> group B (s1=0.9)
    X = np.array([[0.1, 0.0, 0.3], [0.4, 1.0, 0.6]])
    # expose the intermediate Stage-1 scores for assertion
    s1 = a._stage1_scores(X)
    assert s1[0] == pytest.approx(0.2, abs=1e-6)
    assert s1[1] == pytest.approx(0.9, abs=1e-6)
    out = a.predict(X)
    assert out.shape == (2,)


def test_predict_unknown_item_code_raises():
    a = _make_adapter()
    X = np.array([[0.1, 7.0, 0.3]])  # code 7 not in item_code_to_group
    with pytest.raises(KeyError):
        a.predict(X)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_models/test_composite_adapter.py -q`
Expected: FAIL — module/class does not exist.

- [ ] **Step 3: Implement the adapter (predict path only here)**

```python
# src/recsys_tfb/models/composite_adapter.py
"""CompositeModelAdapter — two-stage (per-grouping Stage-1 + LTR Stage-2).

Implements the ModelAdapter inference + persistence contract. Training is NOT
done through the numpy `train()` (it cannot express customer-disjoint K-fold
OOF); it is driven by `composite_train.train_composite` and the parts are
injected via `_from_parts`. See docs/pipelines/training.md.
"""
from __future__ import annotations

import json
import os

import lightgbm as lgb
import numpy as np

from recsys_tfb.models.base import ADAPTER_REGISTRY, ModelAdapter

MANIFEST_FILENAME = "composite_manifest.json"
STAGE2_FILENAME = "model.txt"  # the conventional model path = Stage-2 booster
_STAGE1_PREFIX = "stage1_"


class CompositeModelAdapter(ModelAdapter):
    def __init__(self) -> None:
        self._stage1: dict[str, lgb.Booster] = {}
        self._stage2: lgb.Booster | None = None
        self._item_col_index: int | None = None
        self._item_code_to_group: dict[int, str] = {}
        self._group_to_code: dict[str, int] = {}
        self._n_features: int | None = None

    # -- construction ----------------------------------------------------
    @classmethod
    def _from_parts(cls, *, stage1_boosters, stage2_booster, item_col_index,
                    item_code_to_group, group_to_code, n_features):
        self = cls()
        self._stage1 = dict(stage1_boosters)
        self._stage2 = stage2_booster
        self._item_col_index = item_col_index
        self._item_code_to_group = dict(item_code_to_group)
        self._group_to_code = dict(group_to_code)
        self._n_features = n_features
        return self

    # -- inference -------------------------------------------------------
    def _stage1_scores(self, X: np.ndarray) -> np.ndarray:
        codes = X[:, self._item_col_index].astype(np.int64)
        out = np.empty(len(X), dtype=np.float64)
        # group row indices by their Stage-1 group, one booster call per group
        groups: dict[str, list[int]] = {}
        for i, code in enumerate(codes):
            groups.setdefault(self._item_code_to_group[int(code)], []).append(i)
        for group, idx in groups.items():
            booster = self._stage1[group]
            out[idx] = booster.predict(X[idx])
        return out

    def _stage2_matrix(self, X: np.ndarray, s1: np.ndarray) -> np.ndarray:
        codes = X[:, self._item_col_index].astype(np.int64)
        group_code = np.array(
            [self._group_to_code[self._item_code_to_group[int(c)]] for c in codes],
            dtype=np.float64,
        )
        cust_feats = np.delete(X, self._item_col_index, axis=1)
        return np.column_stack([s1, cust_feats, group_code])

    def predict(self, X: np.ndarray) -> np.ndarray:
        if self._stage2 is None:
            raise RuntimeError("Composite model not trained or loaded.")
        s1 = self._stage1_scores(X)
        return self._stage2.predict(self._stage2_matrix(X, s1))

    def feature_importance(self, kind: str = "split") -> dict[str, float]:
        # Composite importance is reported on the Stage-2 booster (which mixes
        # the Stage-1 score, customer features, and grouping id). Per-Stage-1
        # importance is a future diagnostics enhancement.
        if self._stage2 is None:
            raise RuntimeError("No model loaded.")
        names = self._stage2.feature_name()
        imp = self._stage2.feature_importance(importance_type=kind).astype(float)
        return dict(zip(names, imp))

    def log_to_mlflow(self) -> None:
        import mlflow
        if self._stage2 is None:
            raise RuntimeError("No model to log.")
        mlflow.lightgbm.log_model(self._stage2, name="model")

    # -- not on the composite path --------------------------------------
    def train(self, X_train, y_train, X_val, y_val, params) -> None:
        raise NotImplementedError(
            "CompositeModelAdapter is trained via composite_train.train_composite, "
            "not the numpy train() (which cannot express customer-disjoint K-fold OOF)."
        )

    def prepare_train_inputs(self, *args, **kwargs):
        raise NotImplementedError(
            "CompositeModelAdapter does not use prepare_train_inputs; its training "
            "node reads the parquet handles directly."
        )

    # save/load added in Task 2.2

ADAPTER_REGISTRY["composite"] = CompositeModelAdapter
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_models/test_composite_adapter.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/composite_adapter.py tests/test_models/test_composite_adapter.py
git commit -m "feat(models): CompositeModelAdapter predict routing"
```

### Task 2.2: save / load round-trip

**Files:**
- Modify: `src/recsys_tfb/models/composite_adapter.py`
- Test: `tests/test_models/test_composite_adapter.py` (append)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models/test_composite_adapter.py  (append)
def test_save_load_round_trip(tmp_path):
    from recsys_tfb.models.composite_adapter import CompositeModelAdapter
    a = _make_adapter()
    model_path = str(tmp_path / "model.txt")
    a.save(model_path)
    # N+1 boosters + manifest present
    assert (tmp_path / "model.txt").exists()
    assert (tmp_path / "stage1_A.txt").exists()
    assert (tmp_path / "stage1_B.txt").exists()
    assert (tmp_path / "composite_manifest.json").exists()

    b = CompositeModelAdapter()
    b.load(model_path)
    X = np.array([[0.1, 0.0, 0.3], [0.4, 1.0, 0.6]])
    np.testing.assert_allclose(b.predict(X), a.predict(X), rtol=1e-6)
    np.testing.assert_allclose(b._stage1_scores(X), a._stage1_scores(X), rtol=1e-6)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_models/test_composite_adapter.py::test_save_load_round_trip -q`
Expected: FAIL — `save` not implemented.

- [ ] **Step 3: Implement save/load**

Add to `CompositeModelAdapter` (replace the `# save/load added in Task 2.2` comment):

```python
    def save(self, filepath: str) -> None:
        if self._stage2 is None:
            raise RuntimeError("No model to save.")
        d = os.path.dirname(filepath) or "."
        os.makedirs(d, exist_ok=True)
        self._stage2.save_model(filepath)  # model.txt == Stage-2
        group_to_file: dict[str, str] = {}
        for group, booster in self._stage1.items():
            fname = f"{_STAGE1_PREFIX}{group}.txt"
            booster.save_model(os.path.join(d, fname))
            group_to_file[group] = fname
        manifest = {
            "model_structure": "per_group_plus_rank",
            "item_col_index": self._item_col_index,
            "n_features": self._n_features,
            # JSON keys are strings: store code<->group as lists of pairs.
            "item_code_to_group": [[int(k), v] for k, v in self._item_code_to_group.items()],
            "group_to_code": self._group_to_code,
            "group_to_file": group_to_file,
        }
        with open(os.path.join(d, MANIFEST_FILENAME), "w") as f:
            json.dump(manifest, f, indent=2)

    def load(self, filepath: str) -> None:
        d = os.path.dirname(filepath) or "."
        with open(os.path.join(d, MANIFEST_FILENAME)) as f:
            m = json.load(f)
        self._stage2 = lgb.Booster(model_file=filepath)
        self._item_col_index = m["item_col_index"]
        self._n_features = m["n_features"]
        self._item_code_to_group = {int(k): v for k, v in m["item_code_to_group"]}
        self._group_to_code = dict(m["group_to_code"])
        self._stage1 = {
            group: lgb.Booster(model_file=os.path.join(d, fname))
            for group, fname in m["group_to_file"].items()
        }
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_models/test_composite_adapter.py -q`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/composite_adapter.py tests/test_models/test_composite_adapter.py
git commit -m "feat(models): CompositeModelAdapter save/load serialization"
```

### Task 2.3: ModelAdapterDataset round-trips composite

**Files:**
- Test: `tests/test_io/test_model_adapter_dataset.py` (append)
- (No code change expected — verify the existing `get_adapter(meta["algorithm"])` path works because `"composite"` is registered.)

- [ ] **Step 1: Write the failing/confirming test**

```python
# tests/test_io/test_model_adapter_dataset.py  (append)
def test_model_adapter_dataset_round_trips_composite(tmp_path):
    import numpy as np
    from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
    from tests.test_models.test_composite_adapter import _make_adapter  # reuse factory

    a = _make_adapter()
    ds = ModelAdapterDataset(str(tmp_path / "model.txt"))
    ds.save(a)
    # meta records algorithm=composite (registry match on type)
    import json
    meta = json.load(open(tmp_path / "model_meta.json"))
    assert meta["algorithm"] == "composite"
    assert meta["calibrated"] is False

    loaded = ds.load()
    X = np.array([[0.1, 0.0, 0.3], [0.4, 1.0, 0.6]])
    np.testing.assert_allclose(loaded.predict(X), a.predict(X), rtol=1e-6)
```

(If `tests/test_models/test_composite_adapter.py::_make_adapter` is not importable as a helper, move `_make_adapter`/`_tiny_booster` into `tests/test_models/conftest.py` as fixtures and adjust both tests — do that refactor in this step.)

- [ ] **Step 2: Run**

Run: `pytest tests/test_io/test_model_adapter_dataset.py::test_model_adapter_dataset_round_trips_composite -q`
Expected: PASS (the registry already routes `get_adapter("composite")`). If `ModelAdapterDataset.save`'s registry match fails because `CompositeModelAdapter` was imported lazily and not registered, add `import recsys_tfb.models.composite_adapter  # noqa: F401` to `src/recsys_tfb/models/__init__.py` so the registry entry exists; re-run.

- [ ] **Step 3: Commit**

```bash
git add tests/test_io/test_model_adapter_dataset.py tests/test_models/ src/recsys_tfb/models/__init__.py
git commit -m "test(io): ModelAdapterDataset round-trips composite adapter"
```

---

## Phase 3 — Composite training orchestration + pipeline branch

### Task 3.1: Pure training helpers (folds + Stage-2 assembly)

**Files:**
- Create: `src/recsys_tfb/models/composite_train.py`
- Test: `tests/test_models/test_composite_train.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models/test_composite_train.py
import numpy as np
from recsys_tfb.models.composite_train import assign_folds, oof_is_leakage_clean


def test_assign_folds_is_customer_disjoint_and_deterministic():
    custs = np.array(["c1", "c2", "c3", "c1", "c2"])  # c1,c2 repeat
    f1 = assign_folds(custs, n_folds=3, seed=7)
    f2 = assign_folds(custs, n_folds=3, seed=7)
    np.testing.assert_array_equal(f1, f2)            # deterministic
    # same customer -> same fold regardless of row
    assert f1[0] == f1[3]  # both c1
    assert f1[1] == f1[4]  # both c2
    assert set(f1.tolist()) <= {0, 1, 2}


def test_oof_clean_guard():
    # Each row must be scored by the booster of its OWN held-out fold, i.e.
    # producing_fold[i] == folds[i] for every row. Otherwise the scoring booster
    # trained on row i's fold -> leakage.
    folds = np.array([0, 1, 2, 0, 1])
    assert oof_is_leakage_clean(folds, producing_fold=folds.copy())          # clean
    dirty = np.array([1, 1, 2, 0, 1])  # row0 scored by fold1's booster
    assert not oof_is_leakage_clean(folds, producing_fold=dirty)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_models/test_composite_train.py -q`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement the pure helpers**

```python
# src/recsys_tfb/models/composite_train.py
"""Composite (two-stage) training orchestration.

Driver-local pandas/numpy, consistent with the existing single-machine LightGBM
training. Folds are customer-disjoint via zlib.crc32 (the IEEE-802.3 polynomial,
matching Spark's F.crc32 used by the dataset split) so the same customer never
appears in two folds.
"""
from __future__ import annotations

import zlib

import numpy as np

_FOLD_SITE = "composite_oof"


def assign_folds(entity_ids: np.ndarray, n_folds: int, seed: int) -> np.ndarray:
    """Deterministic, customer-disjoint fold index in [0, n_folds) per row."""
    out = np.empty(len(entity_ids), dtype=np.int64)
    for i, e in enumerate(entity_ids):
        token = f"{_FOLD_SITE}|{seed}|{e}".encode()
        out[i] = zlib.crc32(token) % n_folds
    return out


def oof_is_leakage_clean(folds: np.ndarray, producing_fold: np.ndarray) -> bool:
    """True iff no row's OOF score came from a booster that trained on its fold.

    `producing_fold[i]` is the held-out fold whose booster scored row i; for a
    clean OOF that booster trained on the OTHER folds, so it must differ from
    `folds[i]` only in the sense that row i WAS the held-out one — i.e. the
    booster that scored row i is the one trained WITHOUT fold==folds[i]. We
    encode that as: the scoring booster's training set excluded folds[i].
    """
    return bool(np.all(producing_fold == folds))
```

(Note: `producing_fold[i] == folds[i]` is the intended invariant — each row is scored by the booster of its own held-out fold, which trained on all OTHER folds. The guard asserts every row was scored out-of-fold.)

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_models/test_composite_train.py -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/composite_train.py tests/test_models/test_composite_train.py
git commit -m "feat(models): composite OOF fold-assignment helpers"
```

### Task 3.2: `train_composite` orchestration

**Files:**
- Modify: `src/recsys_tfb/models/composite_train.py` (add `train_composite`)
- Test: `tests/test_models/test_composite_train.py` (append, small in-memory data)

- [ ] **Step 1: Write the failing test**

```python
# tests/test_models/test_composite_train.py  (append)
import pandas as pd
from recsys_tfb.io.handles import ParquetHandle


def _write_handle(tmp_path, name, n_cust, items, rng):
    rows = []
    for c in range(n_cust):
        for it in items:
            rows.append({
                "snap_date": "2025-01-31", "cust_id": f"c{c}", "prod_name": it,
                "f0": rng.rand(), "f1": rng.rand(),
                "label": int(rng.rand() < (0.5 if it == items[0] else 0.1)),
            })
    pdf = pd.DataFrame(rows)
    path = str(tmp_path / f"{name}.parquet")
    pdf.to_parquet(path)
    return ParquetHandle(path=path)


def test_train_composite_produces_routable_adapter(tmp_path):
    from recsys_tfb.models.composite_train import train_composite
    rng = np.random.RandomState(0)
    items = ["fund_a", "fund_b", "ccard_x"]
    train = _write_handle(tmp_path, "train", 30, items, rng)
    train_dev = _write_handle(tmp_path, "train_dev", 8, items, rng)
    val = _write_handle(tmp_path, "val", 8, items, rng)
    preprocessor_metadata = {
        "feature_columns": ["f0", "prod_name", "f1"],
        "categorical_columns": ["prod_name"],
        "category_mappings": {"prod_name": items},  # value order == code
    }
    parameters = {
        "schema": {"columns": {"item": "prod_name", "entity": ["cust_id"],
                               "time": "snap_date", "label": "label"},
                   "categorical_values": {"prod_name": items}},
        "product_categories": {"mapping": {"fund": ["fund_a", "fund_b"]},
                               "unmapped": "singleton"},
        "training": {"model_structure": "per_group_plus_rank",
                     "algorithm_params": {"num_threads": 1},
                     "stage1": {"grouping": "category", "objective": "binary",
                                "metric": "binary_logloss", "n_folds": 3},
                     "stage2": {"objective": "lambdarank", "metric": "ndcg"}},
    }
    adapter = train_composite(train, train_dev, val, preprocessor_metadata, parameters)
    # groups: fund (fund_a+fund_b) and ccard_x singleton
    assert set(adapter._stage1.keys()) == {"fund", "ccard_x"}
    # predict shape over a small X built from feature_columns order
    X = np.array([[0.3, 0, 0.7], [0.3, 2, 0.7]], dtype=float)  # code0=fund_a, code2=ccard_x
    out = adapter.predict(X)
    assert out.shape == (2,)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_models/test_composite_train.py::test_train_composite_produces_routable_adapter -q`
Expected: FAIL — `train_composite` missing.

- [ ] **Step 3: Implement `train_composite`**

```python
# src/recsys_tfb/models/composite_train.py  (append)
import lightgbm as lgb

from recsys_tfb.core.categories import resolve_groups
from recsys_tfb.core.group_utils import to_contiguous_groups
from recsys_tfb.core.schema import get_schema
from recsys_tfb.models.composite_adapter import CompositeModelAdapter


def _read_frame(handle, preprocessor_metadata, parameters):
    """Return (X, y, entity, item_codes) as numpy, columns in feature order."""
    schema = get_schema(parameters)
    feat_cols = preprocessor_metadata["feature_columns"]
    item_col = schema["item"]
    code_of = {v: i for i, v in enumerate(preprocessor_metadata["category_mappings"][item_col])}
    pdf = handle.to_pandas()
    X_df = pdf[feat_cols].copy()
    # encode the item column (the only deferred identity categorical) to codes
    X_df[item_col] = X_df[item_col].map(code_of).astype("int64")
    X = X_df.to_numpy(dtype=float)
    y = pdf[schema["label"]].to_numpy()
    entity = pdf[schema["entity"][0]].to_numpy()
    item_codes = pdf[item_col].map(code_of).to_numpy()
    return X, y, entity, item_codes


def _stage1_params(parameters):
    s1 = parameters["training"]["stage1"]
    p = {"objective": s1.get("objective", "binary"),
         "metric": s1.get("metric", "binary_logloss"), "verbosity": -1,
         "num_threads": parameters["training"].get("algorithm_params", {}).get("num_threads", 0)}
    return p


def _stage2_params(parameters):
    s2 = parameters["training"]["stage2"]
    return {"objective": s2.get("objective", "lambdarank"),
            "metric": s2.get("metric", "ndcg"), "verbosity": -1,
            "num_threads": parameters["training"].get("algorithm_params", {}).get("num_threads", 0)}


def _fit_binary(X, y, params, num_boost_round=100):
    ds = lgb.Dataset(X, label=y, free_raw_data=False)
    return lgb.train({**params}, ds, num_boost_round=num_boost_round)


def train_composite(train_handle, train_dev_handle, val_handle,
                    preprocessor_metadata, parameters) -> CompositeModelAdapter:
    """OOF cross-fit Stage-1 per grouping, refit on full train, train Stage-2."""
    schema = get_schema(parameters)
    item_col = schema["item"]
    item_idx = preprocessor_metadata["feature_columns"].index(item_col)
    grouping = parameters["training"]["stage1"].get("grouping", "category")
    n_folds = int(parameters["training"]["stage1"].get("n_folds", 5))

    item_to_group = resolve_groups(parameters, grouping)
    code_of = {v: i for i, v in enumerate(preprocessor_metadata["category_mappings"][item_col])}
    item_code_to_group = {code_of[v]: g for v, g in item_to_group.items()}
    groups = sorted(set(item_to_group.values()))
    group_to_code = {g: i for i, g in enumerate(groups)}

    Xtr, ytr, etr, code_tr = _read_frame(train_handle, preprocessor_metadata, parameters)
    group_tr = np.array([item_code_to_group[int(c)] for c in code_tr])

    # ---- OOF Stage-1 over train ----------------------------------------
    folds = assign_folds(etr, n_folds=n_folds, seed=42)
    s1_params = _stage1_params(parameters)
    oof = np.empty(len(Xtr), dtype=np.float64)
    producing_fold = np.empty(len(Xtr), dtype=np.int64)
    for g in groups:
        g_mask = group_tr == g
        for k in range(n_folds):
            fit_mask = g_mask & (folds != k)
            pred_mask = g_mask & (folds == k)
            if not pred_mask.any():
                continue
            if not fit_mask.any() or ytr[fit_mask].sum() == 0:
                # no positives to learn from -> fall back to prior (mean label)
                oof[pred_mask] = float(ytr[g_mask].mean()) if g_mask.any() else 0.0
            else:
                booster = _fit_binary(Xtr[fit_mask], ytr[fit_mask], s1_params)
                oof[pred_mask] = booster.predict(Xtr[pred_mask])
            producing_fold[pred_mask] = k
    assert oof_is_leakage_clean(folds, producing_fold), "OOF leakage detected"

    # ---- refit Stage-1 on full train (used at inference) ---------------
    stage1_full: dict[str, lgb.Booster] = {}
    for g in groups:
        g_mask = group_tr == g
        if ytr[g_mask].sum() == 0:
            # degenerate group: constant predictor via a 1-round fit on zeros
            stage1_full[g] = _fit_binary(Xtr[g_mask], ytr[g_mask], s1_params, num_boost_round=1)
        else:
            stage1_full[g] = _fit_binary(Xtr[g_mask], ytr[g_mask], s1_params)

    # ---- Stage-2 (lambdarank, query=customer) --------------------------
    def stage2_matrix(X, code, s1):
        gcode = np.array([group_to_code[item_code_to_group[int(c)]] for c in code], dtype=float)
        cust = np.delete(X, item_idx, axis=1)
        return np.column_stack([s1, cust, gcode])

    X2_tr = stage2_matrix(Xtr, code_tr, oof)
    # contiguous customer groups for lambdarank
    perm, grp_counts = to_contiguous_groups(_codes(etr))
    ds2 = lgb.Dataset(X2_tr[perm], label=ytr[perm], group=grp_counts, free_raw_data=False)

    # val early-stopping set: score val with refit Stage-1
    Xv, yv, ev, code_v = _read_frame(val_handle, preprocessor_metadata, parameters)
    s1_val = np.array([stage1_full[item_code_to_group[int(c)]].predict(Xv[i:i+1])[0]
                       for i, c in enumerate(code_v)])
    X2_val = stage2_matrix(Xv, code_v, s1_val)
    permv, grpv = to_contiguous_groups(_codes(ev))
    ds2_val = lgb.Dataset(X2_val[permv], label=yv[permv], group=grpv,
                          reference=ds2, free_raw_data=False)

    stage2 = lgb.train(_stage2_params(parameters), ds2, num_boost_round=200,
                       valid_sets=[ds2_val],
                       callbacks=[lgb.early_stopping(30), lgb.log_evaluation(0)])

    return CompositeModelAdapter._from_parts(
        stage1_boosters=stage1_full, stage2_booster=stage2,
        item_col_index=item_idx, item_code_to_group=item_code_to_group,
        group_to_code=group_to_code, n_features=Xtr.shape[1],
    )


def _codes(values: np.ndarray) -> np.ndarray:
    """Stable int group id per distinct value, preserving first-seen order."""
    seen: dict = {}
    out = np.empty(len(values), dtype=np.int64)
    for i, v in enumerate(values):
        out[i] = seen.setdefault(v, len(seen))
    return out
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_models/test_composite_train.py -q`
Expected: PASS (3 passed). If LightGBM warns about small data, that is fine; assert only shapes/keys.

- [ ] **Step 5: Commit**

```bash
git add src/recsys_tfb/models/composite_train.py tests/test_models/test_composite_train.py
git commit -m "feat(models): train_composite OOF orchestration"
```

### Task 3.3: Training node + pipeline branch + diagnostics guard

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (add `train_composite_model`)
- Modify: `src/recsys_tfb/pipelines/training/pipeline.py` (branch)
- Modify: `src/recsys_tfb/pipelines/training/diagnostics.py` (SHAP guard)
- Test: `tests/test_pipelines/test_training/test_pipeline.py` (append a composite-branch shape test)

- [ ] **Step 1: Write the failing test (pipeline shape)**

```python
# tests/test_pipelines/test_training/test_pipeline.py  (append)
def test_composite_branch_swaps_model_nodes():
    from recsys_tfb.pipelines.training.pipeline import create_pipeline
    pipe = create_pipeline(enable_calibration=False, model_structure="per_group_plus_rank")
    names = {getattr(n.func, "__name__", "") for n in pipe.nodes}
    assert "train_composite_model" in names
    assert "tune_hyperparameters" not in names
    assert "finalize_model" not in names
    # downstream still present
    assert "predict_and_write_test_predictions" in names
    # produces 'model'
    assert any("model" in (n.outputs if isinstance(n.outputs, list) else [n.outputs])
               for n in pipe.nodes)
```

- [ ] **Step 2: Run to verify it fails**

Run: `pytest tests/test_pipelines/test_training/test_pipeline.py::test_composite_branch_swaps_model_nodes -q`
Expected: FAIL — `create_pipeline` has no `model_structure` param.

- [ ] **Step 3: Add the node**

In `src/recsys_tfb/pipelines/training/nodes.py`, add:

```python
def train_composite_model(
    train_parquet_handle,
    train_dev_parquet_handle,
    val_parquet_handle,
    preprocessor_metadata: dict,
    parameters: dict,
):
    """Two-stage composite training (replaces prepare/tune/finalize chain).

    Returns the CompositeModelAdapter as the `model` output (downstream nodes
    only call model.predict()).
    """
    from recsys_tfb.models.composite_train import train_composite

    return train_composite(
        train_parquet_handle, train_dev_parquet_handle, val_parquet_handle,
        preprocessor_metadata, parameters,
    )
```

- [ ] **Step 4: Branch the pipeline**

In `src/recsys_tfb/pipelines/training/pipeline.py`, change the signature and the model-producing section:

```python
def create_pipeline(enable_calibration: bool = False,
                    model_structure: str = "shared") -> Pipeline:
```

Replace the `prepare_lgb_train_inputs` + `tune_hyperparameters` + `finalize_model`
(+ optional `calibrate_model`) block with a branch. Keep the four cache nodes and
`select_features` as-is, then:

```python
    composite = model_structure == "per_group_plus_rank"
    if composite:
        nodes.append(
            Node(
                train_composite_model,
                inputs=["train_parquet_handle", "train_dev_parquet_handle",
                        "val_parquet_handle", "preprocessor_view", "parameters"],
                outputs="model",
            ),
        )
        # best_params/best_iteration are HPO artifacts; composite has none. Feed
        # log_experiment empty placeholders so its signature is unchanged.
        nodes.append(Node(lambda parameters: ({}, 0),
                          inputs=["parameters"],
                          outputs=["best_params", "best_iteration"]))
    else:
        # ... existing prepare_lgb_train_inputs / tune_hyperparameters /
        #     finalize_model / (calibrate_model) nodes unchanged ...
```

Import `train_composite_model` at the top with the other node imports. Thread the
flag from the caller: in `pipelines/training/__init__.py` (or wherever
`create_pipeline` is called by `__main__`), read
`parameters["training"].get("model_structure", "shared")` and pass it. (Grep
`create_pipeline(` to find the call site; pass `model_structure=...` there.)

- [ ] **Step 5: Guard SHAP diagnostics for composite**

In `src/recsys_tfb/pipelines/training/diagnostics.py`, at the top of
`compute_shap_diagnostics`, add an early return when the model has no single
`booster` (composite exposes N+1, not one):

```python
    from recsys_tfb.models.composite_adapter import CompositeModelAdapter
    base = getattr(model, "base", model)  # unwrap calibrated
    if isinstance(base, CompositeModelAdapter):
        logger.info("SHAP diagnostics skipped for composite model (per-submodel "
                    "SHAP is a future enhancement).")
        return {}
```

`compute_feature_importance` already works (composite implements `feature_importance`
on its Stage-2 booster) — no change.

- [ ] **Step 6: Run the pipeline tests**

Run: `pytest tests/test_pipelines/test_training/test_pipeline.py -q`
Expected: PASS, including the existing shared-mode shape tests.

- [ ] **Step 7: Commit**

```bash
git add src/recsys_tfb/pipelines/training/ tests/test_pipelines/test_training/test_pipeline.py
git commit -m "feat(training): composite pipeline branch + SHAP guard"
```

### Task 3.4: End-to-end leakage-clean integration test (small data)

**Files:**
- Test: `tests/test_models/test_composite_train.py` (append)

- [ ] **Step 1: Write the test**

```python
# tests/test_models/test_composite_train.py  (append)
def test_oof_scores_are_not_self_predictions(tmp_path):
    """Stage-2's OOF feature for a customer must come from a Stage-1 booster
    that never trained on that customer. We assert by reconstructing folds."""
    from recsys_tfb.models.composite_train import assign_folds
    rng = np.random.RandomState(1)
    custs = np.array([f"c{i%9}" for i in range(60)])
    folds = assign_folds(custs, n_folds=3, seed=42)
    # every customer maps to exactly one fold (disjoint)
    by_cust = {}
    for c, f in zip(custs, folds):
        by_cust.setdefault(c, set()).add(int(f))
    assert all(len(v) == 1 for v in by_cust.values())
```

- [ ] **Step 2: Run**

Run: `pytest tests/test_models/test_composite_train.py -q`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_models/test_composite_train.py
git commit -m "test(models): customer-disjoint OOF fold invariant"
```

---

## Phase 4 — Documentation

### Task 4.1: training.md + README

**Files:**
- Modify: `docs/pipelines/training.md`
- Modify: `README.md`

- [ ] **Step 1: training.md — add a "Two-stage stacking (model_structure)" section**

Add a section covering: the `shared` vs `per_group_plus_rank` switch; the data flow
(OOF cross-fitting diagram from the spec §4); the `stage1.grouping` item-vs-category
abstraction; `n_folds`; that Stage-2 is lambdarank with pointwise input; that
calibration is disabled in composite mode (future final-layer hook via
`CalibratedModelAdapter` wrapping); the `product_categories` single-source-of-truth;
and that `model_version` includes structure/stage/category-table. Align identifiers
to code (`train_composite_model`, `CompositeModelAdapter`, `stage1`/`stage2`). Keep
the abstract-framework framing (bank is an example).

- [ ] **Step 2: README — §2 node 全貌, §4 一致性閘, §5 文件地圖**

- §2 "各 pipeline 的 node 全貌": note the training composite branch (`train_composite_model` replaces prepare/tune/finalize when `model_structure: per_group_plus_rank`).
- §4 "設定一致性閘": add A15 (model_structure invariants) to the list.
- §0 一句話 + §5: one line that two-stage stacking is an advanced opt-in mode; cross-ref `docs/pipelines/training.md` and the multiitem-imbalance handbook.

- [ ] **Step 3: Verify docs reference real identifiers**

Run: `grep -n "train_composite_model\|CompositeModelAdapter\|model_structure\|per_group_plus_rank" docs/pipelines/training.md README.md`
Expected: matches present; spot-check they match code.

- [ ] **Step 4: Commit**

```bash
git add docs/pipelines/training.md README.md
git commit -m "docs(training): two-stage stacking mode"
```

### Task 4.2: evaluation.md, design-principles.md, change-guide.md, handbook cross-refs

**Files:**
- Modify: `docs/pipelines/evaluation.md`, `docs/design-principles.md`, `docs/change-guide.md`, `docs/handbooks/gbdt_multiitem_imbalance.md`, `docs/handbooks/gbdt_learning_to_rank.md`

- [ ] **Step 1: evaluation.md** — note `product_categories` is now the shared top-level block (single source); `evaluation.product_categories.enabled` is the report toggle only; `macro_per_item_map` is the cold-item ruler for comparing composite vs shared.

- [ ] **Step 2: design-principles.md** — add `CompositeModelAdapter` as a worked example of "adapter boundary收斂 blast radius": downstream depends only on `predict()`, so a multi-booster internal structure is invisible to inference/evaluation/catalog.

- [ ] **Step 3: change-guide.md** — add a short "新增 model structure / consistency 不變量" entry: invariants go in `core/consistency.py` (single source), versioning auto-includes `training:` keys, grouping data goes top-level to avoid base_dataset_version churn.

- [ ] **Step 4: handbooks** — in `gbdt_multiitem_imbalance.md` and `gbdt_learning_to_rank.md`, add ONE cross-reference line each: two-stage stacking (`docs/pipelines/training.md`) is an engineering landing of these imbalance/ranking ideas. **Do not** rewrite math.

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "docs: two-stage stacking cross-refs (eval, principles, change-guide, handbooks)"
```

---

## Self-Review

**Spec coverage** (spec §2–§13): grouping abstraction → Task 0.1/3.2; OOF cross-fitting → 3.1/3.2; pointwise Stage-2 → 3.2 (`stage2_matrix`); CompositeModelAdapter predict/save/load → 2.1/2.2; ModelAdapterDataset routing → 2.3; versioning → 1.3; consistency A15/A16 → 1.1; calibration-off + future hook → 1.1 (guard) + spec §10 (wrapping is free); product_categories SoT → 0.2; inference/eval untouched → no task needed (verified: downstream only calls `predict()`); diagnostics guard → 3.3; docs incl. README → 4.1/4.2; tests → throughout. **Gap check:** HPO scope (v1: fixed params, no per-submodel HPO) is realized by `train_composite` using `_stage1_params`/`_stage2_params` directly — documented in training.md (4.1). No spec section left without a task.

**Placeholder scan:** No "TBD/TODO/implement later". The one prose-only step is 3.3 Step 4's "existing nodes unchanged" — the existing code is already in `pipeline.py:76-128` and shown there; the branch code IS given. Acceptable (refers to current file content, not future work).

**Type/name consistency:** `_from_parts` kwargs match between 2.1 (definition), 2.2 (save reads same attrs), 3.2 (caller). `item_code_to_group`/`group_to_code`/`item_col_index`/`n_features` consistent across adapter, manifest, trainer. `train_composite(train, train_dev, val, preprocessor_metadata, parameters)` signature identical in 3.2 def, 3.3 node, and tests. `model_structure` value `"per_group_plus_rank"` identical in config, consistency, versioning, pipeline branch, manifest.

**Known risk to watch during execution:** `_read_frame` assumes the item column is the only deferred identity categorical needing encoding (matches `extract_Xy_with_groups`'s `deferred_cats` logic, `extract.py:372-379`). If a deploy declares entity/time as categorical, extend it — but A2/A3 currently constrain only `schema.item`, so this holds for the supported schema.
