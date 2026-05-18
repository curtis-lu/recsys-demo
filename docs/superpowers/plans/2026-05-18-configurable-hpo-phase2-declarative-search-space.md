# Configurable HPO — Phase 2: Declarative Search-Space Schema + safe_eval + build_trial_params — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the hard-coded six-parameter Optuna block in `tune_hyperparameters` with a declarative, ordered `search_space` list in YAML, interpreted by a small isolated `build_trial_params`, plus a restricted stdlib-`ast` `safe_eval` foundation (used in Phase 3) — with every schema rule enforced fail-loud at CLI entry through `core/consistency.py`.

**Architecture:** A pure `core/safe_eval.py` (built now, exercised in Phase 3) evaluates a restricted expression grammar with no `eval`/import/attribute. A new `pipelines/training/search_space.py` owns `ParamSpec` parsing + `build_trial_params(trial, search_space)` (define-by-run in list order; imports nothing from `nodes.py`, unit-testable in isolation). A new collect-all consistency predicate `search_space_errors` (invariant **A8**) validates the schema at CLI entry. The YAML migrates dict→ordered-list; this restructures the hashed `training:` block → one **intentional, accepted, one-time `model_version` bump** (no golden-hash test exists, verified). Phases 3 (`when`/expression bounds) and 4 (XGBoost) are separate later plans; Phase 2 parses-and-stores the `when` field but **rejects it fail-loud** until Phase 3 (no silent ignore).

**Tech Stack:** Python 3.10 (stdlib `ast`, `dataclasses`), Optuna 4.5.0, PyYAML 6.0.2, pytest 7.3.1. No new dependencies (production constraint).

---

## Conventions for every command in this plan

```bash
WT=/Users/curtislu/projects/recsys_tfb/.worktrees/configurable-hpo-search-space
PY=/Users/curtislu/projects/recsys_tfb/.venv/bin/python
export PATH="/Users/curtislu/projects/recsys_tfb/.venv/bin:$PATH"   # post-commit graphify hook needs venv python3
# pytest:  PYTHONPATH=$WT/src $PY -m pytest <path> -q
# git:     git -C $WT <args>     (never swallow exit/stdout; verify HEAD after commits;
#                                  `git -C $WT status --porcelain` must never show graphify-out/GRAPH_REPORT.md)
```

Pre-flight (run once; abort if any line is wrong):

```bash
readlink $WT/.venv          # /Users/curtislu/projects/recsys_tfb/.venv
$PY -V                      # Python 3.10.9
git -C $WT rev-parse --abbrev-ref HEAD   # feat/hpo-phase2
git -C $WT merge-base --is-ancestor 074a203 HEAD && echo "Phase 1 in base"   # must print: Phase 1 in base
```

All Phase-2 unit tests are pure-Python / fake-trial — no Spark. The integration suite (`test_nodes.py`) is ~10s. Run only touched files per task.

---

## Design decisions (locked — do not re-litigate during execution)

1. **`safe_eval` is built in Phase 2 but NOT wired into `build_trial_params` yet.** Spec §Phase 2 item 2 mandates it as the foundation, exercised in Phase 3. Phase 2 ships it fully unit-tested and unused-but-ready. This is intentional, not dead code by accident.
2. **`when` / expression-valued `low/high/step` are parsed-and-stored but rejected fail-loud in Phase 2.** `ParamSpec` has an optional `when: str | None` field (so Phase 3 only adds behavior, not schema). The Phase-2 `search_space_errors` predicate raises `ConfigConsistencyError` with an actionable "conditional/expression search space is implemented in Phase 3; not yet supported" message if any spec carries `when` or a non-numeric (string) `low/high/step`. Reconciles spec "schema accepts the keys now" (parser does not crash; field exists) with the project's no-silent-failure ethos (no silently-ignored `when`).
3. **`spec.name` is BOTH the Optuna suggest name and the returned param key** (spec §Phase 2 item 1). `build_trial_params` returns `{spec.name: value}`; `study.best_params` keys therefore flow unchanged into `finalize_model`'s `**best_params` merge — zero change needed downstream.
4. **Optuna kwarg rules enforced in the predicate, not discovered at runtime:** Optuna forbids `step` together with `log=True` for `suggest_float`. `search_space_errors` rejects `log: true` + `step` set. `int` may use `log` or `step` (not both — same rule applied uniformly).
5. **`model_version` one-time bump is intentional and accepted.** The dict→list restructure changes the hashed `training:` block. No test pins a golden hash of the real config (verified: `tests/test_core/test_versioning.py` uses synthetic params only). Documented in the YAML header comment in Task 5.

---

## File Structure

| File | Create / Modify | Responsibility |
|---|---|---|
| `src/recsys_tfb/core/safe_eval.py` | **Create** | Restricted stdlib-`ast` expression evaluator. Pure; no imports beyond `ast`/`operator`. `safe_eval(expr, context) -> Any`, raises `SafeEvalError`. |
| `tests/test_core/test_safe_eval.py` | **Create** | Allow + reject coverage (the security boundary). |
| `src/recsys_tfb/pipelines/training/search_space.py` | **Create** | `ParamSpec`, `parse_search_space(list) -> list[ParamSpec]`, `build_trial_params(trial, search_space) -> dict`. Imports nothing from `nodes.py`. |
| `tests/test_pipelines/test_training/test_search_space.py` | **Create** | `build_trial_params` define-by-run + parsing tests (fake trial). |
| `src/recsys_tfb/core/consistency.py` | **Modify** | Add `search_space_errors` predicate (A8) + legend bullet + wire into `validate_config_consistency`. |
| `tests/test_core/test_consistency.py` | **Modify** | A8 predicate tests. |
| `tests/test_core/test_consistency_cli_wiring.py` | **Modify** | A8 surfaces via `validate_config_consistency`. |
| `src/recsys_tfb/pipelines/training/nodes.py` | **Modify** | Replace the hard-coded `trial_params = {...}` (lines 309-341) with `build_trial_params(trial, search_space)`; add import. |
| `conf/base/parameters_training.yaml` | **Modify** | Migrate `search_space:` dict → ordered list (exact same 6 params/bounds); header note re the one-time `model_version` bump. |
| `tests/test_pipelines/test_training/test_nodes.py` | **Modify** | Migrate `training_parameters` fixture + `test_params_in_search_space` + `test_tune_defaults_ranking_metric` to list form. |
| `tests/test_pipelines/test_training/test_pipeline.py` | **Modify** | Migrate the e2e `search_space` fixture to list form. |

---

## Task 1: `core/safe_eval.py` — restricted expression evaluator

**Files:**
- Create: `src/recsys_tfb/core/safe_eval.py`
- Test: `tests/test_core/test_safe_eval.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_core/test_safe_eval.py`:

```python
"""Tests for recsys_tfb.core.safe_eval — the expression security boundary."""

import pytest

from recsys_tfb.core.safe_eval import SafeEvalError, safe_eval


class TestAllowed:
    def test_arithmetic(self):
        assert safe_eval("2 + 3 * 4", {}) == 14
        assert safe_eval("(10 - 4) / 2", {}) == 3.0
        assert safe_eval("7 // 2", {}) == 3
        assert safe_eval("2 ** 5", {}) == 32
        assert safe_eval("7 % 3", {}) == 1

    def test_names_from_context(self):
        assert safe_eval("num_leaves * 2", {"num_leaves": 8}) == 16

    def test_unary(self):
        assert safe_eval("-x", {"x": 5}) == -5
        assert safe_eval("not flag", {"flag": False}) is True

    def test_comparison_and_bool(self):
        assert safe_eval("a < b and b < 10", {"a": 1, "b": 4}) is True
        assert safe_eval("a == 1 or b == 99", {"a": 0, "b": 99}) is True
        assert safe_eval("3 <= n <= 8", {"n": 5}) is True

    def test_membership(self):
        assert safe_eval("obj in ['lambdarank', 'rank_xendcg']", {"obj": "lambdarank"}) is True
        assert safe_eval("x not in [1, 2]", {"x": 3}) is True

    def test_ifexp(self):
        assert safe_eval("10 if big else 1", {"big": True}) == 10

    def test_allowlisted_calls(self):
        assert safe_eval("min(a, b)", {"a": 3, "b": 7}) == 3
        assert safe_eval("max(1, n, 4)", {"n": 9}) == 9
        assert safe_eval("abs(-x)", {"x": 5}) == 5
        assert safe_eval("int(2.9)", {}) == 2
        assert safe_eval("round(3.14159, 2)", {}) == 3.14


class TestRejected:
    @pytest.mark.parametrize("expr", [
        "__import__('os')",
        "x.__class__",
        "x.foo",
        "().__class__.__bases__",
        "eval('1')",
        "open('f')",
        "lambda: 1",
        "[i for i in range(3)]",
        "{1: 2}",
        "{1, 2}",
        "x[0]",
        "a := 1",
    ])
    def test_disallowed_constructs_raise(self, expr):
        with pytest.raises(SafeEvalError):
            safe_eval(expr, {"x": [1], "a": 1})

    def test_unknown_name_raises(self):
        with pytest.raises(SafeEvalError, match="unknown name"):
            safe_eval("mystery + 1", {})

    def test_call_to_non_allowlisted_raises(self):
        with pytest.raises(SafeEvalError, match="call"):
            safe_eval("sorted([3,1])", {})

    def test_syntax_error_wrapped(self):
        with pytest.raises(SafeEvalError, match="syntax"):
            safe_eval("1 +", {})

    def test_error_is_valueerror(self):
        assert issubclass(SafeEvalError, ValueError)
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_safe_eval.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.core.safe_eval'`

- [ ] **Step 3: Write the module**

Create `src/recsys_tfb/core/safe_eval.py`:

```python
"""Restricted arithmetic/boolean expression evaluator (stdlib ``ast`` only).

Used by the declarative HPO search space: ``when`` guards and
expression-valued bounds (wired in Phase 3). Built and fully tested in
Phase 2 as the security foundation. No ``eval``/``exec``, no imports, no
attribute access, no comprehensions/lambda, calls only to a tiny numeric
allowlist. Production constraint: no third-party expression library.
"""

from __future__ import annotations

import ast
import operator
from typing import Any

__all__ = ["SafeEvalError", "safe_eval"]


class SafeEvalError(ValueError):
    """Raised on a syntax error or any disallowed construct/name/call."""


_BIN = {
    ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
    ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod, ast.Pow: operator.pow,
}
_UNARY = {ast.UAdd: operator.pos, ast.USub: operator.neg, ast.Not: operator.not_}
_CMP = {
    ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt,
    ast.LtE: operator.le, ast.Gt: operator.gt, ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b, ast.NotIn: lambda a, b: a not in b,
}
_CALLS = {
    "min": min, "max": max, "abs": abs, "round": round,
    "int": int, "float": float, "len": len,
}


def safe_eval(expr: str, context: dict) -> Any:
    """Evaluate ``expr`` against name->value ``context``. Raise SafeEvalError
    on a syntax error or any construct outside the allowlist."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise SafeEvalError(f"syntax error in expression {expr!r}: {exc}") from exc
    return _eval(tree.body, context)


def _eval(node: ast.AST, ctx: dict) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id not in ctx:
            raise SafeEvalError(f"unknown name {node.id!r}")
        return ctx[node.id]
    if isinstance(node, (ast.List, ast.Tuple)):
        return [_eval(e, ctx) for e in node.elts]
    if isinstance(node, ast.UnaryOp):
        op = _UNARY.get(type(node.op))
        if op is None:
            raise SafeEvalError(f"disallowed unary op {type(node.op).__name__}")
        return op(_eval(node.operand, ctx))
    if isinstance(node, ast.BinOp):
        op = _BIN.get(type(node.op))
        if op is None:
            raise SafeEvalError(f"disallowed operator {type(node.op).__name__}")
        return op(_eval(node.left, ctx), _eval(node.right, ctx))
    if isinstance(node, ast.BoolOp):
        vals = node.values
        if isinstance(node.op, ast.And):
            result = True
            for v in vals:
                result = _eval(v, ctx)
                if not result:
                    return result
            return result
        result = False
        for v in vals:
            result = _eval(v, ctx)
            if result:
                return result
        return result
    if isinstance(node, ast.Compare):
        left = _eval(node.left, ctx)
        for op_node, comp in zip(node.ops, node.comparators):
            op = _CMP.get(type(op_node))
            if op is None:
                raise SafeEvalError(
                    f"disallowed comparison {type(op_node).__name__}"
                )
            right = _eval(comp, ctx)
            if not op(left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        return _eval(node.body, ctx) if _eval(node.test, ctx) else _eval(node.orelse, ctx)
    if isinstance(node, ast.Call):
        if (
            not isinstance(node.func, ast.Name)
            or node.func.id not in _CALLS
            or node.keywords
        ):
            raise SafeEvalError("disallowed call (only min/max/abs/round/int/float/len, no kwargs)")
        return _CALLS[node.func.id](*[_eval(a, ctx) for a in node.args])
    raise SafeEvalError(f"disallowed expression: {type(node).__name__}")
```

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_safe_eval.py -q`
Expected: PASS (all allow + reject cases). Note: `a := 1` raises at `ast.parse` → wrapped as SafeEvalError "syntax" (walrus in `eval` mode is a SyntaxError) — the parametrized reject test still passes via the SafeEvalError it expects.

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/core/safe_eval.py tests/test_core/test_safe_eval.py
git -C $WT commit -m "feat(hpo): core.safe_eval restricted ast evaluator (Phase 2 foundation)"
git -C $WT rev-parse --short HEAD
git -C $WT status --porcelain   # must NOT show graphify-out/GRAPH_REPORT.md
```

---

## Task 2: `pipelines/training/search_space.py` — ParamSpec + build_trial_params

**Files:**
- Create: `src/recsys_tfb/pipelines/training/search_space.py`
- Test: `tests/test_pipelines/test_training/test_search_space.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pipelines/test_training/test_search_space.py`:

```python
"""Tests for the declarative search-space ParamSpec + build_trial_params."""

import pytest

from recsys_tfb.pipelines.training.search_space import (
    ParamSpec,
    build_trial_params,
    parse_search_space,
)


class FakeTrial:
    """Records suggest_* calls in order; returns the low/first deterministically."""

    def __init__(self):
        self.calls = []

    def suggest_int(self, name, low, high, step=1, log=False):
        self.calls.append(("int", name, low, high, step, log))
        return low

    def suggest_float(self, name, low, high, step=None, log=False):
        self.calls.append(("float", name, low, high, step, log))
        return low

    def suggest_categorical(self, name, choices):
        self.calls.append(("cat", name, tuple(choices)))
        return choices[0]


SPACE = [
    {"name": "learning_rate", "type": "float", "low": 0.001, "high": 0.1, "log": True},
    {"name": "num_leaves", "type": "int", "low": 4, "high": 64},
    {"name": "max_depth", "type": "int", "low": 3, "high": 8, "step": 1},
    {"name": "booster_kind", "type": "categorical", "choices": ["gbdt", "dart"]},
]


class TestParseSearchSpace:
    def test_parses_list_into_paramspecs(self):
        specs = parse_search_space(SPACE)
        assert [s.name for s in specs] == [
            "learning_rate", "num_leaves", "max_depth", "booster_kind"
        ]
        assert all(isinstance(s, ParamSpec) for s in specs)
        lr = specs[0]
        assert lr.type == "float" and lr.low == 0.001 and lr.high == 0.1 and lr.log is True
        assert specs[3].type == "categorical" and specs[3].choices == ["gbdt", "dart"]

    def test_when_field_parsed_but_stored(self):
        specs = parse_search_space([
            {"name": "x", "type": "int", "low": 1, "high": 9, "when": "num_leaves > 8"}
        ])
        assert specs[0].when == "num_leaves > 8"


class TestBuildTrialParams:
    def test_dispatches_in_list_order_with_kwargs(self):
        trial = FakeTrial()
        out = build_trial_params(trial, SPACE)
        assert out == {
            "learning_rate": 0.001, "num_leaves": 4,
            "max_depth": 3, "booster_kind": "gbdt",
        }
        assert trial.calls[0] == ("float", "learning_rate", 0.001, 0.1, None, True)
        assert trial.calls[1] == ("int", "num_leaves", 4, 64, 1, False)
        assert trial.calls[2] == ("int", "max_depth", 3, 8, 1, False)
        assert trial.calls[3] == ("cat", "booster_kind", ("gbdt", "dart"))

    def test_float_step_passed_when_set(self):
        trial = FakeTrial()
        build_trial_params(trial, [
            {"name": "ff", "type": "float", "low": 0.0, "high": 1.0, "step": 0.25}
        ])
        assert trial.calls[0] == ("float", "ff", 0.0, 1.0, 0.25, False)

    def test_name_is_both_suggest_name_and_return_key(self):
        trial = FakeTrial()
        out = build_trial_params(trial, [
            {"name": "min_child_samples", "type": "int", "low": 5, "high": 100}
        ])
        assert list(out.keys()) == ["min_child_samples"]
        assert trial.calls[0][1] == "min_child_samples"
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_search_space.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.pipelines.training.search_space'`

- [ ] **Step 3: Write the module**

Create `src/recsys_tfb/pipelines/training/search_space.py`:

```python
"""Declarative HPO search space: ParamSpec + Optuna define-by-run builder.

The YAML ``training.search_space`` is an ordered list of ParamSpec maps.
Order is meaningful: Optuna samples in list order so a later param's
Phase-3 ``when``/expression may reference earlier params. This module is
schema-only + sampling; ALL validation lives in
``core.consistency.search_space_errors`` (collect-all, CLI entry). It
imports nothing from ``nodes.py`` so it is unit-testable in isolation.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ParamSpec:
    """One hyperparameter. ``name`` is BOTH the Optuna suggest name and the
    algorithm param key (must map 1:1 to a native LightGBM/XGBoost param so
    ``finalize_model``'s ``**best_params`` merge stays correct).

    ``when`` is parsed and stored for Phase 3; Phase 2 rejects it fail-loud
    in the consistency layer (it is never silently ignored here).
    """

    name: str
    type: str
    low: object = None
    high: object = None
    step: object = None
    log: bool = False
    choices: list | None = None
    when: str | None = None


def parse_search_space(raw: list) -> list[ParamSpec]:
    """Turn the raw YAML list into ParamSpec objects, preserving order.

    Pure structural mapping; assumes ``raw`` already passed
    ``core.consistency.search_space_errors`` at CLI entry.
    """
    specs: list[ParamSpec] = []
    for item in raw:
        specs.append(
            ParamSpec(
                name=item["name"],
                type=item["type"],
                low=item.get("low"),
                high=item.get("high"),
                step=item.get("step"),
                log=bool(item.get("log", False)),
                choices=item.get("choices"),
                when=item.get("when"),
            )
        )
    return specs


def build_trial_params(trial, search_space: list) -> dict:
    """Sample one trial's params from the declarative space, in list order.

    Returns ``{spec.name: value}``; ``spec.name`` is also the Optuna suggest
    name so ``study.best_params`` keys flow unchanged into the final refit.
    """
    specs = parse_search_space(search_space)
    out: dict = {}
    for s in specs:
        if s.type == "int":
            kwargs = {"log": s.log}
            if s.step is not None:
                kwargs["step"] = s.step
            out[s.name] = trial.suggest_int(s.name, s.low, s.high, **kwargs)
        elif s.type == "float":
            kwargs = {"log": s.log}
            if s.step is not None:
                kwargs["step"] = s.step
            out[s.name] = trial.suggest_float(s.name, s.low, s.high, **kwargs)
        elif s.type == "categorical":
            out[s.name] = trial.suggest_categorical(s.name, s.choices)
        else:  # unreachable: search_space_errors rejects unknown type at CLI
            raise ValueError(f"unknown ParamSpec.type {s.type!r}")
    return out
```

Note: `FakeTrial.suggest_int` default `step=1` matches Optuna's real default; the test asserts `step` arrives as `1` for `num_leaves` (no `step` in spec → kwarg omitted → FakeTrial default `1`) and `1` for `max_depth` (`step: 1` in spec → passed). Both produce `1`, consistent.

- [ ] **Step 4: Run to verify it passes**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_pipelines/test_training/test_search_space.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git -C $WT add src/recsys_tfb/pipelines/training/search_space.py tests/test_pipelines/test_training/test_search_space.py
git -C $WT commit -m "feat(hpo): declarative ParamSpec + build_trial_params (Phase 2)"
git -C $WT status --porcelain
```

---

## Task 3: A8 consistency predicate — `search_space_errors`

**Files:**
- Modify: `src/recsys_tfb/core/consistency.py` (legend after the A7 bullet ~line 41; predicate after `ranking_objective_conflicts` ~line 220; wire into `validate_config_consistency` after the A7 loop ~line 265)
- Test: `tests/test_core/test_consistency.py` (append), `tests/test_core/test_consistency_cli_wiring.py` (append)

- [ ] **Step 1: Write the failing predicate tests**

Append to `tests/test_core/test_consistency.py`:

```python
from recsys_tfb.core.consistency import search_space_errors


class TestSearchSpaceErrors:
    def _p(self, space):
        return {"training": {"search_space": space}}

    VALID = [
        {"name": "learning_rate", "type": "float", "low": 0.001, "high": 0.1, "log": True},
        {"name": "num_leaves", "type": "int", "low": 4, "high": 64},
        {"name": "max_depth", "type": "int", "low": 3, "high": 8, "step": 1},
        {"name": "kind", "type": "categorical", "choices": ["gbdt", "dart"]},
    ]

    def test_valid_space_ok(self):
        assert search_space_errors(self._p(self.VALID)) == []

    def test_absent_search_space_ok(self):
        assert search_space_errors({"training": {}}) == []

    def test_must_be_list_not_dict(self):
        errs = search_space_errors(self._p({"learning_rate": {"low": 1, "high": 2}}))
        assert len(errs) == 1 and "must be a list" in errs[0]

    def test_missing_name_or_type(self):
        errs = search_space_errors(self._p([{"type": "int", "low": 1, "high": 2}]))
        assert any("name" in e for e in errs)

    def test_unknown_type(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "loguniform", "low": 1, "high": 2}]))
        assert any("type" in e and "loguniform" in e for e in errs)

    def test_duplicate_names(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 2},
            {"name": "x", "type": "int", "low": 3, "high": 4},
        ]))
        assert any("duplicate" in e for e in errs)

    def test_low_ge_high(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "int", "low": 9, "high": 4}]))
        assert any("low" in e and "high" in e for e in errs)

    def test_log_requires_positive_low(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "float", "low": 0.0, "high": 1.0, "log": True}
        ]))
        assert any("log" in e and "positive" in e for e in errs)

    def test_log_and_step_mutually_exclusive(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "float", "low": 0.1, "high": 1.0, "log": True, "step": 0.1}
        ]))
        assert any("log" in e and "step" in e for e in errs)

    def test_step_must_be_positive(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 9, "step": 0}
        ]))
        assert any("step" in e and "positive" in e for e in errs)

    def test_categorical_needs_nonempty_choices(self):
        errs = search_space_errors(self._p([{"name": "x", "type": "categorical", "choices": []}]))
        assert any("choices" in e for e in errs)

    def test_when_rejected_phase3(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": 9, "when": "num_leaves > 8"}
        ]))
        assert any("Phase 3" in e for e in errs)

    def test_string_expression_bound_rejected_phase3(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "int", "low": 1, "high": "num_leaves"}
        ]))
        assert any("Phase 3" in e for e in errs)

    def test_collects_all(self):
        errs = search_space_errors(self._p([
            {"name": "x", "type": "bogus", "low": 1, "high": 2},
            {"name": "x", "type": "int", "low": 5, "high": 1},
        ]))
        assert len(errs) >= 3  # unknown type + duplicate name + low>=high
```

- [ ] **Step 2: Run to verify it fails**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency.py -q -k SearchSpaceErrors`
Expected: FAIL — `ImportError: cannot import name 'search_space_errors'`

- [ ] **Step 3: Add the predicate, legend, and wiring**

In `src/recsys_tfb/core/consistency.py`, add the A8 legend bullet immediately after the A7 bullet (the block ending `` ``ranking_objective_conflicts``. `` at ~line 41), before the blank line preceding `Layer 2 — data-stage validation`:

```
* A8 — ``training.search_space`` declarative schema validity: must be an
  ordered list of ParamSpec maps; each needs ``name`` (unique) + ``type`` ∈
  {int,float,categorical}; numeric ``low < high``; positive ``step``;
  ``log: true`` ⟹ ``low > 0`` and no ``step``; categorical needs non-empty
  ``choices``. ``when`` / string-expression bounds are rejected until
  Phase 3. Predicate: ``search_space_errors``.
```

Add the predicate immediately after `ranking_objective_conflicts` returns (after its final `return errors`, before `def validate_config_consistency`):

```python
_SS_TYPES = frozenset({"int", "float", "categorical"})


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
            num = (int, float)
            if isinstance(low, num) and isinstance(high, num) and not (low < high):
                errors.append(f"{tag}: low ({low}) must be < high ({high}).")
            if isinstance(step, num) and step <= 0:
                errors.append(f"{tag}: step must be positive (got {step}).")
            log = bool(item.get("log", False))
            if log and isinstance(low, num) and low <= 0:
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
```

In `validate_config_consistency`, immediately after the existing A7 loop
(`for msg in ranking_objective_conflicts(parameters): errors.append(msg)`)
and before `if errors:`, add:

```python
    for msg in search_space_errors(parameters):
        errors.append(msg)
```

- [ ] **Step 4: Run predicate + full consistency file**

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency.py -q`
Expected: PASS (A1–A8; the existing A1–A7 tests stay green — `search_space_errors` returns `[]` when `search_space` absent, which all A1–A7 fixtures satisfy).

- [ ] **Step 5: Add + run the CLI-wiring test**

Append to `tests/test_core/test_consistency_cli_wiring.py`:

```python
def test_a8_search_space_schema_surfaces_via_validate():
    import pytest

    from recsys_tfb.core.consistency import (
        ConfigConsistencyError,
        validate_config_consistency,
    )

    params = {
        "schema": {"columns": {
            "time": "snap_date", "entity": ["cust_id"],
            "item": "prod_name", "label": "label"}},
        "training": {"search_space": {"learning_rate": {"low": 1, "high": 2}}},
    }
    with pytest.raises(ConfigConsistencyError, match="must be a list"):
        validate_config_consistency(params)
```

Run: `PYTHONPATH=$WT/src $PY -m pytest $WT/tests/test_core/test_consistency_cli_wiring.py -q`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git -C $WT add src/recsys_tfb/core/consistency.py tests/test_core/test_consistency.py tests/test_core/test_consistency_cli_wiring.py
git -C $WT commit -m "feat(hpo): A8 consistency — declarative search_space schema (Phase 2)"
git -C $WT status --porcelain
```

---

## Task 4: Integrate — swap `tune_hyperparameters` + migrate YAML & all fixtures

This task is atomic: the moment `tune_hyperparameters` consumes the list form,
the YAML **and every test fixture** must be list form, or real-path tests break.

**Files:**
- Modify: `src/recsys_tfb/pipelines/training/nodes.py` (lines 309-341 — the `trial_params = {...}` literal block; +1 import line)
- Modify: `conf/base/parameters_training.yaml` (lines 48-66 — `search_space:` dict → list)
- Modify: `tests/test_pipelines/test_training/test_nodes.py` (fixture lines 74-81; `test_params_in_search_space` lines ~226-237; `test_tune_defaults_ranking_metric` `search_space` block)
- Modify: `tests/test_pipelines/test_training/test_pipeline.py` (lines 208-215)

- [ ] **Step 1: Swap the hard-coded block in `nodes.py`**

In `src/recsys_tfb/pipelines/training/nodes.py`, replace the entire literal
assignment (current lines 309-341):

```python
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
```

with:

```python
        trial_params = build_trial_params(trial, search_space)
```

And add the import at the top of the `objective` scope's enclosing
function — place it right after the existing
`from recsys_tfb.core.group_utils import default_metric_for_objective`
line (currently line 283):

```python
    from recsys_tfb.pipelines.training.search_space import build_trial_params
```

(`search_space` is already bound at line 277:
`search_space = training_params["search_space"]` — now a list.)

- [ ] **Step 2: Migrate `conf/base/parameters_training.yaml`**

Replace the `search_space:` block (current lines 48-66) with the ordered-list
form, preserving the exact same six parameters and bounds:

```yaml
  # search_space: ordered list of ParamSpec maps (declarative HPO, Phase 2).
  # `name` is BOTH the Optuna suggest name and the LightGBM param key.
  # type ∈ int|float|categorical. int/float: low,high (+optional step, log).
  # categorical: choices. `when` / expression bounds are Phase 3 (rejected
  # fail-loud until then by consistency A8). NOTE: migrating from the old
  # dict form restructures the hashed `training:` block — this is a
  # deliberate, accepted ONE-TIME model_version bump (see versioning.py).
  search_space:
    - name: learning_rate
      type: float
      low: 0.001
      high: 0.1
      log: true
    - name: num_leaves
      type: int
      low: 4
      high: 64
    - name: max_depth
      type: int
      low: 3
      high: 8
    - name: min_child_samples
      type: int
      low: 5
      high: 100
    - name: subsample
      type: float
      low: 0.6
      high: 1.0
    - name: colsample_bytree
      type: float
      low: 0.6
      high: 1.0
```

- [ ] **Step 3: Migrate the `test_nodes.py` fixture + assertions**

In `tests/test_pipelines/test_training/test_nodes.py`, replace the
`training_parameters` fixture's `search_space` dict (lines 74-81) with:

```python
            "search_space": [
                {"name": "learning_rate", "type": "float", "low": 0.01, "high": 0.3, "log": True},
                {"name": "num_leaves", "type": "int", "low": 16, "high": 64},
                {"name": "max_depth", "type": "int", "low": 3, "high": 8},
                {"name": "min_child_samples", "type": "int", "low": 5, "high": 50},
                {"name": "subsample", "type": "float", "low": 0.6, "high": 1.0},
                {"name": "colsample_bytree", "type": "float", "low": 0.6, "high": 1.0},
            ],
```

Replace `test_params_in_search_space` (the body that does
`space = training_parameters["training"]["search_space"]` and indexes
`space["num_leaves"]["low"]`) with a list-aware lookup:

```python
    def test_params_in_search_space(
        self, lgb_handles, synthetic_model_inputs, preprocessor_metadata, training_parameters
    ):
        train_lgb_h, train_dev_lgb_h = lgb_handles
        _, _, val_h, *_ = synthetic_model_inputs
        space = {s["name"]: s for s in training_parameters["training"]["search_space"]}
        best_params, _, _ = tune_hyperparameters(
            train_lgb_h, train_dev_lgb_h, val_h, preprocessor_metadata, training_parameters,
        )

        assert space["num_leaves"]["low"] <= best_params["num_leaves"] <= space["num_leaves"]["high"]
        assert space["max_depth"]["low"] <= best_params["max_depth"] <= space["max_depth"]["high"]
```

Replace the `search_space` dict inside `test_tune_defaults_ranking_metric`
(the `"search_space": { ... }` block) with the list form:

```python
            "search_space": [
                {"name": "learning_rate", "type": "float", "low": 0.01, "high": 0.1, "log": True},
                {"name": "num_leaves", "type": "int", "low": 4, "high": 8},
                {"name": "max_depth", "type": "int", "low": 3, "high": 5},
                {"name": "min_child_samples", "type": "int", "low": 5, "high": 10},
                {"name": "subsample", "type": "float", "low": 0.6, "high": 1.0},
                {"name": "colsample_bytree", "type": "float", "low": 0.6, "high": 1.0},
            ],
```

- [ ] **Step 4: Migrate the `test_pipeline.py` fixture**

In `tests/test_pipelines/test_training/test_pipeline.py`, replace the
`search_space` dict (lines 208-215) with:

```python
                "search_space": [
                    {"name": "learning_rate", "type": "float", "low": 0.05, "high": 0.2, "log": True},
                    {"name": "num_leaves", "type": "int", "low": 8, "high": 32},
                    {"name": "max_depth", "type": "int", "low": 3, "high": 6},
                    {"name": "min_child_samples", "type": "int", "low": 2, "high": 10},
                    {"name": "subsample", "type": "float", "low": 0.8, "high": 1.0},
                    {"name": "colsample_bytree", "type": "float", "low": 0.8, "high": 1.0},
                ],
```

- [ ] **Step 5: Run the full integration surface**

Run:
```bash
PYTHONPATH=$WT/src $PY -m pytest \
  $WT/tests/test_pipelines/test_training/test_nodes.py \
  $WT/tests/test_pipelines/test_training/test_pipeline.py \
  $WT/tests/test_core/test_safe_eval.py \
  $WT/tests/test_pipelines/test_training/test_search_space.py \
  $WT/tests/test_core/test_consistency.py \
  $WT/tests/test_core/test_consistency_cli_wiring.py \
  -q
```
Expected: PASS — all green. `test_nodes.py` exercises the real `tune_hyperparameters` through `build_trial_params` with the migrated list fixture; `test_params_in_search_space` and `test_tune_defaults_ranking_metric` use the list form; `test_pipeline.py` e2e uses the list form.

- [ ] **Step 6: Commit**

```bash
git -C $WT add src/recsys_tfb/pipelines/training/nodes.py conf/base/parameters_training.yaml tests/test_pipelines/test_training/test_nodes.py tests/test_pipelines/test_training/test_pipeline.py
git -C $WT commit -m "feat(hpo): wire build_trial_params; migrate search_space dict->list (Phase 2)"
git -C $WT status --porcelain
```

---

## Task 5: model_version bump verification + Phase 2 final sweep

**Files:** none modified (verification + graph refresh + docs note already in YAML header from Task 4 Step 2).

- [ ] **Step 1: Confirm the one-time model_version bump is real and intentional**

Run:
```bash
PYTHONPATH=$WT/src $PY -c "
import yaml
from recsys_tfb.core.versioning import _model_version_payload, compute_model_version
p = yaml.safe_load(open('$WT/conf/base/parameters_training.yaml'))
ss = _model_version_payload(p)['training']['search_space']
print('search_space type in hashed payload:', type(ss).__name__)
print('len:', len(ss), 'first:', ss[0])
v = compute_model_version(p, 'base0000', 'trai0000')
print('model_version (new list schema):', v)
"
```
Expected: `search_space type in hashed payload: list` and an 8-char hex
`model_version`. This confirms the hashed `training:` block now contains the
list structure — the deliberate one-time bump. (No golden-hash test pins the
real config — verified: `tests/test_core/test_versioning.py` only uses
synthetic params, so nothing to update.)

- [ ] **Step 2: Full Phase-2 touched test sweep**

Run:
```bash
PYTHONPATH=$WT/src $PY -m pytest \
  $WT/tests/test_core/test_safe_eval.py \
  $WT/tests/test_pipelines/test_training/test_search_space.py \
  $WT/tests/test_core/test_consistency.py \
  $WT/tests/test_core/test_consistency_cli_wiring.py \
  $WT/tests/test_pipelines/test_training/test_nodes.py \
  $WT/tests/test_pipelines/test_training/test_pipeline.py \
  $WT/tests/test_core/test_versioning.py \
  -q
```
Expected: PASS — all green, 0 failures.

- [ ] **Step 3: Refresh the graphify code graph (CLAUDE.md rule after code changes)**

Run:
```bash
cd $WT && $PY -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
git -C $WT status --porcelain   # graphify-out/GRAPH_REPORT.md must remain untracked (absent)
```

- [ ] **Step 4: Commit (no-op-safe; only if graph artifacts or nothing staged)**

```bash
git -C $WT add -A
git -C $WT commit -m "chore(hpo): Phase 2 verification — model_version bump confirmed intentional" --allow-empty
git -C $WT log --oneline 33eb37d..HEAD   # review the Phase 2 commit series
```

---

## Self-Review

**1. Spec coverage (spec §Phase 2 items 1-5):**
- Item 1 (ordered ParamSpec list: name/type/int-float low,high,step,log/categorical choices; `when` keys accepted) → Task 2 (`ParamSpec`/`parse_search_space`) + Task 3 (A8 validates; `when` parsed-but-rejected per locked Design Decision 2) + Task 4 Step 2 (YAML list) ✓
- Item 2 (`safe_eval` foundation, isolated, unit-tested, exercised Phase 3) → Task 1 ✓ (Design Decision 1 records the intentional Phase-2-unused state)
- Item 3 (`build_trial_params` replaces hard-coded block; in `search_space.py`; imports nothing from `nodes.py`) → Task 2 + Task 4 Step 1 ✓
- Item 4 (consistency predicates: per-type keys, unique name, int step positive, log⟹low>0, categorical non-empty; collect-all CLI) → Task 3 (A8) ✓ (+ log/step mutual-exclusion per Design Decision 4)
- Item 5 (YAML migration preserving exact 6 params/bounds; one-time model_version bump documented) → Task 4 Step 2 + Task 5 Step 1 ✓

**2. Placeholder scan:** No "TBD"/"add validation"/"similar to". Every code step has complete code; every run step has an exact command + expected result. The `else:` in `build_trial_params` is unreachable-by-contract (A8 rejects unknown type at CLI) and raises explicitly — not a placeholder.

**3. Type consistency:** `ParamSpec` fields (`name,type,low,high,step,log,choices,when`) are identical across Task 2 (definition), Task 3 (A8 reads the same keys from raw dicts), Task 4 (YAML keys match). `build_trial_params(trial, search_space) -> dict` signature identical in Task 2 def and Task 4 call site. `search_space_errors(parameters) -> list[str]` matches the collect-all predicate contract (same shape as `ranking_objective_conflicts`/`config_role_conflicts`). `safe_eval(expr, context)` / `SafeEvalError` consistent (Task 1 only; Phase 3 will consume). YAML `name` keys (`learning_rate`/`num_leaves`/`max_depth`/`min_child_samples`/`subsample`/`colsample_bytree`) map 1:1 to the LightGBM params the old hard-coded block used → `study.best_params` → `finalize_model` `**best_params` unchanged.

No gaps found.

---

## Execution Handoff

(Provided by the orchestrator after plan approval — see the writing-plans skill's handoff options.)
