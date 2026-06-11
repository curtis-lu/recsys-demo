# Pipeline 切片（--from-node / --only-node）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓四個 pipeline CLI 指令（dataset/training/inference/evaluation）支援從指定 node 開始跑或只跑單一 node，缺料輸入自動往上游擴張補跑。

**Architecture:** `Pipeline` 新增 `slice_from`/`slice_only`（反向擴張至 `catalog.exists()` 為真的產物為止，回傳 `(Pipeline, SlicePlan)`）；CLI 在 `_execute_pipeline` 統一切片並印執行計畫；Runner 零改動。catalog 補落地兩個 HPO 產物讓「跳過 HPO」可行。`RESUME_CONTRACTS` 契約測試釘住各 pipeline 的接續點品質。

**Tech Stack:** Python 3.10 / Typer / pytest / PySpark（僅整合 smoke）。Spec：`docs/superpowers/specs/2026-06-10-pipeline-node-slicing-design.md`。

**執行環境鐵則**（CLAUDE.md worktree SOP）：

- Worktree root：`/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing`（下稱 `<WT>`）。所有檔案操作用**含 `.worktrees/pipeline-node-slicing` 的絕對路徑**。
- 跑測試：`PYTHONPATH=<WT>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`（一律絕對 venv python，不可裸 `pytest`）。
- git 一律 `git -C <WT>`。

## File Structure

| 檔案 | 動作 | 職責 |
|---|---|---|
| `src/recsys_tfb/core/pipeline.py` | Modify | `SlicePlan` dataclass、`slice_from`、`slice_only`、`_slice_with_expansion`、`_node_index` |
| `conf/base/catalog.yaml` | Modify | 新增 `best_iteration`（JSONDataset）、`hpo_best_model`（ModelAdapterDataset，**hpo/ 子目錄**） |
| `src/recsys_tfb/__main__.py` | Modify | `_slice_pipeline`/`_format_slice_plan`/`_format_node_list`/`_slice_extra` helpers；`_execute_pipeline` 切片整合；四指令加 flags 與 manifest 留痕 |
| `tests/test_core/test_pipeline_slicing.py` | Create | 切片演算法單元測試（純 DAG，無 Spark） |
| `tests/test_io/test_model_adapter_dataset.py` | Create | ModelAdapterDataset round-trip 與 sidecar 隔離 |
| `tests/test_pipelines/test_resume_contracts.py` | Create | `RESUME_CONTRACTS` 契約測試 + catalog 條目 lint |
| `tests/test_cli.py` | Modify | helper 單元測試 + 四指令 help flags 測試 |
| `docs/operations/pipeline-slicing.md` | Create | 使用說明 + 開發守則 |
| `CLAUDE.md` | Modify | 一行指回 docs |

---

### Task 1: `Pipeline.slice_from` / `slice_only` + `SlicePlan`

**Files:**
- Modify: `src/recsys_tfb/core/pipeline.py`
- Test: `tests/test_core/test_pipeline_slicing.py`

- [ ] **Step 1: Write the failing tests**

建立 `tests/test_core/test_pipeline_slicing.py`，完整內容：

```python
"""Tests for Pipeline.slice_from / slice_only — resume-oriented forward slicing.

can_load is a plain callable; no catalog/Spark involved. Node funcs are inert
lambdas — slicing is pure DAG analysis and never calls them.
"""

import pytest

from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def _n(name, inputs=None, outputs=None):
    return Node(func=lambda *a: None, inputs=inputs, outputs=outputs, name=name)


def _chain():
    """A -> a -> B -> b -> C -> c -> D (linear)."""
    return Pipeline([
        _n("A", outputs="a"),
        _n("B", inputs=["a"], outputs="b"),
        _n("C", inputs=["b"], outputs="c"),
        _n("D", inputs=["c"], outputs="d"),
    ])


ALL_LOADABLE = lambda name: True
NONE_LOADABLE = lambda name: False


class TestSliceFrom:
    def test_basic_skip_upstream_when_loadable(self):
        pipe, plan = _chain().slice_from("C", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["C", "D"]
        assert plan.mode == "from"
        assert plan.requested == ("C", "D")
        assert plan.auto_included == {}
        assert plan.skipped == ("A", "B")
        assert plan.skipped_side_effect == ()

    def test_expansion_one_level(self):
        # b not loadable -> pull B back; a loadable -> stop, A stays skipped.
        can_load = lambda name: name != "b"
        pipe, plan = _chain().slice_from("C", can_load)
        assert [n.name for n in pipe.nodes] == ["B", "C", "D"]
        assert plan.auto_included == {"B": ("b",)}
        assert plan.skipped == ("A",)

    def test_expansion_recursive(self):
        # b and a both missing -> pull B then A.
        can_load = lambda name: name not in {"a", "b"}
        pipe, plan = _chain().slice_from("C", can_load)
        assert [n.name for n in pipe.nodes] == ["A", "B", "C", "D"]
        assert plan.auto_included == {"B": ("b",), "A": ("a",)}
        assert plan.skipped == ()

    def test_worst_case_degrades_to_full_pipeline(self):
        pipe, plan = _chain().slice_from("D", NONE_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["A", "B", "C", "D"]

    def test_per_dataset_not_per_node(self):
        # M outputs m1 (loadable) and m2 (not). X needs only m1 -> M skipped.
        nodes = [
            _n("M", outputs=["m1", "m2"]),
            _n("X", inputs=["m1"], outputs="x"),
        ]
        can_load = lambda name: name == "m1"
        pipe, plan = Pipeline(nodes).slice_from("X", can_load)
        assert [n.name for n in pipe.nodes] == ["X"]
        assert plan.skipped == ("M",)

    def test_per_dataset_pulls_when_memory_output_needed(self):
        nodes = [
            _n("M", outputs=["m1", "m2"]),
            _n("Y", inputs=["m2"], outputs="y"),
        ]
        can_load = lambda name: name == "m1"
        pipe, plan = Pipeline(nodes).slice_from("Y", can_load)
        assert [n.name for n in pipe.nodes] == ["M", "Y"]
        assert plan.auto_included == {"M": ("m2",)}

    def test_side_effect_node_never_pulled_and_reported(self):
        nodes = [
            _n("guard", inputs=["src"], outputs=None),   # zero-output gate
            _n("A", inputs=["src"], outputs="a"),
            _n("B", inputs=["a"], outputs="b"),
        ]
        pipe, plan = Pipeline(nodes).slice_from("B", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["B"]
        assert plan.skipped == ("A",)
        assert plan.skipped_side_effect == ("guard",)

    def test_topological_position_includes_parallel_branch(self):
        # B independent of A; slice_from("B") keeps C (which needs a, loadable).
        nodes = [
            _n("A", outputs="a"),
            _n("B", outputs="b"),
            _n("C", inputs=["a", "b"], outputs="c"),
        ]
        pipe, plan = Pipeline(nodes).slice_from("B", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["B", "C"]
        assert plan.skipped == ("A",)

    def test_at_handle_input_stripped(self):
        # "@x" resolves dataset name x for can_load / producer lookup.
        nodes = [
            _n("P", outputs="x"),
            _n("Q", inputs=["@x"], outputs="q"),
        ]
        can_load = lambda name: name != "x"
        pipe, plan = Pipeline(nodes).slice_from("Q", can_load)
        assert [n.name for n in pipe.nodes] == ["P", "Q"]
        assert plan.auto_included == {"P": ("x",)}

    def test_unknown_node_raises_with_available_names(self):
        with pytest.raises(ValueError, match="Unknown node 'nope'"):
            _chain().slice_from("nope", ALL_LOADABLE)
        with pytest.raises(ValueError, match="A, B, C, D"):
            _chain().slice_from("nope", ALL_LOADABLE)

    def test_external_pipeline_input_never_expands(self):
        # "src" has no producer in the pipeline -> ignored even if not loadable.
        nodes = [_n("A", inputs=["src"], outputs="a")]
        pipe, plan = Pipeline(nodes).slice_from("A", NONE_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["A"]
        assert plan.auto_included == {}


class TestSliceOnly:
    def test_single_node_when_inputs_loadable(self):
        pipe, plan = _chain().slice_only("C", ALL_LOADABLE)
        assert [n.name for n in pipe.nodes] == ["C"]
        assert plan.mode == "only"
        assert plan.requested == ("C",)
        assert plan.skipped == ("A", "B", "D")

    def test_single_node_with_expansion(self):
        can_load = lambda name: name != "b"
        pipe, plan = _chain().slice_only("C", can_load)
        assert [n.name for n in pipe.nodes] == ["B", "C"]
        assert plan.auto_included == {"B": ("b",)}

    def test_unknown_node_raises(self):
        with pytest.raises(ValueError, match="Unknown node"):
            _chain().slice_only("nope", ALL_LOADABLE)
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_core/test_pipeline_slicing.py -q
```
Expected: 全部 FAIL，`AttributeError: 'Pipeline' object has no attribute 'slice_from'`。

- [ ] **Step 3: Implement in `core/pipeline.py`**

檔案開頭 import 區改為：

```python
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable

from recsys_tfb.core.node import Node
```

在 `Pipeline` class 之前加入：

```python
@dataclass(frozen=True)
class SlicePlan:
    """Execution plan produced by Pipeline.slice_from / slice_only.

    Pure description — printing and assertions only, no runtime behavior.
    ``auto_included`` records, per pulled-in node, the missing dataset(s)
    that triggered inclusion (first trigger only when one node feeds the
    same producer twice).
    """

    mode: str                                  # "from" | "only"
    requested: tuple[str, ...]                 # node names explicitly selected
    auto_included: dict[str, tuple[str, ...]] = field(default_factory=dict)
    skipped: tuple[str, ...] = ()              # nodes with outputs, not run
    skipped_side_effect: tuple[str, ...] = ()  # zero-output nodes, not run
```

在 `Pipeline` class 內（`only_nodes_with_outputs` 之後）加入：

```python
    def slice_from(
        self, start_node: str, can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        """Forward slice: start_node and everything after it (topological
        position semantics), plus the minimal upstream closure for inputs
        that ``can_load`` reports unavailable.

        Counterpart of ``only_nodes_with_outputs`` (which cuts downstream
        and is catalog-agnostic); this cuts upstream and consults the
        catalog through ``can_load``.
        """
        idx = self._node_index(start_node)
        return self._slice_with_expansion("from", self._sorted[idx:], can_load)

    def slice_only(
        self, node_name: str, can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        """Slice down to a single node plus its minimal upstream closure."""
        idx = self._node_index(node_name)
        return self._slice_with_expansion("only", [self._sorted[idx]], can_load)

    def _node_index(self, name: str) -> int:
        for i, node in enumerate(self._sorted):
            if node.name == name:
                return i
        available = ", ".join(n.name for n in self._sorted)
        raise ValueError(
            f"Unknown node '{name}'. Available nodes (topological order): {available}"
        )

    def _slice_with_expansion(
        self, mode: str, requested: list[Node], can_load: Callable[[str], bool]
    ) -> tuple["Pipeline", SlicePlan]:
        producer: dict[str, Node] = {}
        for node in self._sorted:
            for out in node.outputs:
                producer[out] = node

        keep = set(requested)
        auto: dict[str, list[str]] = {}
        queue = deque(requested)
        while queue:
            node = queue.popleft()
            for inp in node.inputs:
                name = inp[1:] if inp.startswith("@") else inp
                p = producer.get(name)
                if p is None or p in keep:
                    continue
                if not can_load(name):
                    keep.add(p)
                    auto.setdefault(p.name, []).append(name)
                    queue.append(p)

        kept_nodes = [n for n in self._sorted if n in keep]
        plan = SlicePlan(
            mode=mode,
            requested=tuple(n.name for n in requested),
            auto_included={k: tuple(v) for k, v in auto.items()},
            skipped=tuple(
                n.name for n in self._sorted if n not in keep and n.outputs
            ),
            skipped_side_effect=tuple(
                n.name for n in self._sorted if n not in keep and not n.outputs
            ),
        )
        return Pipeline(kept_nodes), plan
```

- [ ] **Step 4: Run tests to verify they pass**

同 Step 2 指令。Expected: 全 PASS。同時跑既有測試確認不退化：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_core/ -q
```
Expected: 全 PASS。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add src/recsys_tfb/core/pipeline.py tests/test_core/test_pipeline_slicing.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "feat(core): Pipeline.slice_from/slice_only 前向切片＋缺料自動擴張"
```

---

### Task 2: catalog 補落地 HPO 產物（含 sidecar 隔離）

**背景（不可省略的陷阱）**：`ModelAdapterDataset.save` 會在 `dirname(filepath)` 寫 `model_meta.json` sidecar（`src/recsys_tfb/io/model_adapter_dataset.py:31-32`）。若 `hpo_best_model.txt` 與 `model.txt` 同放 `data/models/${model_version}/`，兩個 dataset 互踩同一個 sidecar——calibration 開啟時 `model` 的 meta 寫 `calibrated: true`，之後 load `hpo_best_model` 會誤包 `CalibratedModelAdapter`。**因此 `hpo_best_model` 必須放 `hpo/` 子目錄。**

`best_iteration` 是 int（`tune_hyperparameters` 回傳，`training/nodes.py:512`），JSONDataset 可直接序列化。`hpo_best_model` 在正常流程必非 None（第一個 trial 的 score ≥ 0 > 初始 -1.0 必然寫入 `best_state["model"]`；`n_trials=0` 時 `study.best_params` 本來就會先炸）——不需 None 防護，此決議寫進 docs（Task 6）。

**Files:**
- Modify: `conf/base/catalog.yaml`（`best_params` 條目之後，目前在 189-191 行附近）
- Test: `tests/test_io/test_model_adapter_dataset.py`（Create）

- [ ] **Step 1: Write the failing tests**

建立 `tests/test_io/test_model_adapter_dataset.py`：

```python
"""Tests for ModelAdapterDataset round-trip and sidecar isolation.

The hpo_best_model catalog entry lives in a hpo/ subdirectory precisely so
its model_meta.json sidecar cannot collide with the final model's — these
tests pin that behavior.
"""

import numpy as np

from recsys_tfb.io.model_adapter_dataset import ModelAdapterDataset
from recsys_tfb.models.base import get_adapter


def _tiny_adapter():
    rng = np.random.default_rng(42)
    X = rng.normal(size=(80, 3))
    y = (X[:, 0] > 0).astype(int)
    adapter = get_adapter("lightgbm")
    adapter.train(
        X_train=X, y_train=y, X_val=X, y_val=y,
        params={"objective": "binary", "num_iterations": 5,
                "min_child_samples": 5, "verbose": -1},
    )
    return adapter, X


class TestModelAdapterDatasetRoundTrip:
    def test_save_load_predict_consistency(self, tmp_path):
        adapter, X = _tiny_adapter()
        ds = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds.save(adapter)
        assert ds.exists()
        loaded = ds.load()
        np.testing.assert_allclose(loaded.predict(X), adapter.predict(X))

    def test_sidecar_isolation_between_model_and_hpo_model(self, tmp_path):
        adapter, _ = _tiny_adapter()
        ds_model = ModelAdapterDataset(filepath=str(tmp_path / "model.txt"))
        ds_hpo = ModelAdapterDataset(filepath=str(tmp_path / "hpo" / "model.txt"))
        ds_model.save(adapter)
        ds_hpo.save(adapter)
        # each directory carries its own sidecar — no cross-talk
        assert (tmp_path / "model_meta.json").exists()
        assert (tmp_path / "hpo" / "model_meta.json").exists()
        assert ds_hpo.load() is not None
        assert ds_model.load() is not None
```

> 若 `adapter.train` 簽名與上述不符（以 `tests/test_models/` 既有 LightGBMAdapter 測試為準），對齊既有測試的訓練呼叫方式後再繼續，不要改 production code。

- [ ] **Step 2: Run tests to verify current state**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_io/test_model_adapter_dataset.py -q
```
Expected: PASS（這兩個測試驗證既有 IO 類別行為，為 catalog 變更鋪底；若 FAIL 表示對 adapter API 認知有誤，先修測試）。

- [ ] **Step 3: Add catalog entries**

`conf/base/catalog.yaml`，在 `best_params` 條目（`filepath: data/models/${model_version}/best_params.json`）之後插入：

```yaml
best_iteration:
  type: JSONDataset
  filepath: data/models/${model_version}/best_iteration.json

# hpo/ subdirectory is REQUIRED: ModelAdapterDataset writes a model_meta.json
# sidecar next to its filepath; sharing a directory with `model` would let the
# two datasets overwrite each other's sidecar (calibration flag cross-talk).
hpo_best_model:
  type: ModelAdapterDataset
  filepath: data/models/${model_version}/hpo/model.txt
```

- [ ] **Step 4: Run broader io + cli tests**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_io/ /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_cli.py -q
```
Expected: 全 PASS（catalog 新條目不影響既有流程——node 輸出名字已存在，多了落地定義只是讓 `catalog.save` 走檔案而非 MemoryDataset）。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add conf/base/catalog.yaml tests/test_io/test_model_adapter_dataset.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "feat(catalog): 落地 best_iteration/hpo_best_model（hpo/ 子目錄隔離 sidecar）"
```

---

### Task 3: CLI helpers（純函式，先於接線）

**Files:**
- Modify: `src/recsys_tfb/__main__.py`
- Test: `tests/test_cli.py`（檔尾新增）

- [ ] **Step 1: Write the failing tests**

`tests/test_cli.py` 檔尾加入（檔案已 import `re`/`yaml`/`CliRunner`/`app`；補 import 放測試類別前）：

```python
from recsys_tfb.__main__ import (
    _format_node_list,
    _format_slice_plan,
    _slice_extra,
    _slice_pipeline,
)
from recsys_tfb.core.node import Node
from recsys_tfb.core.pipeline import Pipeline


def _slice_test_pipe():
    return Pipeline([
        Node(func=lambda: None, outputs="a", name="A"),
        Node(func=lambda a: None, inputs=["a"], outputs="b", name="B"),
        Node(func=lambda b: None, inputs=["b"], outputs="c", name="C"),
    ])


class TestSlicingHelpers:
    def test_slice_pipeline_mutual_exclusion(self):
        import pytest
        with pytest.raises(ValueError, match="mutually exclusive"):
            _slice_pipeline(_slice_test_pipe(), lambda n: True, "B", "C")

    def test_slice_pipeline_no_flags_passthrough(self):
        pipe = _slice_test_pipe()
        out, plan = _slice_pipeline(pipe, lambda n: True, None, None)
        assert out is pipe
        assert plan is None

    def test_slice_pipeline_from_node(self):
        out, plan = _slice_pipeline(_slice_test_pipe(), lambda n: True, "B", None)
        assert [n.name for n in out.nodes] == ["B", "C"]
        assert plan.mode == "from"

    def test_slice_pipeline_only_node(self):
        out, plan = _slice_pipeline(_slice_test_pipe(), lambda n: True, None, "B")
        assert [n.name for n in out.nodes] == ["B"]
        assert plan.mode == "only"

    def test_format_slice_plan_contents(self):
        _, plan = _slice_pipeline(
            _slice_test_pipe(), lambda n: n != "a", "C", None
        )
        lines = _format_slice_plan(plan, total=3)
        text = "\n".join(lines)
        assert "auto-included" in text
        assert "B" in text and "<- a" in text
        assert "skipped" in text and "A" not in plan.auto_included
        assert "WARNING" in text
        assert "running 2 of 3 nodes" in text

    def test_format_node_list_one_line_per_node(self):
        lines = _format_node_list(_slice_test_pipe(), lambda n: True)
        joined = "\n".join(lines)
        assert all(name in joined for name in ("A", "B", "C"))

    def test_slice_extra(self):
        assert _slice_extra("X", None) == {"resumed_from": "X"}
        assert _slice_extra(None, "Y") == {"only_node": "Y"}
        assert _slice_extra(None, None) is None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_cli.py -q -k Slicing
```
Expected: FAIL，`ImportError: cannot import name '_slice_pipeline'`。

- [ ] **Step 3: Implement helpers in `__main__.py`**

在 `_execute_pipeline` 定義之前加入：

```python
def _slice_pipeline(pipe, can_load, from_node, only_node):
    """Apply --from-node/--only-node slicing. Returns (pipeline, plan|None).

    Raises ValueError on conflicting flags or unknown node names (the
    Pipeline methods list available node names in their message).
    """
    if from_node and only_node:
        raise ValueError("--from-node and --only-node are mutually exclusive")
    if from_node:
        return pipe.slice_from(from_node, can_load)
    if only_node:
        return pipe.slice_only(only_node, can_load)
    return pipe, None


def _format_slice_plan(plan, total: int) -> list[str]:
    """Render a SlicePlan as [plan]-prefixed lines for logging."""
    lines = [
        f"[plan] mode={plan.mode}; requested: {', '.join(plan.requested)}",
    ]
    if plan.auto_included:
        lines.append("[plan] auto-included (missing input -> producer re-run):")
        for name, missing in plan.auto_included.items():
            lines.append(f"[plan]   {name}  <- {', '.join(missing)}")
    if plan.skipped:
        lines.append(
            f"[plan] skipped (inputs satisfied from catalog): {', '.join(plan.skipped)}"
        )
    if plan.skipped_side_effect:
        lines.append(
            "[plan] skipped side-effect nodes (outputs=None, not re-validated): "
            + ", ".join(plan.skipped_side_effect)
        )
    lines.append(
        "[plan] WARNING: resume assumes parameters are unchanged since the "
        "skipped artifacts were produced (overwrite-style Hive tables are not "
        "version-stamped)."
    )
    running = len(plan.requested) + len(plan.auto_included)
    lines.append(f"[plan] running {running} of {total} nodes")
    return lines


def _format_node_list(pipe, can_load) -> list[str]:
    """One line per node: name + what a --from-node start there would re-run."""
    lines = ["[nodes] # node  (auto-included when starting here)"]
    for i, node in enumerate(pipe.nodes):
        _, plan = pipe.slice_from(node.name, can_load)
        extra = ", ".join(plan.auto_included) if plan.auto_included else "-"
        lines.append(f"[nodes] {i + 1:>2}  {node.name}  (+ {extra})")
    return lines


def _slice_extra(from_node, only_node):
    """Manifest extra_metadata breadcrumb for sliced runs."""
    if from_node:
        return {"resumed_from": from_node}
    if only_node:
        return {"only_node": only_node}
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

同 Step 2 指令。Expected: 全 PASS。

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add src/recsys_tfb/__main__.py tests/test_cli.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "feat(cli): 切片 helpers（_slice_pipeline/_format_slice_plan/_format_node_list/_slice_extra）"
```

---

### Task 4: 四指令接線（flags、計畫輸出、dry-run、list-nodes、manifest 留痕）

**Files:**
- Modify: `src/recsys_tfb/__main__.py`（`_execute_pipeline` 與 `dataset`/`training`/`inference`/`evaluation` 四個 command）
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing help-text tests**

`tests/test_cli.py` 的 `TestSlicingHelpers` 之後加入：

```python
class TestSlicingCLIFlags:
    def test_all_four_commands_advertise_slicing_flags(self):
        for cmd in ("dataset", "training", "inference", "evaluation"):
            result = runner.invoke(app, [cmd, "--help"])
            assert result.exit_code == 0
            out = re.sub(r"\s+", " ", result.output)
            for flag in ("--from-node", "--only-node", "--dry-run", "--list-nodes"):
                assert flag in out, f"{cmd} missing {flag}"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_cli.py -q -k SlicingCLIFlags
```
Expected: FAIL（assert `--from-node` missing）。

- [ ] **Step 3: Modify `_execute_pipeline`**

簽名加 keyword-only 參數並回傳 bool（True = 真的執行了 Runner；False = dry-run / list-nodes 提前結束）。`_execute_pipeline` 現有結構見 `__main__.py:98-140`，改成：

```python
def _execute_pipeline(
    pipeline_name: str,
    pipeline_kwargs: dict,
    runtime_params: dict,
    config: ConfigLoader,
    params: dict,
    env: str,
    *,
    from_node: Optional[str] = None,
    only_node: Optional[str] = None,
    dry_run: bool = False,
    list_nodes: bool = False,
) -> bool:
    try:
        pipe = get_pipeline(pipeline_name, **pipeline_kwargs)
    except KeyError:
        available = ", ".join(list_pipelines())
        logger.error("Unknown pipeline '%s'. Available: %s", pipeline_name, available)
        raise typer.Exit(code=1)

    source_model_version = runtime_params.pop("source_model_version", None)
    substitution_params = {**params, **runtime_params}
    catalog_config = config.get_catalog_config(runtime_params=substitution_params)

    # Auto-inject cache source_tables from catalog config so cache nodes don't
    # need a parallel parameters yaml mapping. Catalog.yaml's HiveTableDataset
    # `table` field is the single source of truth for cache table resolution.
    inject_cache_source_tables(substitution_params, catalog_config)

    # For inference: when no explicit --model-version is given, the model
    # artifact should be read via the "best" symlink; swap the model filepath.
    if pipeline_name == "inference" and source_model_version is None:
        mv = runtime_params["model_version"]
        if "model" in catalog_config:
            catalog_config["model"]["filepath"] = catalog_config["model"][
                "filepath"
            ].replace(mv, "best")

    catalog = DataCatalog(catalog_config)
    catalog.add("parameters", MemoryDataset(data=substitution_params))

    if list_nodes:
        for line in _format_node_list(pipe, catalog.exists):
            logger.info(line)
        return False

    total = len(pipe.nodes)
    try:
        pipe, plan = _slice_pipeline(pipe, catalog.exists, from_node, only_node)
    except ValueError as exc:
        logger.error(str(exc))
        raise typer.Exit(code=1)

    if plan is not None:
        for line in _format_slice_plan(plan, total):
            logger.info(line)
    if dry_run:
        logger.info("[plan] dry-run: nothing executed, nothing written")
        return False

    logger.info("Running pipeline '%s' (env=%s)", pipeline_name, env)
    try:
        runner = Runner()
        runner.run(pipe, catalog)
    except Exception:
        logger.exception("Pipeline '%s' failed", pipeline_name)
        raise typer.Exit(code=1)
    return True
```

- [ ] **Step 4: Wire flags into the four commands**

四個 command 各加四個 typer options（與既有選項並列，文字一致）：

```python
    from_node: Optional[str] = typer.Option(
        None, "--from-node",
        help="Start from this node (topological position); missing upstream "
             "artifacts are auto re-run",
    ),
    only_node: Optional[str] = typer.Option(
        None, "--only-node",
        help="Run a single node (plus minimal upstream re-runs for missing inputs)",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Print the slice execution plan and exit"
    ),
    list_nodes: bool = typer.Option(
        False, "--list-nodes",
        help="List pipeline nodes with their resume cost and exit",
    ),
```

各 command 的 `_execute_pipeline(...)` 呼叫改為（以 dataset 為例，`__main__.py:375`）：

```python
    executed = _execute_pipeline(
        "dataset", pipeline_kwargs, runtime_params, config, params, env,
        from_node=from_node, only_node=only_node,
        dry_run=dry_run, list_nodes=list_nodes,
    )
    if not executed:
        return
```

training（`:493`）、inference（`:597`）、evaluation（`:701`）同形改法。**post-run manifest 區塊因 early return 自然不寫**（dry-run / list-nodes 不留任何痕跡）。

manifest 留痕——各 command 的 `_write_pipeline_manifest` 呼叫加 slice extra：

- `dataset`：只改 base manifest 那次呼叫（`:379`），加參數 `extra_metadata=_slice_extra(from_node, only_node),`（train/calibration variant manifests 不加，留痕一處即可）。
- `training`（`:508-516`）：
  ```python
      extra = _sample_weight_extra(version_dir) or {}
      slice_extra = _slice_extra(from_node, only_node)
      if slice_extra:
          extra.update(slice_extra)
  ```
  並把 `extra_metadata=_sample_weight_extra(version_dir),` 改成 `extra_metadata=extra or None,`。
- `inference`（`:612-619`）：加 `extra_metadata=_slice_extra(from_node, only_node),`。
- `evaluation`（`:705-716`）：
  ```python
      extra = {"snap_date": snap_date, "post_training": post_training}
      slice_extra = _slice_extra(from_node, only_node)
      if slice_extra:
          extra.update(slice_extra)
  ```
  並把 `extra_metadata={"snap_date": snap_date, "post_training": post_training},` 改成 `extra_metadata=extra,`。

- [ ] **Step 5: Run the full CLI test file**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_cli.py -q
```
Expected: 全 PASS（既有 mock 式 CLI 測試不受影響——未給 flag 時 `_execute_pipeline` 行為與簽名相容、回傳值未被既有測試檢查）。

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add src/recsys_tfb/__main__.py tests/test_cli.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "feat(cli): 四 pipeline 指令接 --from-node/--only-node/--dry-run/--list-nodes"
```

---

### Task 5: RESUME_CONTRACTS 契約測試 + catalog lint

**Files:**
- Create: `tests/test_pipelines/test_resume_contracts.py`

- [ ] **Step 1: Write the tests（直接寫，預期立即通過——它們驗證 Task 1-2 完成後的真實狀態；任何 FAIL 都代表前面任務或本檔契約宣告有錯，必須查明而非調整斷言遷就）**

```python
"""Resume-point contracts: pin the auto-included set for declared resume nodes.

Node inputs/outputs are descriptive (what a slice WILL re-run); these
contracts are normative (what it SHOULD only re-run). When a future change
adds a memory-only intermediate that degrades a declared resume point, this
test fails loudly — either persist the new dataset in catalog.yaml, or
consciously amend the contract here (visible in PR review).

Pure DAG + catalog-key stub; no Spark, no filesystem state. The stub assumes
every catalog-defined dataset exists — i.e. contracts describe the
"previous full run succeeded" scenario.
"""

from pathlib import Path

import yaml

from recsys_tfb.pipelines import get_pipeline

REPO_ROOT = Path(__file__).resolve().parents[2]


def _catalog_defined() -> set[str]:
    cfg = yaml.safe_load(
        (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
    )
    return set(cfg) | {"parameters"}


# pipeline -> {resume node -> exact allowed auto-included set}
RESUME_CONTRACTS = {
    "dataset": {
        # all upstream artifacts (keys tables, feature/label tables) persisted
        "fit_preprocessor_metadata": set(),
        "build_train_model_input": set(),
    },
    "training": {
        # the "skip HPO, retrain final model" scenario: only cheap
        # view/handle builders may re-run, never tune_hyperparameters
        "finalize_model": {
            "select_features",
            "cache_train_model_input",
            "cache_train_dev_model_input",
            "cache_test_model_input",
        },
    },
    "inference": {
        # scoring_dataset is memory-only by design (cheap Spark transform)
        "rank_predictions": {"build_scoring_dataset"},
    },
    "evaluation": {
        # eval_predictions/metrics are memory-only: report regeneration
        # re-runs the metric chain. Documented cost, pinned here.
        "generate_report": {
            "prepare_eval_data",
            "compute_metrics",
            "compute_baseline_metrics",
        },
    },
}


class TestResumeContracts:
    def test_declared_resume_points_hold(self):
        defined = _catalog_defined()
        can_load = lambda name: name in defined
        failures = []
        for pipeline_name, contracts in RESUME_CONTRACTS.items():
            pipe = get_pipeline(pipeline_name)
            for start, allowed in contracts.items():
                _, plan = pipe.slice_from(start, can_load)
                actual = set(plan.auto_included)
                if actual != allowed:
                    failures.append(
                        f"{pipeline_name}::{start}: auto-included {sorted(actual)} "
                        f"!= contract {sorted(allowed)}.\n"
                        f"  New memory-only dataset degrading this resume point? "
                        f"Either persist it in conf/base/catalog.yaml or amend "
                        f"RESUME_CONTRACTS with justification."
                    )
        assert not failures, "\n".join(failures)

    def test_training_skip_hpo_requires_persisted_outputs(self):
        # Guard the catalog half of the contract: tune_hyperparameters'
        # three outputs must all be catalog-persisted.
        defined = _catalog_defined()
        for name in ("best_params", "best_iteration", "hpo_best_model"):
            assert name in defined, f"{name} must stay defined in catalog.yaml"

    def test_hpo_model_sidecar_isolated_from_final_model(self):
        # ModelAdapterDataset writes model_meta.json next to its filepath;
        # hpo_best_model must live in its own directory.
        cfg = yaml.safe_load(
            (REPO_ROOT / "conf" / "base" / "catalog.yaml").read_text()
        )
        model_dir = Path(cfg["model"]["filepath"]).parent
        hpo_dir = Path(cfg["hpo_best_model"]["filepath"]).parent
        assert model_dir != hpo_dir
```

- [ ] **Step 2: Run the tests**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_pipelines/test_resume_contracts.py -q
```
Expected: 3 passed。若 `test_declared_resume_points_hold` FAIL，比對錯誤訊息中的 actual 集合與 spec §6-2——先確認是「契約宣告錯」還是「pipeline/catalog 認知錯」，修正根因。

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add tests/test_pipelines/test_resume_contracts.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "test: RESUME_CONTRACTS 契約測試釘住各 pipeline 接續點品質"
```

---

### Task 6: 文件 + CLAUDE.md

**Files:**
- Create: `docs/operations/pipeline-slicing.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write `docs/operations/pipeline-slicing.md`**

```markdown
# Pipeline 切片：從某個 node 開始跑 / 只跑某個 node

設計 spec：`docs/superpowers/specs/2026-06-10-pipeline-node-slicing-design.md`

## 使用

四個 pipeline 指令（`dataset` / `training` / `inference` / `evaluation`）皆支援：

```bash
python -m recsys_tfb training --list-nodes                       # 看 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run   # 只印執行計畫
python -m recsys_tfb training --from-node finalize_model         # 從該 node（含其後全部）接續
python -m recsys_tfb dataset  --only-node build_train_model_input # 只跑單一 node
```

- `--from-node X`：X 與拓撲序在其後的全部 node。涵蓋失敗接續／改了下游程式碼重跑／跳過昂貴上游。
- `--only-node X`：只跑 X。單獨 debug 某 node 用。
- 兩者互斥；皆會在開跑前印 `[plan]` 執行計畫（skipped / auto-included / 警語）。
- `--dry-run`：印計畫即退，不執行、不寫任何東西。

## 自動擴張補跑

被跳過 node 的輸出若「catalog 有定義且存在」（`catalog.exists()`），直接從落地讀；
否則（memory-only、或落地但上次沒跑到）自動把生產者 node 拉回必跑集合、遞迴向上，
直到全部輸入可得。最壞情況退化成 full run——任何起點都合法，絕不靜默缺料。
昂貴 node 若被拉回，會出現在計畫的 auto-included 清單，跑之前看得到。

## 使用前提與限制

- **參數未變**才能接續：`exists()` 不驗證落地產物是否由當前參數產生。版本化路徑
  （`${base_dataset_version}` 等）天然防呆；**不帶版本的覆寫式 Hive 表**
  （`recsys_prod_train_keys` 等）存在 ≠ 新鮮，風險自負（計畫輸出有固定警語）。
- **side-effect node（outputs=None）不重跑**：位於起點前的守門 node
  （如 dataset 的 `validate_data_consistency` B1/B5 資料閘）在接續時跳過、
  不重新驗證，計畫輸出會列出。資料有變請跑 full run。
- manifest 照常寫，metadata 多 `resumed_from` / `only_node` 留痕。

## 開發守則（改 pipeline 結構的人必讀）

接續點品質是會被新增 node 默默破壞的契約：

1. node 輸出要不要進 catalog 落地，判準＝「是不是某個宣告接續點的必要輸入」×
   「重算貴不貴」。便宜的（view、handle、cheap transform）留 memory-only，
   讓擴張補跑；貴的（HPO 輸出）落地。
2. `tests/test_pipelines/test_resume_contracts.py` 的 `RESUME_CONTRACTS`
   釘住各 pipeline 承諾的接續點與允許補跑集合。改壞會紅燈——
   要嘛給新產物補 catalog 條目，要嘛修改契約並在 PR 說明為什麼接受變貴。
3. 改完跑 `--list-nodes` 肉眼確認各 node 的接續成本。

## 已知設計決議

- `hpo_best_model` 落地在 `data/models/${model_version}/hpo/model.txt`
  ——`ModelAdapterDataset` 的 `model_meta.json` sidecar 寫在 filepath 同目錄，
  與 `model.txt` 同目錄會互踩（calibration meta 串台）。
- `hpo_best_model` 不做 None 防護：HPO 第一個 trial 必然寫入 best model
  （score ≥ 0 > 初始 -1.0）；`n_trials=0` 在 `study.best_params` 就先炸。
- `tune_hyperparameters` 會被跳過的前提是三個輸出（`best_params` /
  `best_iteration` / `hpo_best_model`）都已落地——缺一個就會整顆重跑 HPO。
```

- [ ] **Step 2: Add CLAUDE.md pointer**

`CLAUDE.md` 的「本機 Spark 測試」一節 bullet list 加一行（在「端到端 smoke」之前）：

```markdown
- 部分執行：`--from-node`/`--only-node`/`--dry-run`/`--list-nodes`（四個 pipeline 指令皆有；
  缺料自動補跑上游）。細節與開發守則見 `docs/operations/pipeline-slicing.md`。
```

- [ ] **Step 3: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add docs/operations/pipeline-slicing.md CLAUDE.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "docs: pipeline 切片使用說明＋開發守則；CLAUDE.md 指回"
```

---

### Task 7: 本機 Spark 整合 smoke

**前置**：worktree `data/` 必須是自己的真目錄（CLAUDE.md R3）。Spark 指令可能 >2 分鐘，**用 background 執行**。

- [ ] **Step 1: Pre-flight + 本機資料重建**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing && pwd
readlink .venv && /Users/curtislu/projects/recsys_tfb/.venv/bin/python -V
export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/local_spark_setup.py --check-isolation
```
Expected: pre-flight 全過、`--check-isolation` 通過。

- [ ] **Step 2: dataset full run（background）→ 從 fit_preprocessor_metadata 接續**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing && export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local
# 完成後：
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local --list-nodes
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local --from-node fit_preprocessor_metadata --dry-run
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb dataset --env local --from-node fit_preprocessor_metadata
```
Expected：
- `--list-nodes`：每個 node 一行，`fit_preprocessor_metadata` 的 auto-included 為 `-`。
- `--dry-run`：`[plan]` 顯示 skipped 含 select_sample_keys/split_train_keys/select_val_keys/select_test_keys、skipped side-effect 含 `validate_data_consistency`，結尾 `dry-run: nothing executed`。
- 接續 run：成功完成，且**不出現** `select_sample_keys` 的 node_started log；manifest（`data/dataset/<base_v>/manifest.json`）含 `"resumed_from": "fit_preprocessor_metadata"`。

- [ ] **Step 3: training full run（background）→ 從 finalize_model 接續（驗證跳過 HPO）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing && export SPARK_CONF_DIR=$PWD/conf/spark-local
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local
# 完成後確認新落地物存在：
ls data/models/*/best_iteration.json data/models/*/hpo/model.txt
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m recsys_tfb training --env local --from-node finalize_model
```
Expected：
- full run 產出 `best_iteration.json` 與 `hpo/model.txt`（+ `hpo/model_meta.json`）。
- 接續 run 的 `[plan]`：`tune_hyperparameters` 在 skipped、auto-included 恰為 select_features + cache_train/cache_train_dev/cache_test 三個 cache node；**log 不出現 optuna trial**；總時間遠小於 full run。

- [ ] **Step 4: 回歸（針對性，不跑全量）**

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_core /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_cli.py /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_io /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing/tests/test_pipelines -q
```
Expected: 全 PASS（既知例外：`TestPrepareTrainInputsWeight` 兩測試在 main 本來就 failing——若只有它們紅，與本功能無關，照實記錄不修）。

- [ ] **Step 5: Commit（若 smoke 過程有任何修正）**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing add -A
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/pipeline-node-slicing commit -m "fix: 本機 Spark 整合 smoke 修正"
```

---

## 執行偏差紀錄

- **Task 3 Step 1 測試片段修正**（實作時發現 plan 自身的 bug）：`test_format_slice_plan_contents` 的 `can_load` 應為 `lambda n: n == "a"`（原 `n != "a"` 不會觸發擴張），斷言應為 `"<- b"`（原 `"<- a"`）。已依修正版實作（commit `28528da`）。
- **Task 4 追加**（Task 3 quality review 建議）：`--list-nodes` 呼叫端以 dict/lru_cache memo 包 `catalog.exists`（每 node 重複試算造成 ~6x 重複 exists 呼叫，Hive 後端是 metastore round-trip）；`test_format_node_list_one_line_per_node` 收緊為 `len(lines) == 4` 並斷言 `"(+ -)"`。
- **Task 5 追加**（Task 1/2 quality review 建議）：node 名稱唯一性 lint（`_node_index` 首匹配，重名會切錯）；sidecar 隔離測試補 calibrated 串台情境（top-level 存 CalibratedModelAdapter 後斷言 hpo load 不被誤包）。
- **Task 6 追加**（Task 2 quality review 建議）：文件註明 manifest `artifacts` 不列 `hpo/` 子目錄檔案；catalog `hpo_best_model` 註解補「為 resume 跳過 HPO 而落地」半行。

## Task 7 執行結果（2026-06-11）

- **PySpark 3.3.2 `tableExists("db.t")` 恆 False quirk**：本分支原 `HiveTableDataset.exists()` 踩中（會讓 dataset/inference resume 退化成補跑全部 Hive 上游）。main 的 PR#74 已以 SHOW TABLES 版 `_table_exists` 修復 → 已 merge origin/main（`03dae66`，乾淨無衝突），合併後驗證 `exists()` 正確。
- **dataset smoke**：full run（base `8301a89c`）→ `--list-nodes`（含 `build_test_model_input` 起跑會自動補 `build_val_model_input` 的正確擴張展示）→ `--dry-run` → `--from-node fit_preprocessor_metadata` 接續成功：11/15 node、keys 三表正確跳過、B1 列 skipped side-effect、manifest `resumed_from` 留痕。
- **training smoke**：full run（model `6059dcef`）產出 `best_iteration.json`(161) 與 `hpo/model.txt`＋獨立 sidecar（calibration 開啟下 top-level `calibrated: true`、hpo `false`——隔離設計實證必要）→ `--from-node finalize_model`：`tune_hyperparameters` 跳過、log 零 optuna trial、auto-included 恰為 calibration 契約 5 node、12/17、42s 完成、manifest 留痕。
- **回歸**：針對性套件（core/cli/io/pipelines）843 passed；2 failed（`TestSchemaEvolutionIntegration::test_add_then_drop_column_across_versions`、`test_persist_and_catalog_load_roundtrip`）經查為 **main 既有**的組合執行干擾（同選擇集在 main ac7ae14 重現同樣 2 fail；單獨跑皆過），非本分支造成，不在此修。

## Self-Review 紀錄

- **Spec coverage**：§2 語意/演算法/判準→Task 1；§3 元件→Task 1+3+4；§3.2 計畫輸出→Task 3+4；§4 補落地（含 hpo/ sidecar、None 決議）→Task 2；§5 邊界（未知名/互斥/守門/handle/manifest/警語）→Task 1+3+4 測試；§6 三道防線→Task 4（list-nodes）+5（契約）+6（docs）；§7 測試策略 1-5→Task 1/5/3-4/2/7；§8 不做→無對應 task（正確）。
- **型別一致性**：`SlicePlan` 欄位（mode/requested/auto_included/skipped/skipped_side_effect）在 Task 1 定義、Task 3/5 使用一致；`slice_from`/`slice_only` 回傳 `(Pipeline, SlicePlan)` 各處一致；`_execute_pipeline` 回傳 bool、四指令以 `executed` 接。
- **Placeholder**：無 TBD；Task 2 adapter API 不確定處已給明確 fallback 指示（以 tests/test_models 為準、不改 production code）。
