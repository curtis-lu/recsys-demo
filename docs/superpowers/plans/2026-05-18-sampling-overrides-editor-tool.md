# Sampling Overrides Editor Tool — Implementation Plan (Plan B)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone Typer dev script `scripts/sampling_overrides_editor.py` that profiles `sample_pool`, computes data-driven suggestions for downsample ratios and cold-product weights, emits a self-contained HTML matrix editor, and converts the edited JSON export into sparse YAML snippets (validated by A5/A9) for manual paste into config.

**Architecture:** Two Typer subcommands. `profile` runs a Spark `groupBy(cust_segment_typ, prod_name, label)`, computes per-(segment,product) `n_pos/n_neg`, applies the D8 suggestion formulas, and writes a self-contained HTML (pure stdlib templating + embedded vanilla JS, no extra packages) to `data/profiling/`. `to-yaml` reads the browser's JSON export, runs the A5/A9 consistency predicates, and prints two sparse YAML blocks. Not part of the production DAG. Source of truth: `docs/superpowers/specs/2026-05-18-sampling-overrides-editor-design.md` (D1–D4, D8). **Depends on Plan A having defined the `training.sample_weights` config schema (key format `"<cust_segment_typ>|<prod_name>"`).**

**Tech Stack:** Python 3.10, Typer 0.20.1, PySpark 3.3.2, numpy, PyYAML, pytest 7.3.1. No additional packages.

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor` (branch `feat/sampling-overrides-editor`).

**Conventions (same as Plan A):**
- `PYTEST <paths>` = `PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q`
- `GIT ...` = `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor ...`
- Mirror `scripts/suggest_categorical_cols.py` for Typer/Spark-loading style and the `data/profiling/` output convention.

---

## Module layout (decomposition)

> **POST-IMPLEMENTATION ERRATUM (review correction).** The original two-file
> split below (`scripts/` thin CLI + `src/recsys_tfb/tooling/sampling_suggest.py`
> pure lib) was **reverted during review**. Its stated rationale — "pure logic
> must be unit-testable without Spark/CLI, so it must live in `src/`" — was
> based on a false premise: the repo already unit-tests `scripts/` modules
> cleanly via `from scripts.X import ...` in `tests/scripts/` (see
> `tests/scripts/test_promote_model.py`, `test_suggest_categorical_cols.py`),
> with no importlib hack. The convention-consistent design (and the one
> shipped) is a **single self-contained `scripts/sampling_overrides_editor.py`**
> (logic + Typer CLI in one file, ~330 lines), mirroring `promote_model.py` /
> `suggest_categorical_cols.py`, tested by
> `tests/scripts/test_sampling_overrides_editor.py`. This also keeps dev-only
> tooling out of the `pip install`-shipped `recsys_tfb` production package.
> Tasks 1–6 below still describe the intermediate `src/recsys_tfb/tooling/`
> form for provenance; the final commit consolidated them. `grid_to_yaml`
> still imports the single-source A5/A9 predicates from
> `recsys_tfb.core.consistency` (that import works fine from a `scripts/` file).

- `scripts/sampling_overrides_editor.py` — Typer app (`profile`, `to-yaml`) + Spark loader + the pure suggestion / sparsify / HTML-render / JSON→YAML logic, all in one self-contained file. Not part of the production DAG.
- `tests/scripts/test_sampling_overrides_editor.py` — unit tests via `from scripts.sampling_overrides_editor import ...` (Spark test marked `@pytest.mark.spark`), matching the existing `tests/scripts/` convention.

---

## Task 1: Pure suggestion formulas (`suggest_ratios`, `suggest_weights`)

**Files:**
- Create: `src/recsys_tfb/tooling/__init__.py` (empty)
- Create: `src/recsys_tfb/tooling/sampling_suggest.py`
- Test: `tests/test_tooling/test_sampling_suggest.py` (create; `tests/test_tooling/__init__.py` if the suite needs it — check sibling test dirs for `__init__.py` presence and match)

- [ ] **Step 1: Write the failing test**

Create `tests/test_tooling/test_sampling_suggest.py`:

```python
import math
import pytest
from recsys_tfb.tooling.sampling_suggest import suggest_ratio, suggest_weight


class TestSuggestRatio:
    def test_downsamples_negatives_to_target_ratio(self):
        # n_pos=10, n_neg=100, R=5 -> 5*10/100 = 0.5
        assert suggest_ratio(n_pos=10, n_neg=100, target_neg_pos=5) == 0.5

    def test_already_balanced_clamps_to_one(self):
        # 5*50/100 = 2.5 -> clamp 1.0
        assert suggest_ratio(n_pos=50, n_neg=100, target_neg_pos=5) == 1.0

    def test_zero_negatives_returns_one(self):
        assert suggest_ratio(n_pos=10, n_neg=0, target_neg_pos=5) == 1.0


class TestSuggestWeight:
    def test_inverse_frequency_with_sqrt_damping(self):
        # median=800, n_pos=200 -> (800/200)**0.5 = 2.0
        assert suggest_weight(n_pos=200, median_pos=800, alpha=0.5, w_max=5.0) == 2.0

    def test_hot_product_clamped_to_one(self):
        # n_pos >= median -> ratio<=1 -> clamp lower bound 1.0
        assert suggest_weight(n_pos=8000, median_pos=800, alpha=0.5, w_max=5.0) == 1.0

    def test_extreme_tail_capped_at_w_max(self):
        # (800/8)**0.5 = 10 -> cap 5.0
        assert suggest_weight(n_pos=8, median_pos=800, alpha=0.5, w_max=5.0) == 5.0

    def test_zero_pos_capped_at_w_max(self):
        assert suggest_weight(n_pos=0, median_pos=800, alpha=0.5, w_max=5.0) == 5.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'recsys_tfb.tooling'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/recsys_tfb/tooling/__init__.py` (empty file).

Create `src/recsys_tfb/tooling/sampling_suggest.py`:

```python
"""Pure data-driven suggestion logic for the sampling overrides editor.

No Spark, no Typer, no I/O — unit-testable in isolation. Implements the D8
formulas from the design spec.
"""

from __future__ import annotations


def suggest_ratio(n_pos: int, n_neg: int, target_neg_pos: float) -> float:
    """Downsample ratio for negatives: keep all positives, target neg:pos = R.

    neg_ratio = clamp(R * n_pos / n_neg, 0, 1). n_neg == 0 -> 1.0 (nothing to
    downsample).
    """
    if n_neg <= 0:
        return 1.0
    return min(1.0, max(0.0, target_neg_pos * n_pos / n_neg))


def suggest_weight(
    n_pos: int, median_pos: float, alpha: float, w_max: float
) -> float:
    """Cold-product boost weight: clamp((median_pos/n_pos)**alpha, 1.0, w_max).

    n_pos <= 0 -> treated as maximally cold (returns w_max).
    """
    if n_pos <= 0:
        return w_max
    raw = (median_pos / n_pos) ** alpha
    return min(w_max, max(1.0, raw))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py -q`
Expected: PASS (7 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/tooling/__init__.py src/recsys_tfb/tooling/sampling_suggest.py tests/test_tooling/test_sampling_suggest.py
GIT commit -m "feat(tooling): pure suggest_ratio / suggest_weight (D8 formulas)"
```

---

## Task 2: Build the suggestion grid from group stats (`build_grid`)

**Files:**
- Modify: `src/recsys_tfb/tooling/sampling_suggest.py` (add `build_grid`)
- Test: `tests/test_tooling/test_sampling_suggest.py` (add `TestBuildGrid`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tooling/test_sampling_suggest.py`:

```python
from recsys_tfb.tooling.sampling_suggest import build_grid


class TestBuildGrid:
    def test_grid_has_stats_and_suggestions_per_cell(self):
        # stats: list of (segment, product, n_pos, n_neg)
        stats = [
            ("mass", "a", 200, 4000),
            ("mass", "b", 800, 1600),
            ("hnw", "a", 8, 50),
        ]
        grid = build_grid(stats, target_neg_pos=5, alpha=0.5, w_max=5.0)
        by = {(r["segment"], r["product"]): r for r in grid}
        # median_pos over cells [200, 800, 8] = 200
        assert by[("mass", "a")]["n_pos"] == 200
        assert by[("mass", "a")]["suggested_weight"] == 1.0  # n_pos == median
        # hnw|a: (200/8)**0.5 = 5.0 -> cap
        assert by[("hnw", "a")]["suggested_weight"] == 5.0
        # mass|a downsample: 5*200/4000 = 0.25
        assert by[("mass", "a")]["suggested_ratio"] == 0.25
        # every row carries pos_rate
        assert abs(by[("hnw", "a")]["pos_rate"] - 8 / 58) < 1e-9
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestBuildGrid -q`
Expected: FAIL — `ImportError: cannot import name 'build_grid'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/tooling/sampling_suggest.py`:

```python
import statistics


def build_grid(
    stats: list[tuple[str, str, int, int]],
    target_neg_pos: float,
    alpha: float,
    w_max: float,
) -> list[dict]:
    """Turn per-(segment,product) (n_pos, n_neg) stats into editor grid rows.

    ``median_pos`` is the per-cell median of n_pos across the whole grid
    (D8). Each row carries the raw stats plus suggested_ratio /
    suggested_weight starting values.
    """
    pos_counts = [np for (_, _, np, _) in stats]
    median_pos = float(statistics.median(pos_counts)) if pos_counts else 1.0
    grid: list[dict] = []
    for seg, prod, n_pos, n_neg in stats:
        total = n_pos + n_neg
        grid.append({
            "segment": seg,
            "product": prod,
            "n_pos": n_pos,
            "n_neg": n_neg,
            "pos_rate": (n_pos / total) if total else 0.0,
            "suggested_ratio": suggest_ratio(n_pos, n_neg, target_neg_pos),
            "suggested_weight": suggest_weight(n_pos, median_pos, alpha, w_max),
        })
    return grid
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestBuildGrid -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/tooling/sampling_suggest.py tests/test_tooling/test_sampling_suggest.py
GIT commit -m "feat(tooling): build_grid (per-cell median, suggestions)"
```

---

## Task 3: Sparse JSON→YAML conversion with A5/A9 validation (`grid_to_yaml`)

**Files:**
- Modify: `src/recsys_tfb/tooling/sampling_suggest.py` (add `grid_to_yaml`)
- Test: `tests/test_tooling/test_sampling_suggest.py` (add `TestGridToYaml`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tooling/test_sampling_suggest.py`:

```python
import yaml
from recsys_tfb.tooling.sampling_suggest import grid_to_yaml


def _params():
    return {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["a", "b"]}},
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]},
                    "sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
    }


class TestGridToYaml:
    def test_sparse_emits_only_non_default(self):
        # default ratio 1.0, default weight 1.0 -> only deviating cells emitted
        export = [
            {"segment": "mass", "product": "a", "ratio": 0.5, "weight": 1.0},
            {"segment": "mass", "product": "b", "ratio": 1.0, "weight": 3.0},
            {"segment": "hnw", "product": "a", "ratio": 1.0, "weight": 1.0},
        ]
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.5}}
        assert sw == {"sample_weights": {"mass|b": 3.0}}

    def test_unknown_product_raises_with_collected_message(self):
        export = [{"segment": "mass", "product": "zzz", "ratio": 0.5, "weight": 2.0}]
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestGridToYaml -q`
Expected: FAIL — `ImportError: cannot import name 'grid_to_yaml'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/tooling/sampling_suggest.py`:

```python
import yaml

from recsys_tfb.core.consistency import (
    override_unknown_items,
    weight_unknown_items,
)

# Override key is '|'-joined sample_group_keys; cold-product downsample
# targets negatives, so the label component is fixed to "0".
_NEG_LABEL = "0"


def grid_to_yaml(
    export: list[dict],
    parameters: dict,
    default_ratio: float,
    default_weight: float = 1.0,
) -> dict:
    """Convert the browser JSON export into two sparse YAML blocks.

    Emits only cells deviating from defaults. Validates the resulting keys
    via the A5 / A9 consistency predicates (collect-all) BEFORE returning, so
    a bad product value is caught before the user pastes into config.
    """
    overrides: dict[str, float] = {}
    weights: dict[str, float] = {}
    for row in export:
        seg, prod = row["segment"], row["product"]
        ratio = float(row.get("ratio", default_ratio))
        weight = float(row.get("weight", default_weight))
        if ratio != default_ratio:
            overrides[f"{seg}|{prod}|{_NEG_LABEL}"] = ratio
        if weight != default_weight:
            weights[f"{seg}|{prod}"] = weight

    # Reuse the single-source consistency predicates (A5 + A9).
    probe = {**parameters}
    probe.setdefault("dataset", {})
    probe["dataset"] = {**probe["dataset"], "sample_ratio_overrides": overrides}
    probe["training"] = {**probe.get("training", {}), "sample_weights": weights}
    bad = sorted(set(override_unknown_items(probe)) | set(weight_unknown_items(probe)))
    if bad:
        raise ValueError(
            f"editor export references unknown product value(s) {bad} "
            f"absent from schema.categorical_values[item]; fix before paste."
        )

    return {
        "sample_ratio_overrides_yaml": yaml.safe_dump(
            {"sample_ratio_overrides": overrides}, sort_keys=True,
            allow_unicode=True, default_flow_style=False),
        "sample_weights_yaml": yaml.safe_dump(
            {"sample_weights": weights}, sort_keys=True,
            allow_unicode=True, default_flow_style=False),
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestGridToYaml -q`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/tooling/sampling_suggest.py tests/test_tooling/test_sampling_suggest.py
GIT commit -m "feat(tooling): grid_to_yaml sparse emit + A5/A9 validation"
```

---

## Task 4: Self-contained HTML editor renderer (`render_html`)

**Files:**
- Modify: `src/recsys_tfb/tooling/sampling_suggest.py` (add `render_html`)
- Test: `tests/test_tooling/test_sampling_suggest.py` (add `TestRenderHtml`)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_tooling/test_sampling_suggest.py`:

```python
import json
from recsys_tfb.tooling.sampling_suggest import render_html


class TestRenderHtml:
    def test_html_is_self_contained_and_embeds_grid(self):
        grid = [{"segment": "mass", "product": "a", "n_pos": 200,
                 "n_neg": 4000, "pos_rate": 0.047,
                 "suggested_ratio": 0.25, "suggested_weight": 1.0}]
        html = render_html(grid, default_ratio=1.0)
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert "mass" in html and "0.25" in html
        # the grid is embedded as JSON for the export button
        assert json.dumps(grid) in html
        # no external resource references (self-contained)
        assert "http://" not in html and "https://" not in html
        assert "Export JSON" in html and "Export YAML" in html
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestRenderHtml -q`
Expected: FAIL — `ImportError: cannot import name 'render_html'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/tooling/sampling_suggest.py`:

```python
import json as _json

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Sampling Overrides Editor</title>
<style>
 body{{font-family:system-ui,monospace;margin:1.5rem}}
 table{{border-collapse:collapse}} td,th{{border:1px solid #ccc;padding:4px 8px}}
 th{{background:#f2f2f2}} td.edit{{background:#fffbe6}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<p>default ratio = <b>{default_ratio}</b>. 編輯 ratio / weight 欄；
只匯出 ≠ default 的 cell。</p>
<table id="g"><thead><tr>
<th>segment</th><th>product</th><th class="stat">n_pos</th>
<th class="stat">n_neg</th><th class="stat">pos_rate</th>
<th>ratio</th><th>weight</th></tr></thead><tbody></tbody></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const GRID={grid_json};
const DR={default_ratio};
const tb=document.querySelector('#g tbody');
GRID.forEach((r,i)=>{{
 const tr=document.createElement('tr');
 tr.innerHTML=`<td>${{r.segment}}</td><td>${{r.product}}</td>`+
  `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
  `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
  `<td class=edit contenteditable data-k=ratio data-i=${{i}}>`+
  `${{r.suggested_ratio}}</td>`+
  `<td class=edit contenteditable data-k=weight data-i=${{i}}>`+
  `${{r.suggested_weight}}</td>`;
 tb.appendChild(tr);
}});
function collect(){{
 const o=GRID.map(r=>({{segment:r.segment,product:r.product,
  ratio:r.suggested_ratio,weight:r.suggested_weight}}));
 document.querySelectorAll('td.edit').forEach(td=>{{
  o[+td.dataset.i][td.dataset.k]=parseFloat(td.textContent);}});
 return o;
}}
function exp(kind){{
 const o=collect();
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a');a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json';a.click();
 }}else{{
  const ov={{}},sw={{}};
  o.forEach(r=>{{ if(r.ratio!==DR) ov[r.segment+'|'+r.product+'|0']=r.ratio;
   if(r.weight!==1.0) sw[r.segment+'|'+r.product]=r.weight; }});
  document.getElementById('out').textContent=
   '# -> parameters_dataset.yaml (dataset.sample_ratio_overrides)\\n'+
   'sample_ratio_overrides:\\n'+
   Object.entries(ov).map(([k,v])=>'  "'+k+'": '+v).join('\\n')+
   '\\n\\n# -> parameters_training.yaml (training.sample_weights)\\n'+
   'sample_weights:\\n'+
   Object.entries(sw).map(([k,v])=>'  "'+k+'": '+v).join('\\n');
 }}
}}
</script></body></html>"""


def render_html(grid: list[dict], default_ratio: float) -> str:
    """Render a self-contained HTML editor (pure stdlib, no external assets)."""
    return _HTML_TEMPLATE.format(
        default_ratio=default_ratio,
        grid_json=_json.dumps(grid),
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_suggest.py::TestRenderHtml -q`
Expected: PASS (1 passed). (Note: `json.dumps(grid)` in the test matches `_json.dumps(grid)` in impl — identical separators.)

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/tooling/sampling_suggest.py tests/test_tooling/test_sampling_suggest.py
GIT commit -m "feat(tooling): self-contained HTML editor renderer"
```

---

## Task 5: Spark profiling aggregation (`profile_stats`)

**Files:**
- Modify: `src/recsys_tfb/tooling/sampling_suggest.py` (add `profile_stats` — takes a Spark DataFrame, returns the pure stats list)
- Test: `tests/test_tooling/test_sampling_suggest_spark.py` (create; function-scoped `spark` fixture)

- [ ] **Step 1: Write the failing test**

Create `tests/test_tooling/test_sampling_suggest_spark.py`:

```python
import pandas as pd
from recsys_tfb.tooling.sampling_suggest import profile_stats


class TestProfileStats:
    def test_groups_by_segment_product_and_counts_pos_neg(self, spark):
        df = spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a", "a", "a", "b", "b", "b"],
            "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
            "label": [1, 0, 0, 1, 1, 0],
        }))
        stats = profile_stats(
            df, [pd.Timestamp("2025-01-31")],
            segment_col="cust_segment_typ", item_col="prod_name",
            label_col="label", time_col="snap_date")
        d = {(s, p): (np_, nn_) for (s, p, np_, nn_) in stats}
        assert d[("mass", "a")] == (1, 2)
        assert d[("hnw", "b")] == (2, 1)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_suggest_spark.py -q`
Expected: FAIL — `ImportError: cannot import name 'profile_stats'`.

- [ ] **Step 3: Write minimal implementation**

Append to `src/recsys_tfb/tooling/sampling_suggest.py`:

```python
def profile_stats(
    df,
    snap_dates: list,
    *,
    segment_col: str,
    item_col: str,
    label_col: str,
    time_col: str,
) -> list[tuple[str, str, int, int]]:
    """Spark groupBy -> per-(segment,product) (n_pos, n_neg) over snap_dates.

    Single Spark action (one .collect of a tiny grouped frame). No UDF.
    """
    from pyspark.sql import functions as F

    rows = (
        df.filter(F.col(time_col).isin(list(snap_dates)))
        .groupBy(segment_col, item_col)
        .agg(
            F.sum(F.col(label_col)).alias("n_pos"),
            F.sum(F.lit(1) - F.col(label_col)).alias("n_neg"),
        )
        .collect()
    )
    return [
        (r[segment_col], r[item_col], int(r["n_pos"]), int(r["n_neg"]))
        for r in rows
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_suggest_spark.py -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
GIT add src/recsys_tfb/tooling/sampling_suggest.py tests/test_tooling/test_sampling_suggest_spark.py
GIT commit -m "feat(tooling): Spark profile_stats aggregation"
```

---

## Task 6: Typer CLI script wiring (`profile` / `to-yaml`)

**Files:**
- Create: `scripts/sampling_overrides_editor.py`
- Test: `tests/test_tooling/test_sampling_editor_cli.py` (create; uses `typer.testing.CliRunner`, no Spark — exercises `to-yaml` end-to-end on a JSON fixture; `profile`'s Spark path is covered by Task 5's `profile_stats` test)

- [ ] **Step 1: Write the failing test**

Create `tests/test_tooling/test_sampling_editor_cli.py`:

```python
import json
import importlib.util
from pathlib import Path
from typer.testing import CliRunner

SPEC = Path("/Users/curtislu/projects/recsys_tfb/.worktrees/"
            "sampling-overrides-editor/scripts/sampling_overrides_editor.py")


def _load_app():
    spec = importlib.util.spec_from_file_location("soe", SPEC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.app


class TestToYamlCli:
    def test_to_yaml_prints_both_blocks(self, tmp_path):
        export = [{"segment": "mass", "product": "a", "ratio": 0.5, "weight": 1.0},
                  {"segment": "mass", "product": "b", "ratio": 1.0, "weight": 3.0}]
        jf = tmp_path / "e.json"
        jf.write_text(json.dumps(export))
        # minimal params yaml the command reads for A5/A9
        params = tmp_path / "p.yaml"
        params.write_text(
            "schema:\n  columns:\n    item: prod_name\n"
            "  categorical_values:\n    prod_name: [a, b]\n"
            "dataset:\n  prepare_model_input:\n"
            "    categorical_columns: [prod_name]\n"
            "  sample_group_keys: [cust_segment_typ, prod_name, label]\n")
        r = CliRunner().invoke(
            _load_app(), ["to-yaml", str(jf), "--params", str(params)])
        assert r.exit_code == 0, r.output
        assert "sample_ratio_overrides:" in r.output
        assert "mass|a|0" in r.output
        assert "sample_weights:" in r.output
        assert "mass|b" in r.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTEST tests/test_tooling/test_sampling_editor_cli.py -q`
Expected: FAIL — file `scripts/sampling_overrides_editor.py` does not exist.

- [ ] **Step 3: Write minimal implementation**

Create `scripts/sampling_overrides_editor.py`:

```python
"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool, write a self-contained HTML
                     editor to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5/A9-validated) for manual paste into config.

Mirrors scripts/suggest_categorical_cols.py conventions. Not part of the
production DAG.

Usage:
  python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
  python scripts/sampling_overrides_editor.py profile data/sample_pool.parquet
  python scripts/sampling_overrides_editor.py to-yaml \\
      data/profiling/sampling_overrides_export.json
"""

from __future__ import annotations

import json
from pathlib import Path

import typer
import yaml

from recsys_tfb.tooling.sampling_suggest import (
    build_grid,
    grid_to_yaml,
    profile_stats,
    render_html,
)

PROFILING_DIR = Path("data/profiling")

app = typer.Typer(
    help="Sampling overrides editor for parameters_dataset/training.yaml",
    add_completion=False,
)


def _load_spark_df(source: str):
    """Load a Hive table (db.table) or a parquet path into a Spark DataFrame.

    Same dual-input convention as scripts/suggest_categorical_cols.py.
    """
    from recsys_tfb.utils.spark import get_or_create_spark_session

    spark = get_or_create_spark_session({"app_name": "sampling-overrides-editor"})
    if "/" in source or source.endswith(".parquet"):
        return spark.read.parquet(source)
    return spark.table(source)


@app.command()
def profile(
    source: str = typer.Argument(..., help="Hive table db.table or parquet path"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="dataset params yaml"),
    target_neg_pos: float = typer.Option(5.0, help="downsample target neg:pos R"),
    alpha: float = typer.Option(0.5, help="cold-weight damping exponent"),
    w_max: float = typer.Option(5.0, help="cold-weight cap"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds = cfg.get("dataset", cfg)
    snap_dates = ds["train_snap_dates"]
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    df = _load_spark_df(source)
    stats = profile_stats(
        df, snaps,
        segment_col="cust_segment_typ", item_col="prod_name",
        label_col="label", time_col="snap_date",
    )
    grid = build_grid(stats, target_neg_pos, alpha, w_max)
    html = render_html(grid, default_ratio=float(ds.get("sample_ratio", 1.0)))
    PROFILING_DIR.mkdir(parents=True, exist_ok=True)
    out = PROFILING_DIR / "sampling_overrides_editor.html"
    out.write_text(html)
    typer.echo(f"Wrote {out} ({len(grid)} cells). Open it in a browser.")


@app.command("to-yaml")
def to_yaml(
    export_json: Path = typer.Argument(..., help="browser JSON export"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="params yaml for A5/A9"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    export = json.loads(export_json.read_text())
    default_ratio = float(cfg.get("dataset", cfg).get("sample_ratio", 1.0))
    try:
        out = grid_to_yaml(export, cfg, default_ratio)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo("# --> conf/base/parameters_dataset.yaml (under dataset:)")
    typer.echo(out["sample_ratio_overrides_yaml"])
    typer.echo("# --> conf/base/parameters_training.yaml (under training:)")
    typer.echo(out["sample_weights_yaml"])


if __name__ == "__main__":
    app()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTEST tests/test_tooling/test_sampling_editor_cli.py -q`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
GIT add scripts/sampling_overrides_editor.py tests/test_tooling/test_sampling_editor_cli.py
GIT commit -m "feat(tooling): sampling_overrides_editor Typer CLI (profile/to-yaml)"
```

---

## Task 7: Full tool sweep + graph refresh

**Files:** none (verification + graph maintenance)

- [ ] **Step 1: Run the whole tooling test suite**

Run: `PYTEST tests/test_tooling -q`
Expected: PASS (all green).

- [ ] **Step 2: Smoke the CLI help (no Spark, proves Typer wiring)**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor/src \
 /Users/curtislu/projects/recsys_tfb/.venv/bin/python \
 scripts/sampling_overrides_editor.py --help
```
Expected: shows `profile` and `to-yaml` subcommands, exit 0.

- [ ] **Step 3: Refresh graphify graph (CLAUDE.md mandate after code changes)**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-overrides-editor && \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
"from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
```
Expected: rebuild completes; `graphify-out/GRAPH_REPORT.md` untracked (do not stage).

- [ ] **Step 4: Final commit (if fixups were made)**

```bash
GIT add -A -- src tests scripts
GIT commit -m "test(tooling): editor sweep fixups" || echo "nothing to commit"
```

---

## Self-review notes (author)

- **Spec coverage:** D1 (YAML snippet manual paste) → Tasks 3,6 (`to-yaml` prints, never writes config). D2 (self-contained HTML, sparse export) → Task 4 (no external assets; export filters `!= default`). D3 (data-driven, item axis from schema) → Tasks 2,5 + A5/A9 reuse in Task 3. D4 (train-only, sparse) → Task 3 (`_NEG_LABEL="0"`, sparse emit). D8 formulas → Tasks 1,2. A5/A9 reuse (single source) → Task 3 imports the consistency predicates rather than re-implementing.
- **Placeholder scan:** none — full code in every step, exact commands + expected output.
- **Type/name consistency:** `build_grid`→rows with `segment/product/n_pos/n_neg/pos_rate/suggested_ratio/suggested_weight` consumed identically by `render_html` (Task 4) and the CLI (Task 6); `grid_to_yaml(export, parameters, default_ratio)` signature consistent Tasks 3,6; `profile_stats(df, snap_dates, *, segment_col, item_col, label_col, time_col)` consistent Tasks 5,6.
- **Dependency on Plan A:** `grid_to_yaml` imports `weight_unknown_items` (created in Plan A Task 1). Plan A MUST be merged/available before Plan B Task 3 runs. The `_load_spark_df` helper assumes `recsys_tfb.utils.spark.get_or_create_spark_session` (verify exact import path against `scripts/suggest_categorical_cols.py` when implementing Task 6; adjust to match that script's loader if it differs).
