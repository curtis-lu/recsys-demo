# Sampling Overrides Editor — Divergent group/weight Keys Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/sampling_overrides_editor.py` correctly read, edit, export, and validate sampling ratio (keyed by `dataset.sample_group_keys`) and training weight (keyed by `training.sample_weight_keys`) when the two key-sets are independently configured and differ.

**Architecture:** Profile `sample_pool` once at the finest granularity `union(group_keys ∪ weight_keys)\{label}`. A pure, unit-tested `aggregate_surfaces` rolls that up into two surfaces: a **ratio surface** at `(segment,item)` granularity and a **weight surface** at `sample_weight_keys` granularity, where the weight surface's `n_neg`/`pos_rate` reflect post-downsampling counts via per-cell ratio projection. The browser HTML mirrors `aggregate_surfaces` in JS for a live two-tab editor; `grid_to_yaml` emits `seg|prod|0` ratio keys (A5-validated) and `"|".join(weight_keys-values)` weight keys (A9b/A9c-validated against the **real** `sample_weight_keys`).

**Tech Stack:** Python 3.10, Typer, PyYAML, PySpark 3.3.2 (profiling only), pytest. Single-file dev tool + `tests/scripts/` (project convention — do NOT create a `src/` package).

**Baseline:** branch `feat/sample-weight-editor-keys`, commit `72f0a86` (WIP rework carried in: neg_mult primary knob + 3-mode HTML). The 3-mode radio UI and several of its tests are **replaced** by this plan.

**Spec:** `docs/superpowers/specs/2026-05-31-editor-divergent-sampling-weight-keys-design.md`.

**Run tests with (worktree-absolute venv + PYTHONPATH, per CLAUDE.md):**
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sample-weight-editor-keys
PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py -q
```
(Bare `pytest` or relative paths read main's `src` and can ELOOP — always use the absolute venv python + `PYTHONPATH=src`.)

**Consistency predicates reused (single source of truth in `src/recsys_tfb/core/consistency.py`, do NOT duplicate):**
- `override_unknown_items(parameters)` (A5) — `dataset.sample_ratio_overrides` keys whose item component ∉ `resolved_item_values`. Item index taken from `dataset.sample_group_keys.index(item)`.
- `weight_key_arity_mismatch(parameters)` (A9b) — `training.sample_weights` keys whose `|`-segment count != `len(training.sample_weight_keys)`.
- `weight_unknown_items(parameters)` (A9c) — `training.sample_weights` keys whose item component ∉ `resolved_item_values`. Item index from `training.sample_weight_keys.index(item)`; short-circuits to `[]` if item ∉ `sample_weight_keys`.

---

## File Structure

- **Modify:** `scripts/sampling_overrides_editor.py` — all logic, CLI, HTML in one file (project convention `feedback_dev_tooling_in_scripts`).
  - **Remove:** `build_grid` (subsumed by `profile_stats`→`aggregate_surfaces`), `resolve_columns` (replaced by `resolve_keys`).
  - **Keep:** `suggest_ratio`, `suggest_weight` (pure formulas, reused + JS-mirrored), `_NEG_LABEL`.
  - **Add:** `resolve_keys`, `aggregate_surfaces`. **Rewrite:** `profile_stats` (union dims), `grid_to_yaml` (new export shape), `_HTML_TEMPLATE` + `render_html` (two tabs), `profile`/`to-yaml` CLI commands.
- **Modify:** `tests/scripts/test_sampling_overrides_editor.py` — replace obsolete 3-mode/`build_grid`/`resolve_columns` tests with the new ones below.

---

## Task 1: `resolve_keys` — resolve ratio dims, weight keys, union dims (replaces `resolve_columns`)

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (replace `resolve_columns`, lines 104-139)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (replace `class TestResolveColumns`, lines 85-114)

- [ ] **Step 1: Replace the `TestResolveColumns` test class with `TestResolveKeys`**

```python
class TestResolveKeys:
    _SCHEMA = {"columns": {"item": "prod_name", "label": "label",
                           "time": "snap_date"}}

    def test_case1_weight_subset_of_group_keys(self):
        # group=[seg,item,label], weight=[item] -> union dims = [seg,item]
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["prod_name"]}, self._SCHEMA)
        assert out["segment_col"] == "cust_segment_typ"
        assert out["item_col"] == "prod_name"
        assert out["label_col"] == "label"
        assert out["time_col"] == "snap_date"
        assert out["weight_keys"] == ["prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_case2_weight_adds_carry_dim_extends_union(self):
        # weight=[risk_attr,item] adds risk_attr to the union (label excluded)
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["risk_attr", "prod_name"]}, self._SCHEMA)
        assert out["weight_keys"] == ["risk_attr", "prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name", "risk_attr"]

    def test_empty_weight_keys_union_is_group_dims(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {}, self._SCHEMA)
        assert out["weight_keys"] == []
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_rejects_group_keys_not_a_segment_item_label_triple(self):
        with pytest.raises(ValueError, match="sample_group_keys"):
            resolve_keys({"sample_group_keys": ["prod_name", "label"]},
                         {"sample_weight_keys": ["prod_name"]}, self._SCHEMA)

    def test_rejects_label_in_weight_keys(self):
        with pytest.raises(ValueError, match="label"):
            resolve_keys(
                {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
                {"sample_weight_keys": ["prod_name", "label"]}, self._SCHEMA)

    def test_rejects_missing_schema_column(self):
        with pytest.raises(ValueError, match="schema.columns"):
            resolve_keys(
                {"sample_group_keys": ["seg", "prod_name", "label"]},
                {"sample_weight_keys": ["prod_name"]},
                {"columns": {"item": "prod_name", "label": "label"}})
```

- [ ] **Step 2: Update the import in the test file**

Change the import block (lines 10-19) to drop `build_grid`, `resolve_columns` and add `resolve_keys`, `aggregate_surfaces`:

```python
from scripts.sampling_overrides_editor import (
    aggregate_surfaces,
    app,
    grid_to_yaml,
    profile_stats,
    render_html,
    resolve_keys,
    suggest_ratio,
    suggest_weight,
)
```

- [ ] **Step 3: Run the new tests to verify they fail**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestResolveKeys -q`
Expected: FAIL with `ImportError: cannot import name 'resolve_keys'` (and `aggregate_surfaces`).

- [ ] **Step 4: Implement `resolve_keys` (replace `resolve_columns`)**

Replace the whole `resolve_columns` function with:

```python
def resolve_keys(dataset_cfg: dict, training_cfg: dict, schema_cfg: dict) -> dict:
    """Resolve ratio dims, weight keys, and the finest profiling granularity.

    ``item``/``label``/``time`` come from ``schema.columns``; ``segment`` is the
    single ``dataset.sample_group_keys`` entry that is neither item nor label
    (the ratio surface is segment x item, label fixed to 0 on export). The
    weight surface is keyed by ``training.sample_weight_keys`` (arbitrary
    available columns). ``union_dims`` is the finest granularity to profile at:
    ``(sample_group_keys ∪ sample_weight_keys) \\ {label}``, ratio dims first.

    Fails fast unless ``sample_group_keys`` is exactly one segment + item +
    label, and unless ``label`` is absent from ``sample_weight_keys`` (the
    editor's per-group n_pos/n_neg model splits on label via sum(label), so a
    label weight key is self-contradictory — hand-write those weights instead).
    """
    cols = schema_cfg.get("columns", {})
    try:
        item_col, label_col, time_col = cols["item"], cols["label"], cols["time"]
    except KeyError as exc:
        raise ValueError(
            f"schema.columns is missing {exc}; cannot resolve profiling "
            "columns. Check the base parameters yaml."
        ) from exc
    group_keys = list(dataset_cfg.get("sample_group_keys", []))
    segments = [k for k in group_keys if k not in (item_col, label_col)]
    if len(group_keys) != 3 or len(segments) != 1:
        raise ValueError(
            "sampling editor expects sample_group_keys = [segment, "
            f"{item_col!r}, {label_col!r}] (one segment + item + label); "
            f"got {group_keys}."
        )
    weight_keys = list(training_cfg.get("sample_weight_keys") or [])
    if label_col in weight_keys:
        raise ValueError(
            f"sampling editor cannot edit weights keyed by the label column "
            f"{label_col!r} (per-group n_pos/n_neg is derived by splitting on "
            f"label). Remove it from sample_weight_keys, or hand-write those "
            f"sample_weights."
        )
    # union dims: ratio dims (group_keys minus label, in order) then any extra
    # weight-key columns not already present; label never enters group-by.
    union_dims: list[str] = [k for k in group_keys if k != label_col]
    for k in weight_keys:
        if k != label_col and k not in union_dims:
            union_dims.append(k)
    return {
        "segment_col": segments[0],
        "item_col": item_col,
        "label_col": label_col,
        "time_col": time_col,
        "weight_keys": weight_keys,
        "union_dims": union_dims,
    }
```

Also remove the now-dead `build_grid` function (lines 71-101) — it is replaced by `profile_stats`+`aggregate_surfaces` and removed in Task 3.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestResolveKeys -q`
Expected: PASS (6 tests). (Other tests in the file are temporarily broken until later tasks — that is expected.)

- [ ] **Step 6: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): resolve_keys — ratio dims + weight keys + union dims"
```

---

## Task 2: `profile_stats` — group by union dims, return dict rows

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (rewrite `profile_stats`, lines 513-540)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (rewrite `class TestProfileStats`, lines 305-321)

- [ ] **Step 1: Rewrite the `TestProfileStats` class**

```python
@pytest.mark.spark
class TestProfileStats:
    def _df(self, spark):
        return spark.createDataFrame(pd.DataFrame({
            "snap_date": pd.to_datetime(["2025-01-31"] * 6),
            "cust_id": [1, 2, 3, 4, 5, 6],
            "prod_name": ["a", "a", "a", "b", "b", "b"],
            "cust_segment_typ": ["mass", "mass", "mass", "hnw", "hnw", "hnw"],
            "risk_attr": ["lo", "lo", "hi", "lo", "hi", "hi"],
            "label": [1, 0, 0, 1, 1, 0],
        }))

    def test_groups_by_union_dims_and_counts_pos_neg(self, spark):
        stats = profile_stats(
            self._df(spark), [pd.Timestamp("2025-01-31")],
            union_dims=["cust_segment_typ", "prod_name"],
            label_col="label", time_col="snap_date")
        d = {(r["cust_segment_typ"], r["prod_name"]): (r["n_pos"], r["n_neg"])
             for r in stats}
        assert d[("mass", "a")] == (1, 2)
        assert d[("hnw", "b")] == (2, 1)

    def test_groups_at_finer_union_with_extra_dim(self, spark):
        stats = profile_stats(
            self._df(spark), [pd.Timestamp("2025-01-31")],
            union_dims=["cust_segment_typ", "prod_name", "risk_attr"],
            label_col="label", time_col="snap_date")
        # every returned row carries all three union dims plus counts
        assert all({"cust_segment_typ", "prod_name", "risk_attr",
                    "n_pos", "n_neg"} <= set(r) for r in stats)
        d = {(r["cust_segment_typ"], r["prod_name"], r["risk_attr"]):
             (r["n_pos"], r["n_neg"]) for r in stats}
        # mass|a|lo: rows (1,label1),(2,label0) -> n_pos1 n_neg1
        assert d[("mass", "a", "lo")] == (1, 1)

    def test_missing_union_column_raises(self, spark):
        with pytest.raises(ValueError, match="not in"):
            profile_stats(
                self._df(spark), [pd.Timestamp("2025-01-31")],
                union_dims=["cust_segment_typ", "prod_name", "no_such_col"],
                label_col="label", time_col="snap_date")
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestProfileStats -q`
Expected: FAIL with `TypeError` (old `profile_stats` has `segment_col`/`item_col` kwargs, not `union_dims`).

- [ ] **Step 3: Rewrite `profile_stats`**

```python
def profile_stats(
    df,
    snap_dates: list,
    *,
    union_dims: list,
    label_col: str,
    time_col: str,
) -> list[dict]:
    """Spark groupBy -> per-(union_dims) (n_pos, n_neg) over snap_dates.

    ``union_dims`` is the finest granularity = (sample_group_keys ∪
    sample_weight_keys) \\ {label}; label is the count source, never a group-by
    column. Single Spark action (one .collect of a tiny grouped frame). No UDF.
    Returns a list of dicts: one per group, each union dim value plus n_pos /
    n_neg. Raises ValueError if any union dim is absent from the frame (usually
    a weight key not added to dataset.carry_columns).
    """
    from pyspark.sql import functions as F

    missing = [c for c in union_dims if c not in df.columns]
    if missing:
        raise ValueError(
            f"profiling columns {missing} not in source columns {df.columns}; "
            "weight keys must be identity/label/carry columns present in "
            "sample_pool (add them to dataset.carry_columns)."
        )
    rows = (
        df.filter(F.col(time_col).isin(list(snap_dates)))
        .groupBy(*union_dims)
        .agg(
            F.sum(F.col(label_col)).alias("n_pos"),
            F.sum(F.lit(1) - F.col(label_col)).alias("n_neg"),
        )
        .collect()
    )
    return [
        {**{d: r[d] for d in union_dims},
         "n_pos": int(r["n_pos"]), "n_neg": int(r["n_neg"])}
        for r in rows
    ]
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestProfileStats -q`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): profile_stats groups by union dims, dict rows + column guard"
```

---

## Task 3: `aggregate_surfaces` — ratio + weight surfaces with downsample-coupled projection

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (add `aggregate_surfaces` after `suggest_weight`)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (add `class TestAggregateSurfaces` after `TestSuggestWeight`)

- [ ] **Step 1: Write the failing tests**

```python
class TestAggregateSurfaces:
    # 4 fine cells over (segment, item); weight default keys = [item]
    _STATS = [
        {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 100, "n_neg": 9000},
        {"cust_segment_typ": "hnw",  "prod_name": "a", "n_pos": 60,  "n_neg": 500},
        {"cust_segment_typ": "mass", "prod_name": "b", "n_pos": 80,  "n_neg": 2000},
        {"cust_segment_typ": "hnw",  "prod_name": "b", "n_pos": 0,   "n_neg": 40},
    ]

    def test_case1_weight_by_item_couples_to_ratio_downsample(self):
        # neg_mult: mass|a=5 (ratio=clamp(5*100/9000)=0.0556), others keep-all
        # (neg_mult huge -> ratio clamps to 1.0)
        nm = {("mass", "a"): 5.0, ("hnw", "a"): 1e9,
              ("mass", "b"): 1e9, ("hnw", "b"): 1e9}
        out = aggregate_surfaces(
            self._STATS, nm, segment_col="cust_segment_typ",
            item_col="prod_name", weight_keys=["prod_name"],
            alpha=0.5, w_max=5.0, default_neg_mult=5.0)
        rr = {(r["segment"], r["product"]): r for r in out["ratio_rows"]}
        # mass|a downsampled: kept ~= round(9000*0.05556)=500
        assert abs(rr[("mass", "a")]["ratio"] - (5 * 100 / 9000)) < 1e-9
        assert rr[("mass", "a")]["kept_neg"] == 500
        # weight surface keyed by item; post-downsample n_neg for 'a' =
        # round(9000*0.05556 + 500*1.0) = round(500+500) = 1000
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("a",)]["n_pos"] == 160          # 100+60, unchanged by downsample
        assert wr[("a",)]["n_neg_post"] == 1000
        # item 'b' negatives fully kept (no downsample): 2000+40 = 2040
        assert wr[("b",)]["n_neg_post"] == 2040

    def test_case2_cross_dimension_shares_ratio_over_dropped_dim(self):
        # weight keys = [risk_attr, item]; the extra risk_attr dim is dropped
        # when projecting to the ratio (segment,item) cell, so both risk values
        # under mass|a share ratio[mass,a].
        stats = [
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "lo",
             "n_pos": 60, "n_neg": 6000},
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "hi",
             "n_pos": 40, "n_neg": 3000},
        ]
        nm = {("mass", "a"): 5.0}   # ratio = 5*100/9000 = 0.05556
        out = aggregate_surfaces(
            stats, nm, segment_col="cust_segment_typ", item_col="prod_name",
            weight_keys=["risk_attr", "prod_name"], alpha=0.5, w_max=5.0,
            default_neg_mult=5.0)
        ratio = 5 * 100 / 9000
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        # each risk sub-cell uses the SAME ratio[mass,a]
        assert wr[("lo", "a")]["n_neg_post"] == round(6000 * ratio)
        assert wr[("hi", "a")]["n_neg_post"] == round(3000 * ratio)
        assert wr[("lo", "a")]["keys"] == ["lo", "a"]

    def test_no_round_until_display_totals_match(self):
        # ratio-surface kept_neg total == weight-surface n_neg_post total only
        # holds if rounding is deferred to display (sum of fractions, not sum of
        # rounded). Use the rounded *totals* recomputed from fractions.
        nm = {("mass", "a"): 3.0, ("hnw", "a"): 3.0,
              ("mass", "b"): 3.0, ("hnw", "b"): 3.0}
        out = aggregate_surfaces(
            self._STATS, nm, segment_col="cust_segment_typ",
            item_col="prod_name", weight_keys=["prod_name"],
            alpha=0.5, w_max=5.0, default_neg_mult=3.0)
        kept_total = sum(r["kept_neg"] for r in out["ratio_rows"])
        post_total = sum(r["n_neg_post"] for r in out["weight_rows"])
        # both derive from the same Σ n_neg*ratio; equal within per-row rounding
        assert abs(kept_total - post_total) <= len(out["ratio_rows"])

    def test_empty_weight_keys_yields_no_weight_rows(self):
        out = aggregate_surfaces(
            self._STATS, {}, segment_col="cust_segment_typ",
            item_col="prod_name", weight_keys=[], alpha=0.5, w_max=5.0,
            default_neg_mult=5.0)
        assert out["weight_rows"] == []
        assert len(out["ratio_rows"]) == 4
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestAggregateSurfaces -q`
Expected: FAIL with `NameError`/`ImportError` for `aggregate_surfaces`.

- [ ] **Step 3: Implement `aggregate_surfaces`**

Insert after `suggest_weight` (before the removed `build_grid` location):

```python
def aggregate_surfaces(
    stats: list[dict],
    neg_mults: dict,
    *,
    segment_col: str,
    item_col: str,
    weight_keys: list,
    alpha: float,
    w_max: float,
    default_neg_mult: float,
) -> dict:
    """Roll finest-granularity stats up into the ratio and weight surfaces.

    Pure: no Spark, no I/O. ``stats`` are union-granularity dict rows from
    profile_stats. ``neg_mults`` maps ``(segment_val, item_val)`` -> target
    neg:pos multiplier (missing -> ``default_neg_mult``).

    Ratio surface: aggregate fine cells to ``(segment, item)``; each row's
    keep-rate ``ratio = clamp(neg_mult * n_pos / n_neg, 0, 1)`` (the exported
    value). Weight surface: aggregate fine cells to the ``weight_keys`` tuple;
    ``n_pos`` is unchanged by downsampling (positives all kept) while
    ``n_neg_post`` is the post-downsample count — each fine cell contributes
    ``n_neg * ratio[(its segment, its item)]`` (projection drops any extra
    weight dims; the dataset sampler downsamples uniformly within a
    (segment,item) group, so the shared ratio is exact). Negatives are summed
    as fractions and rounded only for display, so the ratio surface's
    ``kept_neg`` and the weight surface's ``n_neg_post`` stay mutually
    consistent. ``suggested_weight`` uses the weight-surface median n_pos.
    """
    # --- ratio surface: aggregate to (segment, item) ---
    racc: dict = {}
    for s in stats:
        k = (s[segment_col], s[item_col])
        a = racc.setdefault(k, [0, 0])
        a[0] += s["n_pos"]
        a[1] += s["n_neg"]
    ratio_by_si: dict = {}
    ratio_rows: list[dict] = []
    for (seg, item), (npos, nneg) in racc.items():
        mult = float(neg_mults.get((seg, item), default_neg_mult))
        ratio = suggest_ratio(npos, nneg, mult)
        ratio_by_si[(seg, item)] = ratio
        kept = round(nneg * ratio)
        total = npos + kept
        ratio_rows.append({
            "segment": seg, "product": item, "n_pos": npos, "n_neg": nneg,
            "pos_rate": (npos / (npos + nneg) if npos + nneg else 0.0),
            "neg_mult": mult, "ratio": ratio, "kept_neg": kept,
            "new_pos_rate": (npos / total if total else 0.0),
        })
    ratio_rows.sort(key=lambda r: (r["segment"], r["product"]))

    # --- weight surface: aggregate to weight_keys tuple (post-downsample) ---
    weight_rows: list[dict] = []
    if weight_keys:
        wacc: dict = {}
        for s in stats:
            wk = tuple(s[k] for k in weight_keys)
            a = wacc.setdefault(wk, [0, 0.0])
            a[0] += s["n_pos"]
            a[1] += s["n_neg"] * ratio_by_si[(s[segment_col], s[item_col])]
        pos_list = [v[0] for v in wacc.values()]
        median_pos = float(statistics.median(pos_list)) if pos_list else 1.0
        for wk, (npos, nneg_post) in wacc.items():
            nneg_round = round(nneg_post)
            total = npos + nneg_round
            weight_rows.append({
                "keys": list(wk), "n_pos": npos, "n_neg_post": nneg_round,
                "pos_rate_post": (npos / total if total else 0.0),
                "suggested_weight": round(
                    suggest_weight(npos, median_pos, alpha, w_max), 4),
            })
        weight_rows.sort(key=lambda r: tuple(str(x) for x in r["keys"]))

    return {"ratio_rows": ratio_rows, "weight_rows": weight_rows}
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestAggregateSurfaces -q`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): aggregate_surfaces — ratio+weight surfaces, downsample-coupled"
```

---

## Task 4: `grid_to_yaml` — new export shape, real weight-key validation

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (rewrite `grid_to_yaml`, lines 142-198)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (rewrite `class TestGridToYaml`, lines 129-153; update `_params`, lines 120-126)

- [ ] **Step 1: Update `_params` to also carry training.sample_weight_keys, and rewrite `TestGridToYaml`**

Replace `_params` (lines 120-126) and `TestGridToYaml` (lines 129-153) with:

```python
def _params(weight_keys=("prod_name",)):
    return {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["a", "b"]}},
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]},
                    "sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
        "training": {"sample_weight_keys": list(weight_keys)},
    }


def _export(ratio_rows, weight_rows, *, group_keys=None, weight_keys=None):
    return {
        "sample_group_keys": group_keys or ["cust_segment_typ", "prod_name", "label"],
        "sample_weight_keys": weight_keys if weight_keys is not None else ["prod_name"],
        "ratio_rows": ratio_rows,
        "weight_rows": weight_rows,
    }


class TestGridToYaml:
    def test_sparse_emits_only_non_default(self):
        export = _export(
            ratio_rows=[
                {"segment": "mass", "product": "a", "ratio": 0.5},
                {"segment": "mass", "product": "b", "ratio": 1.0}],
            weight_rows=[{"keys": ["a"], "weight": 1.0},
                         {"keys": ["b"], "weight": 3.0}])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.5}}
        assert sw == {"sample_weights": {"b": 3.0}}

    def test_weight_key_joined_in_weight_keys_order(self):
        # weight_keys = [risk_attr, prod_name] -> key "lo|a" (arity 2)
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["lo", "a"], "weight": 2.0}],
            weight_keys=["risk_attr", "prod_name"])
        out = grid_to_yaml(
            export, _params(weight_keys=("risk_attr", "prod_name")),
            default_ratio=1.0)
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert sw == {"sample_weights": {"lo|a": 2.0}}

    def test_unknown_product_ratio_raises(self):
        export = _export(
            ratio_rows=[{"segment": "mass", "product": "zzz", "ratio": 0.5}],
            weight_rows=[])
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)

    def test_unknown_product_weight_only_raises_with_real_weight_keys(self):
        # weight_keys=[prod_name] (arity 1): the A9c probe must use the REAL
        # sample_weight_keys, else it short-circuits and the unknown slips by.
        export = _export(
            ratio_rows=[],
            weight_rows=[{"keys": ["zzz"], "weight": 2.0}])
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)

    def test_export_keys_must_match_config(self):
        export = _export(ratio_rows=[], weight_rows=[],
                         weight_keys=["cust_segment_typ"])
        with pytest.raises(ValueError, match="sample_weight_keys"):
            grid_to_yaml(export, _params(weight_keys=("prod_name",)),
                         default_ratio=1.0)
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestGridToYaml -q`
Expected: FAIL (old `grid_to_yaml` expects a list `export` and `row["segment"]`/`row["weight"]`, raises `TypeError`/`KeyError`).

- [ ] **Step 3: Rewrite `grid_to_yaml`**

```python
def grid_to_yaml(
    export: dict,
    parameters: dict,
    default_ratio: float,
    default_weight: float = 1.0,
) -> dict:
    """Convert the browser JSON export into two sparse YAML blocks.

    ``export`` is the self-describing object emitted by the editor:
    ``{sample_group_keys, sample_weight_keys, ratio_rows, weight_rows}``.
    Emits only cells deviating from defaults. The export's key-sets must match
    the config (guards against pasting a stale export onto changed config).
    Validates via the single-source consistency predicates BEFORE returning:
    ratio keys through ``override_unknown_items`` (A5), weight keys through
    ``weight_key_arity_mismatch`` (A9b) + ``weight_unknown_items`` (A9c) — the
    probe declares the *real* ``sample_weight_keys`` so A9c actually inspects
    the product component instead of short-circuiting.
    """
    schema = get_schema(parameters)
    item_col, label_col = schema["item"], schema["label"]
    cfg_group = (parameters.get("dataset", {}) or {}).get("sample_group_keys", [])
    cfg_weight = (parameters.get("training", {}) or {}).get("sample_weight_keys") or []
    exp_group = export.get("sample_group_keys", [])
    exp_weight = export.get("sample_weight_keys", [])
    if list(exp_group) != list(cfg_group):
        raise ValueError(
            f"export sample_group_keys {exp_group} != config {cfg_group}; "
            "re-profile against the current config before pasting.")
    if list(exp_weight) != list(cfg_weight):
        raise ValueError(
            f"export sample_weight_keys {exp_weight} != config {cfg_weight}; "
            "re-profile against the current config before pasting.")

    overrides: dict[str, float] = {}
    for row in export.get("ratio_rows", []):
        ratio = float(row["ratio"])
        if ratio != default_ratio:
            overrides[f"{row['segment']}|{row['product']}|{_NEG_LABEL}"] = ratio
    weights: dict[str, float] = {}
    for row in export.get("weight_rows", []):
        weight = float(row["weight"])
        if weight != default_weight:
            weights["|".join(str(v) for v in row["keys"])] = weight

    probe = {**parameters}
    probe.setdefault("dataset", {})
    probe["dataset"] = {**probe["dataset"], "sample_ratio_overrides": overrides}
    probe["training"] = {
        **probe.get("training", {}),
        "sample_weights": weights,
        "sample_weight_keys": list(cfg_weight),
    }
    bad = sorted(
        set(override_unknown_items(probe))
        | set(weight_unknown_items(probe))
    )
    if bad:
        raise ValueError(
            f"editor export references unknown product value(s) {bad} "
            f"absent from schema.categorical_values[item]; fix before paste."
        )
    arity_bad = weight_key_arity_mismatch(probe)
    if arity_bad:
        raise ValueError(
            f"weight key(s) {arity_bad} do not have "
            f"{len(cfg_weight)} '|'-segment(s) to match sample_weight_keys "
            f"{cfg_weight}; fix before paste."
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

Add `weight_key_arity_mismatch` to the consistency import block (lines 31-34):

```python
from recsys_tfb.core.consistency import (
    override_unknown_items,
    weight_key_arity_mismatch,
    weight_unknown_items,
)
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestGridToYaml -q`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): grid_to_yaml — new export shape, real weight-key A9b/A9c validation"
```

---

## Task 5: HTML editor — two config-driven tabs mirroring `aggregate_surfaces`

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (rewrite `_HTML_TEMPLATE` lines 201-486 and `render_html` lines 489-510)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (rewrite `class TestRenderHtml`, lines 159-299)

The browser mirrors `aggregate_surfaces` in JS for live recompute. Embed the union-granularity `STATS`, the resolved `SEG`/`ITEM`/`WKEYS`, and the tuning constants. Two tabs (`ratio` / `weight`); switching to the weight tab recomputes its rows from the current ratio edits (the downsample coupling); a single Export emits the new self-describing object + YAML.

- [ ] **Step 1: Rewrite `TestRenderHtml`**

```python
class TestRenderHtml:
    _STATS = [
        {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 200,
         "n_neg": 4000},
        {"cust_segment_typ": "hnw", "prod_name": "a", "n_pos": 8, "n_neg": 50},
    ]
    _KW = dict(segment_col="cust_segment_typ", item_col="prod_name",
               weight_keys=["prod_name"], default_ratio=1.0)

    def test_self_contained_and_embeds_stats_and_keys(self):
        html = render_html(self._STATS, **self._KW)
        assert html.lstrip().startswith("<!DOCTYPE html>")
        assert json.dumps(self._STATS) in html
        assert 'const SEG="cust_segment_typ"' in html
        assert 'const ITEM="prod_name"' in html
        assert 'const WKEYS=["prod_name"]' in html
        assert "http://" not in html and "https://" not in html
        assert "Export JSON" in html and "Export YAML" in html

    def test_two_tabs_ratio_and_weight(self):
        html = render_html(self._STATS, **self._KW)
        assert "function setTab(" in html
        assert "setTab('ratio')" in html and "setTab('weight')" in html

    def test_builds_ratio_store_and_keep_rate(self):
        html = render_html(self._STATS, **self._KW)
        assert "function buildRatio(" in html
        # keep-rate mirrors suggest_ratio: clamp(nm*n_pos/n_neg,0,1)
        assert "function keepRate(" in html
        assert "nm*np/nn" in html

    def test_weight_tab_recomputes_post_downsample_from_ratio_edits(self):
        html = render_html(self._STATS, **self._KW)
        assert "function rebuildWeight(" in html
        # per-(seg,item) effective ratio projected onto fine cells
        assert "function ratioBySI(" in html
        # n_neg_post accumulates n_neg * projected ratio
        assert "s.n_neg*rbs.get(" in html
        # rebuildWeight runs when entering the weight tab
        assert "rebuildWeight()" in html

    def test_neg_mult_is_primary_knob_ratio_readonly(self):
        html = render_html(self._STATS, **self._KW, target_neg_pos=5.0)
        assert "負樣本倍率" in html
        assert "data-k=neg_mult" in html
        assert "data-k=ratio" not in html
        assert "實際倍率" in html and "function achMult(" in html
        assert "const R=5.0" in html
        assert "td.warn" in html and "已全留" in html

    def test_export_emits_self_describing_object(self):
        html = render_html(self._STATS, **self._KW)
        assert "function exp(" in html
        # cell ratio key gets the fixed |0 label; weight key joins WKEYS values
        assert "'|0'" in html
        assert "sample_group_keys" in html and "sample_weight_keys" in html
        assert "ratio_rows" in html and "weight_rows" in html

    def test_empty_weight_keys_hides_weight_tab(self):
        html = render_html(self._STATS, segment_col="cust_segment_typ",
                           item_col="prod_name", weight_keys=[],
                           default_ratio=1.0)
        assert "const WKEYS=[]" in html
        # weight tab disabled note when no weight keys configured
        assert "WKEYS.length" in html

    def test_edits_survive_sort_and_filter(self):
        html = render_html(self._STATS, **self._KW)
        assert "function syncEdits(" in html and "syncEdits()" in html
        assert "function sortBy(" in html and "function flt(" in html

    def test_explains_logic_with_configured_values(self):
        html = render_html(self._STATS, **self._KW,
                           target_neg_pos=3.0, alpha=0.7, w_max=8.0)
        assert "sample_ratio_overrides" in html and "sample_weights" in html
        assert "3.0" in html and "0.7" in html and "8.0" in html
        assert "http://" not in html and "https://" not in html
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q`
Expected: FAIL (old `render_html(grid, default_ratio=...)` positional signature; new asserts absent).

- [ ] **Step 3: Replace `_HTML_TEMPLATE` and `render_html`**

Replace the entire `_HTML_TEMPLATE = """..."""` string (lines 201-486) and `render_html` (lines 489-510) with the following. Note `{{`/`}}` are literal braces for `str.format`; single-brace tokens (`{stats_json}`, `{seg_col}`, `{item_col}`, `{wkeys_json}`, `{default_ratio}`, `{target_neg_pos}`, `{alpha}`, `{w_max}`) are substituted.

```python
_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Sampling Overrides Editor</title>
<style>
 body{{font-family:system-ui,monospace;margin:1.5rem}}
 table{{border-collapse:collapse}} td,th{{border:1px solid #ccc;padding:4px 8px}}
 th{{background:#f2f2f2;cursor:pointer;user-select:none}}
 td.edit{{background:#fffbe6}}
 td.calc{{background:#eaffea;color:#060}} th.calc{{cursor:default}}
 td.warn{{background:#fff3cd;color:#8a4b00;font-weight:bold}}
 .stat{{color:#666}} button{{margin:.3rem;padding:.4rem .8rem}}
 pre{{background:#f7f7f7;padding:1rem;white-space:pre-wrap}}
 details{{background:#eef6ff;border:1px solid #cde;padding:.5rem 1rem;
  margin:.6rem 0;max-width:60rem}}
 details summary{{cursor:pointer;font-weight:bold}}
 details code{{background:#fff;padding:0 .25rem;border:1px solid #ddd}}
 #tabs{{margin:.6rem 0}}
 #tabs button.active{{background:#cde;font-weight:bold}}
 #flt{{margin:.5rem 0;padding:.35rem;width:22rem}}
 tfoot td{{background:#fff7e6;font-weight:bold;border-top:2px solid #999}}
</style></head><body>
<h2>Sampling Overrides Editor</h2>
<details open><summary>各欄是什麼？用途是什麼？（點此展開/收合）</summary>
<p><b>兩個面（分頁切換，各自獨立）</b>：<code>ratio 面</code>依
<code>sample_group_keys</code>（segment×item）調抽樣下採樣；<code>weight 面</code>依
<code>sample_weight_keys</code>調訓練樣本權重。兩組 keys 可不同，匯出時各以自己的
key-set 驗證。</p>
<p><b>負樣本倍率 — 目標 neg:pos（ratio 面主旋鈕，可編輯）。</b>
設定每列希望的負:正樣本倍數 R（每列預設 <code>{target_neg_pos}</code>）。保留<b>全部</b>
正樣本，下採負樣本逼近此倍率。</p>
<p><b>ratio — 負樣本保留率（唯讀，由倍率推導）。</b>
<code>ratio = clamp(倍率 × n_pos / n_neg, 0, 1)</code>，即匯出值（key
<code>segment|item|0</code>，label 固定 0）。<code>ratio = {default_ratio}</code> = 不下採。</p>
<p><b>實際倍率（唯讀）。</b>下採後實際 neg:pos；負樣本不足以達標時 ratio 夾到 1.0、
此欄低於目標並以琥珀底 ⚠ 標示（已全留）。</p>
<p><b>weight 面 n_neg / pos_rate（唯讀，連動下採樣後）。</b>weight 作用在下採樣後的
訓練資料；正樣本全留故 <code>n_pos</code> 不變，負樣本依 ratio 面設定上捲，故此面的
n_neg/pos_rate 反映實際訓練分佈。</p>
<p><b>weight — 冷門加權</b>（訓練時 loss 權重）。建議值 =
clamp((median_pos / n_pos) ^ <code>{alpha}</code>, 1.0, <code>{w_max}</code>)，
median 取 weight 面各列 n_pos 中位數。<code>weight = 1.0</code> = 不加權。匯出對應
<code>training.sample_weights</code>。</p>
</details>
<div id="tabs">
<button id="tb_ratio" class="active" onclick="setTab('ratio')">ratio 面 (sample_group_keys)</button>
<button id="tb_weight" onclick="setTab('weight')">weight 面 (sample_weight_keys)</button>
</div>
<div id="note"></div>
<input id="flt" placeholder="篩選…" oninput="flt()">
<table id="g"><thead></thead><tbody></tbody><tfoot><tr id="foot"></tr></tfoot></table>
<button onclick="exp('json')">Export JSON</button>
<button onclick="exp('yaml')">Export YAML snippet</button>
<pre id="out"></pre>
<script>
const STATS={stats_json};
const SEG="{seg_col}";
const ITEM="{item_col}";
const WKEYS={wkeys_json};
const DR={default_ratio};
const R={target_neg_pos};
const ALPHA={alpha};
const WMAX={w_max};
const SEP='\\u0001';
function median(arr){{ const s=arr.slice().sort((a,b)=>a-b),n=s.length;
 return n?(n%2?s[(n-1)/2]:(s[n/2-1]+s[n/2])/2):1; }}
function suggestWeight(np,med,a,wmax){{ if(np<=0) return wmax;
 return Math.min(wmax,Math.max(1,Math.pow(med/np,a))); }}
function keepRate(nm,np,nn){{ if(nn<=0) return 1;
 return Math.min(1,Math.max(0,nm*np/nn)); }}
// ratio store: one row per (segment,item); neg_mult editable, default R.
function buildRatio(){{
 const m=new Map();
 STATS.forEach(s=>{{ const k=s[SEG]+SEP+s[ITEM];
  const a=m.get(k)||{{segment:s[SEG],product:s[ITEM],n_pos:0,n_neg:0}};
  a.n_pos+=s.n_pos; a.n_neg+=s.n_neg; m.set(k,a); }});
 const rows=[...m.values()];
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.suggested_neg_mult=R; }});
 return rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
const RATIO=buildRatio();
let WEIGHT=[];
// effective keep-rate per (segment,item) from current neg_mult edits.
function ratioBySI(){{
 const m=new Map();
 RATIO.forEach(r=>m.set(r.segment+SEP+r.product,
  keepRate(parseFloat(r.suggested_neg_mult),r.n_pos,r.n_neg)));
 return m;
}}
// weight store: aggregate STATS to WKEYS tuple; n_neg post-downsample via the
// projected (segment,item) ratio. user weight edits preserved by key.
function rebuildWeight(){{
 if(!WKEYS.length){{ WEIGHT=[]; return; }}
 const prev=new Map(WEIGHT.map(w=>[w.keyStr,w.weight]));
 const rbs=ratioBySI(),m=new Map();
 STATS.forEach(s=>{{ const wk=WKEYS.map(k=>s[k]),ks=wk.join('|');
  const a=m.get(ks)||{{keys:wk,keyStr:ks,n_pos:0,_nn:0}};
  a.n_pos+=s.n_pos; a._nn+=s.n_neg*rbs.get(s[SEG]+SEP+s[ITEM]); m.set(ks,a); }});
 const rows=[...m.values()],med=median(rows.map(r=>r.n_pos));
 rows.forEach(r=>{{ r.n_neg_post=Math.round(r._nn);
  const t=r.n_pos+r.n_neg_post; r.pos_rate_post=(t>0?r.n_pos/t:0);
  r.suggested_weight=+suggestWeight(r.n_pos,med,ALPHA,WMAX).toFixed(4);
  r.weight=prev.has(r.keyStr)?prev.get(r.keyStr):r.suggested_weight; }});
 WEIGHT=rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
let tab='ratio',sortKey=null,sortAsc=true;
function rows(){{ return tab==='ratio'?RATIO:WEIGHT; }}
function preview(r,nm){{
 if(isNaN(nm)) return {{ratio:'—',kn:'—',pr:'—',clamped:false,achieved:0,noNeg:false}};
 if(r.n_neg<=0) return {{ratio:'1.0000',kn:'0',pr:(r.n_pos>0?1:0).toFixed(4),
   clamped:false,achieved:0,noNeg:true}};
 const raw=nm*r.n_pos/r.n_neg,ratio=Math.min(1,Math.max(0,raw));
 const keptNeg=Math.round(r.n_neg*ratio),total=r.n_pos+keptNeg;
 return {{ratio:ratio.toFixed(4),kn:String(keptNeg),
  pr:(total>0?r.n_pos/total:0).toFixed(4),clamped:raw>1,
  achieved:(r.n_pos>0?keptNeg/r.n_pos:0),noNeg:false}};
}}
function achMult(pv){{
 if(pv.ratio==='—') return {{cls:'calc',html:'—',title:''}};
 if(pv.noNeg) return {{cls:'calc',html:'0.0',title:'無負樣本，不下採樣'}};
 if(pv.clamped) return {{cls:'warn',html:pv.achieved.toFixed(1)+' ⚠',
  title:'負樣本不足以達到目標倍率 '+R+'，已全留'}};
 return {{cls:'calc',html:pv.achieved.toFixed(1),title:''}};
}}
function syncEdits(){{
 document.querySelectorAll('#g td.edit').forEach(td=>{{
  const v=parseFloat(td.textContent); if(isNaN(v)) return;
  const r=rows()[+td.dataset.i];
  if(td.dataset.k==='neg_mult') r.suggested_neg_mult=v; else r.weight=v;
 }});
}}
function recalc(td){{
 const r=rows()[+td.dataset.i],tr=td.closest('tr'),nm=parseFloat(td.textContent);
 r.suggested_neg_mult=nm;
 const pv=preview(r,nm),am=achMult(pv);
 tr.querySelector('td.rt').textContent=pv.ratio;
 const a=tr.querySelector('td.am'); a.className=am.cls+' am';
 a.innerHTML=am.html; a.title=am.title;
 tr.querySelector('td.kn').textContent=pv.kn;
 tr.querySelector('td.pr').textContent=pv.pr;
}}
function renderRatio(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr><th onclick="sortBy('segment')">segment ⇅</th>`+
  `<th onclick="sortBy('product')">product ⇅</th>`+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat onclick="sortBy('n_neg')">n_neg ⇅</th>`+
  `<th class=stat>pos_rate</th><th>負樣本倍率</th><th class=calc>ratio</th>`+
  `<th class=calc>實際倍率</th><th class=calc>kept_neg</th>`+
  `<th class=calc>new_pos_rate</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],pv=preview(r,parseFloat(r.suggested_neg_mult));
  const am=achMult(pv),tr=document.createElement('tr');
  tr.innerHTML=`<td>${{r.segment}}</td><td>${{r.product}}</td>`+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=neg_mult data-i=${{i}} `+
   `oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`+
   `<td class="calc rt">${{pv.ratio}}</td>`+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`;
  tb.appendChild(tr); }});
}}
function renderWeight(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+WKEYS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat>n_neg(後)</th><th class=stat>pos_rate(後)</th>`+
  `<th>weight</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],tr=document.createElement('tr');
  tr.innerHTML=r.keys.map(v=>`<td>${{v}}</td>`).join('')+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg_post}}</td>`+
   `<td class=stat>${{r.pos_rate_post.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=weight data-i=${{i}}>${{r.weight}}</td>`;
  tb.appendChild(tr); }});
}}
function render(){{
 const data=rows();
 // weight sort keys k0..kN map onto the keys[] array
 data.forEach(r=>{{ if(r.keys) r.keys.forEach((v,j)=>r['k'+j]=v); }});
 const cols=tab==='ratio'?['segment','product']:WKEYS.map((_,j)=>'k'+j);
 const q=(document.getElementById('flt').value||'').toLowerCase();
 let idx=data.map((_,i)=>i);
 if(q) idx=idx.filter(i=>cols.map(c=>data[i][c]).join(' ').toLowerCase().indexOf(q)>=0);
 if(sortKey) idx.sort((a,b)=>{{ let x=data[a][sortKey],y=data[b][sortKey];
  if(typeof x==='string'){{x=x.toLowerCase();y=y.toLowerCase();}}
  return (x<y?-1:x>y?1:0)*(sortAsc?1:-1); }});
 if(tab==='ratio') renderRatio(data,idx); else renderWeight(data,idx);
 document.getElementById('foot').innerHTML='';
}}
function sortBy(k){{ syncEdits(); if(sortKey===k){{sortAsc=!sortAsc;}}
 else{{sortKey=k;sortAsc=true;}} render(); }}
function flt(){{ syncEdits(); render(); }}
function setTab(t){{
 syncEdits();
 if(t==='weight' && !WKEYS.length){{
  document.getElementById('note').textContent=
   'sample_weight_keys 為空，無 weight 面可編輯。'; return; }}
 tab=t; sortKey=null;
 document.getElementById('tb_ratio').className=(t==='ratio'?'active':'');
 document.getElementById('tb_weight').className=(t==='weight'?'active':'');
 document.getElementById('note').textContent=
  (t==='weight'?'n_neg(後)/pos_rate(後) 反映 ratio 面目前的下採樣設定。':'');
 if(t==='weight') rebuildWeight();
 render();
}}
function exp(kind){{
 syncEdits(); rebuildWeight();
 const ratio_rows=RATIO.map(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  return {{segment:r.segment,product:r.product,
   ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))}}; }});
 const weight_rows=WEIGHT.map(r=>({{keys:r.keys,weight:parseFloat(r.weight)}}));
 const o={{sample_group_keys:[SEG,ITEM,'label'],sample_weight_keys:WKEYS,
  ratio_rows:ratio_rows,weight_rows:weight_rows}};
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a'); a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json'; a.click();
 }}else{{
  const ov={{}},sw={{}};
  ratio_rows.forEach(r=>{{ if(r.ratio!==DR) ov[r.segment+'|'+r.product+'|0']=r.ratio; }});
  weight_rows.forEach(r=>{{ if(r.weight!==1.0) sw[r.keys.join('|')]=r.weight; }});
  document.getElementById('out').textContent=
   '# -> conf/base/parameters_dataset.yaml (under dataset:)\\n'+
   'sample_ratio_overrides:\\n'+
   Object.entries(ov).map(([k,v])=>'  "'+k+'": '+v).join('\\n')+
   '\\n\\n# -> conf/base/parameters_training.yaml (under training:)\\n'+
   'sample_weights:\\n'+
   Object.entries(sw).map(([k,v])=>'  "'+k+'": '+v).join('\\n');
 }}
}}
setTab('ratio');
</script></body></html>"""


def render_html(
    stats: list[dict],
    *,
    segment_col: str,
    item_col: str,
    weight_keys: list,
    default_ratio: float,
    target_neg_pos: float = 5.0,
    alpha: float = 0.5,
    w_max: float = 5.0,
) -> str:
    """Render a self-contained two-tab HTML editor (pure stdlib, no assets).

    ``stats`` are union-granularity dict rows from profile_stats; the browser
    mirrors aggregate_surfaces in JS to build the ratio and weight surfaces
    live. The tuning knobs are surfaced in the help text so it reflects the
    configured values.
    """
    return _HTML_TEMPLATE.format(
        stats_json=json.dumps(stats),
        seg_col=segment_col,
        item_col=item_col,
        wkeys_json=json.dumps(weight_keys),
        default_ratio=default_ratio,
        target_neg_pos=target_neg_pos,
        alpha=alpha,
        w_max=w_max,
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): two config-driven tabs mirroring aggregate_surfaces"
```

---

## Task 6: CLI wiring — read training params, profile at union dims, render

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (`profile` lines 565-612, `to-yaml` lines 615-632)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (rewrite `class TestToYamlCli`, lines 327-347)

- [ ] **Step 1: Rewrite `TestToYamlCli` for the new export shape + train params**

```python
class TestToYamlCli:
    def _write_params(self, tmp_path):
        params = tmp_path / "p.yaml"
        params.write_text(
            "schema:\n  columns:\n    item: prod_name\n"
            "  categorical_values:\n    prod_name: [a, b]\n"
            "dataset:\n  prepare_model_input:\n"
            "    categorical_columns: [prod_name]\n"
            "  sample_group_keys: [cust_segment_typ, prod_name, label]\n")
        train = tmp_path / "t.yaml"
        train.write_text("training:\n  sample_weight_keys: [prod_name]\n")
        return params, train

    def test_to_yaml_prints_both_blocks(self, tmp_path):
        export = {
            "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
            "sample_weight_keys": ["prod_name"],
            "ratio_rows": [{"segment": "mass", "product": "a", "ratio": 0.5}],
            "weight_rows": [{"keys": ["b"], "weight": 3.0}],
        }
        jf = tmp_path / "e.json"
        jf.write_text(json.dumps(export))
        params, train = self._write_params(tmp_path)
        r = CliRunner().invoke(app, [
            "to-yaml", str(jf), "--params", str(params),
            "--train-params", str(train)])
        assert r.exit_code == 0, r.output
        assert "sample_ratio_overrides:" in r.output and "mass|a|0" in r.output
        assert "sample_weights:" in r.output and "b" in r.output
```

- [ ] **Step 2: Run to verify failure**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestToYamlCli -q`
Expected: FAIL (no `--train-params` option; old `grid_to_yaml`/`to_yaml` shapes).

- [ ] **Step 3: Rewrite the `profile` command**

```python
@app.command()
def profile(
    source: str = typer.Argument(..., help="Hive table db.table or parquet path"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="dataset params yaml"),
    train_params: Path = typer.Option(
        Path("conf/base/parameters_training.yaml"),
        help="training params yaml — source of sample_weight_keys"),
    base_params: Path = typer.Option(
        Path("conf/base/parameters.yaml"),
        help="base params yaml — source of schema.columns"),
    target_neg_pos: float = typer.Option(5.0, help="downsample target neg:pos R"),
    alpha: float = typer.Option(0.5, help="cold-weight damping exponent"),
    w_max: float = typer.Option(5.0, help="cold-weight cap"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds = cfg.get("dataset", cfg)
    training_cfg = yaml.safe_load(train_params.read_text()).get("training", {}) or {}
    schema_cfg = yaml.safe_load(base_params.read_text()).get("schema", {})
    snap_dates = ds["train_snap_dates"]
    try:
        keys = resolve_keys(ds, training_cfg, schema_cfg)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f"[1/4] config: {len(snap_dates)} snap date(s) from {params}; "
        f"segment={keys['segment_col']} item={keys['item_col']} "
        f"weight_keys={keys['weight_keys']} union_dims={keys['union_dims']}"
    )
    import pandas as pd
    snaps = [pd.Timestamp(d) for d in snap_dates]

    typer.echo(
        "[2/4] starting SparkSession + reading source… "
        "(client-template-local local[*] is far faster for this script)"
    )
    df = _load_spark_df(source)
    typer.echo("[3/4] profiling: Spark groupBy + single collect over snap dates…")
    try:
        stats = profile_stats(
            df, snaps, union_dims=keys["union_dims"],
            label_col=keys["label_col"], time_col=keys["time_col"])
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"[3/4] {len(stats)} union-granularity cell(s) profiled")
    typer.echo("[4/4] rendering self-contained HTML…")
    html = render_html(
        stats, segment_col=keys["segment_col"], item_col=keys["item_col"],
        weight_keys=keys["weight_keys"],
        default_ratio=float(ds.get("sample_ratio", 1.0)),
        target_neg_pos=target_neg_pos, alpha=alpha, w_max=w_max,
    )
    PROFILING_DIR.mkdir(parents=True, exist_ok=True)
    out = PROFILING_DIR / "sampling_overrides_editor.html"
    out.write_text(html)
    typer.echo(f"Wrote {out} ({len(stats)} cells). Open it in a browser.")
```

- [ ] **Step 4: Rewrite the `to-yaml` command**

```python
@app.command("to-yaml")
def to_yaml(
    export_json: Path = typer.Argument(..., help="browser JSON export"),
    params: Path = typer.Option(
        Path("conf/base/parameters_dataset.yaml"), help="dataset params yaml"),
    train_params: Path = typer.Option(
        Path("conf/base/parameters_training.yaml"),
        help="training params yaml — source of sample_weight_keys"),
    base_params: Path = typer.Option(
        Path("conf/base/parameters.yaml"),
        help="base params yaml — source of schema for A5/A9"),
) -> None:
    cfg = yaml.safe_load(params.read_text())
    ds_cfg = cfg.get("dataset", cfg)
    # Merge the three configs into one parameters dict for the predicates.
    merged: dict = {}
    if base_params.exists():
        merged.update(yaml.safe_load(base_params.read_text()) or {})
    merged.setdefault("dataset", {}).update(ds_cfg if "dataset" in cfg else {})
    if "dataset" not in cfg:
        merged["dataset"].update(ds_cfg)
    merged["training"] = yaml.safe_load(train_params.read_text()).get("training", {}) or {}
    if "schema" in cfg:
        merged["schema"] = cfg["schema"]
    export = json.loads(export_json.read_text())
    default_ratio = float(ds_cfg.get("sample_ratio", 1.0))
    try:
        out = grid_to_yaml(export, merged, default_ratio)
    except ValueError as e:
        typer.echo(f"ERROR: {e}", err=True)
        raise typer.Exit(code=1)
    typer.echo("# --> conf/base/parameters_dataset.yaml (under dataset:)")
    typer.echo(out["sample_ratio_overrides_yaml"])
    typer.echo("# --> conf/base/parameters_training.yaml (under training:)")
    typer.echo(out["sample_weights_yaml"])
```

Note: the test passes a single `--params` yaml that already contains `schema:`, `dataset:`, and (separately) `--train-params` with `training:`. The merge above tolerates the test's single-file `--params` (which has `schema` + `dataset` at top level) by copying `schema`/`dataset` from `cfg` and `training` from `train_params`. Verify the merge yields a dict where `get_schema`, `override_unknown_items`, and `weight_unknown_items` all resolve (the test asserts a clean `exit_code == 0`).

- [ ] **Step 5: Run to verify pass**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py::TestToYamlCli -q`
Expected: PASS (1 test).

- [ ] **Step 6: Run the FULL editor test file (regression)**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py -q`
Expected: PASS (all tests across Tasks 1-6; no leftover references to removed `build_grid`/`resolve_columns`/old shapes).

- [ ] **Step 7: Commit**

```bash
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git commit -m "feat(editor): CLI reads training params, profiles at union dims"
```

---

## Task 7: Update module docstring + parameters_training.yaml pointer

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (module docstring lines 1-20)
- Modify: `conf/base/parameters_training.yaml` (`sample_weight_keys` comment block, lines 39-48)

- [ ] **Step 1: Update the module docstring**

Replace the docstring's Usage block to document `--train-params` and the two-surface behavior:

```python
"""Sampling overrides editor — profile sample_pool, edit in browser, emit YAML.

Subcommands:
  profile <table>   Spark-profile sample_pool at union(sample_group_keys ∪
                     sample_weight_keys)\\{label} granularity, write a
                     self-contained two-tab HTML editor (ratio surface keyed by
                     sample_group_keys, weight surface keyed by
                     sample_weight_keys) to data/profiling/.
  to-yaml <json>    Convert the browser JSON export into sparse YAML snippets
                     (A5 for ratio, A9b/A9c for weights against the real
                     sample_weight_keys) for manual paste into config.

Self-contained dev tool (logic + CLI in one file), mirroring the
scripts/promote_model.py / scripts/suggest_categorical_cols.py convention —
unit-tested via ``from scripts.sampling_overrides_editor import ...`` in
tests/scripts/. Not part of the production DAG.

Usage:
  python scripts/sampling_overrides_editor.py profile ml_recsys.sample_pool
  python scripts/sampling_overrides_editor.py to-yaml \\
      data/profiling/sampling_overrides_export.json
"""
```

- [ ] **Step 2: Add an editor pointer to the `sample_weight_keys` comment**

In `conf/base/parameters_training.yaml`, append one line to the `sample_weight_keys` comment block (after line 46, before `sample_weight_keys:`):

```yaml
  #   可用 scripts/sampling_overrides_editor.py 互動式產生（weight 面依此 keys
  #   組成 key；與 dataset.sample_group_keys 可不同）。
```

- [ ] **Step 3: Run the full editor test file once more**

Run: `PYTHONPATH=src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py -q`
Expected: PASS (unchanged from Task 6 — docstring/yaml comments don't affect tests).

- [ ] **Step 4: Commit**

```bash
git add scripts/sampling_overrides_editor.py conf/base/parameters_training.yaml
git commit -m "docs(editor): document --train-params + two-surface behavior"
```

---

## Self-Review

**1. Spec coverage:**
- §3 ratio 面 maintained `[segment,item,label]` → Task 1 `resolve_keys` enforces; ratio surface in Task 3/5. ✓
- §3 weight 面 arbitrary `sample_weight_keys` → Task 1 union_dims, Task 3 weight surface. ✓
- §3/§6.1 label ∈ weight_keys rejected → Task 1 `test_rejects_label_in_weight_keys`. ✓
- §3/§6.2 empty weight_keys → Task 1 `test_empty_weight_keys`, Task 3 `test_empty_weight_keys_yields_no_weight_rows`, Task 5 `test_empty_weight_keys_hides_weight_tab`. ✓
- §4.2 profiling union dims + column guard → Task 2. ✓
- §4.4 per-cell ratio projection (Case 1 + Case 2 cross-dimension), no fine-cell rounding → Task 3 `test_case1.../test_case2.../test_no_round...`. ✓
- §4.5 remove radio, two tabs, single Export → Task 5. ✓
- §5.1 export shape → Task 4 `_export`, Task 5 `test_export_emits_self_describing_object`. ✓
- §5.2 ratio A5, weight A9b/A9c with real keys, key cross-check → Task 4. ✓
- §5.3 CLI `--train-params` → Task 6. ✓
- §5.4 python source-of-truth + JS mirror → Task 3 (python) + Task 5 (JS asserts mirror formulas). ✓
- §7 tests → Tasks 1-6 each. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows full code; every test step shows full test. ✓

**3. Type consistency:**
- `resolve_keys` returns `segment_col/item_col/label_col/time_col/weight_keys/union_dims` — used consistently in Task 2 (`union_dims`), Task 6 (`keys['segment_col']` etc.). ✓
- `profile_stats(df, snaps, *, union_dims, label_col, time_col)` → dict rows — consumed by `aggregate_surfaces(stats, ...)` and `render_html(stats, ...)`. ✓
- `aggregate_surfaces` returns `{ratio_rows, weight_rows}` with `ratio_rows[].{segment,product,ratio,kept_neg,...}` and `weight_rows[].{keys,n_pos,n_neg_post,suggested_weight}` — JS `rebuildWeight`/`exp` produce the same fields; export `ratio_rows[].{segment,product,ratio}` + `weight_rows[].{keys,weight}` consumed by `grid_to_yaml`. ✓
- `grid_to_yaml(export: dict, parameters, default_ratio)` — Task 6 `to_yaml` passes `merged` dict + export dict. ✓
- `render_html(stats, *, segment_col, item_col, weight_keys, default_ratio, ...)` — Task 6 `profile` calls with these kwargs; Task 5 tests use same. ✓

One consistency note fixed inline: ratio export rows carry only `{segment, product, ratio}` (neg_mult not needed downstream); `grid_to_yaml` reads `row["ratio"]` only. JS `exp` emits exactly that. Weight export rows carry `{keys, weight}`; `grid_to_yaml` reads both. Consistent.
