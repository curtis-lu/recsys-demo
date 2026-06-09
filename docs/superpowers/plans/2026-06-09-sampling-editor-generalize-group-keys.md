# Sampling Overrides Editor 泛化 group_keys + 試算面板 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 `scripts/sampling_overrides_editor.py` 的 ratio 面支援框架能做的任意 `sample_group_keys`(label + 0/1/多個維度),並新增即時樣本量試算面板。

**Architecture:** ratio 面比照既有 weight 面的「變長 `keys` tuple + 動態 N 欄渲染」模式;`resolve_keys` 改回傳 `ratio_dims`(= group_keys 去 label,保序)並要求 label 必在;`grid_to_yaml` 以「走訪完整 group_keys、label 位置填 0」重建 override key;HTML 加 summary 面板做 roll-up。唯一硬假設保留:label 是 pos/neg 切分軸。

**Tech Stack:** Python 3.10、Typer、PyYAML、純 stdlib HTML 模板(`str.format`);pytest。

**Spec:** `docs/superpowers/specs/2026-06-09-sampling-editor-generalize-group-keys-design.md`

---

## 慣例(每個 task 共用)

- **工作目錄**:worktree root `/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys`(以下 `<WT>`)。所有 Bash 指令以 `cd <WT> && ...` 開頭。
- **跑測試**(絕對 venv python + worktree 的 PYTHONPATH;裸跑會抓 main 的 src):
  ```bash
  cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
  PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py -q -m "not spark"
  ```
  (`-m "not spark"` 跳過 `TestProfileStats`——它測的 `profile_stats` 本案不改;最後一個 task 才連 spark 一起跑。)
- **檔案**:全程只動兩檔
  - `scripts/sampling_overrides_editor.py`
  - `tests/scripts/test_sampling_overrides_editor.py`
- **重要**:中間過程只有 Spark 入口 `profile()` 暫時與新介面不一致(它非 spark 單元測試涵蓋),在 **Task 6** 重接並驗證;**非 spark 測試在每個 task 結束都應為綠**。
- HTML 是 `str.format` 模板,字面大括號以 `{{`/`}}` 跳脫;新增 JS 必須維持跳脫,否則 `render_html` 會在 `.format()` 直接炸(`TestRenderHtml` 會接到)。

---

## Task 1: `resolve_keys` 泛化為 ratio_dims + label-required guard

**Files:**
- Modify: `scripts/sampling_overrides_editor.py:72-125`(`resolve_keys`)
- Test: `tests/scripts/test_sampling_overrides_editor.py:58-105`(`TestResolveKeys`)

- [ ] **Step 1: 改寫 `TestResolveKeys` 為新介面**

把 `tests/scripts/test_sampling_overrides_editor.py` 的 `class TestResolveKeys`(整段 58-105 行)替換為:

```python
class TestResolveKeys:
    _SCHEMA = {"columns": {"item": "prod_name", "label": "label",
                           "time": "snap_date"}}

    def test_case1_weight_subset_of_group_keys(self):
        # group=[seg,item,label], weight=[item] -> ratio_dims=[seg,item]
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["prod_name"]}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["label_col"] == "label"
        assert out["time_col"] == "snap_date"
        assert out["weight_keys"] == ["prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_case2_weight_adds_carry_dim_extends_union(self):
        # weight=[risk_attr,item] adds risk_attr to the union (label excluded)
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {"sample_weight_keys": ["risk_attr", "prod_name"]}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["weight_keys"] == ["risk_attr", "prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name", "risk_attr"]

    def test_empty_weight_keys_union_is_ratio_dims(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name", "label"]},
            {}, self._SCHEMA)
        assert out["weight_keys"] == []
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name"]
        assert out["union_dims"] == ["cust_segment_typ", "prod_name"]

    def test_accepts_segment_label_only(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ"]
        assert out["union_dims"] == ["cust_segment_typ"]

    def test_accepts_item_label_only(self):
        out = resolve_keys(
            {"sample_group_keys": ["prod_name", "label"]},
            {"sample_weight_keys": ["prod_name"]}, self._SCHEMA)
        assert out["ratio_dims"] == ["prod_name"]
        assert out["union_dims"] == ["prod_name"]

    def test_accepts_multi_dim(self):
        out = resolve_keys(
            {"sample_group_keys": ["cust_segment_typ", "prod_name",
                                   "risk_attr", "label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == ["cust_segment_typ", "prod_name", "risk_attr"]

    def test_accepts_label_only_global(self):
        out = resolve_keys(
            {"sample_group_keys": ["label"]},
            {"sample_weight_keys": []}, self._SCHEMA)
        assert out["ratio_dims"] == []
        assert out["union_dims"] == []

    def test_rejects_group_keys_without_label(self):
        with pytest.raises(ValueError, match="label"):
            resolve_keys({"sample_group_keys": ["cust_segment_typ", "prod_name"]},
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

- [ ] **Step 2: 跑測試確認 FAIL**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestResolveKeys
```
Expected: FAIL(`KeyError: 'ratio_dims'` / 舊 guard 仍擋掉 `[seg,label]`)。

- [ ] **Step 3: 改寫 `resolve_keys`**

把 `scripts/sampling_overrides_editor.py` 的 `def resolve_keys(...)`(整段 72-125 行)替換為:

```python
def resolve_keys(dataset_cfg: dict, training_cfg: dict, schema_cfg: dict) -> dict:
    """Resolve ratio dims, weight keys, and the finest profiling granularity.

    The ratio surface is keyed by ``ratio_dims`` = ``sample_group_keys`` minus
    the label column (order preserved); it may be any length (0, 1, or many) —
    matching what the framework's ``select_keys`` supports. ``label`` MUST be a
    ``sample_group_key``: the editor splits each cell into n_pos/n_neg via
    sum(label) and fixes the label component to "0" on export, so a group-key
    set without label is incompatible (hand-write those overrides instead).

    The weight surface is keyed by ``training.sample_weight_keys`` (arbitrary
    available columns). ``union_dims`` is the finest granularity to profile at:
    ``(sample_group_keys ∪ sample_weight_keys) \\ {label}``, ratio dims first.
    ``label`` must be absent from ``sample_weight_keys`` (the per-group
    n_pos/n_neg model splits on label).
    """
    cols = schema_cfg.get("columns", {})
    try:
        label_col, time_col = cols["label"], cols["time"]
    except KeyError as exc:
        raise ValueError(
            f"schema.columns is missing {exc}; cannot resolve profiling "
            "columns. Check the base parameters yaml."
        ) from exc
    group_keys = list(dataset_cfg.get("sample_group_keys", []))
    if label_col not in group_keys:
        raise ValueError(
            f"sampling editor requires the label column {label_col!r} in "
            f"sample_group_keys (it is the pos/neg split axis); got "
            f"{group_keys}. For label-free group keys, hand-write "
            "sample_ratio_overrides."
        )
    weight_keys = list(training_cfg.get("sample_weight_keys") or [])
    if label_col in weight_keys:
        raise ValueError(
            f"sampling editor cannot edit weights keyed by the label column "
            f"{label_col!r} (per-group n_pos/n_neg is derived by splitting on "
            f"label). Remove it from sample_weight_keys, or hand-write those "
            f"sample_weights."
        )
    # ratio dims = group keys minus label, order preserved (may be empty).
    ratio_dims = [k for k in group_keys if k != label_col]
    # union dims: ratio dims first, then any extra weight-key columns.
    union_dims: list[str] = list(ratio_dims)
    for k in weight_keys:
        if k not in union_dims:
            union_dims.append(k)
    return {
        "ratio_dims": ratio_dims,
        "label_col": label_col,
        "time_col": time_col,
        "weight_keys": weight_keys,
        "union_dims": union_dims,
    }
```

- [ ] **Step 4: 跑測試確認 PASS**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestResolveKeys
```
Expected: PASS。(其他 class 此時可能 FAIL——Task 2-4 處理。)

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): resolve_keys 泛化為 ratio_dims + label-required guard"
```

---

## Task 2: `aggregate_surfaces` ratio 面改以 ratio_dims tuple 聚合

**Files:**
- Modify: `scripts/sampling_overrides_editor.py:128-209`(`aggregate_surfaces`)
- Test: `tests/scripts/test_sampling_overrides_editor.py:363-442`(`TestAggregateSurfaces`)

- [ ] **Step 1: 改寫 `TestAggregateSurfaces`**

把整段 `class TestAggregateSurfaces`(363-442 行)替換為(沿用原 4 個 fine cells;把 `segment_col=,item_col=` 改成 `ratio_dims=`,`rr` 改以 `tuple(r["keys"])` 取鍵,並新增多種形狀):

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
        nm = {("mass", "a"): 5.0, ("hnw", "a"): 1e9,
              ("mass", "b"): 1e9, ("hnw", "b"): 1e9}
        out = aggregate_surfaces(
            self._STATS, nm, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_keys=["prod_name"], alpha=0.5, w_max=5.0, default_neg_mult=5.0)
        rr = {tuple(r["keys"]): r for r in out["ratio_rows"]}
        assert abs(rr[("mass", "a")]["ratio"] - (5 * 100 / 9000)) < 1e-9
        assert rr[("mass", "a")]["kept_neg"] == 500
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("a",)]["n_pos"] == 160          # 100+60, unchanged by downsample
        assert wr[("a",)]["n_neg_post"] == 1000     # round(9000*0.0556 + 500*1.0)
        assert wr[("b",)]["n_neg_post"] == 2040     # 2000+40, fully kept

    def test_case2_cross_dimension_shares_ratio_over_dropped_dim(self):
        # weight keys = [risk_attr, item]; risk_attr dropped when projecting to
        # the ratio (segment,item) cell, so both risk values share ratio[mass,a].
        stats = [
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "lo",
             "n_pos": 60, "n_neg": 6000},
            {"cust_segment_typ": "mass", "prod_name": "a", "risk_attr": "hi",
             "n_pos": 40, "n_neg": 3000},
        ]
        nm = {("mass", "a"): 5.0}   # ratio = 5*(60+40)/(6000+3000) = 0.05556
        out = aggregate_surfaces(
            stats, nm, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_keys=["risk_attr", "prod_name"], alpha=0.5, w_max=5.0,
            default_neg_mult=5.0)
        ratio = 5 * 100 / 9000
        wr = {tuple(r["keys"]): r for r in out["weight_rows"]}
        assert wr[("lo", "a")]["n_neg_post"] == round(6000 * ratio)
        assert wr[("hi", "a")]["n_neg_post"] == round(3000 * ratio)
        assert wr[("lo", "a")]["keys"] == ["lo", "a"]

    def test_no_round_until_display_totals_match(self):
        stats = [
            {"cust_segment_typ": "mass", "prod_name": "a", "n_pos": 1, "n_neg": 100},
            {"cust_segment_typ": "hnw",  "prod_name": "a", "n_pos": 1, "n_neg": 100},
        ]
        nm = {("mass", "a"): 0.5, ("hnw", "a"): 0.5}   # ratio = 0.5*1/100 = 0.005
        out = aggregate_surfaces(
            stats, nm, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_keys=["prod_name"], alpha=0.5, w_max=5.0, default_neg_mult=0.5)
        kept_total = sum(r["kept_neg"] for r in out["ratio_rows"])
        post_total = sum(r["n_neg_post"] for r in out["weight_rows"])
        assert kept_total == 0
        assert post_total == 1
        assert kept_total != post_total

    def test_empty_weight_keys_yields_no_weight_rows(self):
        out = aggregate_surfaces(
            self._STATS, {}, ratio_dims=["cust_segment_typ", "prod_name"],
            weight_keys=[], alpha=0.5, w_max=5.0, default_neg_mult=5.0)
        assert out["weight_rows"] == []
        assert len(out["ratio_rows"]) == 4

    def test_ratio_surface_by_segment_only(self):
        # ratio_dims=[segment]: products collapse into one row per segment
        nm = {("mass",): 1e9, ("hnw",): 1e9}   # keep-all
        out = aggregate_surfaces(
            self._STATS, nm, ratio_dims=["cust_segment_typ"],
            weight_keys=[], alpha=0.5, w_max=5.0, default_neg_mult=5.0)
        rr = {tuple(r["keys"]): r for r in out["ratio_rows"]}
        assert rr[("mass",)]["n_pos"] == 180 and rr[("mass",)]["n_neg"] == 11000
        assert rr[("hnw",)]["n_pos"] == 60 and rr[("hnw",)]["n_neg"] == 540

    def test_ratio_surface_global_single_row(self):
        out = aggregate_surfaces(
            self._STATS, {}, ratio_dims=[], weight_keys=[],
            alpha=0.5, w_max=5.0, default_neg_mult=1e9)
        assert len(out["ratio_rows"]) == 1
        r = out["ratio_rows"][0]
        assert r["keys"] == []
        assert r["n_pos"] == 240 and r["n_neg"] == 11540
```

- [ ] **Step 2: 跑測試確認 FAIL**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestAggregateSurfaces
```
Expected: FAIL(`TypeError: unexpected keyword 'ratio_dims'`)。

- [ ] **Step 3: 改寫 `aggregate_surfaces`**

把 `scripts/sampling_overrides_editor.py` 的 `def aggregate_surfaces(...)`(整段 128-209 行)替換為:

```python
def aggregate_surfaces(
    stats: list[dict],
    neg_mults: dict,
    *,
    ratio_dims: list,
    weight_keys: list,
    alpha: float,
    w_max: float,
    default_neg_mult: float,
) -> dict:
    """Roll finest-granularity stats up into the ratio and weight surfaces.

    Pure: no Spark, no I/O. ``stats`` are union-granularity dict rows from
    profile_stats. ``ratio_dims`` is the (possibly empty) list of ratio-surface
    dimensions (sample_group_keys minus label). ``neg_mults`` maps a
    ``tuple(row[d] for d in ratio_dims)`` -> target neg:pos multiplier
    (missing -> ``default_neg_mult``).

    Ratio surface: aggregate fine cells to the ratio_dims tuple; each row's
    keep-rate ``ratio = clamp(neg_mult * n_pos / n_neg, 0, 1)`` (n_pos==0 ->
    1.0, keep all negatives). Weight surface: aggregate to the weight_keys
    tuple; n_pos unchanged by downsampling, n_neg_post = sum of
    ``n_neg * ratio[ fine cell's ratio_dims projection ]`` (negatives summed as
    fractions, rounded only for display). Both ratio_rows and weight_rows carry
    a variable-length ``keys`` list.
    """
    # --- ratio surface: aggregate to ratio_dims tuple ---
    racc: dict = {}
    for s in stats:
        k = tuple(s[d] for d in ratio_dims)
        a = racc.setdefault(k, [0, 0])
        a[0] += s["n_pos"]
        a[1] += s["n_neg"]
    ratio_by_key: dict = {}
    ratio_rows: list[dict] = []
    for key, (npos, nneg) in racc.items():
        mult = float(neg_mults.get(key, default_neg_mult))
        # n_pos == 0 -> keep all negatives (ratio 1.0): suggest_ratio would give
        # 0 and silently drop a cold cell's entire negative set. Any JS mirror
        # MUST apply the same n_pos==0 guard.
        ratio = 1.0 if npos == 0 else suggest_ratio(npos, nneg, mult)
        ratio_by_key[key] = ratio
        kept = round(nneg * ratio)
        total = npos + kept
        ratio_rows.append({
            "keys": list(key), "n_pos": npos, "n_neg": nneg,
            "pos_rate": (npos / (npos + nneg) if npos + nneg else 0.0),
            "neg_mult": mult, "ratio": ratio, "kept_neg": kept,
            "new_pos_rate": (npos / total if total else 0.0),
        })
    ratio_rows.sort(key=lambda r: tuple(str(x) for x in r["keys"]))

    # --- weight surface: aggregate to weight_keys tuple (post-downsample) ---
    weight_rows: list[dict] = []
    if weight_keys:
        wacc: dict = {}
        for s in stats:
            wk = tuple(s[k] for k in weight_keys)
            rk = tuple(s[d] for d in ratio_dims)
            a = wacc.setdefault(wk, [0, 0.0])
            a[0] += s["n_pos"]
            a[1] += s["n_neg"] * ratio_by_key[rk]
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

- [ ] **Step 4: 跑測試確認 PASS**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestAggregateSurfaces
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): aggregate_surfaces ratio 面以 ratio_dims tuple 聚合"
```

---

## Task 3: `grid_to_yaml` override key 以完整 group_keys 重建(label 位置填 0)

**Files:**
- Modify: `scripts/sampling_overrides_editor.py:33-37`(import)、`:212-288`(`grid_to_yaml`)
- Test: `tests/scripts/test_sampling_overrides_editor.py:111-189`(`_params`/`_export`/`TestGridToYaml`)、`:448-477`(`TestToYamlCli`)

- [ ] **Step 1: 改寫 `_params`/`_export` 與 `TestGridToYaml`、`TestToYamlCli` 的 ratio_rows**

把 `_params` 與 `_export`(111-128 行)替換為(`_params` 加 `group_keys` 參數):

```python
def _params(weight_keys=("prod_name",),
            group_keys=("cust_segment_typ", "prod_name", "label")):
    return {
        "schema": {"columns": {"item": "prod_name"},
                   "categorical_values": {"prod_name": ["a", "b"]}},
        "dataset": {"prepare_model_input": {"categorical_columns": ["prod_name"]},
                    "sample_group_keys": list(group_keys)},
        "training": {"sample_weight_keys": list(weight_keys)},
    }


def _export(ratio_rows, weight_rows, *, group_keys=None, weight_keys=None):
    return {
        "sample_group_keys": group_keys or ["cust_segment_typ", "prod_name", "label"],
        # None sentinel (not []) so a caller can pass [] to mean "no weight keys".
        "sample_weight_keys": weight_keys if weight_keys is not None else ["prod_name"],
        "ratio_rows": ratio_rows,
        "weight_rows": weight_rows,
    }
```

把 `class TestGridToYaml`(131-189 行)替換為(ratio_rows 由 `{segment,product}` 改 `{keys}`,新增多維/無 segment 案例):

```python
class TestGridToYaml:
    def test_sparse_emits_only_non_default(self):
        export = _export(
            ratio_rows=[
                {"keys": ["mass", "a"], "ratio": 0.5},
                {"keys": ["mass", "b"], "ratio": 1.0}],
            weight_rows=[{"keys": ["a"], "weight": 1.0},
                         {"keys": ["b"], "weight": 3.0}])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        sw = yaml.safe_load(out["sample_weights_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.5}}
        assert sw == {"sample_weights": {"b": 3.0}}

    def test_weight_key_joined_in_weight_keys_order(self):
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
            ratio_rows=[{"keys": ["mass", "zzz"], "ratio": 0.5}],
            weight_rows=[])
        with pytest.raises(ValueError, match=r"zzz"):
            grid_to_yaml(export, _params(), default_ratio=1.0)

    def test_unknown_product_weight_only_raises_with_real_weight_keys(self):
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

    def test_zero_pos_group_override_round_trips(self):
        export = _export(
            ratio_rows=[{"keys": ["mass", "a"], "ratio": 0.3}],
            weight_rows=[])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.3}}

    def test_no_segment_group_keys_reconstructs_key(self):
        gk = ["prod_name", "label"]
        export = _export(
            ratio_rows=[{"keys": ["a"], "ratio": 0.5}], weight_rows=[],
            group_keys=gk)
        out = grid_to_yaml(export, _params(group_keys=gk), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"a|0": 0.5}}

    def test_multi_dim_group_keys_reconstructs_key(self):
        gk = ["cust_segment_typ", "prod_name", "risk_attr", "label"]
        export = _export(
            ratio_rows=[{"keys": ["mass", "a", "lo"], "ratio": 0.5}],
            weight_rows=[], group_keys=gk)
        out = grid_to_yaml(export, _params(group_keys=gk), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|lo|0": 0.5}}
```

把 `TestToYamlCli.test_to_yaml_prints_both_blocks` 內的 `ratio_rows`(466 行)由
`[{"segment": "mass", "product": "a", "ratio": 0.5}]` 改為
`[{"keys": ["mass", "a"], "ratio": 0.5}]`(其餘不動;斷言仍是 `mass|a|0`)。

- [ ] **Step 2: 跑測試確認 FAIL**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k "TestGridToYaml or TestToYamlCli"
```
Expected: FAIL(舊 `grid_to_yaml` 讀 `row['segment']` → `KeyError: 'segment'`)。

- [ ] **Step 3: 加 import + 改寫 `grid_to_yaml`**

在 import 區塊(`scripts/sampling_overrides_editor.py:33-37`)的 `from recsys_tfb.core.consistency import (...)` 之後加一行:

```python
from recsys_tfb.core.schema import get_schema
```

把 `grid_to_yaml` 內「組 overrides」的迴圈(目前 243-247 行)替換為:

```python
    label_col = get_schema(parameters)["label"]
    overrides: dict[str, float] = {}
    for row in export.get("ratio_rows", []):
        ratio = float(row["ratio"])
        if ratio == default_ratio:
            continue
        # Walk the full group_keys order: label position -> "0", every other
        # position -> the next value from this row's ratio_dims keys.
        vals = iter(row["keys"])
        parts = [_NEG_LABEL if k == label_col else str(next(vals))
                 for k in cfg_group]
        overrides["|".join(parts)] = ratio
```

(其餘——`weights` 迴圈、probe 組裝、A5/A9b/A9c 驗證、回傳的兩段 YAML——皆不變。`cfg_group` 變數在函式上方已存在。)

- [ ] **Step 4: 跑測試確認 PASS**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k "TestGridToYaml or TestToYamlCli"
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): grid_to_yaml 以完整 group_keys 重建 override key(label 填 0)"
```

---

## Task 4: HTML/JS 模板泛化 SEG/ITEM → GKEYS + render_html 簽名

**Files:**
- Modify: `scripts/sampling_overrides_editor.py:291-570`(`_HTML_TEMPLATE` + `render_html`)
- Test: `tests/scripts/test_sampling_overrides_editor.py:195-312`(`TestRenderHtml`)

- [ ] **Step 1: 改 `TestRenderHtml` 的 `_KW` 與相關斷言**

把 `_KW`(201-202 行)替換為:

```python
    _KW = dict(ratio_dims=["cust_segment_typ", "prod_name"],
               group_keys=["cust_segment_typ", "prod_name", "label"],
               weight_keys=["prod_name"], label_col="label", default_ratio=1.0)
```

在 `test_self_contained_and_embeds_stats_and_keys` 中,把
```python
        assert 'const SEG="cust_segment_typ"' in html
        assert 'const ITEM="prod_name"' in html
```
替換為:
```python
        assert 'const GKEYS=["cust_segment_typ", "prod_name"]' in html
        assert 'const GROUP_KEYS=["cust_segment_typ", "prod_name", "label"]' in html
        assert 'const LABEL="label"' in html
```

在 `test_weight_tab_recomputes_post_downsample_from_ratio_edits` 中,把
```python
        assert "function ratioBySI(" in html
```
替換為 `assert "function ratioByKey(" in html`;把
```python
        assert "s.n_neg*rbs.get(" in html
```
替換為 `assert "s.n_neg*rbk.get(" in html`。

把 `test_export_emits_self_describing_object` 中
```python
        assert "'|0'" in html
```
替換為:
```python
        assert "function ratioKey(" in html
        assert "k===LABEL?'0'" in html
```

把 `test_empty_weight_keys_hides_weight_tab`(262-268 行)替換為:
```python
    def test_empty_weight_keys_hides_weight_tab(self):
        html = render_html(self._STATS, ratio_dims=["cust_segment_typ", "prod_name"],
                           group_keys=["cust_segment_typ", "prod_name", "label"],
                           weight_keys=[], label_col="label", default_ratio=1.0)
        assert "const WKEYS=[]" in html
        assert "WKEYS.length" in html
```

把 `test_escapes_cell_values_and_threads_label_col`(282-289 行)替換為:
```python
    def test_escapes_cell_values_and_threads_label_col(self):
        html = render_html(self._STATS, ratio_dims=["cust_segment_typ", "prod_name"],
                           group_keys=["cust_segment_typ", "prod_name", "label"],
                           weight_keys=["prod_name"], label_col="label",
                           default_ratio=1.0)
        assert "function esc(" in html
        assert "r.keys.map(v=>" in html and "esc(v)" in html
        assert 'const LABEL="label"' in html
        assert "sample_group_keys:GROUP_KEYS" in html
```

(其餘 `TestRenderHtml` 測試不動——`buildRatio`/`keepRate`/`nm*np/nn`/`achMult`/`負樣本倍率`/`data-k=neg_mult`/`class="calc rt"`/`data-k=ratio_direct`/`r.ratio_direct=1`/`parseFloat(r.ratio_direct)`/`if(!editingRatio) tr.querySelector('td.rt')`/`neg:pos 無定義`/`維持 ratio 1.0` 不在 等斷言在新模板都仍成立。)

- [ ] **Step 2: 跑測試確認 FAIL**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestRenderHtml
```
Expected: FAIL(舊模板有 `const SEG`、無 `const GKEYS`;且 `render_html` 不收 `ratio_dims`/`group_keys`)。

- [ ] **Step 3a: 改模板常數區塊**

把 `_HTML_TEMPLATE` 內常數區塊(345-354 行)替換為:

```
const STATS={stats_json};
const GKEYS={gkeys_json};
const GROUP_KEYS={group_keys_json};
const LABEL="{label_col}";
const WKEYS={wkeys_json};
const DR={default_ratio};
const R={target_neg_pos};
const ALPHA={alpha};
const WMAX={w_max};
const SEP='\\u0001';
```

- [ ] **Step 3b: 改 `buildRatio`**(364-373 行)替換為:

```
function buildRatio(){{
 const m=new Map();
 STATS.forEach(s=>{{ const keys=GKEYS.map(k=>s[k]),ks=keys.join(SEP);
  const a=m.get(ks)||{{keys:keys,n_pos:0,n_neg:0}};
  a.n_pos+=s.n_pos; a.n_neg+=s.n_neg; m.set(ks,a); }});
 const rows=[...m.values()];
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.suggested_neg_mult=R; r.ratio_direct=1;
  r.keys.forEach((v,j)=>r['k'+j]=v); }});
 return rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
```

- [ ] **Step 3c: 改 `ratioBySI` → `ratioByKey`**(377-382 行)替換為:

```
function ratioByKey(){{
 const m=new Map();
 RATIO.forEach(r=>m.set(r.keys.join(SEP),
  keepRate(parseFloat(r.suggested_neg_mult),r.n_pos,r.n_neg)));
 return m;
}}
```

- [ ] **Step 3d: 改 `rebuildWeight`**(385-398 行)替換為:

```
function rebuildWeight(){{
 if(!WKEYS.length){{ WEIGHT=[]; return; }}
 const prev=new Map(WEIGHT.map(w=>[w.keyStr,w.weight]));
 const rbk=ratioByKey(),m=new Map();
 STATS.forEach(s=>{{ const wk=WKEYS.map(k=>s[k]),ks=wk.join('|');
  const rk=GKEYS.map(k=>s[k]).join(SEP);
  const a=m.get(ks)||{{keys:wk,keyStr:ks,n_pos:0,_nn:0}};
  a.n_pos+=s.n_pos; a._nn+=s.n_neg*rbk.get(rk); m.set(ks,a); }});
 const rows=[...m.values()],med=median(rows.map(r=>r.n_pos));
 rows.forEach(r=>{{ r.n_neg_post=Math.round(r._nn);
  const t=r.n_pos+r.n_neg_post; r.pos_rate_post=(t>0?r.n_pos/t:0);
  r.suggested_weight=+suggestWeight(r.n_pos,med,ALPHA,WMAX).toFixed(4);
  r.weight=prev.has(r.keyStr)?prev.get(r.keyStr):r.suggested_weight; }});
 WEIGHT=rows.sort((x,y)=>y.n_pos-x.n_pos);
}}
```

- [ ] **Step 3e: 改 `renderRatio`**(443-468 行)替換為:

```
function renderRatio(data,idx){{
 document.querySelector('#g thead').innerHTML=
  `<tr>`+GKEYS.map((k,j)=>`<th onclick="sortBy('k${{j}}')">${{k}} ⇅</th>`).join('')+
  `<th class=stat onclick="sortBy('n_pos')">n_pos ⇅</th>`+
  `<th class=stat onclick="sortBy('n_neg')">n_neg ⇅</th>`+
  `<th class=stat>pos_rate</th><th>負樣本倍率</th><th class=calc>ratio</th>`+
  `<th class=calc>實際倍率</th><th class=calc>kept_neg</th>`+
  `<th class=calc>new_pos_rate</th></tr>`;
 const tb=document.querySelector('#g tbody'); tb.innerHTML='';
 idx.forEach(i=>{{ const r=data[i],pv=preview(r,parseFloat(r.suggested_neg_mult));
  const am=achMult(pv),tr=document.createElement('tr'),noPos=r.n_pos<=0;
  const negMultCell=noPos
   ?`<td class=calc title="無正樣本，倍率無定義；改用 ratio 欄直接設保留率">—</td>`
   :`<td class=edit contenteditable data-k=neg_mult data-i=${{i}} oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`;
  const ratioCell=noPos
   ?`<td class="edit rt" contenteditable data-k=ratio_direct data-i=${{i}} oninput="recalc(this)">${{r.ratio_direct}}</td>`
   :`<td class="calc rt">${{pv.ratio}}</td>`;
  tr.innerHTML=r.keys.map(v=>`<td>${{esc(v)}}</td>`).join('')+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   negMultCell+ratioCell+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`;
  tb.appendChild(tr); }});
}}
```

- [ ] **Step 3f: 改 `render()` 的 cols 行**(486 行)

把
```
 const cols=tab==='ratio'?['segment','product']:WKEYS.map((_,j)=>'k'+j);
```
替換為:
```
 const cols=(tab==='ratio'?GKEYS:WKEYS).map((_,j)=>'k'+j);
```

- [ ] **Step 3g: 改 `exp()` 並前置 `ratioKey()`**(511-536 行)替換為:

```
function ratioKey(keys){{
 let it=0;
 return GROUP_KEYS.map(k=>k===LABEL?'0':String(keys[it++])).join('|');
}}
function exp(kind){{
 syncEdits(); rebuildWeight();
 const ratio_rows=RATIO.map(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  return {{keys:r.keys,ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))}}; }});
 const weight_rows=WEIGHT.map(r=>({{keys:r.keys,weight:parseFloat(r.weight)}}));
 const o={{sample_group_keys:GROUP_KEYS,sample_weight_keys:WKEYS,
  ratio_rows:ratio_rows,weight_rows:weight_rows}};
 if(kind==='json'){{
  document.getElementById('out').textContent=JSON.stringify(o,null,2);
  const b=new Blob([JSON.stringify(o,null,2)],{{type:'application/json'}});
  const a=document.createElement('a'); a.href=URL.createObjectURL(b);
  a.download='sampling_overrides_export.json'; a.click();
 }}else{{
  const ov={{}},sw={{}};
  ratio_rows.forEach(r=>{{ if(r.ratio!==DR) ov[ratioKey(r.keys)]=r.ratio; }});
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
```

- [ ] **Step 3h: 改說明文字**(313-316 行)

把 `<details>` 內
```
<p><b>兩個面（分頁切換，各自獨立）</b>：<code>ratio 面</code>依
<code>sample_group_keys</code>（segment×item）調抽樣下採樣；<code>weight 面</code>依
```
中的 `（segment×item）` 改為 `（label 以外的任意維度）`。

- [ ] **Step 3i: 改 `render_html` 簽名與 `.format` 參數**(541-570 行)替換為:

```python
def render_html(
    stats: list[dict],
    *,
    ratio_dims: list,
    group_keys: list,
    label_col: str,
    weight_keys: list,
    default_ratio: float,
    target_neg_pos: float = 5.0,
    alpha: float = 0.5,
    w_max: float = 5.0,
) -> str:
    """Render a self-contained two-tab HTML editor (pure stdlib, no assets).

    ``stats`` are union-granularity dict rows from profile_stats; the browser
    mirrors aggregate_surfaces in JS to build the ratio and weight surfaces
    live. ``ratio_dims`` (= sample_group_keys minus label) keys the ratio
    surface; ``group_keys`` (the full sample_group_keys incl. label, in order)
    is used to reconstruct override keys on export. The tuning knobs are
    surfaced in the help text so it reflects the configured values.
    """
    return _HTML_TEMPLATE.format(
        stats_json=json.dumps(stats),
        gkeys_json=json.dumps(ratio_dims),
        group_keys_json=json.dumps(group_keys),
        label_col=label_col,
        wkeys_json=json.dumps(weight_keys),
        default_ratio=default_ratio,
        target_neg_pos=target_neg_pos,
        alpha=alpha,
        w_max=w_max,
    )
```

- [ ] **Step 4: 跑測試確認 PASS**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestRenderHtml
```
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): HTML ratio 面泛化 SEG/ITEM→GKEYS、動態 N 欄、export 帶 keys"
```

---

## Task 5: 試算面板(summary)— 總計 + 依單一維度分組

**Files:**
- Modify: `scripts/sampling_overrides_editor.py`(`_HTML_TEMPLATE`:body + JS)
- Test: `tests/scripts/test_sampling_overrides_editor.py`(`TestRenderHtml` 新增一測試)

- [ ] **Step 1: 新增 summary 測試**

在 `class TestRenderHtml` 末尾(`test_help_text_describes_zero_pos_editable_ratio` 之後)新增:

```python
    def test_summary_panel_present_and_groups_by_dim(self):
        html = render_html(self._STATS, **self._KW)
        assert "function renderSummary(" in html
        assert "function initSummary(" in html
        assert "id=grp" in html or 'id="grp"' in html
        assert "id=summary" in html or 'id="summary"' in html
        # grand total + per-group pos_rate roll-up over RATIO via preview()
        assert "a.np/t" in html
        # recomputed live on every cell edit
        assert "renderSummary()" in html
```

- [ ] **Step 2: 跑測試確認 FAIL**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" \
-k "TestRenderHtml and summary"
```
Expected: FAIL(`renderSummary` 尚不存在)。

- [ ] **Step 3a: body 加 summary 容器**

把 `_HTML_TEMPLATE` body 的
```
<div id="note"></div>
<input id="flt" placeholder="篩選…" oninput="flt()">
```
替換為:
```
<div id="note"></div>
<div id="sumbox"><label>分組試算（下採後）：<select id="grp" onchange="renderSummary()"></select></label>
<div id="summary"></div></div>
<input id="flt" placeholder="篩選…" oninput="flt()">
```

- [ ] **Step 3b: 加 `initSummary` / `renderSummary`**

在 `<script>` 內 `function exp(kind)` 之前(即 `ratioKey` 之前)插入:

```
function initSummary(){{
 const sel=document.getElementById('grp');
 sel.innerHTML='<option value="">（全部）</option>'+
  GKEYS.map((k,j)=>`<option value="k${{j}}">${{k}}</option>`).join('');
}}
function renderSummary(){{
 const box=document.getElementById('sumbox');
 if(tab!=='ratio'){{ box.style.display='none'; return; }}
 box.style.display='';
 syncEdits();
 const by=document.getElementById('grp').value;
 const agg=new Map();
 RATIO.forEach(r=>{{ const pv=preview(r,parseFloat(r.suggested_neg_mult));
  const np=r.n_pos,kn=parseInt(pv.kn)||0;
  const g=(by===''?'（全部）':String(r[by]));
  const a=agg.get(g)||{{np:0,kn:0}}; a.np+=np; a.kn+=kn; agg.set(g,a); }});
 let h='<table><thead><tr><th>分組</th><th>n_pos</th><th>n_neg(後)</th>'+
  '<th>總數</th><th>pos_rate</th></tr></thead><tbody>';
 [...agg.entries()].sort().forEach(([g,a])=>{{ const t=a.np+a.kn;
  h+=`<tr><td>${{esc(g)}}</td><td>${{a.np}}</td><td>${{a.kn}}</td>`+
   `<td>${{t}}</td><td>${{(t>0?a.np/t:0).toFixed(4)}}</td></tr>`; }});
 h+='</tbody></table>';
 document.getElementById('summary').innerHTML=h;
}}
```

- [ ] **Step 3c: 把 summary 接上即時更新**

(1) `recalc(td)` 函式結尾(`tr.querySelector('td.pr').textContent=pv.pr;` 之後、`}}` 之前)加一行:
```
 renderSummary();
```
(2) `render()` 函式結尾把
```
 if(tab==='ratio') renderRatio(data,idx); else renderWeight(data,idx);
```
替換為:
```
 if(tab==='ratio'){{ renderRatio(data,idx); renderSummary(); }}
 else renderWeight(data,idx);
```
(3) 啟動處(檔案最末 `setTab('ratio');` 之前)加一行:
```
initSummary();
```

- [ ] **Step 4: 跑測試確認 PASS（含整個 TestRenderHtml 不回歸）**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark" -k TestRenderHtml
```
Expected: PASS（含新 summary 測試）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): 加試算面板(總計 + 依單一維度分組,即時連動)"
```

---

## Task 6: 重接 `profile()` CLI + 全套件 + 手動 smoke

**Files:**
- Modify: `scripts/sampling_overrides_editor.py:637-695`(`profile` command)

- [ ] **Step 1: 改 `profile()` 用新 `resolve_keys` 與 `render_html` 簽名**

把 `[1/4] config:` 的 `typer.echo(...)`(662-667 行)替換為:

```python
    typer.echo(
        f"[1/4] config: {len(snap_dates)} snap date(s) from {params}; "
        f"ratio_dims={keys['ratio_dims']} label={keys['label_col']} "
        f"weight_keys={keys['weight_keys']} union_dims={keys['union_dims']}"
    )
```

把 `render_html(...)` 呼叫(686-691 行)替換為:

```python
    html = render_html(
        stats, ratio_dims=keys["ratio_dims"],
        group_keys=list(ds.get("sample_group_keys", [])),
        label_col=keys["label_col"], weight_keys=keys["weight_keys"],
        default_ratio=float(ds.get("sample_ratio", 1.0)),
        target_neg_pos=target_neg_pos, alpha=alpha, w_max=w_max,
    )
```

- [ ] **Step 2: 全套件(非 spark)PASS**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -m "not spark"
```
Expected: 全綠。

- [ ] **Step 3: spark 子集 PASS(profile_stats 不受影響,確認沒回歸)**

`TestProfileStats` 用 conftest `spark` fixture(cold start 偏慢),背景執行不阻塞:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
SPARK_CONF_DIR=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/conf/spark-local \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
tests/scripts/test_sampling_overrides_editor.py -q -k TestProfileStats
```
Expected: PASS。(若本機無 `conf/spark-local`,移除該 env var 用預設 session。)

- [ ] **Step 4(手動 smoke,可選): 對本機 sample_pool 跑一次 `profile`**

驗證 CLI 端到端產出 HTML(用一個「不含 segment」的 group_keys 證明泛化生效)。先確認本機有 `data/sample_pool.parquet`;暫時把 `conf/base/parameters_dataset.yaml` 的 `sample_group_keys` 改成 `[prod_name, label]` 後:

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
SPARK_CONF_DIR=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/conf/spark-local \
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys/src \
/Users/curtislu/projects/recsys_tfb/.venv/bin/python scripts/sampling_overrides_editor.py \
profile data/sample_pool.parquet
```
Expected: 印出 `ratio_dims=['prod_name'] ...` 與 `Wrote data/profiling/sampling_overrides_editor.html`。檢查後**還原** `sample_group_keys`。(此步碰 Spark、>2min 時用 background 執行;非必過閘,單元測試才是。)

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
git add scripts/sampling_overrides_editor.py && \
git commit -q -m "feat(sampling-editor): profile CLI 重接 ratio_dims/group_keys 新介面"
```

---

## 完成後

- 更新 graphify(改了 code）:
  ```bash
  cd /Users/curtislu/projects/recsys_tfb/.worktrees/sampling-editor-general-groupkeys && \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"
  ```
- 開 PR(由 main 為 base)。**model promote / 任何外部觸發不自動做**。

## Self-Review(planning 階段已核)

- **Spec 覆蓋**:§4.1→Task1、§4.2→Task2、§4.3→Task3、§4.4/§4.5→Task4、§5→Task5、§3 label-required→Task1 guard+測試、profile 重接→Task6。皆有對應。
- **型別一致**:`resolve_keys` 回 `ratio_dims/label_col/time_col/weight_keys/union_dims`(不再有 `segment_col/item_col`),Task2/4/6 一致使用;`aggregate_surfaces`/JS 一律 `keys` tuple;`ratioBySI`→`ratioByKey`(rbs→rbk)在模板與測試同步更名;`render_html(ratio_dims, group_keys, label_col, ...)` 簽名在 Task4 定義、Task6 呼叫一致。
- **無 placeholder**:每個 code step 皆為完整可貼上的程式碼。
