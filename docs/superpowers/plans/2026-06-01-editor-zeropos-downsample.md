# Editor 0-positive Group Direct Downsample — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the sampling-overrides HTML editor set a direct negative-keep-rate on 0-positive groups (where the neg:pos multiplier is undefined), so those groups can be downsampled and the value flows through export → YAML → config.

**Architecture:** The drop is entirely in the browser JS embedded in `scripts/sampling_overrides_editor.py`'s `_HTML_TEMPLATE`: `preview()` pins `ratio=1.0` for `n_pos<=0` and the export reads that. The Python `grid_to_yaml` and the Spark sampler (`select_keys`) already honor any sub-1.0 ratio for `label=0` rows. So this is a near-pure JS change: for 0-positive rows, the read-only `ratio` column becomes directly editable (default 1.0) while the `負樣本倍率` column greys out. No change to the sampler, consistency predicates, or `grid_to_yaml` logic.

**Tech Stack:** Python 3.10, Typer CLI + self-contained HTML/vanilla-JS editor (string template), pytest. Tests assert on the rendered HTML string (the established pattern for this JS-in-Python file; interactive behavior is manually verified).

**Worktree:** `/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample` (branch `feat/editor-zeropos-downsample`).

**Spec:** `docs/superpowers/specs/2026-06-01-editor-zeropos-downsample-design.md`

---

## File Structure

- `scripts/sampling_overrides_editor.py` — single self-contained dev tool. The `_HTML_TEMPLATE` JS (functions `buildRatio`, `preview`, `achMult`, `syncEdits`, `recalc`, `renderRatio`) gains 0-positive handling; the `<details>` help text gains one corrected sentence. No Python logic change.
- `tests/scripts/test_sampling_overrides_editor.py` — add `TestRenderHtml` string-marker assertions and one `TestGridToYaml` regression test.

**Note on `exp()`:** No change needed. Its `ratio_rows` map already does `ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))`; once `preview()` returns the real keep-rate for 0-positive rows, `parseFloat(pv.ratio)` carries it and the `ratio !== DR` filter naturally keeps sub-default values.

**Note on `aggregate_surfaces` / `suggest_ratio`:** No change. They are not in the live path (only referenced by tests/docstring); their `n_pos==0 → 1.0` stays the *suggested initial value*, which is exactly the default the browser now lets the user edit.

---

### Task 1: 0-positive rows get a directly-editable keep-rate (JS)

All edits are in `scripts/sampling_overrides_editor.py` inside the `_HTML_TEMPLATE` string. The template is a Python `.format()` string, so literal JS braces are doubled (`{{`/`}}`) and `${{...}}` is a literal JS template-literal placeholder — **preserve the doubling exactly** in every edit below.

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (JS in `_HTML_TEMPLATE`, ~lines 364-473)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (`TestRenderHtml`)

- [ ] **Step 1: Write the failing tests** (append to `class TestRenderHtml`)

```python
    def test_zero_pos_ratio_cell_editable(self):
        # 0-positive rows: ratio column becomes directly editable (data-k
        # ratio_direct), the neg:pos multiplier column greys out.
        html = render_html(self._STATS, **self._KW)
        assert "data-k=ratio_direct" in html
        assert "r.ratio_direct=1" in html  # buildRatio seeds the default

    def test_zero_pos_preview_reads_direct_keep_rate(self):
        # preview() noPos branch must derive ratio from r.ratio_direct, not
        # pin it to a hard-coded 1.0000 literal.
        html = render_html(self._STATS, **self._KW)
        assert "parseFloat(r.ratio_direct)" in html
        # recalc must NOT write back into the ratio cell while it is the one
        # being edited (would wash the cursor).
        assert "if(!editingRatio) tr.querySelector('td.rt')" in html
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q
```
Expected: FAIL — `assert 'data-k=ratio_direct' in html` (and the others) fail; the markers do not exist yet.

- [ ] **Step 3a: Seed `ratio_direct` default in `buildRatio`**

In `buildRatio`, the per-row init currently reads (note doubled braces):
```javascript
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.suggested_neg_mult=R; }});
```
Replace the second line with:
```javascript
 rows.forEach(r=>{{ r.pos_rate=(r.n_pos+r.n_neg>0?r.n_pos/(r.n_pos+r.n_neg):0);
  r.suggested_neg_mult=R; r.ratio_direct=1; }});
```

- [ ] **Step 3b: Reorder `preview` and make the `noPos` branch read `ratio_direct`**

Replace these two leading branches of `preview` (the `isNaN(nm)` line followed by the `n_pos<=0` two-line return):
```javascript
 if(isNaN(nm)) return {{ratio:'—',kn:'—',pr:'—',clamped:false,achieved:0,noNeg:false,noPos:false}};
 if(r.n_pos<=0) return {{ratio:'1.0000',kn:String(r.n_neg),pr:'0.0000',
   clamped:false,achieved:0,noNeg:false,noPos:true}};
```
with (n_pos check first so a disabled/`NaN` multiplier on 0-pos rows cannot short-circuit to `'—'`):
```javascript
 if(r.n_pos<=0){{ let kr=parseFloat(r.ratio_direct);
   if(isNaN(kr)) kr=1; kr=Math.min(1,Math.max(0,kr));
   return {{ratio:kr.toFixed(4),kn:String(Math.round(r.n_neg*kr)),pr:'0.0000',
   clamped:false,achieved:0,noNeg:false,noPos:true}}; }}
 if(isNaN(nm)) return {{ratio:'—',kn:'—',pr:'—',clamped:false,achieved:0,noNeg:false,noPos:false}};
```

- [ ] **Step 3c: Update the `achMult` noPos tooltip**

Replace:
```javascript
 if(pv.noPos) return {{cls:'calc',html:'—',title:'無正樣本，不下採樣'}};
```
with:
```javascript
 if(pv.noPos) return {{cls:'calc',html:'—',title:'無正樣本，neg:pos 無定義；保留率可直接設定'}};
```

- [ ] **Step 3d: Branch `syncEdits` on the new `data-k`**

Replace:
```javascript
  if(td.dataset.k==='neg_mult') r.suggested_neg_mult=v; else r.weight=v;
```
with:
```javascript
  if(td.dataset.k==='neg_mult') r.suggested_neg_mult=v;
  else if(td.dataset.k==='ratio_direct') r.ratio_direct=v;
  else r.weight=v;
```

- [ ] **Step 3e: Branch `recalc` so editing the ratio cell does not wash the cursor**

Replace the first four lines of `recalc`:
```javascript
 const r=rows()[+td.dataset.i],tr=td.closest('tr'),nm=parseFloat(td.textContent);
 r.suggested_neg_mult=nm;
 const pv=preview(r,nm),am=achMult(pv);
 tr.querySelector('td.rt').textContent=pv.ratio;
```
with:
```javascript
 const r=rows()[+td.dataset.i],tr=td.closest('tr'),v=parseFloat(td.textContent);
 const editingRatio=td.dataset.k==='ratio_direct';
 if(editingRatio) r.ratio_direct=v; else r.suggested_neg_mult=v;
 const pv=preview(r,parseFloat(r.suggested_neg_mult)),am=achMult(pv);
 if(!editingRatio) tr.querySelector('td.rt').textContent=pv.ratio;
```

- [ ] **Step 3f: Render the multiplier cell greyed and the ratio cell editable for 0-pos rows**

Replace this block inside `renderRatio`'s `idx.forEach` (from `const am=...` through the `td.innerHTML=...` assignment, i.e. the lines that build `am`/`tr` and the full `tr.innerHTML` template):
```javascript
  const am=achMult(pv),tr=document.createElement('tr');
  tr.innerHTML=`<td>${{esc(r.segment)}}</td><td>${{esc(r.product)}}</td>`+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   `<td class=edit contenteditable data-k=neg_mult data-i=${{i}} `+
   `oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`+
   `<td class="calc rt">${{pv.ratio}}</td>`+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`;
```
with:
```javascript
  const am=achMult(pv),tr=document.createElement('tr'),noPos=r.n_pos<=0;
  const negMultCell=noPos
   ?`<td class=calc title="無正樣本，倍率無定義；改用 ratio 欄直接設保留率">—</td>`
   :`<td class=edit contenteditable data-k=neg_mult data-i=${{i}} oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`;
  const ratioCell=noPos
   ?`<td class="edit rt" contenteditable data-k=ratio_direct data-i=${{i}} oninput="recalc(this)">${{r.ratio_direct}}</td>`
   :`<td class="calc rt">${{pv.ratio}}</td>`;
  tr.innerHTML=`<td>${{esc(r.segment)}}</td><td>${{esc(r.product)}}</td>`+
   `<td class=stat>${{r.n_pos}}</td><td class=stat>${{r.n_neg}}</td>`+
   `<td class=stat>${{r.pos_rate.toFixed(4)}}</td>`+
   negMultCell+ratioCell+
   `<td class="${{am.cls}} am" title="${{am.title}}">${{am.html}}</td>`+
   `<td class="calc kn">${{pv.kn}}</td><td class="calc pr">${{pv.pr}}</td>`;
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py -q
```
Expected: PASS — all of `TestRenderHtml` (new + existing) green, and the rest of the file unaffected.

- [ ] **Step 5: Manual verification (interactive behavior the string tests cannot cover)**

Generate an editor HTML containing a 0-positive row and open it, OR write a tiny one-off using `render_html` with a stat row whose `n_pos=0`. Confirm:
1. The 0-positive row shows `—` (greyed) in the `負樣本倍率` column and an editable (yellow) `ratio` cell defaulting to `1`.
2. Typing `0.3` into that ratio cell updates `kept_neg` to `round(n_neg*0.3)` and leaves `new_pos_rate` at `0.0000`, without the cursor jumping / value resetting.
3. `實際倍率` stays `—` with the new tooltip.
4. Export JSON shows `ratio: 0.3` for that row; Export YAML snippet emits `segment|product|0: 0.3`.

One-off generator (run from the worktree root):
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "
from scripts.sampling_overrides_editor import render_html
stats=[{'cust_segment_typ':'mass','prod_name':'a','n_pos':200,'n_neg':4000},
       {'cust_segment_typ':'mass','prod_name':'cold','n_pos':0,'n_neg':5000}]
html=render_html(stats, segment_col='cust_segment_typ', item_col='prod_name',
    weight_keys=['prod_name'], label_col='label', default_ratio=1.0)
open('/tmp/editor_zeropos.html','w').write(html); print('wrote /tmp/editor_zeropos.html')
"
```
Then `open /tmp/editor_zeropos.html`.

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample commit -q \
  -m "feat(editor): editable keep-rate for 0-positive sampling groups"
```

---

### Task 2: Correct the help text for 0-positive rows

**Files:**
- Modify: `scripts/sampling_overrides_editor.py` (`<details>` block, ~line 323)
- Test: `tests/scripts/test_sampling_overrides_editor.py` (`TestRenderHtml`)

- [ ] **Step 1: Write the failing test** (append to `class TestRenderHtml`)

```python
    def test_help_text_describes_zero_pos_editable_ratio(self):
        html = render_html(self._STATS, **self._KW)
        # stale claim ("維持 ratio 1.0") must be gone; new wording present.
        assert "維持 ratio 1.0" not in html
        assert "neg:pos 無定義" in html
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  "/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml::test_help_text_describes_zero_pos_editable_ratio" -q
```
Expected: FAIL — `assert "維持 ratio 1.0" not in html` fails (the stale sentence is still there).

- [ ] **Step 3: Update the help sentence**

In the `<details>` block, replace:
```html
n_pos = 0 的冷門列維持 ratio 1.0（全留負樣本）。</p>
```
with:
```html
n_pos = 0 的冷門列因 neg:pos 無定義，改為在 ratio 欄直接填保留率（預設 1.0 = 全留負樣本）。</p>
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample commit -q \
  -m "docs(editor): help text reflects 0-positive editable keep-rate"
```

---

### Task 3: Regression test — 0-positive override round-trips through `grid_to_yaml`

This pins the export→config contract for the feature: a product with zero positives is still a valid product, and a sub-1.0 ratio for it must emit `seg|item|0: ratio` (no special-casing/dropping). No implementation change — `grid_to_yaml` is already general; this guards against a future regression.

**Files:**
- Test: `tests/scripts/test_sampling_overrides_editor.py` (`TestGridToYaml`)

- [ ] **Step 1: Write the test** (append to `class TestGridToYaml`)

```python
    def test_zero_pos_group_override_round_trips(self):
        # A 0-positive product ("a", present in schema) downsampled to 0.3 must
        # survive to config. grid_to_yaml has no n_pos visibility, so this also
        # documents that it must never special-case "cold" products away.
        export = _export(
            ratio_rows=[{"segment": "mass", "product": "a", "ratio": 0.3}],
            weight_rows=[])
        out = grid_to_yaml(export, _params(), default_ratio=1.0)
        ov = yaml.safe_load(out["sample_ratio_overrides_yaml"])
        assert ov == {"sample_ratio_overrides": {"mass|a|0": 0.3}}
```

- [ ] **Step 2: Run the test to verify it passes**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  "/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py::TestGridToYaml::test_zero_pos_group_override_round_trips" -q
```
Expected: PASS immediately (no impl change). If it FAILS, stop — it means `grid_to_yaml` is dropping the override and Task 1's export path is broken upstream.

- [ ] **Step 3: Run the full editor test file**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py -q
```
Expected: PASS — entire file green.

- [ ] **Step 4: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample add \
  tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample commit -q \
  -m "test(editor): 0-positive override round-trips through grid_to_yaml"
```

---

## Verification (after all tasks)

Run the full editor test file once more (fast, no Spark):
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py -q
```
Expected: all green. Then finish via `superpowers:finishing-a-development-branch`.

## Acceptance criteria (from spec)

1. 0-positive rows: `負樣本倍率` greyed, `ratio` editable, default 1.0. ✔ Task 1
2. Typing `0.3` updates `kept_neg`/`new_pos_rate` live without washing the cursor. ✔ Task 1 (recalc branch) + manual verify
3. Export JSON / YAML snippet / `to-yaml` all emit `segment|item|0: 0.3`. ✔ Task 1 (preview→exp) + Task 3 (grid_to_yaml)
4. No 0-pos edit ⇒ output identical to before (default 1.0, filtered out). ✔ Task 1
5. Normal (positive) rows unchanged. ✔ Task 1 (noPos branch only)
6. Existing tests stay green + new tests pass. ✔ all tasks
