# Sampling Editor: ratio 輸入模式 + 群組/批次選取 + weight 負樣本基數 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 讓 `scripts/sampling_overrides_editor.py` 的 ratio 面能用「負樣本倍率」或「保留率」兩種方式擇一輸入、能依群組/批次一次選多列套同一值；weight 面能選「連動 ratio 面」或「不連動（全域 φ）」作為地板的負樣本基數。

**Architecture:** ratio 面三互動只影響瀏覽器端每列算出的保留率，匯出契約 `ratio_rows:[{keys,ratio}]` 不變 → 改動集中在 `_HTML_TEMPLATE` 內嵌 JS。weight 面負樣本基數開關需同步改 live JS `rebuildWeight()` 與 Python 參考實作 `aggregate_surfaces`（兩者鏡像），匯出 `w_pos/w_neg` 鍵值格式不變。

**Tech Stack:** Python 3.10 / Typer CLI / 自包 HTML+vanilla JS（`str.format()` 模板）/ pytest 7.3.1。**無 Spark 需求**（測試與手動驗證都不需 SparkSession）。

**行為基準（canonical reference）：** 已核可的原型 `data/profiling/prototype_sampling_editor.html`（獨立檔、未 double-brace）。設計 spec：`docs/superpowers/specs/2026-06-12-sampling-editor-ratio-input-modes-batch-design.md`。

---

## 重要實作守則

1. **`_HTML_TEMPLATE` 是 `str.format()` 模板**：插入的 JS 中**所有** `{` / `}` 必須寫成 `{{` / `}}`，且不可與既有佔位符 `{stats_json}`/`{t}`/`{alpha}`/`{target_neg_pos}`/`{default_ratio}`/`{label_col}`/`{gkeys_json}`/`{group_keys_json}`/`{wdims_json}`/`{wkeys_json}` 衝突。原型是單 brace 版的行為基準；移植時逐段轉義。
2. **JS 無 pytest 行為測試**：沿用既有 `tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml` 的「字串存在性斷言」模式（驗 render_html 輸出含哪些函式名/字串/控制項 id）。行為正確性靠 Task 6 手動瀏覽器驗證；spec 已接受此取捨。
3. **唯一可 TDD 的 Python 邏輯是 `aggregate_surfaces`**（Task 1）：weight 面負樣本基數的新分支在此用 pytest 驗證，JS `rebuildWeight()` 鏡像同一公式。
4. **跑測試一律絕對 venv python + worktree PYTHONPATH**（worktree+venv SOP）：
   ```bash
   PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
     /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest <paths> -q
   ```
   （此工具測試 import 路徑為 `from scripts.sampling_overrides_editor import ...`，pytest 從 worktree root 執行即可找到 `scripts/`。）
5. **不動**：`grid_to_yaml` / `suggest_ratio` / `profile_stats` / `resolve_keys` 及其測試；ratio 匯出契約 `ratio_rows:[{keys,ratio}]` 與 `sample_weights` 鍵值格式。

---

## File Structure

- Modify: `scripts/sampling_overrides_editor.py`
  - `aggregate_surfaces()` — 新增 `neg_base` / `phi` 參數（Task 1）。
  - `_HTML_TEMPLATE` — ratio 面模式切換（Task 2）、群組/批次選取（Task 3）、weight 面負樣本基數（Task 4）、export 取生效保留率（Task 5）。
  - `render_html()` — 若新增 φ 預設佔位符（Task 4，採內嵌預設、不改簽章）。
- Modify: `tests/scripts/test_sampling_overrides_editor.py`
  - `TestAggregateSurfaces` — 新增 decoupled parity 測試（Task 1）。
  - `TestRenderHtml` — 新增三互動 + weight 負樣本基數的字串斷言（Task 2/3/4）。
- Reference only（不進版控，data/ gitignored）: `data/profiling/prototype_sampling_editor.html`。

---

## Task 1: `aggregate_surfaces` 支援不連動負樣本基數（Python, TDD）

**Files:**
- Modify: `scripts/sampling_overrides_editor.py`（`aggregate_surfaces`，約 168–254 行）
- Test: `tests/scripts/test_sampling_overrides_editor.py`（`TestAggregateSurfaces`，約 519 行起）

設計：`aggregate_surfaces` 新增關鍵字參數 `neg_base: str = "coupled"`、`phi: float = 1.0`。
weight surface 聚合時的 `n_neg_post` 改為：
- `neg_base == "coupled"`（預設，現況）：`Σ n_neg * ratio_by_key[rk]`。
- `neg_base == "decoupled"`：`Σ n_neg * phi`（與 ratio 面無關）。
其餘（floor v、attention A、w_pos/w_neg、ratio surface）公式不變。預設值確保既有 6 個測試不改即綠。

- [ ] **Step 1: 寫失敗測試（decoupled φ=1 → n_neg_post == 原始 n_neg）**

加到 `TestAggregateSurfaces` class：

```python
def test_decoupled_phi_one_uses_raw_negatives(self):
    # 兩個 fine cell，ratio_dims=[item]，weight_dims=[item]；給一個 < 1 的 neg_mult
    # 讓 coupled 會下採，decoupled 不應受影響。
    stats = [
        {"item": "a", "n_pos": 100, "n_neg": 1000},
        {"item": "b", "n_pos": 50, "n_neg": 4000},
    ]
    neg_mults = {("a",): 1.0, ("b",): 1.0}  # ratio = clamp(1*npos/nneg)
    out = aggregate_surfaces(
        stats, neg_mults, ratio_dims=["item"], weight_dims=["item"],
        alpha=0.5, t=1 / 6, default_neg_mult=1.0,
        neg_base="decoupled", phi=1.0)
    rows = {tuple(r["keys"]): r for r in out["weight_rows"]}
    assert rows[("a",)]["n_neg_post"] == 1000   # raw, not downsampled
    assert rows[("b",)]["n_neg_post"] == 4000
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestAggregateSurfaces::test_decoupled_phi_one_uses_raw_negatives -q
```
Expected: FAIL — `aggregate_surfaces() got an unexpected keyword argument 'neg_base'`.

- [ ] **Step 3: 實作 — 加參數與分支**

修改 `aggregate_surfaces` 簽章與 weight 聚合迴圈。簽章加 `neg_base: str = "coupled", phi: float = 1.0`；docstring 補一句說明。weight surface 聚合處（現為 `a[1] += s["n_neg"] * ratio_by_key[rk]`）改為：

```python
    # --- weight surface: two-factor (floor v + attention A) per weight_dims ---
    weight_rows: list[dict] = []
    if weight_dims:
        wacc: dict = {}
        for s in stats:
            wk = tuple(s[d] for d in weight_dims)
            rk = tuple(s[d] for d in ratio_dims)
            a = wacc.setdefault(wk, [0, 0.0])
            a[0] += s["n_pos"]
            if neg_base == "decoupled":
                a[1] += s["n_neg"] * phi          # decoupled: raw * phi, 與 ratio 無關
            else:
                a[1] += s["n_neg"] * ratio_by_key[rk]   # coupled: post-downsample
```

（其餘 masses / m_min / two_factor_weights / weight_rows.append 不動。）

- [ ] **Step 4: 跑新測試確認通過**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestAggregateSurfaces::test_decoupled_phi_one_uses_raw_negatives -q
```
Expected: PASS.

- [ ] **Step 5: 加 φ<1 測試並跑整個 class（含回歸）**

加測試：

```python
def test_decoupled_phi_scales_raw_negatives(self):
    stats = [{"item": "a", "n_pos": 100, "n_neg": 1000}]
    out = aggregate_surfaces(
        stats, {("a",): 1.0}, ratio_dims=["item"], weight_dims=["item"],
        alpha=0.5, t=1 / 6, default_neg_mult=1.0, neg_base="decoupled", phi=0.2)
    row = out["weight_rows"][0]
    assert row["n_neg_post"] == round(1000 * 0.2)   # 200
```

Run（整個 class，確認既有 coupled 6 測試仍綠）：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestAggregateSurfaces -q
```
Expected: 全數 PASS（既有 + 2 新）。

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes commit -q -m \
  "feat(sampling-editor): aggregate_surfaces decoupled neg-base (phi)"
```

---

## Task 2: ratio 面輸入模式切換（依倍率 / 依保留率）

**Files:**
- Modify: `scripts/sampling_overrides_editor.py`（`_HTML_TEMPLATE`：HTML 控制項 + JS `buildRatio`/`preview`/`renderRatio`/`recalc`/`syncEdits`/`exp`/`renderSummary`）
- Test: `tests/scripts/test_sampling_overrides_editor.py`（`TestRenderHtml`）

設計：新增全域 `let RMODE='mult';`（mult｜keep）。每列沿用既有兩欄位 `suggested_neg_mult`（倍率）與 `ratio_direct`（保留率），由 `RMODE` 決定哪一欄可編、哪一欄算出。`n_pos<=0` 的列**不論模式**都用 `ratio_direct`（同現況）。切換不互洗兩欄值。**行為基準＝原型的 `setMode`/`effKeep`/`preview`/`renderRatio`。**

- [ ] **Step 1: 寫失敗測試（render_html 含模式切換控制項與函式）**

加到 `TestRenderHtml`：

```python
def test_ratio_input_mode_toggle_present(self):
    html = render_html(self._STATS, **self._KW)
    assert "function setRmode(" in html
    assert "let RMODE='mult'" in html
    assert "依負樣本倍率" in html and "依保留率" in html
    assert 'name="rmode"' in html        # radio group
    # n_pos=0 fallback 與 clamp 警告仍在（不破壞既有）
    assert "n_pos<=0" in html or "r.n_pos<=0" in html
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml::test_ratio_input_mode_toggle_present -q
```
Expected: FAIL — `setRmode` not in html。

- [ ] **Step 3: 加 HTML 模式列**（`_HTML_TEMPLATE`，置於 `<div id="tabs">…</div>` 之後、`<div id="knobs">` 之前）

插入（**注意 double-brace**；此段無內嵌 `{}` 故原樣即可）：

```html
<div id="rmodebar">ratio 面輸入模式：
 <label><input type="radio" name="rmode" value="mult" checked onclick="setRmode('mult')"> 依負樣本倍率</label>
 <label><input type="radio" name="rmode" value="keep" onclick="setRmode('keep')"> 依保留率</label></div>
```

- [ ] **Step 4: 加 JS 全域與 `setRmode`**（`_HTML_TEMPLATE` `<script>` 內，靠近 `let tab='ratio'…`）

```javascript
let RMODE='mult';
function setRmode(m){{ syncEdits(); RMODE=m; if(tab==='ratio') render(); }}
```

- [ ] **Step 5: 改 `preview(r,nm)` 支援 keep 模式**（取代現有 `preview`）

行為：keep 模式或 `n_pos<=0` → 用 `ratio_direct` 當保留率，achieved = kept/n_pos（n_pos<=0 則 noPos）。否則維持倍率邏輯。

```javascript
function preview(r,nm){{
 const useDirect = (RMODE==='keep') || (r.n_pos<=0);
 if(useDirect){{
   let kr=parseFloat(r.ratio_direct); if(isNaN(kr)) kr=1;
   kr=Math.min(1,Math.max(0,kr));
   const kept=Math.round(r.n_neg*kr), total=r.n_pos+kept;
   return {{ratio:kr.toFixed(4),kn:String(kept),
     pr:(total>0?r.n_pos/total:0).toFixed(4),clamped:false,
     achieved:(r.n_pos>0?kept/r.n_pos:0),noNeg:r.n_neg<=0,noPos:r.n_pos<=0}};
 }}
 if(isNaN(nm)) return {{ratio:'1.0000',kn:String(r.n_neg),
   pr:(r.n_pos/(r.n_pos+r.n_neg)).toFixed(4),clamped:false,
   achieved:(r.n_pos>0?r.n_neg/r.n_pos:0),noNeg:false,noPos:false}};
 if(r.n_neg<=0) return {{ratio:'1.0000',kn:'0',pr:(r.n_pos>0?1:0).toFixed(4),
   clamped:false,achieved:0,noNeg:true,noPos:false}};
 const raw=nm*r.n_pos/r.n_neg,ratio=Math.min(1,Math.max(0,raw));
 const keptNeg=Math.round(r.n_neg*ratio),total=r.n_pos+keptNeg;
 return {{ratio:ratio.toFixed(4),kn:String(keptNeg),
  pr:(total>0?r.n_pos/total:0).toFixed(4),clamped:raw>1,
  achieved:(r.n_pos>0?keptNeg/r.n_pos:0),noNeg:false,noPos:false}};
}}
```

- [ ] **Step 6: 改 `renderRatio` 讓可編欄隨模式切換**（取代現有 `renderRatio` 的 cell 組裝）

規則：`editKeep = (RMODE==='keep') || noPos`。`editKeep` 時保留率欄可編（`ratio_direct`）、倍率欄算出（顯示 achieved 或 '—'）；否則維持現況（倍率欄可編、保留率欄算出）。沿用既有 `achMult(pv)` 與欄位。將現有 `renderRatio` 內 `negMultCell` / `ratioCell` 兩段取代為：

```javascript
  const editKeep=(RMODE==='keep')||noPos;
  const negMultCell=editKeep
   ? (noPos
       ? `<td class=calc title="無正樣本，倍率無定義">—</td>`
       : `<td class="calc">${{pv.achieved.toFixed(1)}}</td>`)
   : `<td class=edit contenteditable data-k=neg_mult data-i=${{i}} oninput="recalc(this)">${{r.suggested_neg_mult}}</td>`;
  const ratioCell=editKeep
   ? `<td class="edit rt" contenteditable data-k=ratio_direct data-i=${{i}} oninput="recalc(this)">${{r.ratio_direct}}</td>`
   : `<td class="calc rt">${{pv.ratio}}</td>`;
```

- [ ] **Step 7: 確認 `recalc` / `syncEdits` / `exp` / `renderSummary` 走 effective keep**

- `recalc(td)`：已依 `td.dataset.k`（`neg_mult` / `ratio_direct`）寫回對應欄並呼叫 `preview`。改一行：`preview` 呼叫不再需要傳 mult 的特例——保留現有 `preview(r,parseFloat(r.suggested_neg_mult))` 呼叫即可（keep 模式時 `preview` 內部走 `ratio_direct` 分支，傳入的 nm 被忽略）。**確認 `recalc` 內計算 `pv` 的那行為** `const pv=preview(r,parseFloat(r.suggested_neg_mult));`（原樣保留）。
- `syncEdits` / `exp` / `renderSummary`：均透過 `preview(r,parseFloat(r.suggested_neg_mult))` 取值，**無需改動**（已自動反映 RMODE）。確認 `exp()` 內 `ratio_rows` 仍 `ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))`。

- [ ] **Step 8: double-brace 檢查 + 跑 render_html 字串測試 + 產檔自檢**

double-brace 自檢（render_html 不可在 format 時爆）：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -c \
  "from scripts.sampling_overrides_editor import render_html; \
   print('OK' if render_html([{'cust_segment_typ':'mass','prod_name':'p','n_pos':1,'n_neg':2}], \
     ratio_dims=['cust_segment_typ','prod_name'], group_keys=['cust_segment_typ','prod_name','label'], \
     label_col='label', weight_keys=['prod_name','label'], weight_dims=['prod_name'], default_ratio=1.0)[:15] else 'BAD')"
```
Expected: 印出 `OK`（無 `KeyError`/`IndexError`）。

Run 測試（新 + 既有 TestRenderHtml 全綠）：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q
```
Expected: 全 PASS。

- [ ] **Step 9: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes commit -q -m \
  "feat(sampling-editor): ratio input mode toggle (neg-mult / keep-rate)"
```

---

## Task 3: ratio 面群組選取 + 批次套用

**Files:**
- Modify: `scripts/sampling_overrides_editor.py`（`_HTML_TEMPLATE`：HTML 群組/批次列 + checkbox 欄 + JS 選取集/群組/批次函式 + `renderRatio` 加勾選欄）
- Test: `tests/scripts/test_sampling_overrides_editor.py`（`TestRenderHtml`）

設計：統一一個選取集 `const RSEL=new Set();`（以 row index `i` 識別，配合既有 `RATIO` 陣列）。群組選取＝維度下拉（`GKEYS`）＋值下拉（該維度 distinct 值）＋「加入選取/只選此群組」。批次套用＝輸入值寫進所有選取列的「當前模式可編欄」（mult 模式寫 `suggested_neg_mult`、keep 模式寫 `ratio_direct`；mult 模式對 `n_pos<=0` 列自動略過）。**行為基準＝原型的 `groupSelect`/`applyBatch`/`selectAllVisible`。**

- [ ] **Step 1: 寫失敗測試**

```python
def test_group_and_batch_select_present(self):
    html = render_html(self._STATS, **self._KW)
    assert "function groupSelect(" in html
    assert "function applyBatch(" in html
    assert "依群組選取" in html and "套用到選取列" in html
    assert "const RSEL=new Set(" in html
    assert "function fillGroupVals(" in html
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml::test_group_and_batch_select_present -q
```
Expected: FAIL。

- [ ] **Step 3: 加 HTML 群組 + 批次列**（`_HTML_TEMPLATE`，置於 `<input id="flt"…>` 之前）

```html
<div id="grpsel">依群組選取：維度
 <select id="gdim" onchange="fillGroupVals()"></select> =
 <select id="gval"></select>
 <button onclick="groupSelect('add')">加入選取</button>
 <button onclick="groupSelect('only')">只選此群組</button></div>
<div id="batchsel">批次套用：對 <b id="rselcount">0</b> 個選取列設
 <input id="rbatchval" type="number" step="0.1" placeholder="值">
 <button onclick="applyBatch()">套用到選取列</button>
 <button onclick="selectAllVisible(true)">全選（可見）</button>
 <button onclick="clearRsel()">清除選取</button>
 <label><input type="checkbox" id="rclearafter"> 套用後清除</label></div>
```

- [ ] **Step 4: 加 JS 選取集 + 群組 + 批次函式**（`_HTML_TEMPLATE` `<script>` 內）

```javascript
const RSEL=new Set();
function rselCount(){{ const el=document.getElementById('rselcount'); if(el) el.textContent=RSEL.size; }}
function visIdx(){{
 const q=(document.getElementById('flt').value||'').toLowerCase();
 const cols=GKEYS.map((_,j)=>'k'+j);
 return RATIO.map((_,i)=>i).filter(i=>!q ||
   cols.map(c=>RATIO[i][c]).join(' ').toLowerCase().indexOf(q)>=0);
}}
function toggleRow(i,on){{ if(on) RSEL.add(i); else RSEL.delete(i); rselCount(); }}
function selectAllVisible(on){{ visIdx().forEach(i=>{{ if(on===false) RSEL.delete(i); else RSEL.add(i); }});
 if(tab==='ratio') render(); }}
function clearRsel(){{ RSEL.clear(); if(tab==='ratio') render(); }}
function initGroupSel(){{
 const gd=document.getElementById('gdim');
 gd.innerHTML=GKEYS.map((k,j)=>`<option value="${{j}}">${{k}}</option>`).join('');
 fillGroupVals();
}}
function fillGroupVals(){{
 const j=+document.getElementById('gdim').value;
 const vals=[...new Set(RATIO.map(r=>r['k'+j]))].sort();
 document.getElementById('gval').innerHTML=
   vals.map(v=>`<option value="${{esc(v)}}">${{esc(v)}}</option>`).join('');
}}
function groupSelect(kind){{
 const j=+document.getElementById('gdim').value, val=document.getElementById('gval').value;
 if(kind==='only') RSEL.clear();
 RATIO.forEach((r,i)=>{{ if(String(r['k'+j])===String(val)) RSEL.add(i); }});
 if(tab==='ratio') render();
}}
function applyBatch(){{
 const raw=document.getElementById('rbatchval').value;
 if(raw==='') return;
 let skipped=0;
 RSEL.forEach(i=>{{ const r=RATIO[i];
   if(RMODE==='keep') r.ratio_direct=raw;
   else if(r.n_pos<=0) {{ skipped++; r.ratio_direct=raw; }}  // 倍率模式 n_pos=0 → 寫保留率
   else r.suggested_neg_mult=raw; }});
 if(document.getElementById('rclearafter').checked) RSEL.clear();
 if(tab==='ratio') render();
 if(skipped) alert(`已套用；${{skipped}} 個 n_pos=0 的列改以保留率設定（倍率模式無倍率）`);
}}
```

- [ ] **Step 5: `renderRatio` 加勾選欄**（在表頭與每列最前面加 checkbox）

表頭 `<tr>` 開頭、`GKEYS.map(...)` 之前加：
```javascript
  `<th><input type=checkbox onclick="selectAllVisible(this.checked)"></th>`+
```
每列 `tr.innerHTML=` 開頭加：
```javascript
  `<td><input type=checkbox ${{RSEL.has(i)?'checked':''}} onclick="toggleRow(${{i}},this.checked)"></td>`+
```
並在 `renderRatio` 末尾呼叫 `rselCount();`。

- [ ] **Step 6: 在 `setTab('ratio')` 初始流程接上群組下拉初始化**

於 `<script>` 結尾既有 `initSummary(); setTab('ratio');` 之間或之後加 `initGroupSel();`（需在 `RATIO` 建好之後）。最終結尾為：
```javascript
initSummary();
initGroupSel();
setTab('ratio');
```

- [ ] **Step 7: double-brace 自檢 + 測試**

Run（同 Task 2 Step 8 的 render_html `OK` 自檢指令）→ Expected `OK`。
Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q
```
Expected: 全 PASS。

- [ ] **Step 8: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes commit -q -m \
  "feat(sampling-editor): ratio group-select + batch-apply"
```

---

## Task 4: weight 面負樣本基數開關（連動 / 不連動 + 全域 φ）

**Files:**
- Modify: `scripts/sampling_overrides_editor.py`（`_HTML_TEMPLATE`：knobs div 加 radio+φ、JS 全域 `WBASE`/`PHI`、`rebuildWeight`、`onKnob`）
- Test: `tests/scripts/test_sampling_overrides_editor.py`（`TestRenderHtml`）

設計：新增全域 `let WBASE='couple', PHI=1;`。`rebuildWeight()` 的 `_nn` 累加改為依 `WBASE`：couple 用 `s.n_neg*rbk.get(rk)`（現況）、decouple 用 `s.n_neg*PHI`。`onKnob()` 多讀 φ；新增 `setWbase(b)` 切換並啟用/停用 φ 輸入。鏡像 Task 1 的 Python `aggregate_surfaces`。**行為基準＝原型 weight 面的 `setWbase`/`renderWeight` 負樣本基數分支。**

- [ ] **Step 1: 寫失敗測試**

```python
def test_weight_neg_base_toggle_present(self):
    html = render_html(self._STATS, **self._KW)
    assert "function setWbase(" in html
    assert "let WBASE='couple'" in html
    assert "負樣本基數" in html
    assert 'name="wbase"' in html
    assert "id=wphi" in html or 'id="wphi"' in html
    # 既有 weight 兩因子函式仍在
    assert "function twoFactor(" in html and "function floorWeight(" in html
```

- [ ] **Step 2: 跑測試確認失敗**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml::test_weight_neg_base_toggle_present -q
```
Expected: FAIL。

- [ ] **Step 3: knobs div 加負樣本基數 radio + φ**（`_HTML_TEMPLATE` 現有 `<div id="knobs">…</div>` 內，t/α 之後）

把現有 knobs div 末尾 `</div>` 前插入（**注意此區在 format 模板內，靜態 HTML 無 `{}` 不需轉義**）：

```html
 · 負樣本基數
 <label><input type=radio name=wbase value=couple checked onclick="setWbase('couple')"> 連動 ratio 面</label>
 <label><input type=radio name=wbase value=decouple onclick="setWbase('decouple')"> 不連動</label>
 φ <input id=wphi type=number step=0.05 min=0 max=1 value="1.00" disabled oninput="onKnob()">
```

- [ ] **Step 4: JS 全域 + `setWbase` + 改 `rebuildWeight` 與 `onKnob`**

加全域（靠近現有 `let T=…; let ALPHA=…;`）：
```javascript
let WBASE='couple';
let PHI=1;
function setWbase(b){{ WBASE=b;
 const el=document.getElementById('wphi'); if(el) el.disabled=(b!=='decouple');
 rebuildWeight(); if(tab==='weight') render(); }}
```

改 `rebuildWeight()` 內聚合那行（現為 `a._nn+=s.n_neg*rbk.get(rk);`）為：
```javascript
  a.n_pos+=s.n_pos; a.n_neg_raw+=s.n_neg;
  a._nn+=s.n_neg*(WBASE==='decouple'?PHI:rbk.get(rk)); m.set(ks,a);
```

改 `onKnob()` 末尾加讀 φ（在 `if(!isNaN(a)&&a>=0) ALPHA=a;` 之後、`rebuildWeight()` 之前）：
```javascript
 const p=parseFloat(document.getElementById('wphi').value);
 if(!isNaN(p)&&p>=0&&p<=1) PHI=p;
```

- [ ] **Step 5: double-brace 自檢 + 測試**

Run render_html `OK` 自檢（同 Task 2 Step 8）→ Expected `OK`。
Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py::TestRenderHtml -q
```
Expected: 全 PASS。

- [ ] **Step 6: Commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes add \
  scripts/sampling_overrides_editor.py tests/scripts/test_sampling_overrides_editor.py
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes commit -q -m \
  "feat(sampling-editor): weight neg-base toggle (couple / decouple phi)"
```

---

## Task 5: 匯出契約回歸驗證（grid_to_yaml 不變）

**Files:**
- Test: `tests/scripts/test_sampling_overrides_editor.py`（`TestGridToYaml` — 只跑確認，不改）

說明：ratio 面 export `exp()` 仍輸出 `ratio_rows:[{keys,ratio}]`（ratio = 生效保留率，由 `preview` 算）；weight 面仍輸出 `w_pos/w_neg`。`grid_to_yaml` 消費端不變。本 Task 純回歸，確保前面四個 Task 沒動到契約。

- [ ] **Step 1: 跑整檔測試（全綠回歸）**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py -q
```
Expected: 全數 PASS（含 `TestGridToYaml` / `TestAggregateSurfaces` / `TestRenderHtml` / `TestResolveKeys` 等）。若任何既有測試紅 → 回到對應 Task 修正，**不得改測試遷就**（契約是規格）。

- [ ] **Step 2: 確認 grep export 契約字串未被破壞**

Run:
```bash
grep -nE "ratio_rows:|weight_rows:|w_pos:r\.w_pos|ratio:\(pv\.ratio" \
  /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/scripts/sampling_overrides_editor.py
```
Expected: `exp()` 內 `ratio_rows`/`weight_rows` 組裝仍在、`ratio:(pv.ratio==='—'?DR:parseFloat(pv.ratio))` 仍在。

（本 Task 無 commit；若 Step 1 需修正，修正後併入對應 Task 的 commit。）

---

## Task 6: 手動瀏覽器驗證 + 收尾

**Files:**
- 產生臨時 HTML（不進版控）

說明：JS 行為靠手動驗證（spec 已接受）。用 `render_html` + 合成 stats 直接產檔，不需 Spark。

- [ ] **Step 1: 產生驗證用 HTML（含雙維度 + 邊界）**

Run（一次性 python，輸出到 worktree data/profiling）：
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python - <<'PY'
from scripts.sampling_overrides_editor import render_html
from pathlib import Path
stats=[
 {"cust_segment_typ":s,"prod_name":p,"n_pos":np,"n_neg":nn}
 for s,p,np,nn in [
  ("mass","ccard_ins",1200,2400),("mass","fund_mix",300,90000),
  ("mass","gold",0,9000),("affluent","ccard_ins",400,30000),
  ("affluent","deposit_x",300,0),("hnw","loan_p",40,15000)]
]
html=render_html(stats, ratio_dims=["cust_segment_typ","prod_name"],
  group_keys=["cust_segment_typ","prod_name","label"], label_col="label",
  weight_keys=["prod_name","label"], weight_dims=["prod_name"], default_ratio=1.0)
out=Path("data/profiling/verify_sampling_editor.html"); out.parent.mkdir(parents=True,exist_ok=True)
out.write_text(html); print("wrote", out)
PY
open /Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/data/profiling/verify_sampling_editor.html
```
Expected: 印出 `wrote …`，瀏覽器開啟。

- [ ] **Step 2: 人工點測檢核表（逐項確認）**

ratio 面：
- 切「依負樣本倍率」/「依保留率」→ 可編欄（黃）/算出欄（綠）對調；切換不互洗值。
- `gold`（n_pos=0）列：兩模式都直填保留率。
- clamp 列（如 ccard_ins 填倍率 5）：實際倍率顯示 ⚠。
- 群組：維度 prod_name=ccard_ins → 兩個 segment 的 ccard_ins 同時亮選；「只選此群組」會清掉其他。
- 批次：選取後輸入值 → 套用到選取列；倍率模式對 gold 列提示改用保留率；「套用後清除」生效。
- Export YAML：只列偏離預設的列，key 為 `cust_segment_typ|prod_name|0`。

weight 面（切到 weight tab）：
- 「負樣本基數」radio：連動時 φ 停用；切不連動 φ 可填。
- 不連動 φ=1 對照連動：被 ratio 面下採的產品 v / w_neg 改變。

- [ ] **Step 3: 全測試最終回歸**

Run:
```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/feat-sampling-editor-modes/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  tests/scripts/test_sampling_overrides_editor.py -q
```
Expected: 全 PASS。

- [ ] **Step 4: 更新工具 docstring/註解（若 export 區或 knobs 說明需同步）**

檢查 `scripts/sampling_overrides_editor.py` 頂部 module docstring 與 `profile`/`render_html` docstring 是否需補一句「ratio 面可切倍率/保留率、群組/批次選取；weight 面負樣本基數可連動/不連動」。如需，補上並 commit。

- [ ] **Step 5: 收尾**

呼叫 superpowers:finishing-a-development-branch（或依使用者指示開 PR）。確認 `git -C <worktree> status` 乾淨、`git -C <worktree> log --oneline` 含 Task 1–4 commits。

---

## Self-Review（已執行）

- **Spec coverage**：① 倍率/保留率模式＝Task 2；② 群組/批次選取＝Task 3；③ weight 負樣本基數連動/不連動+φ＝Task 1（Python）+ Task 4（JS）；匯出契約不變＝Task 5；手動驗證＝Task 6。無遺漏。
- **Placeholder scan**：各步驟均有實際 code / 指令 / 預期輸出，無 TBD。
- **Type/naming consistency**：JS 全域 `RMODE`/`RSEL`/`WBASE`/`PHI`、函式 `setRmode`/`groupSelect`/`applyBatch`/`fillGroupVals`/`selectAllVisible`/`clearRsel`/`setWbase` 在定義與引用處一致；Python `aggregate_surfaces(neg_base, phi)` 在 Task 1 定義、Task 4 JS 鏡像。`preview` 回傳欄位（ratio/kn/pr/achieved/clamped/noNeg/noPos）與既有 `achMult`/`renderRatio` 消費端一致。
