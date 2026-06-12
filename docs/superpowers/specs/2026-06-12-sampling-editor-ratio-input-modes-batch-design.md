# Sampling editor：ratio 面雙輸入模式＋群組／批次選取套用＋weight 面負樣本基數開關

日期：2026-06-12
範圍：`scripts/sampling_overrides_editor.py`（ratio 面 UI ＋ weight 面負樣本基數）

## 問題

`sampling_overrides_editor.py` 的 ratio 面（下採樣面）目前只能逐列填「負樣本倍率」
（目標 neg:pos），保留率是算出來的；只有 `n_pos=0` 的列才開放直接填保留率。使用者反映：

1. 想要能**選擇用「負樣本倍率」或「保留率」其中一種方式**控制下採樣，而不是被綁死在倍率。
2. 想要能**一次選取多列（全選、或依群組部分選取）一起套用同一個值**，逐列填太慢。
3. weight 面的地板權重目前**強制連動**下採樣（`n_neg_post = n_neg × ratio`）；想要能**選擇不連動**，
   並在不連動時用一個**全域負樣本保留率 φ** 自訂地板的負樣本基數。

## 設計目標與非目標

- 目標：降低 ratio 面的逐列編輯成本；兩種等價輸入方式擇一；群組／批次選取；
  weight 面負樣本基數可選連動或不連動（不連動用全域 φ）。
- 非目標：weight 面 t/α 旋鈕與雙因子數學不變；輸出格式（`sample_weights` 鍵值）不變；
  HPO（t, α）不在此範圍；不連動 φ 只做全域，不做 per-cell。

## 關鍵範圍判斷：ratio 面只動 JS；weight 面開關另動 `aggregate_surfaces`

ratio 面三互動（模式切換、群組選取、批次套用）都只影響「瀏覽器端每列最終算出的
保留率（keep-rate）」。匯出契約仍是既有的 `ratio_rows: [{keys, ratio}]`（ratio = 保留率）。

weight 面「負樣本基數」開關改變 `n_neg_post` 怎麼算：連動＝`n_neg × ratio`（現況）；
不連動＝`n_neg × φ`。這需要同步改 **live JS `rebuildWeight()`** 與 **Python 參考實作
`aggregate_surfaces`**（兩者必須鏡像；後者有 parity 測試）。匯出的 `w_pos/w_neg` 仍由瀏覽器
算好交給 `grid_to_yaml`，所以 **`grid_to_yaml` 契約與其測試不變**。

因此：

- **不動**：`suggest_ratio` / `grid_to_yaml` / `profile_stats` / `resolve_keys`；匯出契約
  `ratio_rows:[{keys,ratio}]` 與 `sample_weights` 鍵值格式 → `TestGridToYaml` 全綠。
- **要動**：`_HTML_TEMPLATE`（ratio 面三互動 ＋ weight 面負樣本基數開關/φ 旋鈕）、
  `render_html`（若需新增旋鈕字串）、`aggregate_surfaces`（新增「負樣本基數」參數，
  預設連動以維持回歸）。

> 實作提醒：`_HTML_TEMPLATE` 是 `str.format()` 模板，新增 JS 內所有 `{`/`}` 必須改成
> `{{`/`}}`，且不可與既有 `{stats_json}`/`{t}`/`{alpha}` 等佔位符衝突。原型
> （`data/profiling/prototype_sampling_editor.html`，獨立檔、未 double-brace）是行為基準，
> 移植時逐段轉義。

## 三個 ratio 面互動（行為基準＝已核可的原型）

### 1. 輸入模式切換（全域 radio：依倍率 / 依保留率）

- 每列同時保有兩個獨立值：`mult`（負樣本倍率）與 `keep`（保留率）。全域 `MODE` 決定
  哪一欄是黃色可編欄、哪一欄是綠色算出欄；**切換不互洗**已填的另一邊值。
- 依倍率模式（現況）：黃欄＝倍率，`ratio = clamp(mult × n_pos / n_neg, 0, 1)`。
  - `n_pos=0` 的列：neg:pos 無定義 → 該列黃欄 fallback 成直填保留率（同現況）。
  - 負樣本不足以達標（raw>1 被 clamp）：ratio 自動=1.0 全留，顯示「實際倍率 ⚠」（同現況）。
- 依保留率模式（新）：黃欄＝保留率，**全列一致**直填（含 `n_pos=0`）；負樣本倍率變綠色
  算出欄＝`kept_neg / n_pos`（`n_pos=0` 顯示「—」）。
- 「實際倍率」「kept_neg」「new_pos_rate」皆為算出欄，依當前生效保留率即時更新。

### 2. 依群組選取（綠框）

- ratio 面 key 為 `ratio_dims = sample_group_keys \ {label}`（真實設定為
  `[cust_segment_typ, prod_name]`，雙維度，群組選取才有意義）。
- 控制項：維度下拉（列出各 `ratio_dims` 欄）＋ 值下拉（隨維度填入該欄 distinct 值）＋
  「加入選取」（加進現有選取集）／「只選此群組」（先清空再選）。
- 例：維度 `prod_name=ccard_ins` → 一次選到所有 segment 的 ccard_ins；維度
  `cust_segment_typ=mass` → 整個 mass segment。

### 3. 批次套用（橘框）＋逐列選取

- 每列前置 checkbox；表頭「全選」與「篩選」連動（全選只作用於篩選後可見列）。
- 「套用到選取列」：把輸入值一次寫進所有選取列的「當前模式可編欄」（倍率模式寫 `mult`、
  保留率模式寫 `keep`）。倍率模式下對 `n_pos=0` 的列自動略過並提示。
- 「套用後清除選取」為可選勾選框。
- 選取集可由群組選取、全選、逐列勾選任意組合而成（統一一個 selection set）。

## weight 面：負樣本基數開關（連動 / 不連動）

在 weight 面既有 t/α 旋鈕旁新增一個「負樣本基數」radio：

- **連動 ratio 面（預設）**：`n_neg_post = Σ n_neg × ratio[fine cell 的 ratio_dims 投影]`
  （現況）。地板 `v` 在下採後負樣本上算，套用後實際正樣本率精確落在 `t`。
- **不連動**：啟用一個全域「負樣本保留率 φ」旋鈕（預設 1.0），`n_neg_post = n_neg × φ`，
  與 ratio 面無關。φ=1 即「用原始負樣本」；φ<1 等於「假設會下採到該比例」來算地板。
  注意：若 ratio 面同時有下採，套用後實際正樣本率會 > t（overshoot）；UI 以紅字標示
  受影響列（對照「實際 pos_rate（含 ratio 下採）」欄）。

`aggregate_surfaces` 新增參數（如 `neg_base: "coupled" | "decoupled"` 與 `phi: float`，
預設 coupled）使 `n_neg_post` 走對應分支；JS `rebuildWeight()` 鏡像同一邏輯。
`w_pos = A`、`w_neg = A·v` 的兩因子公式與 t/α 語意不變。

## 匯出

`exp()` 改為對每列輸出「當前生效保留率」（等同原型的 `effKeep(r)`，與 MODE 無關），
組成既有的 `ratio_rows:[{keys, ratio}]`。weight 面仍匯出 `w_pos/w_neg`（鍵值格式不變，
只是其數值會反映負樣本基數開關的選擇）。下游 `grid_to_yaml` 不變。

## 測試策略

- **不需 Spark**。沿用既有 `TestRenderHtml` 的字串存在性斷言模式：
  - ratio 面新函式／字串：`function setMode(`、`function groupSelect(`、`function applyBatch(`、
    `依群組選取`、`依保留率`、模式 radio、批次/群組控制項 id。
  - weight 面新字串：`負樣本基數`、`連動`/`不連動` radio、`function setWbase(`、φ 旋鈕 id。
  - 斷言既有不破壞：`負樣本倍率`、clamp ⚠ 警告字串、`n_pos=0` fallback、t/α 旋鈕、
    `function twoFactor(`、`function floorWeight(` 仍在。
- `aggregate_surfaces` parity（純 Python，新增測試）：
  - 預設 `coupled` 行為與既有測試完全一致（回歸保護，現有 6 個 case 不改）。
  - `decoupled` + φ=1：`n_neg_post == n_neg`（不吃 ratio），對應 `v`/`w_neg` 隨之改變。
  - `decoupled` + φ<1：`n_neg_post == round(n_neg × φ)`。
- `TestGridToYaml`：契約不變 → 全數保持綠燈（回歸保護）。
- 手動驗證：`profile` 產生 HTML，瀏覽器開啟，點測 ratio 面三互動 ＋ weight 面負樣本基數
  開關（連動每列實際 pos_rate=t；不連動＋下採時 overshoot 紅字）＋ Export 預覽正確。

## 實作流程

- 於獨立 worktree `.worktrees/feat-sampling-editor-modes`＋`feat/` 分支進行（worktree+venv SOP）。
- 原型檔 `data/profiling/prototype_sampling_editor.html` 為 scratch 行為基準，不進版控
  （`data/` 已 gitignore）；定案來源為 `_HTML_TEMPLATE`。

## 風險

- `str.format()` double-brace 轉義遺漏 → render_html 在啟動時即 `KeyError`/`ValueError`；
  以 `TestRenderHtml` 字串斷言＋實際 `profile` 產檔接住。
- 兩模式各自保值的狀態管理在 JS 端；以原型已驗證的 `mult`/`keep` 雙欄模型移植，降低風險。
- weight 面負樣本基數的 JS `rebuildWeight()` 與 Python `aggregate_surfaces` 須鏡像同一分支
  邏輯（連動/不連動、φ）；以 `aggregate_surfaces` parity 測試＋原型對照接住偏差。
