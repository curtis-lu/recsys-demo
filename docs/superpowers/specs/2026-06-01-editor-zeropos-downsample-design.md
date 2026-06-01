# Editor: 讓 0 正樣本 group 可直接設下採保留率

**Date:** 2026-06-01
**Status:** Approved (brainstorm)
**Scope:** `scripts/sampling_overrides_editor.py`（主要為內嵌 `_HTML_TEMPLATE` 的 JS）+ 對應測試

## 問題

`sampling_overrides_editor.py` 的 ratio 面主旋鈕是「目標 neg:pos 倍率 R」,保留率推導為
`ratio = clamp(R × n_pos / n_neg, 0, 1)`。此參數化**相對於正樣本**:當 `n_pos = 0` 時,
公式塌成 `R × 0 / n_neg = 0`(任何 R 都會把該組負樣本全丟),而留下任何負樣本又使實際
neg:pos = ∞,根本碰不到目標。editor 因此刻意把 0 正樣本列的保留率釘死成 `1.0`(全留),
使用者在「負樣本倍率」格輸入的值完全沒進計算。

後果:**使用者無法用 editor 對 0 正樣本 group 做下採樣**。即使手寫
`sample_ratio_overrides` YAML 可繞過(sampler 對 `label=0` 列直接套保留率,無 `n_pos==0`
特例),那條手寫 override 也無法與 editor 共存 —— 下次 export 一貼就被洗掉,因為 editor
對這些列恆輸出 `ratio = 1.0`(= 預設,被 `ratio != default` 過濾)。

## 根因定位(三個磨掉 0-pos 下採意圖的關卡)

1. **JS `preview()` 的 `noPos` 分支**(`scripts/sampling_overrides_editor.py:403-404`):
   `n_pos<=0` 一律回 `ratio:'1.0000'`,吞掉倍率格輸入。
2. **JS `exp()`**(`:504-506`):0-pos 列的 `ratio` 取自上面的 `pv.ratio`,被釘成 `1.0`。
3. **export 過濾**(JS `:517` 的 `if(r.ratio!==DR)`、Python `grid_to_yaml` 的
   `:246` `if ratio != default_ratio`):`1.0 == 預設` → 整列被濾掉。

**關鍵事實:**
- `aggregate_surfaces`(`:128`)**不在 live 路徑**(只在 docstring 提到 + 被測試引用)。
  實際跑的是 `profile_stats → render_html`(內嵌 STATS + JS),export 走 JS→JSON→`grid_to_yaml`。
- `grid_to_yaml` 與 sampler `select_keys` **本來就吃任何 sub-1.0 ratio**;唯一把 0-pos
  磨掉的是瀏覽器 JS 把 `ratio` 釘成 1.0。

→ 這是一個**幾乎純前端(`_HTML_TEMPLATE` 內 JS)**的改動。

## 設計(已選定:① 只改 0 正樣本列)

### 行為(0 正樣本列;一般列完全不變)

- **倍率欄**:灰掉、不可編輯、顯示 `—`(沿用「實際倍率」欄的 `—` 慣例)。
- **ratio 欄**:變成可編輯(黃底 `td.edit`),**預設值 `1.0`**(= 現狀全留,**不編輯就零行為
  變化**,向後相容)。使用者直接填保留率(如 `0.3` = 保留 30% 負樣本)。
- **連動更新**:`kept_neg = round(n_neg × 保留率)`、`new_pos_rate = 0`(無正樣本)即時更新。
- **實際倍率欄**:維持 `—`,tooltip 改為「無正樣本,neg:pos 無定義;此列保留率可直接設定」。
- **驗證**:輸入值 clamp 到 `[0,1]`;空白 / `NaN` → 視為 `1.0`(全留,不產生 override)。

### 資料流(export)

- JS 為 0-pos 列存一個欄位 `ratio_direct`(預設 `1.0`)。
- `preview()` 的 `noPos` 分支回傳 `ratio_direct`(clamp 後)而非硬寫 `1.0`,並據此算
  `kept_neg`/`new_pos_rate`;保留 `noPos:true` 旗標讓「實際倍率」續顯 `—`。
- `exp()` 既有的 `RATIO.map(... preview ...)` 自然帶出 `0.3`;`ratio != DR` 過濾天然保留
  sub-default 值。
- → JSON / YAML snippet / `to-yaml` 三條路徑都帶得出 `segment|item|0: 0.3`。

### 實作 wrinkle(`recalc` 須依 `data-k` 分支)

正常列的可編輯格是「倍率」(`data-k=neg_mult`),`recalc` 會把推導出的 `ratio` 寫回**唯讀**的
ratio 格(`td.rt`)。0-pos 列的可編輯格**就是 ratio 格**,所以 `recalc` 必須分支:當使用者正在
編 ratio 格(`data-k=ratio_direct`)時,只重算 `kept_neg`/`new_pos_rate`,**不可把值寫回 ratio
格**(否則游標/輸入被洗掉)。`syncEdits()` 同樣要處理 `data-k=ratio_direct` → 寫入
`r.ratio_direct`。

### 不動的部分(明確排除)

- sampler `select_keys`、consistency predicates(A5 等)、`grid_to_yaml`(已通用)。
- `aggregate_surfaces` 的 `n_pos==0 → 1.0` 維持為「建議初始值」,只是現在瀏覽器可編輯它;
  本次不改其邏輯。
- 選項 ②(每列一律可填的絕對保留率欄)**不做**(YAGNI;使用者已選 ①)。

### 說明文字

`<details>` 區塊加一行,說明 0 正樣本列因 neg:pos 無定義,改為直接填保留率(預設 1.0 = 全留)。

## 測試

此檔 JS 不可單測,沿用既有「斷言 rendered HTML 字串」模式(`TestRenderHtml`):

1. **`TestRenderHtml`**:斷言新 JS 標記存在 —— `data-k=ratio_direct`(或等義標記)、`noPos`
   分支現在讀可編輯保留率而非硬寫 `1.0`、`<details>` 新說明文字那行。
2. **`TestGridToYaml`**:新增一個明確的 0-pos round-trip —— item 在 schema、`ratio: 0.3`
   的 ratio_row → 輸出 `seg|item|0: 0.3`(把「export→config」這條釘住;item 須為已知產品值
   以通過 A5)。

**執行**(worktree 絕對 venv python + `PYTHONPATH`):

```bash
PYTHONPATH=/Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/src \
  /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest \
  /Users/curtislu/projects/recsys_tfb/.worktrees/editor-zeropos-downsample/tests/scripts/test_sampling_overrides_editor.py -q
```

## 驗收標準

1. 0 正樣本列在 HTML editor 中:倍率欄灰掉,ratio 欄可編輯,預設 1.0。
2. 在 0-pos 列 ratio 欄填 `0.3` → `kept_neg`/`new_pos_rate` 即時更新,游標不被洗掉。
3. Export JSON / YAML snippet / `to-yaml` 都帶出 `segment|item|0: 0.3`。
4. 不編輯任何 0-pos 列時,輸出與改動前完全相同(向後相容)。
5. 一般(有正樣本)列行為完全不變。
6. 既有測試全綠 + 新增 2 類測試通過。
