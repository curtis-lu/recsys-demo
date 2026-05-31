# Sampling Overrides Editor — 支援 sample_group_keys / sample_weight_keys 獨立分歧 設計

**日期**：2026-05-31
**狀態**：設計核可，待寫實作計畫
**分支**：`feat/sample-weight-editor-keys`（baseline commit `72f0a86`：已 carry WIP rework — neg_mult 主旋鈕 + 三模式雛形）

---

## 1. 問題

`scripts/sampling_overrides_editor.py` 目前把「抽樣 ratio」與「訓練 weight」綁在**同一張 segment×product 表**上：

- `resolve_columns` 硬要求 `dataset.sample_group_keys = [一個 segment, item, label]`。
- `grid_to_yaml` 只消費 cell-mode export，weight key 一律吐成 `segment|product`（arity 2），**從不讀 `training.sample_weight_keys`**；A9c probe 還 hardcode `sample_weight_keys = [segment, item]`。

PR #51 已把 `sample_weights` 廣義化成 `sample_weight_keys` 複合鍵（預設 `[prod_name]`，arity 1），並加上一致性閘 A9a/A9b/A9c。**結果是編輯器與真實 config 脫鉤**：使用者可以各自獨立填 `sample_group_keys`（dataset）與 `sample_weight_keys`（training），但只要 `sample_weight_keys ≠ [segment, item]`（**連預設 `[prod_name]` 都算**），編輯器就會：

1. 吐出 `segment|product`（arity 2）的 weight YAML，與 config 宣告的 arity 不符；
2. 用 hardcode 的 `[segment, item]` 自驗 → 顯示「假的 OK」；
3. 使用者貼進 config 後，training CLI 的 A9b（arity）/ A9c（product 分量）才報錯擋下。

## 2. 目標

讓編輯器在 `sample_group_keys`（dataset）與 `sample_weight_keys`（training）**獨立、可不同**的前提下，正確讀入、編輯、匯出兩者，並各以自己的 key-set 驗證。

**典型情境**：使用者仍想針對 `cust_segment_typ & prod_name & label` 抽樣（ratio 面），但 `sample_weight_keys` 只想針對 `prod_name`（weight 面）。

## 3. 範圍邊界

| 項目 | 決策 |
|---|---|
| ratio（抽樣）面粒度 | **維持現行** `sample_group_keys = [一個 segment, item, label]`，label 匯出固定 `0`（只下採負樣本）。本次不廣義化 ratio 面。 |
| weight（訓練）面粒度 | **廣義化**：`sample_weight_keys` 可為 `identity ∪ {label} ∪ carry_columns` 中**任意欄位組合**。 |
| `label` ∈ `sample_weight_keys` | 編輯器**直接擋**（清楚 error）。理由見 §6。 |
| `sample_weight_keys` 為空 `[]` | weight 面不渲染、匯出 weight 區塊為空；ratio 面照常。 |

## 4. 架構與資料流

### 4.1 三個輸入 config
- `parameters_dataset.yaml` → `dataset.sample_group_keys`、`sample_ratio`、`sample_ratio_overrides`。
- `parameters_training.yaml` →（**新增讀取**）`training.sample_weight_keys`。
- `parameters.yaml` → `schema.columns`（item/label/time）。

### 4.2 Profiling（一次，最細粒度）
`profile_stats` 改 group by

```
union_dims = (sample_group_keys ∪ sample_weight_keys) \ {label}
```

仍以 `sum(label)`→`n_pos`、`sum(1 - label)`→`n_neg` 取得計數（故 `label` 永遠不進 group-by，只當計數來源）。groupBy 前先檢查所有 `union_dims` 都在 `df.columns`，否則清楚 error（通常是 weight 用了未加進 `carry_columns` 的欄）。

範例：
- `group_keys=[cust_segment_typ, prod_name, label]`、`weight_keys=[prod_name]` → `union_dims={cust_segment_typ, prod_name}`（與現況同）。
- `weight_keys=[risk_attr, prod_name]`（`risk_attr` 須是 carry）→ `union_dims={cust_segment_typ, prod_name, risk_attr}`。

### 4.3 兩個面（從同一份細粒度 stats 各自上捲）

**ratio 面**（granularity = `sample_group_keys`，key = `(segment, item)`）：
- 把細 cell 在「掉掉的 union 維度」上 sum 上來得 `n_pos/n_neg`。
- 保留完整下採樣診斷：`n_pos / n_neg / pos_rate / 負樣本倍率(neg_mult 旋鈕) → ratio → kept_neg → new_pos_rate / 實際倍率(⚠ clamp)`。
- `ratio[(seg,item)] = clamp(neg_mult × n_pos / n_neg, 0, 1)`。

**weight 面**（granularity = `sample_weight_keys`，任意組合）：
- 上捲到 weight-key tuple。
- `n_pos` 直接 sum（下採樣全留正樣本，不變 → 冷門加權建議值不變）。
- `n_neg(後)`、`pos_rate(後)` 依下採樣後計算（見 §4.4）。
- `suggested_weight = clamp((median_pos / n_pos)^alpha, 1.0, w_max)`，`median_pos` = weight 面各列 `n_pos` 的中位數。

### 4.4 每個細 cell 的 ratio 投影（核心資料依賴）

`sample_weights` 作用在 train/train_dev model_input，而那份資料是 sample_pool **經 ratio 下採樣之後**的結果。因此 weight 面的 `n_neg(後)/pos_rate(後)` 必須反映下採樣後分佈，且與 ratio 面的編輯**即時連動**。

**投影規則**：每個細 cell `f`，把它的 union-dim 值**只保留 `(segment_col, item_col)` 兩維**做投影，落在哪個 ratio 面列，就套那列的 `ratio`。

**為什麼跨維度也精確**：dataset sampler 在 `sample_group_keys` 粒度（`seg|item|label=0`）做下採樣，對同一 `(seg,item)` 內、weight 多帶的維度（如 `risk_attr`）**一視同仁同一保留率**。故同一 `(seg,item)` 的所有細 cell 共用同一 `ratio`——精確、非近似。

**不在細 cell 層 round**（避免雙重 rounding 誤差）：
```
kept_neg(f) = n_neg(f) × ratio(f 投影到的 (seg,item) 列)      # 分數，只在顯示某列時才 round
```
ratio 面顯示的 `kept_neg` 與 weight 面的 `n_neg(後)` 來自同一個 `Σ n_neg×ratio`、只是聚到不同 key（線性，一致）。

**ratio 用的是 effective 值**：`clamp(...)` 後的保留率（即實際匯出值）。負樣本不足以達標時 clamp 到 1.0（全留），每個細 cell 共用此 effective ratio。

**Case 1（主例）** `weight_keys=[prod_name]`：細 cell `(c,p)` 投影 = `(c,p)` 本身（1:1）；weight 列 `(p)`：`n_neg(後,p) = Σ_c n_neg(c,p) × ratio[(c,p)]`。

**Case 2（cross-dimension）** `weight_keys=[risk_attr, prod_name]`：細 cell `(c,p,r)` 投影**丟掉 `risk_attr`** → `(c,p)`，同 `(c,p)` 不同 `r` 共用 `ratio[(c,p)]`；weight 列 `(r,p)`：`n_neg(後) = Σ_c n_neg(c,p,r) × ratio[(c,p)]`。

### 4.5 UI
- **移除** cell/segment/product radio（粒度改由 config 決定）。
- **Tab 切換** ratio 面 / weight 面；單一 Export 同時吐 ratio + weight 兩塊。
- 批次設定（bulk-set）保留在 ratio 面；weight 面視需要提供同類批次工具（可選，非必要 → YAGNI，先不做）。

## 5. 匯出 / CLI / 驗證

### 5.1 匯出 JSON 形狀（自描述）
```json
{
  "sample_group_keys": ["cust_segment_typ", "prod_name", "label"],
  "sample_weight_keys": ["prod_name"],
  "ratio_rows":  [{"segment": "mass", "product": "ccard", "neg_mult": 5, "ratio": 0.066}],
  "weight_rows": [{"keys": ["ccard"], "weight": 2.3}]
}
```
- `ratio_rows` 固定 `segment`/`product`。
- `weight_rows.keys` 是**依 `sample_weight_keys` 順序**的值 list（任意 arity）。

### 5.2 `grid_to_yaml` 重寫
- **ratio** → key `f"{seg}|{prod}|0"`（對齊 `sample_group_keys=[segment,item,label]`、label 固定 0），以 `override_unknown_items`（A5）驗。
- **weight** → key `"|".join(row["keys"])`（對齊 `sample_weight_keys` 順序與 arity）；probe 的 `training.sample_weight_keys` **設成真正的 `sample_weight_keys`**（不再 hardcode），再以 `weight_key_arity_mismatch`（A9b）＋ `weight_unknown_items`（A9c）驗。**此即修掉 §1 的脫鉤破口。**
- export 內兩組 keys 與傳入 config 不一致 → 直接 error（防止舊 export 套到改過的 config）。

### 5.3 CLI 改動
- `profile`：新增 `--train-params`（預設 `conf/base/parameters_training.yaml`）讀 `sample_weight_keys`；`profile_stats` group by `union_dims`；groupBy 前檢查欄位齊備。
- `to-yaml`：新增 `--train-params`（驗證 weight 需真實 `sample_weight_keys`）。

### 5.4 投影/聚合放 python（可測）、JS 鏡像（live preview）
新增純函式 `aggregate_surfaces(stats, neg_mults, group_keys, weight_keys, schema)` → 回傳 ratio 面 rows + weight 面 rows（含 §4.4 投影/連動計算）。python 是真實來源並被單元測試覆蓋；瀏覽器 JS 鏡像同公式做 live 重算（與既有 `suggest_ratio`/`suggest_weight` 的 python↔JS 鏡像同慣例）。

## 6. Edge cases

1. **`label` ∈ `sample_weight_keys`**：編輯器的 `n_pos/n_neg` 由 group by `union \ {label}` 後 `sum(label)` 得出；若 `label` 同時是分組維就自相矛盾，且冷門加權公式（依 per-group `n_pos`）退化。**編輯器直接擋**（清楚 error，提示這種要手寫）。config 系統本身仍允許（A9 不擋 `label`），硬要的人可手寫 `sample_weights`。
2. **`sample_weight_keys` 為空 `[]`**：weight 面不渲染（顯示說明），匯出 weight 區塊為空；ratio 面照常。（base 預設 `[prod_name]`，空是非典型。）
3. **`union_dims` 有欄不在 `df.columns`**：profile 階段清楚 error（多半是 weight 用了未加進 `carry_columns` 的欄）。

## 7. 測試（純 python、不需 Spark）

- `aggregate_surfaces`：
  - Case 1（1:1）weight 面 `n_neg(後)/pos_rate(後)` 正確。
  - Case 2（cross-dimension 丟維共用 ratio）正確。
  - 不在細 cell 層 round（ratio 面 `kept_neg` 總和 == weight 面 `n_neg(後)` 總和）。
- `grid_to_yaml`：
  - ratio key `seg|prod|0`；weight key 依 `sample_weight_keys` 順序串接、arity 正確。
  - **`weight_keys=[prod_name]`、weight 落在未知 product 時必須 raise**（用真實 `sample_weight_keys` 的 A9c，守住原破口）。
  - export 兩組 keys 與 config 不一致 → raise。
- `resolve`：`union_dims` 計算正確；`label` ∈ `weight_keys` raise；`union_dims` 缺欄 raise。
- `render_html` smoke：含 ratio/weight 兩 tab。

## 8. 不做（YAGNI）

- 不廣義化 ratio 面（`sample_group_keys` 仍限 `[segment, item, label]`）。
- 不支援 `label` ∈ `sample_weight_keys`（編輯器層）。
- weight 面批次工具非必要，先不做。
- 不動 production DAG / consistency 模組的既有 predicate（僅在編輯器端正確調用 A5/A9a/A9b/A9c）。
