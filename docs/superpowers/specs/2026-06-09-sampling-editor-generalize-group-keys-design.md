# Sampling Overrides Editor:泛化 sample_group_keys + 試算面板

- 日期:2026-06-09
- 範圍:`scripts/sampling_overrides_editor.py`(+ `tests/scripts/test_sampling_overrides_editor.py`)
- 性質:self-contained dev 工具,不在 production DAG;不動 `src/`、不動 framework。

## 1. 動機

編輯器的 ratio 面目前把 `sample_group_keys` **寫死**成「剛好一個 segment + item + label」三件組
(`resolve_keys`)。但框架 `select_keys`(`helpers_spark.py`)對 `sample_group_keys` 完全泛型——
`concat_ws("|", *group_keys)`,愛幾欄就幾欄;一致性閘 A5 `override_unknown_items` 也已設計成
「item 不在 group_keys 時就不檢查 item 分量」。也就是說 framework 早就支援:

- `[cust_segment_typ, label]`(不含 product)
- `[prod_name, label]`(不含 segment)
- `[a, b, c, label]`(多維度)
- `[label]`(全域)

唯獨編輯器追不上,使用者只要拿掉 product 或 segment,`profile` 第一步就 `ValueError` 退出。
本案讓編輯器**追平 framework 能做的範圍**。

同時補上使用者要的**試算面板**:調倍率/ratio 時即時看到下採後的整體樣本規模,
方便對訓練集有概念。

> Leakage / 日期範圍(原 Feature 1)**不做**:`profile` 已只讀 `train_snap_dates`
> (`profile_stats` 用 `df.filter(time_col.isin(train_snap_dates))`),val/test/calibration
> 天然排除,現況已足夠。

## 2. 目標 / 非目標

### 目標
1. ratio 面 `sample_group_keys` 支援「**label + 任意維度(0、1、多個)**」,對齊 framework。
2. ratio 面新增即時**試算面板**:總計 n_pos / n_neg(下採後)/ 總數 / 正樣本率,
   可依**單一** ratio 維度分組試算。
3. 既有 weight 面、export/`to-yaml`、A5/A9 驗證行為不回歸。

### 非目標
- 不支援 `sample_group_keys` **不含 label** 的形狀(framework 做得到,但編輯器模型不適用,
  見 §3 不變量)→ fail-fast 報明確錯誤,叫使用者手寫。
- 不做多維度交叉分組試算(那其實就是 ratio 表本身)。
- 不改 framework / `src/` / 一致性 predicate(A5 不需改)。
- 不做日期範圍 / leakage 功能。

## 3. 保留的硬不變量:label 是 pos/neg 切分軸

編輯器招牌功能「**保留全部正樣本、只下採負樣本逼近目標倍率 R**」建立在
「每個 group cell 內用 `sum(label)` 拆出 n_pos / n_neg」之上;export 時 label 分量固定填 `"0"`
(只下採負樣本)。因此:

- **label 必須在 `sample_group_keys` 內**。`resolve_keys` 新增 guard:label 不在 → `ValueError`
  (訊息提示:不含 label 的 group_keys 請手寫 overrides,framework 支援)。
- label 不參與 ratio 面的分群維度,而是切分軸。ratio 面的維度
  `ratio_dims = [k for k in sample_group_keys if k != label]`(**保序**)。

## 4. 做法:ratio 面比照既有 weight 面

weight 面早已支援任意 key 數(變長 `keys` tuple + `renderWeight` 動態渲染 N 欄)。
ratio 面照抄此模式。各處改動:

### 4.1 `resolve_keys`(Python)
- 移除「len(group_keys)==3 且剛好一個 segment」的限制。
- 新增 label-in-group_keys guard。
- 回傳改為:`ratio_dims`(list,= group_keys 去 label,保序)、`label_col`、`time_col`、
  `weight_keys`、`union_dims`。(移除 `segment_col` / `item_col`。)
- `union_dims` 邏輯不變:`(sample_group_keys ∪ sample_weight_keys) \ {label}`,ratio 維度在前。
- 既有「label 不可當 weight key」guard 保留。

### 4.2 `aggregate_surfaces`(Python,測試用的 JS 鏡像)
- ratio 面:group by `ratio_dims` tuple(取代 `(segment_col,item_col)`);
  每列保留 `keys`(list)、`n_pos`、`n_neg`、`pos_rate`、`neg_mult`、`ratio`、`kept_neg`、
  `new_pos_rate`。`n_pos==0 → ratio=1.0` 的冷門守則不變。
- weight 面:下採投影改成「fine cell 投影到 `ratio_dims`」查 keep-rate
  (取代 `s[segment_col]+SEP+s[item_col]`)。其餘(median n_pos、suggested_weight)不變。
- 簽名:`segment_col,item_col` → `ratio_dims`。

### 4.3 `grid_to_yaml`(Python)— override key 重建
ratio_rows 改帶 `keys`(ratio_dims 順序的值 list)。重建 override key 的演算法
(label 在任何位置都正確):

```
ratio_dims = [k for k in cfg_group if k != label_col]   # 保序
for each ratio_row:
    parts = []
    it = iter(row["keys"])           # ratio_dims 順序
    for k in cfg_group:              # 完整 group_keys 順序
        parts.append("0" if k == label_col else str(next(it)))
    key = "|".join(parts)
```

- 驗證:`exp_group != cfg_group` 守則不變(防貼舊 export);A5 `override_unknown_items` /
  A9b/A9c 透過 probe 照常跑,**不需改 predicate**(A5 用 `group_keys.index(item)` 自動定位)。
- export 物件改帶 ratio_rows 的 `keys`(取代 `segment`/`product`),
  並維持頂層 `sample_group_keys`(完整,含 label)供 config-match 守則比對。

### 4.4 JS(`_HTML_TEMPLATE`)
- `SEG`/`ITEM` 常數 → `GKEYS`(= ratio_dims 陣列)。
- `buildRatio`:group STATS by `GKEYS` tuple;每列帶 `keys` + `k0..kN`(同 weight 列)。
- `ratioBySI` → 通用「fine cell 投影到 GKEYS」→ keep-rate map。
- `rebuildWeight`:下採投影改用 GKEYS 投影(取代 SEG/ITEM)。
- `renderRatio`:動態渲染 N 個維度欄(同 `renderWeight`)+ 既有計算欄
  (負樣本倍率 / ratio / 實際倍率 / kept_neg / new_pos_rate)。
- `render` / `flt` / `sortBy`:ratio 面的欄改用 `k0..kN`(weight 面已是如此)。
- `exp`:ratio_rows 輸出 `{keys, ratio}`(取代 `{segment, product, ratio}`);
  YAML 即時預覽的 key 用 §4.3 同法重建(label 位置填 0)。
- `ratio_dims` 為空(group_keys==`[label]`)時:單一全域列、0 維度欄,正常運作。

### 4.5 `render_html` 簽名 / 說明文字
- 簽名:`segment_col,item_col` → `ratio_dims`(傳給 JS 當 `GKEYS`)。
- `<details>` 說明:「segment×item」→「label 以外的任意維度」通用敘述。

## 5. 試算面板(Feature 2)

ratio 面表格上方一塊即時 summary(隨 neg_mult/ratio 編輯連動,純 JS roll-up over RATIO store,
不重跑 Spark):

- **總計**:Σn_pos、Σkept_neg(下採後)、Σtotal、整體 pos_rate = Σn_pos / Σtotal。
- **分組下拉**:選項 =「全部」+ 各 `ratio_dim`。選某維度 → 列出該維度各值的
  n_pos / kept_neg / total / pos_rate;選「全部」只顯示總計列。
- 數字來源 = 每列 `preview()` 的下採後結果(與表格一致,含 n_pos==0 冷門守則),確保面板與表格相符。
- weight 面不加面板(weight 不改變樣本數)。

## 6. 測試(`tests/scripts/test_sampling_overrides_editor.py`)

- **反轉** `test_rejects_group_keys_not_a_segment_item_label_triple`:
  `["prod_name","label"]`、`["cust_segment_typ","label"]` 等現在應**通過**。
- **新增** `resolve_keys`:接受 `[seg,item,label]` / `[seg,label]` / `[item,label]` /
  `[a,b,c,label]` / `[label]`;**不含 label → raise**(新 guard)。
- **新增** `aggregate_surfaces`:對 `[seg,label]`(只分 segment)、`[item,label]`(只分 product)、
  多維度、`[label]`(全域單列)驗證 ratio_rows.keys 與 n_pos/n_neg/kept_neg/ratio。
- **新增** `grid_to_yaml`:多維度 / 不含 segment / label 非末位(若測)→ override key 重建正確;
  A5 unknown-product 在 item 仍在 group_keys 時照常 raise、item 不在時跳過。
- **更新** `render_html` 測試:segment/product 斷言 → GKEYS;新增 summary 面板存在性斷言。
- 跑法(worktree):
  `PYTHONPATH=<wt>/src /Users/curtislu/projects/recsys_tfb/.venv/bin/python -m pytest tests/scripts/test_sampling_overrides_editor.py -q`

## 7. 風險 / 注意

- HTML 是 `str.format` 模板,`{{ }}` 跳脫;新增 JS 要維持跳脫,否則 `render_html` 直接炸
  (測試會接到)。
- ratio 面與 weight 面共用 `render`/`sortBy`/`flt`,泛化後兩面結構趨同,留意 ratio 面額外計算欄
  的渲染分支。
- export schema 改變(ratio_rows 由 `{segment,product}` → `{keys}`):舊 export JSON 會與新
  `grid_to_yaml` 不相容——可接受(`exp_group` 守則本就要求重新 profile),但 release note 註明。
