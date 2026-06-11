# Sampling Overrides Editor:雙因子權重(地板 v + 注意力 A)+ 下採樣降為成本

- 日期:2026-06-10
- 基底:接在 `feat/sampling-editor-general-groupkeys`(PR #70)之上
- 範圍:`scripts/sampling_overrides_editor.py`(+ 測試)。不改框架 `src/`(Phase 2 才動)。
- 來源:與 claude.ai 的抽樣/權重討論(Q1–Q3),對照 `docs/metrics.html`(HPO 目標 = macro per-item mAP)。

## 1. 動機與原則模型

排序模型 `objective=binary`、用預測機率在 query group 內排序;HPO 目標 = **macro per-item mAP@K**
(`hpo_objective: macro_per_item_map`),其權重結構 `ω ∝ 1/|R_p|`(每產品等權)。要讓加權
logloss 逼近它,每列權重拆成兩個**正交**因子:

```
W_(g,p) = A_p × ( 1 if y=1 else v_p )
```

- **v_p(只降負樣本)→ 地板**:把每個產品的有效先驗(分數地板)墊到目標正樣本率 t,消掉
  冷門產品的 `log r_p` 懲罰。**用 post-downsample 計數表示**(φ 已內含於 n_neg_post):
  `v = n_pos·(1−t) / (t · n_neg_post)`。
- **A_p(整個產品同乘)→ 注意力**:讓每產品對 loss 的貢獻向「每產品等權」靠攏,鏡像 mAP 的
  ω。`A = (m_min / m)^α`,`m = n_pos + n_neg_post`,**下調方向**(最冷產品 A=1,越熱越小、≤1,
  不上調少數正樣本)。墊平後 `m ∝ n_pos`,故 A 實質 ∝ `n_pos^(−α)`。

t、α 是策略旋鈕(Phase 2 交 HPO);下採樣只管成本、用 1/φ(即 n_neg_post)自動解耦。
grain = product(segment-only 對組內排序數學上無效;segment×product 可選)。

## 2. 對照現況與本案的「from → to」

| 零件 | 現況 | 本案 |
|---|---|---|
| 地板 | ratio 面用**下採樣**達成(會砍光冷門負樣本) | 搬進 weight 面 **v_p 降負樣本**(留住全部負樣本給 split-finding) |
| 注意力 | weight 面 `(median/n_pos)^α` clamp[1,w_max]**上調** | `A=(m_min/m)^α` **下調方向**(≤1) |
| label 與 weight key | **禁止** label 當 weight key | **要求** label 在 `sample_weight_keys`(pos/neg 切分軸,對稱 ratio 面) |
| 下採樣角色 | 策略(編碼地板) | **純成本**:預設 keep-all,逐格 opt-in 下採;weight 地板自動吃 φ |
| 全域旋鈕 | `target_neg_pos, alpha, w_max` | ratio:可選 cost R(預設 keep-all);weight:**t, α** |

## 3. 對稱性(兩面一致)

兩面都成為 `[dims…, label]`,**label = pos/neg 切分軸,dims = 該面 grain**:
- ratio 面:`ratio_dims = sample_group_keys − label`(現況)
- weight 面:`weight_dims = sample_weight_keys − label`(**新**;label 必須在 weight keys)

## 4. 公式(weight 面,逐 weight_dims cell;post-downsample n_pos / n_neg_post)

```
v = (n_pos>0 and n_neg_post>0) ? n_pos·(1−t)/(t·n_neg_post) : 1.0   # n_pos=0 冷門:keep 負樣本不降
m = n_pos + n_neg_post · v        # 注意力質量 = 地板加權後質量 = n_pos/t（∝ n_pos），非原始 n_neg
A = (m_min / m)^α                 # m_min = 各「n_pos>0」cell 的最小 m；A≤1，最少正樣本的產品=1
w_pos = A
w_neg = A · v
# n_pos==0 的 cell 不參與注意力：A=1, v=1, w_pos=w_neg=1（保留負樣本、不抬不壓）
```

> 關鍵:`m = n_pos + n_neg_post·v = n_pos/t ∝ n_pos`,所以 `A ∝ n_pos^(−α)`——按**正樣本數**正規化
> (少正樣本產品拿較大注意力),不是按負樣本數。對照對話例:m_H/m_C = 141606/366 = 387,α=0.5
> → 注意力比 √387 ≈ 19.7:1。

驗證(面板顯示):
- 加權後有效 pos_rate = `n_pos / (n_pos + n_neg_post·v)` = **t**(所有 cell 相同 → 地板墊平)。
- 地板 logit = `log(n_pos·w_pos / (n_neg_post·w_neg))` = **logit(t)**(所有 cell 相同)。
- nat 差距 = 各產品**自然** logit `log(n_pos/n_neg)` 對最熱門者之差(被消掉的 nats,如 5.97)。
- 注意力佔比 = `A·m` 正規化(看 387:1 被 α 壓到多少)。

## 5. 編輯器改動

- `resolve_keys`(weight 側):`weight_dims = weight_keys − label`;**要求** label 在 weight keys
  (weight keys 非空時),否則 fail-fast(對稱 ratio 面的 label guard)。
- 新增純函式 `two_factor_weights(n_pos, n_neg_post, *, t, alpha, m_min)` → `(w_pos, w_neg, v, A)`。
- `aggregate_surfaces` weight 段:改算 v / A / w_pos / w_neg(取代 cold-boost),回傳每 cell 帶
  `eff_pos_rate / floor_logit / nat_logit / attn_mass`。
- `grid_to_yaml` weight 段:每 cell 匯出兩列 —— 走訪完整 `sample_weight_keys`,label 位置填
  `"1"`/`"0"`、其餘填 dims 值 → `…|1: w_pos`、`…|0: w_neg`(sparse:≠1.0 才出)。A9b/A9c 不變
  (label 分量非 item,不被 A9c 檢)。
- ratio 面:**預設 keep-all**(neg_mult 空 → ratio 1.0);cost 下採為 opt-in。
- HTML/JS:weight 面改雙因子渲染;頂部加 **t、α** 兩個全域輸入(即時重算);試算面板加
  「有效 pos_rate / 地板 logit / nat 差距 / 注意力佔比」;grain=segment-only 警告。
- `render_html` 簽名:加 `weight_dims, t, alpha`(取代 weight 側的 alpha/w_max),保留 ratio 的
  cost R(可選)。`profile` CLI 對應重接;新增 `--t` / `--alpha` options。

## 6. 決策(已與使用者確認 + 預設拍板)

1. 地板搬進權重 v_p(✔ 使用者選)。
2. 注意力 A∝m^−α、下調方向(✔ 使用者選)。
3. Phase 1 先做、靜態 YAML;HPO 參數化留 Phase 2(✔ 使用者選)。
4. (預設)label 在 weight keys 從「禁止」翻「必須」——對稱、可表達 v_p;現有
   `sample_weight_keys:[prod_name]` 需改 `[prod_name,label]`(dev 工具 + config,inference 未部署)。
5. (預設)舊冷門上調公式直接被取代(非並存模式)。

## 7. 非目標(Phase 2,獨立)
t、α 在 train 時參數化生成權重 + 進 HPO search space(改 `io/extract.py`、`search_space.py`),
對既有 `macro_per_item_map` 直接調。編輯器退居定 cost、看分布、給 (t,α) 初值範圍。

## 8. 驗證
- 純函式 + aggregate_surfaces + grid_to_yaml 單元測試(含雙列匯出、label 位置、零正樣本守則)。
- **網頁檢驗**:用合成 profile 統計(比照對話:熱 detime_twd 23601/2.32M、冷 insur_invest
  61/2.34M、含零正樣本格)渲染 `data/profiling/sampling_overrides_editor.html`,開瀏覽器確認
  地板墊平到 t、nat 差距被消、注意力比被 α 壓縮、UX 合理。
