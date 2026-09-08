# LTR objective 支援：實測紀錄與被推翻的三條（2026-09-07）

範圍：#313 這組票（#314 快取隔離、#315 零正例過濾、#316 本檔與文件對齊）的實測結果。

判準依據：[`../agents/pipeline-performance-work.md`](../agents/pipeline-performance-work.md)
——特別是**規則 8**（設定的數值不可跨環境搬）與**規則 9**（負面結果要跟正面結果一起留下）。

> **所有數值都來自合成資料**（本機 `local[*]`、1.5 萬合成客戶／22 個 item，或本檔 §8
> 的 in-memory 合成 fixture），**不代表生產**。生產 entity 母體是百萬級。依規則 8：
> **幅度不可跨環境搬，只有方向與同號性可引用。**

數值的來源分兩種，每處都標了：

| 標記 | 意思 |
|---|---|
| **(#313 那輪)** | 在 #313 spec 那一輪量的。**腳本沒有留下來**（見 §8），只有結論在票裡與本檔 |
| **(本檔複驗)** | 2026-09-07 本輪用 §7 的腳本重新量的，腳本全文附在本檔內，可直接重跑 |

---

## 1. 結論先講

**兩個 LTR objective 對「整組沒有正例的 query group」的需求相反**，而 repo 原本假設它們一樣。

| objective | 零正例 group | 訓練矩陣 | 誰決定 |
|---|---|---|---|
| `lambdarank` | 梯度貢獻**精確為零** | **丟掉** | `core.group_utils.objective_drops_zero_positive_groups` |
| `rank_xendcg` | **會學**（隨機目標分布） | 全留 | 同上（回 `False`） |
| `binary`（現行預設） | pointwise 用到每一列 | 全留 | 同上（回 `False`） |

落地的三件事：

1. lgb-binary 快取路徑改以 objective 為 key（`lgb/lambdarank/`、`lgb/rank_xendcg/`、
   `lgb/binary/`），函式 `objective_family` 更名為 `objective_cache_key`（#314）。
2. `lambdarank` 建 train / train_dev 矩陣前丟掉零正例 group，權重與 group id 隨列一起走
   （#315）。濾了什麼寫進 log 與 `group_filter_report.json`。
3. 文件與 conf 對齊（#316，本檔即其中一項）。

**被實測推翻的三條宣稱在 §5**，佔本輪一半的工作量。

---

## 2. 兩個 objective 對零正例 group 的行為（本檔複驗）

**測了什麼**：200 個 query group、每組 8 個候選、**label 全為 0**，兩個 objective 各訓 50 輪。

**量到什麼**（本檔複驗，LightGBM 4.6.0 / Python 3.10.9）：

```
  lambdarank   trees=  1 trees_with_split=  0 pred_std=0.0000
  rank_xendcg  trees= 50 trees_with_split= 50 pred_std=0.0991
```

**怎麼讀**：`lambdarank` 長出的那 1 棵樹是 0 個 split 的常數樹，預測值零變異——它從這些列
學不到任何東西。`rank_xendcg` 50 棵樹全都有 split，預測有實質變異。

**機制**：`rank_xendcg` 的目標分布是 `q_i = (2^{y_i} − γ_i) / Σ_j (2^{y_j} − γ_j)`，`γ` 每輪
重抽。`y` 全為 0 時 `q_i = (1 − γ_i) / Σ_j (1 − γ_j)`——那是一張**隨機榜單**，不是均勻分布，
所以有東西可以擬合。`lambdarank` 則需要一對 label 相異的列才生得出 `λ`，全零組一對都沒有。

> `pred_std` 的絕對值（0.0991）依 fixture 而異，#313 那輪在別的 fixture 上量到 0.045。
> **可引用的是「是否為零」，不是數值本身。**

**本機合成資料的零正例比例（#313 那輪）**：train **42.2%**、train_dev **46.8%** 的 query group
屬於這一類。**這是 1.5 萬合成客戶的數字，不代表生產**——生產 entity 是百萬級，比例會完全
不同。這個數字只用來說明「值得做」，不用來推生產的節省幅度。

---

## 3. `rank_xendcg` 的 `ndcg` 讀數會虛高（本檔複驗）

**LightGBM 把零正例 query group 的 NDCG 記為 1.0（滿分），不是 0。**

**測了什麼**：同一個 booster，在「含 30% 零正例 group 的評估集」與「濾掉它們的評估集」上
各算 60 輪的 `ndcg@5`，檢驗算式 `NDCG_未濾 = f × 1.0 + (1 − f) × NDCG_已濾`。

**預期什麼**：若 LightGBM 記零正例組為 1.0，等式應在機器精度內成立；若記為 0，`NDCG_未濾`
會低於 `NDCG_已濾`。

**量到什麼**（本檔複驗）：

```
  zero-positive group share f = 0.3000
  iter 1  : full=0.767649  filtered=0.668069  f+(1-f)*filtered=0.767649
  iter 60 : full=0.830910  filtered=0.758443  f+(1-f)*filtered=0.830910
  max |full - (f + (1-f)*filtered)| over 60 iters = 3.33e-16
  argmax iteration: full=60  filtered=60   (same -> early stopping unaffected)
```

`3.33e-16` 是浮點誤差量級——等式成立，**滿分計法被證實**。

**人看到的數字會虛高且被壓縮**：零正例佔比 `f` 越高，讀數越靠近 1。看到 0.99 不是模型
很強，是 `f` 很高。

### 3.1 這條算式有一個前提：**每一列的權重都是 1**

上面那個等式只在權重全為 1 時長成 `f × 1.0 + (1 − f) × NDCG_已濾`。**權重不是 1 的時候，
零正例 group 的貢獻不是 1.0，是 `1/w̄`**（`w̄` ＝該 group 的平均列權重）——LightGBM 把
未加權的 1.0 放進分子，卻用 query 權重和當分母。

**測了什麼**：一個**只有**零正例 group 的評估集，整份設同一個列權重，看 `ndcg@5` 讀數。

**預期什麼**：若貢獻恆為 1.0，讀數應與權重無關；若是 `1/w̄`，讀數應與權重成反比。

**量到什麼**（本檔複驗，§8 的 `ltr_probe3.py`）：

```
== E1. an eval set of ONLY zero-positive groups ==
  every row weight=None    -> ndcg@5 = 1.000000   (1/w = n/a)
  every row weight=1.0     -> ndcg@5 = 1.000000   (1/w = 1.000000)
  every row weight=5.0     -> ndcg@5 = 0.200000   (1/w = 0.200000)
  every row weight=0.5     -> ndcg@5 = 2.000000   (1/w = 2.000000)
  every row weight=0.2632  -> ndcg@5 = 3.799392   (1/w = 3.799392)
```

**逐位相符 `1/w`。讀數可以大於 1。**

**這不是理論上的顧慮，它正好落在生產設定上**：early stopping 的 valid set 是 `train_dev`
（`pipelines/training/steps/hpo_scoring.py`），它的 `.bin` 是帶 per-row `weight=` 建的
（`models/lightgbm_adapter.py`）。而 §4 的雙因子公式給冷門 item 的負例 `w_neg = A·v`、
`v < 1`，零正例 group **整組都是負例列** → `w̄ < 1` → 讀數往上跑。本機 conf 的
`sample_weights` 是 `{}`（權重全 1，等式成立），**生產有值**。

實測（30% 零正例 group，只把那些 group 的列權重設成 `v = 0.2632`）：

```
== E2. does a non-uniform weight still leave argmax alone? ==
  full, weight=1                               last=0.790231  argmax=19
  full, weight=v on the zero-positive groups   last=1.014470  argmax=19
  filtered (zero-positive groups removed)      last=0.700330  argmax=19
```

**讀數 1.014470 已經超過 1。** 所以「0.99 是被 `f` 撐高的上限」這個直覺在加權下也不對——
看到超過 1 的 ndcg，那是加權造成的，不是壞掉。

### 3.2 early stopping 仍然不受影響（理由要換一個）

三條曲線的 argmax 都是第 19 輪（上面的 E2）；未加權的對照（§3 開頭）兩者 argmax 都是第 60
輪；#313 那輪在真實訓練上量到兩者 `best_iteration` 都是 279。

**但理由不是「係數是 `f` 與 `1 − f`」**——加權之後係數不再是那兩個。能撐住的是更弱、也更
穩的版本：**未濾的讀數是已濾讀數的一個正斜率仿射轉換**（每個 group 的貢獻是一個與
iteration 無關的正數），而正斜率仿射轉換不改變 argmax。

**這條保證還有第二個前提**：early stopping 的 `min_delta = 0`。
`models/lightgbm_adapter.py` 呼叫的是 `lgb.early_stopping(stopping_rounds=...)`，沒有傳
`min_delta`，取 LightGBM 4.6.0 預設 `0.0`——**目前成立**。若日後有人設 `min_delta > 0`，
壓縮係數會讓門檻的實際嚴格度改變，這條保證就要重看。

### 3.3 #313 那輪的算式對不上，不要引用

#313 那輪記下的是 `0.7015 × 1.0 + 0.2985 × 0.9534 = 0.98596`。**實算是 0.98609**，
差 `1.3e-4`——而本檔 §3 的複驗把同一條等式收在 `3.33e-16`。差了約 10¹² 倍，所以那不是
浮點誤差，是那三個數至少有一個記錯了（要湊出 0.98596 需要 `f = 0.6987` 或
`NDCG_已濾 = 0.95296`）。

`f = 0.7015` 本身也與 §2 的零正例佔比（train 42.2%、train_dev 46.8%）對不上，而票裡沒有
記載它量在哪個 split。**腳本已不存在（§8），無從追查。**

**處置：這組數字不作為證據使用。** 機制由本檔 §3 的複驗背書，那份的腳本在 §8、可以重跑。
留著這一節是因為它已經被寫進 #313 的票裡，下一個人會看到——這裡說明它為什麼不能引用。

---

## 4. `sample_weights` 在 LTR 下：方向成立、校準失效

完整的適用範圍寫在
[`../operations/user-guides/sampling-overrides-editor.md`](../operations/user-guides/sampling-overrides-editor.md)
§3.6。這裡只留實測。

**測了什麼**：400 個 group × 8 個候選，item 3 是冷門 item（正樣本率 5.0%）。三種權重設定
各訓 60 輪，比 item 3 的平均分數與平均組內名次（名次越小越前面）。

**量到什麼**（本檔複驗；`v = n_pos(1−t)/(t·n_neg) = 0.2632`，`t = 1/6`）：

| objective | 權重設定 | item 3 平均分數 | item 3 平均名次 |
|---|---|---|---|
| `lambdarank` | baseline（全 1.0） | −1.8333 | 4.763 |
| `lambdarank` | **整個 item 3 一律 ×3** | **−2.3360** | **5.112**（更後面）|
| `lambdarank` | 雙因子：只把負例降到 `v` | **−1.2704** | **4.567**（更前面）|
| `rank_xendcg` | baseline | −0.0679 | 4.700 |
| `rank_xendcg` | 整個 item 3 一律 ×3 | −0.1714 | 5.213 |
| `rank_xendcg` | 雙因子：只把負例降到 `v` | +0.0396 | 4.420 |
| `binary` | baseline | +0.0860 | 4.758 |
| `binary` | 整個 item 3 一律 ×3 | +0.0746 | 5.070 |
| `binary` | 雙因子：只把負例降到 `v` | +0.1115 | 4.555 |

**兩個結論**：

1. **把某個 item 的權重「一律調高」，它的分數會下降、名次會變後面。** per-row weight 放大的
   是**該列 label 的方向**，而冷門 item 的列 95% 是負例，放大的絕大部分是「把它往下推」的力。
   **三個 objective 都一樣**——這不是 LTR 專屬的效應。
2. **雙因子公式的方向在 LTR 下仍然成立。** 它之所以有效，正是因為它**不**一律調高：
   `w_pos = A`、`w_neg = A·v`，冷門 item 的 `v < 1` 把負例壓下去。三個 objective 都讓 item 3
   往前移。

> 幅度不可跨環境搬（規則 8）；可引用的是**同號性**：一律調高 → 三者皆下降；雙因子 → 三者皆上升。

**LTR 專屬的額外機制**（#313 那輪的推導，本檔未獨立複驗）：LightGBM 對 ranking objective
是先按 pair 算完 `λ`、彙總到列，**才**逐列乘 weight，所以一對 `(正例 i, 負例 j)` 產生
`+λ_ij·w_i` 與 `−λ_ij·w_j`；`w_i ≠ w_j` 時這一對的推力不對稱。LTR 的標準做法是 query-level
權重而非 row-level，正是為了避開這件事。

---

## 5. 被實測推翻的三條宣稱

規則 9：被否決的假設跟存活的機會一樣是結果。這三條都曾經被寫下來或被主張過。

| # | 測了什麼 | 預期什麼 | 量到什麼 | 為什麼否決 |
|---|---|---|---|---|
| **1** | repo 原本認為**兩個 LTR objective 都能砍零正例群**（寫在 `2026-09-06-dataset-pipeline-profiling.md` §2.3）。用 label 全 0 的 group 各訓 50 輪 | 兩者都學不到東西，都可以砍 | `lambdarank` 1 棵樹／0 split／std 0；`rank_xendcg` 50 棵樹／50 個有 split／std 0.0991（§2） | **`rank_xendcg` 那半是錯的。** 它的目標分布在 label 全 0 時是隨機榜單、不是零，所以砍掉那些 group 等於讓它少學一批它真的會用到的列（本機合成資料上那是 42.2% 的 train **query group**，不是 42.2% 的列）。原文已就地更正 |
| **2** | 本輪一度主張**「逐列抽樣在 LTR 下比 pointwise 更嚴重」**。12 個配對 seed（同 seed、同資料、只差有沒有抽樣，測試集永遠是完整榜單）比 macro mAP **(#313 那輪)** | LTR 對逐列抽樣更敏感 | macro mAP Δ：`lambdarank` −0.0085 [−0.0156, −0.0015]；`rank_xendcg` −0.0137 [−0.0207, −0.0067]；`binary` −0.0133 [−0.0221, −0.0046] | **原主張被否決，但反方向的排名也撐不起來。** 三個 95% CI 大量重疊，依 `pipeline-performance-work.md` 規則 4（判準是區間不是中位數）**不能據此說誰最耐**。能說的是：`lambdarank` 沒有比 pointwise 更嚴重，原主張的方向沒有證據。真正有區別的是第 3 列的名次位移——那一欄 `lambdarank` 的 CI 含 0，另外兩個不含。機制上 pairwise loss 只看組內分數差，沒有絕對基準率可以被抬高 |
| **3** | 本輪一度主張**「被抽樣的 item 會被系統性排到前面」**。同一組 12 seed，量被抽樣 item 的平均名次位移 **(#313 那輪)** | 三個 objective 都會系統性前移 | `lambdarank` +0.29（95% CI **含 0**）；`rank_xendcg` −0.56（12/12 前移）；`binary` −0.85（11/12 前移） | **只在 `binary` 與 `rank_xendcg` 成立。** `lambdarank` 沒有系統性偏移。`binary` 的偏移有閉式解：負例保留率 `r` 讓該 item 的 log-odds 固定平移 `−log r`；`lambdarank` 沒有這個截距 |

**共同的教訓**：三條都是「兩個 ranking objective 名字像、就假設行為一樣」的變體。
`objective_family` 這個舊函式名把這個假設寫進了 code（docstring 明寫
「so lambdarank and rank_xendcg share one group-bearing binary」），#314 已更名為
`objective_cache_key` 並讓兩者各自成段。

---

## 6. 查過之後**沒有**加的旋鈕

規則 10（零成本的淘汰手段先跑完）：這四個都是讀 config／讀 code／跑一次 in-memory 實驗
就能定案的，不需要動 pipeline。

| 旋鈕 | 為什麼不加 |
|---|---|
| `lambdarank_truncation_level` | **它是活的，不是無效參數**，但**預設值多半已經讓它是 no-op**。LightGBM 4.6.0 的預設是 **30**（`booster.model_to_string()` 印得出來）；本檔複驗（每組 8 個候選）：設 3 → 與不設的 maxdiff **1.84463**（模型不同）；設 8 或 30 → maxdiff **0**（完全相同）。**規則是「層級 ≥ 組內候選數 ⇒ no-op」**，所以候選數 ≤ 30 的設定（銀行示例是 22 個 item）現況下這旋鈕根本沒作用；候選數長到 30 以上，預設就開始默默截斷。不加成 config 鍵的理由是**訓練目標會與評估目標對不上**——HPO 的評分是 `hpo_objective: macro_per_item_map`（`conf/base/parameters_training.yaml`），它走 `pipelines/training/steps/hpo_scoring.py` 的 `compute_macro_per_item_map`，而 `evaluation/metrics.py` 的 `k` 預設 `None`＝不截斷、全榜 mAP；把梯度集中到前 N 名等於訓練一件與評分不同的事。（**不要引用 `parameters_evaluation.yaml` 的 `metric.k`**：依該檔自己的註解，那是診斷側 CI 的截斷 k，只有 `diagnosis/metric/` 讀它，主評估的 k 來自 `evaluation.k_values`。）**要用的人今天就能用**：`algorithm_params` 沒有白名單，會被整包 splat 進送給 `lgb.train` 的 params（`pipelines/training/nodes.py` 的 refit 路徑之後只覆寫 `seed`／`feature_pre_filter`／`num_iterations`／`early_stopping_rounds` 與 HPO 的 `best_params`，不含這個鍵），也可以放進 `search_space` 讓 HPO 搜。**陷阱**：它對 `rank_xendcg` 是完全靜默的 no-op——本檔複驗三個層級的 maxdiff 全為 **0**。#313 那輪另外量到 `verbosity=1` 下連 warning 都沒有；§8 的腳本是 `verbosity=-1`，**驗不到這一條** |
| `label_gain` | binary label 只用到預設 `label_gain` 的前兩格 `[0, 1]`，正確。`drop_zero_positive_groups` 用 `y > 0` 判定正例，所以日後改成分級相關度也不會壞 |
| `objective_seed` | `seed=42`（現行已傳入）已讓 `rank_xendcg` 完全可重現：同 seed 兩次跑 maxdiff = 0.0 **(#313 那輪)**。顯式改 `objective_seed` 才會不同 |
| 自動把 `metric` 從 `binary_logloss` 改成 `ndcg` | consistency **A7** 已經 fail-loud 擋下 objective／metric 不搭的組合。fail loud 比靜默修正好 |

另外**撤回**過一個提議：在 `core/consistency.py` 加一條「ranking objective ＋ 非空
`sample_weights` 就 fail」的 predicate。撤回理由見 §7 第 2 條。

---

## 7. 已知的缺口（是已知的，不是被漏掉的）

1. **LTR 版的地板公式沒有人推導過。** `v = n_pos(1−t)/(t·n_neg)` 的存在理由是對抗
   `log(p/(1−p))` 的**截距**懲罰，而 lambdarank 的 loss 只看組內分數差、沒有那個截距。
   所以 `sampling-overrides-editor.md` §3.6 只能回答「這樣調的依據在 LTR 下不成立」，
   **回答不了「那 LTR 下該怎麼調」**。§4 的實測顯示方向仍然對，但沒有校準的目標值。
2. **`scripts/sampling_overrides_editor.py` 不知道 objective 是什麼**（已查證：全檔沒有
   `objective` 字樣），它的匯出值一律照 pointwise 推導。**這是刻意不擋的**——早先提議在
   ranking objective 下 fail loud，前提是「`sample_weights` 目前是空的」，而那只對本機
   conf 成立、**生產有值**，硬擋會在切換 objective 的當下打爆生產。改為只標明適用範圍。
3. **切 objective 後模型不會與濾之前逐位元相同。** 梯度貢獻為零是精確的，但兩個間接效應
   會讓模型不同，兩個都是 LightGBM 的既有行為、不是 bug：`min_data_in_leaf` 用**列數**
   計數（零梯度的列一樣被算進去，會鬆綁 split 限制）、分箱邊界隨資料分布改變。
   #313 那輪的 12,000 列 × 8 候選 × 200 輪實測：

   | 條件 | 組內 Spearman | 最大絕對差 |
   |---|---|---|
   | 共用分箱邊界 ＋ `min_data_in_leaf=1` | **1.000000** | 4.4e-15 |
   | 只共用分箱邊界 | 0.9808 | 2.2 |
   | 只 `min_data_in_leaf=1` | 0.9799 | 2.7 |
   | 兩者皆用預設 | 0.9781 | 2.4 |

   第一行證明梯度貢獻確實是零。**驗收條件因此是「行為正確」而不是「模型不變」**，而且切
   objective 本來就會 bump `model_version`（`objective` 在被 hash 的 `training:` 區塊內）。
4. **沒有生產環境的數字。** 上面每一個比例、每一個幅度都是合成資料。

---

## 8. 怎麼重跑

**#313 那輪的腳本沒有留下來。** 那一輪在 `.worktrees/ltr-support` 進行，
`data/verification/` 目錄現已不存在——所以票裡的 12-seed 表、Spearman 表、`best_iteration`
279 這些數字**無法用一行指令重跑**，只能依上面的描述重建。這是本輪的一個實際損失，
也是為什麼本檔把複驗腳本**全文寫進 git**、而不是指向一個 gitignored 的路徑。

§2、§3 的腳本（不需要 Spark、不碰 repo 任何資料，秒級）：

```python
# ltr_probe.py
import numpy as np
import lightgbm as lgb

RNG = np.random.default_rng(0)
CAND = 8  # candidates per query group


def make(n_groups, pos_per_group):
    """n_groups query groups of CAND rows; pos_per_group positives in each."""
    X = RNG.normal(size=(n_groups * CAND, 6))
    y = np.zeros(n_groups * CAND, dtype=int)
    for g in range(n_groups):
        if pos_per_group:
            idx = RNG.choice(CAND, size=pos_per_group, replace=False)
            y[g * CAND + idx] = 1
    X[:, 0] += y * 1.5           # real signal, so scores are not pure noise
    return X, y, np.full(n_groups, CAND)


def count_splits(booster):
    trees = booster.dump_model()["tree_info"]
    return len(trees), sum(
        1 for t in trees if "split_index" in t["tree_structure"])


print("== A. all-zero-label groups, 50 rounds ==")
Xa, ya, ga = make(200, 0)
assert ya.sum() == 0
for obj in ("lambdarank", "rank_xendcg"):
    ds = lgb.Dataset(Xa, label=ya, group=ga, free_raw_data=False)
    b = lgb.train({"objective": obj, "verbosity": -1, "seed": 42,
                   "num_threads": 4}, ds, num_boost_round=50)
    n_trees, n_split = count_splits(b)
    print(f"  {obj:12s} trees={n_trees:3d} trees_with_split={n_split:3d} "
          f"pred_std={b.predict(Xa).std():.4f}")

print()
print("== B. NDCG of a zero-positive group, and early stopping ==")
n_zero, n_pos_groups = 60, 140
Xz, yz, gz = make(n_zero, 0)
Xp, yp, gp = make(n_pos_groups, 1)
X = np.vstack([Xz, Xp]); y = np.concatenate([yz, yp])
g = np.concatenate([gz, gp])
f = n_zero / (n_zero + n_pos_groups)

Xt, yt, gt = make(300, 1)            # training set, unrelated to the eval sets
ds_tr = lgb.Dataset(Xt, label=yt, group=gt, free_raw_data=False)
ds_full = lgb.Dataset(X, label=y, group=g, reference=ds_tr, free_raw_data=False)
ds_filt = lgb.Dataset(Xp, label=yp, group=gp, reference=ds_tr,
                      free_raw_data=False)

res = {}
lgb.train({"objective": "lambdarank", "metric": "ndcg", "ndcg_eval_at": [5],
           "verbosity": -1, "seed": 42, "num_threads": 4},
          ds_tr, num_boost_round=60,
          valid_sets=[ds_full, ds_filt], valid_names=["full", "filtered"],
          callbacks=[lgb.record_evaluation(res)])

full = np.array(res["full"]["ndcg@5"])
filt = np.array(res["filtered"]["ndcg@5"])
pred = f * 1.0 + (1 - f) * filt
print(f"  zero-positive group share f = {f:.4f}")
print(f"  iter 1  : full={full[0]:.6f}  filtered={filt[0]:.6f}  "
      f"f+(1-f)*filtered={pred[0]:.6f}")
print(f"  iter 60 : full={full[-1]:.6f}  filtered={filt[-1]:.6f}  "
      f"f+(1-f)*filtered={pred[-1]:.6f}")
print(f"  max |full - (f + (1-f)*filtered)| over 60 iters = "
      f"{np.abs(full - pred).max():.2e}")
print(f"  argmax iteration: full={int(full.argmax())+1}  "
      f"filtered={int(filt.argmax())+1}")
```

§4、§6 的腳本（同樣秒級）：

```python
# ltr_probe2.py — truncation level, and the per-row weight direction
import numpy as np
import lightgbm as lgb

CAND = 8
rng = np.random.default_rng(7)
n_groups = 400
X = rng.normal(size=(n_groups * CAND, 6))
item = np.tile(np.arange(CAND), n_groups)   # item id = position in the group
y = np.zeros(n_groups * CAND, dtype=int)
for gi in range(n_groups):
    # item 3 is the cold one: positive in ~5% of groups
    pick = 3 if rng.random() < 0.05 else int(rng.choice([0, 1, 2, 4, 5, 6, 7]))
    y[gi * CAND + pick] = 1
X[:, 0] += y * 1.2
X[:, 1] += (item == 3) * 0.8                # item 3's own feature signature
g = np.full(n_groups, CAND)
is3 = item == 3


def fit(obj, w=None, **extra):
    ds = lgb.Dataset(X, label=y, group=None if obj == "binary" else g,
                     weight=w, free_raw_data=False)
    p = {"objective": obj, "verbosity": -1, "seed": 42, "num_threads": 4,
         "deterministic": True, "force_row_wise": True}
    p.update(extra)
    return lgb.train(p, ds, num_boost_round=60).predict(X)


def mean_rank(s):
    o = (-s.reshape(n_groups, CAND)).argsort(axis=1).argsort(axis=1) + 1
    return o[:, 3].mean()          # mean within-group rank of item 3


print("== C. lambdarank_truncation_level (8 candidates per group) ==")
for obj in ("lambdarank", "rank_xendcg"):
    base = fit(obj)
    for lvl in (3, 8, 30):
        p = fit(obj, lambdarank_truncation_level=lvl)
        print(f"  {obj:12s} truncation_level={lvl:2d}  "
              f"maxdiff vs unset = {np.abs(p - base).max():.6g}")

print()
print("== D. uniform boost vs the two-factor (w_pos=A, w_neg=A*v) shape ==")
n_pos = int(y[is3].sum()); n_neg = int(is3.sum()) - n_pos
t = 1 / 6
v = n_pos * (1 - t) / (t * n_neg)
print(f"  item 3: n_pos={n_pos} n_neg={n_neg}  v = {v:.4f} (t=1/6)")
w_uniform = np.ones(len(y)); w_uniform[is3] = 3.0
w_twofactor = np.ones(len(y)); w_twofactor[is3 & (y == 0)] = v
for obj in ("lambdarank", "rank_xendcg", "binary"):
    print(f"  {obj}")
    for name, w in (("baseline (all 1.0)", np.ones(len(y))),
                    ("uniform 3x on item 3", w_uniform),
                    ("two-factor: w_neg = v on item 3", w_twofactor)):
        s = fit(obj, w)
        print(f"    {name:34s} mean score {s[is3].mean():+.4f}  "
              f"mean rank {mean_rank(s):.3f}")
```

§3.1、§3.2 的腳本：

```python
# ltr_probe3.py — what a zero-positive group is worth once weights are not 1
import numpy as np
import lightgbm as lgb

CAND = 8
rng = np.random.default_rng(0)


def make(n_groups, pos_per_group):
    X = rng.normal(size=(n_groups * CAND, 6))
    y = np.zeros(n_groups * CAND, dtype=int)
    for g in range(n_groups):
        if pos_per_group:
            y[g * CAND + rng.choice(CAND, size=pos_per_group,
                                    replace=False)] = 1
    X[:, 0] += y * 1.5
    return X, y, np.full(n_groups, CAND)


P = {"objective": "lambdarank", "metric": "ndcg", "ndcg_eval_at": [5],
     "verbosity": -1, "seed": 42, "num_threads": 4}
Xt, yt, gt = make(300, 1)
ds_tr = lgb.Dataset(Xt, label=yt, group=gt, free_raw_data=False)
base = lgb.train(P, ds_tr, num_boost_round=20)

print("== E1. an eval set of ONLY zero-positive groups ==")
Xz, yz, gz = make(100, 0)
for w in (None, 1.0, 5.0, 0.5, 0.2632):
    wv = None if w is None else np.full(len(yz), w)
    ds = lgb.Dataset(Xz, label=yz, group=gz, weight=wv, reference=ds_tr,
                     free_raw_data=False)
    res = {}
    lgb.train(P, ds_tr, num_boost_round=1, init_model=base, valid_sets=[ds],
              valid_names=["z"], callbacks=[lgb.record_evaluation(res)])
    print(f"  every row weight={str(w):7s} -> ndcg@5 = "
          f"{res['z']['ndcg@5'][0]:.6f}   (1/w = "
          f"{'n/a' if not w else format(1 / w, '.6f')})")

print()
print("== E2. does a non-uniform weight still leave argmax alone? ==")
Xa, ya, ga = make(60, 0)      # zero-positive groups
Xb, yb, gb = make(140, 1)     # positive-bearing groups
X = np.vstack([Xa, Xb]); y = np.concatenate([ya, yb])
g = np.concatenate([ga, gb])
# cold-item-like weights: the all-negative groups get w_neg = v < 1
w = np.ones(len(y)); w[: len(ya)] = 0.2632
sets = {
    "full, weight=1": (X, y, g, None),
    "full, weight=v on the zero-positive groups": (X, y, g, w),
    "filtered (zero-positive groups removed)": (Xb, yb, gb, None),
}
curves = {}
for name, (Xe, ye, ge, we) in sets.items():
    ds = lgb.Dataset(Xe, label=ye, group=ge, weight=we, reference=ds_tr,
                     free_raw_data=False)
    res = {}
    lgb.train(P, ds_tr, num_boost_round=60, valid_sets=[ds], valid_names=["e"],
              callbacks=[lgb.record_evaluation(res)])
    c = np.array(res["e"]["ndcg@5"]); curves[name] = c
    print(f"  {name:44s} last={c[-1]:.6f}  argmax={int(c.argmax()) + 1}")
```

跑法（任一 worktree，先照 `CLAUDE.md` 的 pre-flight 確認 venv）：

```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python ltr_probe.py
/Users/curtislu/projects/recsys_tfb/.venv/bin/python ltr_probe2.py
/Users/curtislu/projects/recsys_tfb/.venv/bin/python ltr_probe3.py
```

**這三支腳本的輸出就是本檔引用的數字**，不是另一份改寫過的版本——本檔初稿曾把 §6 的
`truncation_level=3` 寫成 maxdiff 1.57，那是另一支結構不同的腳本跑出來的；上面這支跑出的
是 **1.84463**，已更正。貼腳本進文件的意義就在這裡：對不上就查得出來。

環境：Python 3.10.9、LightGBM 4.6.0、numpy 1.25.0。

---

## 9. 這輪的限制

- **沒有跑過一輪真的 `lambdarank` training pipeline。** 過濾的正確性由單元測試與
  `prepare_train_inputs` 的行為測試背書（讀回 `.bin` 斷言 group 向量與列數），不是由
  端到端的分數變化背書。切 objective 之後的模型品質**沒有人量過**。
- **§5 的兩張 12-seed 表是 #313 那輪的數字，本輪沒有複驗**（腳本已不存在，見 §8）。
  它們支撐的是「不要把逐列抽樣綁進這組票」這個範圍決策，不支撐任何交付物的行為。
- **零正例比例 42.2% / 46.8% 只量過一次**，在一份合成資料上。它決定的是「值不值得做」，
  不是「會省多少」。
- **「LightGBM 把零正例 group 記為滿分」只有黑箱證據。** §3、§3.1 是從外部量出來的
  （讀數逐位等於 `1/w̄`），**沒有讀 LightGBM 原始碼確認實作**。wheel 裡只有編譯好的二進位。
  所以 LightGBM 換版之後這個行為變了，這裡的算式不會有人出聲——重跑 §8 的
  `ltr_probe3.py` 是最便宜的複查。
- **加權讀數 > 1 只在合成 fixture 上量過**，沒有拿生產的 `sample_weights` 實際值跑過
  （本機 conf 是 `{}`）。機制（`1/w̄`）已實測，會不會真的越過 1 取決於生產的權重分布。

---

## 相關文件

- [`../agents/pipeline-performance-work.md`](../agents/pipeline-performance-work.md) — 本檔遵循的推理紀律
- [`2026-09-06-dataset-pipeline-profiling.md`](2026-09-06-dataset-pipeline-profiling.md) §2.3 — 被本輪更正的那段
- [`2026-09-07-training-pipeline-profiling.md`](2026-09-07-training-pipeline-profiling.md) — 「98% 的時間在 `lgb.train`」的出處
- [`../operations/user-guides/sampling-overrides-editor.md`](../operations/user-guides/sampling-overrides-editor.md) §3.6 — weight 面在 LTR 下的適用範圍
- [`2026-09-08-lambdarank-weight-scale-anomaly.md`](2026-09-08-lambdarank-weight-scale-anomaly.md) — **未結案**：權重整體縮放會改變 lambdarank 的模型，代數說不該如此
- `conf/base/parameters_training.yaml` 的 LTR 說明區塊 — 操作面的摘要
