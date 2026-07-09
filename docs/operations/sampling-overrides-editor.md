# Sampling Overrides Editor

`scripts/sampling_overrides_editor.py`：先 **profile**（掃 `sample_pool` 算每格正/負樣本數），
再用瀏覽器互動調整，**匯出**兩份貼回設定檔的覆寫表。產物是一份離線、self-contained 的 HTML
（預設落 `data/profiling/sampling_overrides_editor.html`）。

> 它**產生設定、不改設定**：匯出的 YAML 片段要你自己貼回 `parameters_dataset.yaml` /
> `parameters_training.yaml`。手填這兩張表（尤其 `sample_weights`）很容易算錯，這支工具就是
> 用實際資料把該填的數字算給你。
>
> 設計 spec：`docs/superpowers/specs/2026-06-10-sampling-weight-twofactor-design.md`、
> `docs/superpowers/specs/2026-06-12-sampling-editor-ratio-input-modes-batch-design.md`。

HTML 內只放「操作當下必要的提醒＋公式速查」；**完整概念、推導與範例在本文件**。看不懂某個欄位
（`v`、`A`、`t`、`α`、couple/decouple…）時回來查這裡。

---

## 1. 心智模型：兩個面、兩件互不相干的事

不平衡資料的排序模型，有**兩個彼此獨立**的決策，常被混在一起調而互相干擾：

1. **要用多少資料訓練？** —— 純粹是**訓練成本**。負樣本爆量時下採樣可省時間/記憶體，
   對「排序好不好」本身沒有正面貢獻（下採過頭反而傷 split-finding）。
2. **每筆樣本在 loss 裡佔多少分量？** —— 決定**模型的排序行為**：冷門 item 會不會被
   base-rate 壓到永遠墊底、各 item 的 loss 佔比要不要拉平。

本工具把這兩件事拆成**兩個分頁（surface）**，各自獨立、各自匯出、各自的 key-set：

| 面 | 管什麼 | 匯出到 | key 來源 | 機制 |
|---|---|---|---|---|
| **ratio 面** | 訓練**成本**（下採負樣本） | `parameters_dataset.yaml` 的 `sample_ratio_overrides` | `dataset.sample_group_keys` | 保留率 = `clamp(倍率 × n_pos/n_neg, 0, 1)` |
| **weight 面** | 排序**抬升**（樣本權重） | `parameters_training.yaml` 的 `sample_weights` | `training.sample_weight_keys` | 雙因子 `v`（地板）× `A`（注意力） |

**關鍵設計（解耦）**：早期常用「下採冷門 item 的負樣本」同時達成省成本＋墊高冷門 item。
本框架**刻意拆開**——下採只當成本旋鈕，墊地板與拉平 loss 佔比改由 weight 面處理。所以
**冷門 item 不必下採、可保留全部負樣本餵給 split-finding**，地板高低只由旋鈕 `t` 決定、
與你在 ratio 面下採多少無關。

> 「面」是抽象框架詞。下文範例用銀行示例的具體欄位（`prod_name`＝item、`cust_segment_typ`＝
> 一個分群維度），但機制對任何 `item` / 任意分群維度都成立。

---

## 2. ratio 面：下採樣＝成本旋鈕

### 2.1 通用原理

對某一格，若正樣本 `n_pos`、負樣本 `n_neg`，想把負樣本下採到「目標 neg:pos = m」，
要保留的負樣本比例：

```
保留率 = clamp(m × n_pos / n_neg, 0, 1)
```

`m=5` 表示「每個正樣本配 5 個負樣本」。若該格本來就沒那麼多負樣本（`m×n_pos ≥ n_neg`），
保留率夾在 1（全留），不會無中生有。

### 2.2 在本框架

- key＝`dataset.sample_group_keys`（label 以外的任意維度）；`label` 是該 key 的**正/負切分軸**，
  不是分組維度，匯出時 label 分量固定填 `0`（見 §5）。
- **預設留空＝全留負樣本（成本旋鈕關閉）**。只在某些格負樣本真的爆量、想省訓練成本時才填。
- 兩種輸入模式（上方切換，兩欄值切換時互不洗掉）：
  - **依負樣本倍率**：填目標 neg:pos 倍率 `m`，工具算保留率。
  - **依保留率**：直接填保留率（全列一致、也涵蓋 `n_pos=0` 的格）。
- 群組/批次選取：勾選列後可「依群組選取（維度＝值）」再「批次套用」把同一個值套到整組。

### 2.3 範例

某格 `n_pos=200`、`n_neg=4000`（pos-rate 5%）。填倍率 `m=5`：

```
保留率 = clamp(5 × 200 / 4000, 0, 1) = clamp(0.25, 0,1) = 0.25
→ 保留 1000 個負樣本，下採後該格 1200 筆、pos-rate 約 16.7%
```

匯出 → `sample_ratio_overrides`（見 §5）。

### 2.4 分群下採 vs 全域下採：差別與為什麼

「下採」有兩種做法，差別在**每一格用不同的保留率、還是全體共用一個保留率**：

- **全域下採（不分群）**：挑一個保留率 `p`，對**所有**負樣本一視同仁隨機丟。每格負樣本都縮同一個
  倍率——**保留了原本的不平衡輪廓，只是整體變小**。
- **分群下採（本工具的 ratio 面）**：每一格（`sample_group_keys` 定義）用**自己的** `n_pos`/`n_neg`
  算保留率，把該格下採到**同一個目標 neg:pos 倍率 `m`**（§2.1 公式；已比 `m` 稀疏的格 clamp 成全留）。
  結果是**把各格的不平衡拉齊到 `m`**，而不是等比縮小。

一句話：**全域＝等比縮小（保留輪廓）；分群＝拉齊到目標倍率（抹平輪廓）。**

#### 為什麼要分群：同樣的成本預算，別餓死小群

下採的唯一目的是省成本（§1），問題是「省在哪」。假設兩個 item，不平衡程度差很多：

| item | n_pos | n_neg | 原始 neg:pos |
|---|---|---|---|
| A（爆量）| 1000 | 100000 | 100:1 |
| B（清瘦）| 200 | 1000 | 5:1 |

想把負樣本從 101000 砍到約 6000。兩種做法結果天差地別：

- **分群到 `m=5`**：
  - A：`clamp(5×1000/100000,0,1) = 0.05` → 留 5000（丟掉 95000 個冗餘負樣本），變 5:1
  - B：`clamp(5×200/1000,0,1) = 1.0` → **全留 1000**，維持 5:1
  - **整刀砍在爆量的 A，清瘦的 B 一根寒毛沒動**；兩格都落在 5:1。
- **全域到同樣總量 6000**（`p = 6000/101000 ≈ 0.059`）：
  - A：100000×0.059 ≈ 5941 → 仍約 5.9:1
  - B：1000×0.059 ≈ **59** → 剩 200 正 : 59 負（neg:pos ≈ 0.3，pos-rate 衝到 77%）
  - **同一把刀連 B 也砍了 94%**，B 的負樣本從 1000 剩 59，split-finding 幾乎沒有負樣本可定切點。

差別很清楚：分群**把成本砍在冗餘**（A 的第 5001~100000 個負樣本對切點的貢獻遞減）、**保護稀缺**
（B 的負樣本本來就不多）；全域做不到「只砍該砍的」，要壓總量就會**連小群一起餓死**。

#### 但別把分群下採當成平衡工具

分群下採到共同倍率 `m`，**副作用**是改了每格的抽樣 pos-rate（上例兩格都變 1/6）——看起來像在「平衡」。
但框架**刻意不靠它做平衡**（§1、§3）：

- 墊高冷門 item 的有效正樣本率是 weight 面 `v` 的事，`v` 靠**降權**達成、**不丟任何負樣本**（全部留給
  split-finding）；下採靠**丟資料**達成、**不可逆**，丟過頭連 split-finding 都傷。
- 所以分群下採的職責是**把成本砍得聰明**（只削爆量格的冗餘），不是修不平衡。`m` 要**留寬**（預設全留，
  只在真的爆量時才填），削的是冗餘、不是命脈。
- 有 couple 連動（§3.3）：`v` 在**下採後**的負樣本數上計算，所以不管你分群下採了多少，排序抬升仍精確由
  `t` 決定、兩件事不互相干擾。

---

## 3. weight 面：雙因子權重＝排序抬升

### 3.1 通用原理：不平衡為什麼傷排序

GBDT/LR 學到的分數，base-rate 越低的 item 整體被往下壓（`log(p/(1−p))` 的截距越負），
冷門 item 容易**整批墊底**；而且**少數熱門 item 會吃掉大部分 loss**，模型懶得學冷門 item。
兩個問題，對應兩個因子：

- **`v`（地板，floor）→ 對抗 base-rate 懲罰**：把每個 item 的**有效正樣本率墊到同一個目標 `t`**。
  作法是降該 item 負樣本的權重，讓加權後 `pos : (neg·v)` 的正樣本率恰為 `t`：

  ```
  v = n_pos · (1 − t) / (t · n_neg)
  ```

  墊完後每個 item 的「地板 logit」一致＝`log(t/(1−t))`，消掉冷門 item 的截距懲罰。

- **`A`（注意力，attention）→ 拉平 loss 佔比**：把各 item 在總 loss 的佔比，從「依樣本量」
  拉向「等權」（鏡像 macro-averaged per-item 指標）。以加權後有效質量 `m = n_pos + n_neg·v`
  衡量分量，**最輕的 item `A=1`、越熱的 item `A` 越小（≤1）**：

  ```
  A = (m_min / m)^α
  ```

  **為什麼 `A` 其實只看正樣本數。** 把地板 `v` 代進 `m`，負樣本數會被約掉：
  `n_neg·v = n_neg · n_pos(1−t)/(t·n_neg) = n_pos(1−t)/t`，於是

  ```
  m = n_pos + n_pos(1−t)/t = n_pos / t
  ```

  `t` 全域固定，所以 `m ∝ n_pos`，`A = (m_min/m)^α =（正樣本最少那格的 n_pos ÷ 本格 n_pos）^α`。
  換句話說注意力**只按正樣本量排名**：正樣本最少的 item 當基準（`A=1`），正樣本越多壓得越低、
  恆 `≤1`（絕不放大稀少的正樣本，只壓熱門的）。負樣本多寡、你在 ratio 面下採多少，都**不會改變
  `A`**（只影響 `w_neg = A·v`）。此化簡在「該格有正也有負」時成立；`m = n_pos + n_neg·v` 才是
  程式碼裡的定義，`n_pos/t` 是它在正常情況的等價寫法。

最終每筆樣本權重：**`w_pos = A`、`w_neg = A · v`**。

### 3.2 兩個全域旋鈕

| 旋鈕 | 意義 | 直覺 |
|---|---|---|
| **`t`** | 目標正樣本率（地板高度） | 越大＝把冷門 item 墊得越高。常見 `1/6 ≈ 0.167` |
| **`α`** | 注意力阻尼 | `0`＝關閉（不拉平 loss 佔比）；`1`＝完全等權；中間值部分拉平 |

### 3.3 地板分母：連動 / 不連動（couple / decouple）

`v` 的分母 `n_neg` 要用哪個負樣本數？

- **連動 ratio 面（couple，預設）**：用**下採後**的負樣本數。好處是套用後實際正樣本率
  **精確落在 `t`**（因為 weight 面知道 ratio 面下採了多少）。
- **不連動（decouple）**：改用 `原始 n_neg × φ`（`φ` 為全域保留率旋鈕，`φ=1` 即原始），
  與 ratio 面無關。此時若 ratio 面**同時**有下採，套用後實際正樣本率會**高於 `t`**（overshoot）。

> 預設 couple。除非你有特別理由要 weight 面忽略 ratio 面的下採，否則維持連動。

### 3.4 範例（兩個 item，`t = 1/6`、`α = 0.5`、全留負樣本）

| item | n_pos | n_neg | 原始 pos-rate |
|---|---|---|---|
| HOT | 1000 | 4000 | 0.200 |
| COLD | 50 | 4000 | 0.012 |

**地板 `v`**（墊到 `t=1/6`）：

```
v_COLD = 50 ·(1−1/6)/((1/6)·4000) = 0.0625   → 有效負樣本 4000·0.0625 = 250，pos-rate 50/300 = 0.1667 = t ✓
v_HOT  = 1000·(1−1/6)/((1/6)·4000) = 1.25     → 有效負樣本 4000·1.25  = 5000，pos-rate 1000/6000 = 0.1667 = t ✓
```

**注意力 `A`**（`m = n_pos + n_neg·v`，`m_min` 取最輕者）：

```
m_COLD = 50 + 250  = 300   m_HOT = 1000 + 5000 = 6000   m_min = 300
A_COLD = (300/300)^0.5 = 1.000     A_HOT = (300/6000)^0.5 = 0.224
```

**匯出權重**：

| item | `w_pos = A` | `w_neg = A·v` |
|---|---|---|
| COLD | 1.000 | 1.000 × 0.0625 = **0.0625** |
| HOT | 0.224 | 0.224 × 1.25 = **0.2795** |

讀法：冷門 item 整體被放大（相對熱門 item，`A_COLD ≫ A_HOT`），且其負樣本被大幅降權
（`v_COLD` 很小）把地板墊高；熱門 item 整體被壓低，避免它吃掉所有 loss。

### 3.5 加權後驗證欄（HTML 綠/藍欄）

匯出前，weight 面右側幾欄是**套上權重後**的自我檢查，各驗一件事（沿用 §3.4 的 COLD/HOT）：

- **eff pos_rate（有效正樣本率）**＝把負樣本用 `v` 折算後的正樣本佔比：

  ```
  eff = n_pos / (n_pos + n_neg·v) = n_pos / m
  ```

  分母正是 `m = n_pos/t`，所以 `eff = t`——**每列都應等於 `t`**，這就是「地板 `v` 生效」的證據。
  注意 `A` 在分子分母同時出現、直接約掉，所以 **`A` 不影響 eff**（拉平 loss 佔比與墊高 pos-rate
  是解耦的兩件事）。COLD：`50 / (50 + 4000×0.0625) = 50/300 = 0.1667 = t` ✓。

- **地板 logit（後）**＝`log(eff/(1−eff))`。既然每列 eff 都 = `t`，這欄每列都應**相同**＝
  `log(t/(1−t))`；冷門 item 原本偏負的截距懲罰被抹平。

- **A·m（loss 佔比）**＝這一格套完地板與注意力後、實際壓在 loss 上的總權重：

  ```
  A·m = n_pos·w_pos + n_neg·w_neg = A·(n_pos + n_neg·v) = m_min^α · m^(1−α)
  ```

  它就是**注意力想拉平的那個量**：一個 item 對 loss（與梯度、分裂增益）的貢獻正比於它內部樣本
  權重的總和。`α=0` 時 `A·m = m`（∝ 正樣本、完全沒拉平，熱門 item 主宰）；`α=1` 時 `A·m = m_min`
  （每格收斂到同一個數＝完全等權）；中間值是 `m` 與 `m_min` 的幾何內插。**調 `α` 就盯這欄收不收斂**：
  越靠攏＝各 item 的 loss 佔比越平均。上例（`α=0.5`，此時 `A·m = √(m_min·m)`）：
  `A·m_COLD = √(300×300) = 300`、`A·m_HOT = √(300×6000) ≈ 1342`；把 `α` 拉到 1，兩者都會壓到
  `m_min = 300`。

---

## 4. 邊界情況

- **`n_pos = 0` 的格（冷門到該期沒有正樣本）**：neg:pos 倍率無定義；ratio 欄改為**直接填保留率**，
  weight 端權重中性（`w_pos=w_neg=1`）。
- **未命中的 key**：`sample_weights` / `sample_ratio_overrides` 裡打錯或資料期間不存在的值 →
  該筆不中、權重 `1.0`。training 會把這些列進 `sample_weight_report.json` 的 `unmatched_keys`
  （見 [`../pipelines/training.md`](../pipelines/training.md) §3.5）。
- **`label` 必須在 `sample_weight_keys`（非空時）**：`label` 是雙因子模型 `w_pos` vs `w_neg`
  的正/負切分軸；少了它 profile 會在啟動 Spark 前就報錯。要嘛加上 `label`，要嘛把
  `sample_weight_keys` 設空跳過 weight 面。**`label` 放在 keys 哪個位置都行**（它不進分組維度），
  但位置決定匯出 key 字串裡 `0`/`1` 分量的位置（見 §5）。

---

## 5. 匯出語意：key 怎麼組、貼到哪

兩張表的 key 都是**各自 key-set 的值用 `|` 串接**，`label` 分量代入 `0`（負）或 `1`（正）：

| 匯出 | 貼到 | key 組法 | value |
|---|---|---|---|
| `sample_ratio_overrides` | `parameters_dataset.yaml`（`dataset:` 下） | `sample_group_keys` 串接、label 分量＝`0` | 保留率 |
| `sample_weights` | `parameters_training.yaml`（`training:` 下） | `sample_weight_keys` 串接，每格出兩筆：label＝`1`→`w_pos`、label＝`0`→`w_neg` | 權重 |

`label` 在 keys 的位置就是 `0`/`1` 在 key 字串的位置：

```
sample_weight_keys: [label, prod_name]  →  "1|ccard_ins"（w_pos）、"0|ccard_ins"（w_neg）
sample_weight_keys: [prod_name, label]  →  "ccard_ins|1"、"ccard_ins|0"
```

只有 `!= 1.0`（weight）/ `!= 預設保留率`（ratio）的格會被匯出（稀疏）。

> 改 `sample_weight_keys` 或 `sample_weights` 會 bump `model_version`（屬 training block），
> 不動 `train_variant_id`；一致性檢查 A9a/A9b/A9c 驗欄位、段數、item 分量。詳見
> [`../pipelines/training.md`](../pipelines/training.md) §3.5、§7。

---

## 6. 操作流程

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local        # 本機 Spark（見 local-spark-setup.md）
PYTHONPATH=src .venv/bin/python scripts/sampling_overrides_editor.py profile <db.table 或 parquet 路徑> \
  [--params conf/base/parameters_dataset.yaml] \
  [--train-params conf/base/parameters_training.yaml] \
  [--base-params conf/base/parameters.yaml] \
  [--t 0.1666] [--alpha 0.5] [--target-neg-pos 5.0]
```

1. **profile**：對 `sample_group_keys ∪ sample_weight_keys`（去 label）的最細粒度，掃 `sample_pool`
   各格 `n_pos`/`n_neg`，產出 HTML。
2. **編輯**：瀏覽器開 HTML，ratio 面調下採（預設全留）、weight 面轉旋鈕 `t`/`α` 看診斷欄。
3. **匯出**：按 Export JSON / Export YAML snippet。
4. **貼回**：把片段貼進對應設定檔（§5），重跑 dataset / training。

`--t`／`--alpha`／`--target-neg-pos` 只是 HTML 的**初始**旋鈕值，瀏覽器裡可即時改；真正落地的是
你匯出貼回設定檔的數字。

---

## 7. 術語速查

| 詞 | 意義 |
|---|---|
| ratio 面 | 下採樣覆寫（成本）；→ `sample_ratio_overrides` |
| weight 面 | 雙因子樣本權重（排序抬升）；→ `sample_weights` |
| `n_pos` / `n_neg` | 某格的正/負樣本數（profile 掃出來的） |
| 保留率 | 下採後保留的負樣本比例 `clamp(m·n_pos/n_neg,0,1)` |
| `t` | 目標正樣本率（地板高度） |
| `v` | 地板因子，降負樣本權重把有效 pos-rate 墊到 `t`；`v = n_pos(1−t)/(t·n_neg)` |
| `α` | 注意力阻尼（0 關、1 等權） |
| `A` | 注意力因子，拉平各 item 的 loss 佔比；`A = (m_min/m)^α` |
| `m` | 加權後有效質量 `n_pos + n_neg·v`；有正有負時 `= n_pos/t`，故 `A` 只看正樣本量 |
| eff pos_rate | 有效正樣本率 `n_pos/(n_pos+n_neg·v) = n_pos/m`，設計上 = `t`；只受 `v` 影響、與 `A` 無關 |
| `A·m` | 該格套完 `v`、`A` 後的 loss 佔比 `= m_min^α·m^(1−α)`；`α=1` 各格收斂到 `m_min`（完全等權） |
| `w_pos` / `w_neg` | 匯出的正/負樣本權重；`w_pos=A`、`w_neg=A·v` |
| couple / decouple | `v` 分母用下採後 `n_neg`（連動）/ 原始 `n_neg×φ`（不連動） |
| `φ` | 不連動時的全域負樣本保留率旋鈕 |

## 相關文件

- [`../pipelines/training.md`](../pipelines/training.md) §3.5 — `sample_weights` 設定、一致性檢查、`unmatched_keys` 報告
- [`../pipelines/dataset.md`](../pipelines/dataset.md) — `sample_ratio_overrides` 在 dataset 抽樣的落點
- [`local-spark-setup.md`](local-spark-setup.md) — 本機跑 `profile` 的 Spark 環境
