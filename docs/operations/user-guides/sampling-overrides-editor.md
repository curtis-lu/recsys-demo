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
| **ratio 面** | 訓練**成本**（下採正、負樣本） | `parameters_dataset.yaml` 的 `sample_ratio_overrides` | `dataset.sample_group_keys` | 正樣本：直接填保留率；負樣本：`clamp(倍率 × n_pos(後)/n_neg, 0, 1)` |
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

一格有兩個**各自獨立**的下採旋鈕，兩個都只為省訓練成本，都不帶排序意圖：

```
n_pos(後) = round(n_pos × 正樣本保留率)          ← 先算這個
負樣本保留率 = clamp(倍率 m × n_pos(後) / n_neg, 0, 1)
```

`m=5` 表示「每個**留下來的**正樣本配 5 個負樣本」。**倍率的分子是下採後的正樣本數**，
所以你把正樣本砍一半，負樣本會跟著砍一半，實際 neg:pos 仍等於你填的 `m`——「實際倍率」欄
就是拿來核對這件事的。若該格本來就沒那麼多負樣本（`m × n_pos(後) ≥ n_neg`），保留率夾在
1（全留），不會無中生有。

> **為什麼砍正樣本不算「調排序」**：砍掉正樣本只是讓模型看到的樣本變少，並不會讓某個 item
> 在 loss 裡的相對分量變高。要抬升冷門 item 請用 weight 面的 `A` 因子（§3）——那才是設計來
> 做這件事的旋鈕，而且不必犧牲資料。

### 2.2 在本框架

- key＝`dataset.sample_group_keys`（label 以外的任意維度）；`label` 是該 key 的**正/負切分軸**，
  不是分組維度。一格最多匯出兩筆：label 分量 `1`＝正樣本保留率、`0`＝負樣本保留率（見 §5）。
- **兩欄的預設值都＝設定檔的 `dataset.sample_ratio`**，不是 1.0。理由：pipeline 對一個沒有
  override 的 key 就是套 `sample_ratio`，兩欄若預設 1.0，`sample_ratio < 1` 時整張表的試算
  會比實際多算一倍以上的資料量。
- 正樣本欄**永遠是直接填保留率**（0~1）。正樣本沒有「倍率」可言（倍率是相對正樣本算的），
  所以它不受下面的模式切換影響。
- 負樣本有兩種輸入模式（上方切換，兩欄值切換時互不洗掉）：
  - **依負樣本倍率**：填目標 neg:pos 倍率 `m`，工具算保留率。
  - **依保留率**：直接填保留率（`n_pos(後)=0` 的格一律走這個，因為倍率無定義）。
- 群組/批次選取：勾選列後可「依群組選取（維度＝值）」，再用批次框一次設定正樣本保留率與
  負樣本值（兩個輸入框，有填的才套）。
- **`n_pos(後)` 欄會標色**：低於警告門檻（`--pos-warn-min`，瀏覽器裡可改）標黃、**歸零標紅**。
  紅色是硬警告，理由見 §4。

### 2.3 範例

某格 `n_pos=200`、`n_neg=4000`（pos-rate 5%），`sample_ratio: 1.0`。

只填倍率 `m=5`、正樣本全留：

```
n_pos(後) = 200
負樣本保留率 = clamp(5 × 200 / 4000, 0, 1) = 0.25
→ 留 1000 個負樣本，該格 1200 筆、pos-rate 約 16.7%
```

正樣本保留率再填 `0.5`（想再省一半成本）：

```
n_pos(後) = 100
負樣本保留率 = clamp(5 × 100 / 4000, 0, 1) = 0.125
→ 留 500 個負樣本，該格 600 筆、pos-rate 仍是 16.7%、實際倍率仍是 5
```

**成本減半，正負比例不變。** 這正是把倍率定義在 `n_pos(後)` 上的用意。分頁下方的「分組試算」
會把這些逐格結果加總，最後一列「總計　訓練列數」直接給你 `原始 → 下採後（−XX%）`。

匯出 → `sample_ratio_overrides`（見 §5）。

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

最終每筆樣本權重：**`w_pos = A`、`w_neg = A · v`**。

### 3.2 兩個全域旋鈕

| 旋鈕 | 意義 | 直覺 |
|---|---|---|
| **`t`** | 目標正樣本率（地板高度） | 越大＝把冷門 item 墊得越高。常見 `1/6 ≈ 0.167` |
| **`α`** | 注意力阻尼 | `0`＝關閉（不拉平 loss 佔比）；`1`＝完全等權；中間值部分拉平 |

### 3.3 地板分母：連動 / 不連動（couple / decouple）

`v = n_pos(1−t)/(t·n_neg)` 的兩個數要用下採前還是下採後的？這個開關**同時管正負兩邊**，
所以它只有一個意思：weight 面到底看不看 ratio 面。

- **連動 ratio 面（couple，預設）**：正負都用**下採後**的數。好處是套用後實際正樣本率
  **精確落在 `t`**（因為 weight 面知道 ratio 面砍了多少）。
- **不連動（decouple）**：正負都回到**原始**數，負樣本再乘上全域旋鈕 `φ`（`φ=1` 即原始），
  與 ratio 面無關。此時若 ratio 面**同時**有下採，套用後實際正樣本率會偏離 `t`。

> 預設 couple。「不連動」是個 what-if 旋鈕（想在不決定 ratio 面的情況下先看地板長怎樣），
> 不是給正式設定用的。特別注意：**你真的砍了正樣本又選不連動時，`v` 會算得太高**——因為它
> 是拿沒被砍的正樣本數去算的。這是「不連動」的定義使然，不是 bug。

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

### 3.5 加權後驗證（HTML 綠/藍欄）

匯出前，weight 面的診斷欄是**加權後**的檢查：

- **eff pos_rate（有效正樣本率，後）**：每列應 ≈ `t`（地板生效）。
- **地板 logit（後）**：每列應**相同**＝`log(t/(1−t))`（截距懲罰被消掉）。

---

## 4. 邊界情況

- **`n_pos(後) = 0` 的格**：可能是該期本來就沒正樣本，也可能是你把正樣本保留率設成 0
  砍出來的。兩者行為相同：neg:pos 倍率無定義，負樣本欄改為**直接填保留率**（預設＝
  `sample_ratio`，不是「全留」——沒有 `|0` override 的 key，pipeline 本來就會套 `sample_ratio`）。
- **`n_pos(後) = 0` 在 weight 面會整格退出權重模型**，這是砍正樣本最大的地雷。該格的
  `v = A = 1`、`w_pos = w_neg = 1`，在表上跟「這格不需要調權重」長得一模一樣，**不會報錯**。
  所以 ratio 面把這種格標紅、weight 面的「狀態」欄直接寫「退出權重模型」。看到紅色就回頭
  確認你是不是砍過頭了。
- **正樣本剩太少（但不為 0）**：`--pos-warn-min` 以下標黃。這是個**經驗值、不是推導值**，
  預設 30 只是個起點，請依你的資料自己設；工具不會替你決定多少算夠。
- **未命中的 key**：`sample_weights` / `sample_ratio_overrides` 裡打錯或資料期間不存在的值 →
  該筆不中、權重 `1.0`。training 會把這些列進 `sample_weight_report.json` 的 `unmatched_keys`
  （見 [`../pipelines/training.md`](../../pipelines/training.md) §3.5）。
- **`label` 必須在 `sample_weight_keys`（非空時）**：`label` 是雙因子模型 `w_pos` vs `w_neg`
  的正/負切分軸；少了它 profile 會在啟動 Spark 前就報錯。要嘛加上 `label`，要嘛把
  `sample_weight_keys` 設空跳過 weight 面。**`label` 放在 keys 哪個位置都行**（它不進分組維度），
  但位置決定匯出 key 字串裡 `0`/`1` 分量的位置（見 §5）。

---

## 5. 匯出語意：key 怎麼組、貼到哪

兩張表的 key 都是**各自 key-set 的值用 `|` 串接**，`label` 分量代入 `0`（負）或 `1`（正）：

| 匯出 | 貼到 | key 組法 | value |
|---|---|---|---|
| `sample_ratio_overrides` | `parameters_dataset.yaml`（`dataset:` 下） | `sample_group_keys` 串接，每格最多兩筆：label＝`1`→正樣本保留率、label＝`0`→負樣本保留率 | 保留率 |
| `sample_weights` | `parameters_training.yaml`（`training:` 下） | `sample_weight_keys` 串接，每格出兩筆：label＝`1`→`w_pos`、label＝`0`→`w_neg` | 權重 |

`label` 在 keys 的位置就是 `0`/`1` 在 key 字串的位置：

```
sample_weight_keys: [label, prod_name]  →  "1|ccard_ins"（w_pos）、"0|ccard_ins"（w_neg）
sample_weight_keys: [prod_name, label]  →  "ccard_ins|1"、"ccard_ins|0"
```

只有 `!= 1.0`（weight）/ `!= dataset.sample_ratio`（ratio）的格會被匯出（稀疏），**兩個 class
各自判斷**：只砍正樣本的格就只出一筆 `…|1`。

> 舊版（只能砍負樣本）匯出的 JSON 沒有 `ratio_pos` 欄。`to-yaml` 會把它讀成「正樣本沒動」，
> 產出跟以前一樣的單筆 `…|0`，不會憑空生出 `…|1`。

> 改 `sample_weight_keys` 或 `sample_weights` 會 bump `model_version`（屬 training block），
> 不動 `train_variant_id`；一致性檢查 A9a/A9b/A9c 驗欄位、段數、item 分量。詳見
> [`../pipelines/training.md`](../../pipelines/training.md) §3.5、§7。

---

## 6. 操作流程

```bash
cd <repo-or-worktree-root>
export SPARK_CONF_DIR=$PWD/conf/spark-local        # 本機 Spark（見 local-spark-setup.md）
PYTHONPATH=src .venv/bin/python scripts/sampling_overrides_editor.py profile <db.table 或 parquet 路徑> \
  [--params conf/base/parameters_dataset.yaml] \
  [--train-params conf/base/parameters_training.yaml] \
  [--base-params conf/base/parameters.yaml] \
  [--t 0.1666] [--alpha 0.5] [--target-neg-pos 5.0] [--pos-warn-min 30]
```

1. **profile**：對 `sample_group_keys ∪ sample_weight_keys`（去 label）的最細粒度，掃 `sample_pool`
   各格 `n_pos`/`n_neg`，產出 HTML。
2. **編輯**：瀏覽器開 HTML，ratio 面調正/負兩個保留率（預設都＝`sample_ratio`）、看下方
   「分組試算」的總計列確認省了多少；weight 面轉旋鈕 `t`/`α` 看診斷欄。
3. **匯出**：按 Export JSON / Export YAML snippet。
4. **貼回**：把片段貼進對應設定檔（§5），重跑 dataset / training。

`--t`／`--alpha`／`--target-neg-pos`／`--pos-warn-min` 只是 HTML 的**初始**旋鈕值，瀏覽器裡可
即時改；真正落地的是你匯出貼回設定檔的數字。

---

## 7. 術語速查

| 詞 | 意義 |
|---|---|
| ratio 面 | 下採樣覆寫（成本，正負各一個保留率）；→ `sample_ratio_overrides` |
| weight 面 | 雙因子樣本權重（排序抬升）；→ `sample_weights` |
| `n_pos` / `n_neg` | 某格的正/負樣本數（profile 掃出來的） |
| 正樣本保留率 | 直接填的 0~1；`n_pos(後) = round(n_pos × 它)`。預設＝`dataset.sample_ratio` |
| 負樣本保留率 | `clamp(m · n_pos(後) / n_neg, 0, 1)`，或在保留率模式直接填。預設同上 |
| `n_pos(後)` | 下採後的正樣本數。歸零＝該格退出 weight 面的權重模型（標紅） |
| `t` | 目標正樣本率（地板高度） |
| `v` | 地板因子，降負樣本權重把有效 pos-rate 墊到 `t`；`v = n_pos(1−t)/(t·n_neg)` |
| `α` | 注意力阻尼（0 關、1 等權） |
| `A` | 注意力因子，拉平各 item 的 loss 佔比；`A = (m_min/m)^α` |
| `m` | 加權後有效質量 `n_pos + n_neg·v` |
| `w_pos` / `w_neg` | 匯出的正/負樣本權重；`w_pos=A`、`w_neg=A·v` |
| couple / decouple | `v` 的正負兩邊都用下採後（連動）/ 都回原始、負樣本再 ×φ（不連動） |
| `φ` | 不連動時的全域負樣本保留率旋鈕 |

## 相關文件

- [`../pipelines/training.md`](../../pipelines/training.md) §3.5 — `sample_weights` 設定、一致性檢查、`unmatched_keys` 報告
- [`../pipelines/dataset.md`](../../pipelines/dataset.md) — `sample_ratio_overrides` 在 dataset 抽樣的落點
- [`local-spark-setup.md`](../dev-setup/local-spark-setup.md) — 本機跑 `profile` 的 Spark 環境
