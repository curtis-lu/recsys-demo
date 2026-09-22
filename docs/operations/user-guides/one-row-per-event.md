# 一次事件一列：宣告 `event` 角色

你的資料裡，**同一個 query group 底下同一個 item 出現了很多次**——例如同一週、同一個版位，同一個廣告素材被展示了五次，每一次有自己的當下情境，label 是「那一次有沒有被點」。

框架預設不接受這種資料：它假設一個 query group 裡每個 item 只出現一次，多出來的列會被當成上游重複而擋下。宣告 `event` 角色就是告訴框架「這些不是重複，它們是不同的事件」。

**先確認你要的是 `event`：** 如果你要讓名次只在「同一次請求一起被排的那幾個候選」之間比，要宣告的是 `occasion`，不是 `event`；兩者怎麼選見 [`impression-data-shapes.md`](impression-data-shapes.md)。

**再確認你屬於哪一種：**

- 你想把多次事件**聚合成一列**（例如「這一週有沒有點過」）→ 不要宣告 `event`，在來源 SQL 用 `GROUP BY` ＋ `MAX(label)` 聚起來就好，本文不適用。
- 你要**保留每一次事件各自一列**，因為每一次的情境不同、label 也不同 → 往下讀。

> **這件事只有你知道。** 框架不會替你判斷哪一種對：聚合會丟掉逐次的資訊，不聚合會讓同一個 item 在一次排序裡跟自己競爭名次。兩種都是合法的部署。

## 一分鐘看懂它改變了什麼

```
沒宣告 event（框架預設）              宣告 event ＝ impression_id
┌─ 第 3 週 × 使用者 u1 ──────┐       ┌─ 第 3 週 × 使用者 u1 ─────────────┐
│ 素材 A      label 1       │       │ 素材 A（曝光 i1）  label 1        │
│ 素材 B      label 0       │       │ 素材 A（曝光 i2）  label 0        │ ← 同一 item 多列
└───────────────────────────┘       │ 素材 A（曝光 i3）  label 0        │
  一組 2 列、2 個 item                │ 素材 B（曝光 i4）  label 0        │
                                     └──────────────────────────────────┘
                                       一組 4 列、2 個 item
```

- **identity 變寬**：從 `time ＋ entity ＋ item` 變成 `time ＋ entity ＋ item ＋ event`。
- **query group 不變**：`event` **不**進分組。同一個 item 的多次事件是在**同一次排序裡互相競爭名次**，不是各自成為一次排序。
- **版本號會變**：宣告了就進版本雜湊，`base_dataset_version` 會翻新——你是在建另一份資料集，不該讀回舊產物。

## 要改哪些地方（缺一不可）

### ① 設定：宣告角色

```yaml
# conf/base/parameters.yaml
schema:
  columns:
    time: snap_date
    entity: [user_id, slot_id]
    item: ad_creative
    event: impression_id      # 一欄；多欄就寫成 list
```

一欄或多欄都可以。多欄時**宣告的順序有意義**：同分時照這個順序逐欄決勝。

角色之間不得共用同一欄——`event` 不能同時是 `item`，也不能是 `time` 或 `entity` 的任何一欄。寫錯了 CLI 一進來就會說「identity_columns contain duplicates」。

打錯角色名（例如 `evnet:`）現在會報錯並列出全部不認識的鍵。以前會被靜默丟掉，版本號也不動——跑完排的是一列一個 item，而你以為排的是一次事件一列。

### ② 來源 SQL：`sample_pool` 與 `label_table` 都要帶齊那些欄

兩張表都要有 `event` 的每一欄。少一欄的話，dataset pipeline 的第一個節點就會擋下（B11），訊息會點名是哪張表缺哪一欄。

`label_table` 照舊可以只放正例——接不到的列一律當負例。

**`feature_table` 不用帶**：特徵表是 entity 層級，以 `time ＋ entity` 接到候選列上，`event` 不加寬那把鑰匙。想讓模型看到「這一次事件當下」的特徵，另外宣告一張候選層級特徵表（[dataset §3.8](../../pipelines/dataset.md#38-候選層級特徵表選用)）：它以 identity 接，宣告了 `event` 時 identity 就包含它。代價是離線推論不能用。

### ③ 來源表的 `primary_key` 要跟著加

```yaml
# conf/base/parameters_label_etl.yaml
      primary_key: [snap_date, user_id, slot_id, ad_creative, impression_id]
      quality_checks:
        max_duplicate_key_ratio: 0.0
```

少列一欄，`max_duplicate_key_ratio: 0.0` 會把「同一素材本週的多次事件」判成重複鍵，整條 ETL 停在那裡。

### ④ catalog：預測表要宣告 `event` 的欄

```yaml
# conf/base/catalog.yaml
training_eval_predictions:
  columns:
    - {name: user_id, type: STRING}
    - {name: slot_id, type: STRING}
    - {name: impression_id, type: STRING}   # ← 這一行
    ...
```

預測表落地時只寫宣告過的欄，**沒宣告的會被靜默丟掉**。漏了它，發布出來的表就是同一個 item 有好幾列而分不出來，evaluation 會對一份「寫出來時其實是對的」表報「重複」。A39 在 training 一進來就會擋，所以你不會真的跑到那一步。

### ⑤ `event` 的欄不得當特徵

把它列進 `dataset.prepare_model_input.categorical_columns` 會報錯（A38）。

`item` 是靠那份清單成為特徵的，`event` 刻意不開這個出口：事件 ID 當特徵等於讓樹背下「哪幾次事件被點了」；到秒的時間戳更糟——它會跟著你資料裡的期間內動態走（例如「同一個素材看越多次越不想點」），**訓練好、離線評估也好，學到的東西在線上不存在**。

想讓模型看到「事件發生在幾點」，在特徵表裡另外算一欄。

## mAP 怎麼算：同一個 item 事件兩次的完整例子

**mAP 按列算，不按 item 算。** 同一個 item 的多列各自是一個候選，在同一次排序裡各自佔一個名次。

一個 query group（第 3 週 × 使用者 u1），四列，模型分數與 label 如下：

| 名次 | 列 | score | label |
|---|---|---|---|
| 1 | 素材 A（曝光 i1） | 0.9 | **1** |
| 2 | 素材 A（曝光 i2） | 0.7 | 0 |
| 3 | 素材 B（曝光 i4） | 0.5 | **1** |
| 4 | 素材 A（曝光 i3） | 0.2 | 0 |

AP ＝ 每個正例列在它自己名次上的精確率，平均：

- 第 1 名是正例 → 該點的精確率 ＝ 1／1 ＝ 1.0
- 第 3 名是正例 → 到第 3 名為止有 2 個正例 → 2／3 ≈ 0.667

AP ＝ (1.0 ＋ 0.667) ／ 2 ＝ **0.833**。

mAP 就是把每個 query group 的 AP 平均起來。

**沒宣告 `event` 的話**，這四列會先被聚成兩列（素材 A、素材 B），素材 A 的 label 取 `MAX` ＝ 1，兩列都是正例，AP ＝ 1.0。**同一份原始資料，兩個數字都對，只是在回答不同的問題**：前者問「你排出來的這一串事件，好的排在前面嗎」，後者問「你排出來的素材清單，會被點的排在前面嗎」。

### `@K` 在短的 query group 上

`recall@K` 在 `K ≥ 該組列數` 時恆為 1；`precision@K` 的分母永遠是 `K`，所以這時它是「該組正例數 ÷ K」，不是該組的正例率（只有 1 列、而且被點了的組，`precision@34` 是 1／34）。宣告 `event` 之後每組的列數差距通常很大（有人一週看了 19 次、有人只看了 1 次），所以同一個 `K` 在不同組裡的意思差很多。`dataset_overview` 沒有每組列數的分佈；最寬一組有幾列，看下一段的 `all_k`。

`k_values: "all"` 的意思也跟著變：沒宣告 `event` 時它解析成 item 的種數，宣告了則取**最寬的那個 query group 的列數**。不然一組 30 列、12 個 item 時，`"all"` 會變成 `map@12`，那已經截斷了。這個數記在評估指標檔 `metrics.json` 的 `all_k`，指標存在 `map@<all_k>` 底下；報表的 `@all` 欄與 training 記的 test mAP 都讀它。大類那一層不受影響——聚合之後同一組裡每個大類只剩一列，`"all"` 仍是大類種數。名次熱圖的欄也跟著延伸到最深的名次，不在 item 種數截斷。

## 同分：宣告了 `event` 卻沒有逐筆特徵時，幾乎每一列都同分

今天框架讀得到的特徵都在 `time ＋ entity` 粒度。**同一個 item 在同一組裡的多列，模型看到的特徵完全一樣，所以分數完全一樣。** 名次因此不是由分數決定，而是由同分決勝規則決定。

決勝規則是：score 由高到低 → `item` 由小到大 → `event` 各欄由小到大。

**這個方向不是中性的。** `event` 是時間戳（或與時間同序的流水號）時，早的事件永遠排在同分的晚事件前面。如果你的資料有「同一個東西看越多次越不想點」這種效應，早的事件本來就比較容易是正例，於是 mAP 被系統性地墊高。本 repo 的廣告示例實測過：同分方向讓 mAP 差約 **0.02**，而模型贏熱門度基準線只有 0.136——**比兩個差距小於 0.02 的模型時，光是同分方向就足以翻轉誰贏。**

框架不替你選（考慮過改用雜湊決勝，代價是要在 dataset 多落一欄讓 Spark 與 numpy 算出同一個值，不值得），而是**把數字印出來**：評估報表的〈完整性檢查〉一段有「同分列佔比」，告訴你這件事在你的資料上有多大。

**真正的解法**是接上逐筆的即時特徵，讓分數本來就分得出高下——用候選層級特徵表（[dataset §3.8](../../pipelines/dataset.md#38-候選層級特徵表選用)）。本 repo 的廣告示例接上三個逐筆特徵之後，報表的同分列佔比是 0。`event` 通常是跟它一起用的；只宣告 `event` 而沒有逐筆特徵，你得到的主要是同分。

## 別把沒發生過的候選當負例放進 `sample_pool`

宣告 `event` 之後每組的候選列數自然不齊（有人 19 列、有人 1 列）。**不要為了「補齊」而把沒被展示過的 item 補進去、label 填 0。**

沒被展示過的 item，你**不知道**它會不會得到正向回應。填 0 等於斷言「它一定不會」，而那個斷言的內容其實是「舊系統沒選它」——你等於在教模型模仿舊系統的選擇偏好，然後用同一份偏好去評估它，兩邊都看起來很好。

這是曝光偏差，它的正解是隨機流量或傾向分數加權，不是補零。

### 負向回應怎麼表達

「被展示了而且被明確拒絕」跟「被展示了但沒反應」是不同的事，但 label 只有 0 與 1。用既有的兩個鍵把前者加重：

```yaml
dataset:
  carry_columns: [response_type]        # 把那一欄從 sample_pool 帶到 model_input

training:
  sample_weight_keys: [response_type]
  sample_weights:
    dismissed: 3.0                      # 明確拒絕的列，訓練時加重
```

## 幾件會被擋下、或行為改變的事

| 事情 | 宣告 `event` 之後 |
|---|---|
| `evaluation` 的**監控模式**（不帶 `--post-training`） | **在 CLI 入口被擋下**（A40）。監控模式評估的是離線推論發布的表，而離線推論的候選是框架自己產生的 entity × item 網格，那裡沒有事件可以命名——兩邊認列的方式不同，接出來的是**錯的答案不是少的答案**。用 `--post-training`，它完整支援。 |
| 離線推論（`inference`） | 照跑，但**忽略** `event`：候選仍是 entity × item，產物沒有 `event` 的欄。 |
| 三項排序診斷（`config_shift`、`item_ability`、`suppression`）**與頭號指標的信賴區間**（`evaluation.diagnosis.ci`） | **自動跳過**，報表的〈完整性檢查〉會寫出原因。四者共用同一份診斷抽樣，而且都在 driver 上以「分數 → item」取組內名次——宣告 `event` 之後同 item 同分的多列順序變成任意的。`suppression` 還多一層：它把「負例排在正例之上」彙總成 item 對，同一個 item 會變成壓制自己。信賴區間跳過的理由要特別看一眼：**頭號 mAP 本身照算**（那是 Spark 算的、有接 `event` 決勝欄），跳掉的只是它的區間——不然報表同一行會並排印出兩個用不同同分順序算出來的數字。`model_capacity` 照跑——它只讀模型本身，不碰抽樣。 |
| `time` 的粒度 | **不變**：`time` 仍然是「時段」（例如一週一個），不是到秒的時間。到秒的時間放 `event`。 |
| 重複檢查（粒度閘 B10、evaluation 的 identity 檢查） | **語意不變**：重複仍然是上游壞了。只是「同一筆」的定義多了欄位——`event` 不同的兩列不是重複，連 `event` 都相同的兩列仍然報錯。錯誤訊息會寫出完整的 identity 欄位。 |

## 驗收：怎麼確認真的生效了

1. `base_dataset_version` 應該**變了**（宣告新角色進雜湊）。沒變代表設定沒被讀到，先回頭看是不是改錯了 `conf/` 目錄。
2. `sample_pool` 的列數應該等於你原始事件紀錄的列數，不是聚合後的列數。
3. 評估報表〈完整性檢查〉裡應該出現「同分列數」與「同分列佔比」兩列。沒出現代表 `event` 沒被宣告到。
4. 報表的診斷入口應該少三頁、頭號指標旁邊沒有信賴區間，且〈完整性檢查〉四項各寫著為什麼。

## 相關文件

- [ADR-0021](../../adr/0021-time-stays-a-period-event-role.md)——`time` 維持時段、`event` 是選用角色。
- [ADR-0025](../../adr/0025-query-group-widened-by-occasion-role.md)——identity 的組成與欄位順序、同分規則、重複檢查語意的更正。
- [`impression-data-shapes.md`](impression-data-shapes.md)——`event`、`occasion` 怎麼選；宣告 `occasion` 的操作與代價。
- `CONTEXT.md`——**event**、**identity**、**query group**、**候選** 的定義。
- [examples/ad](../../../examples/ad/README.md)——曝光資料的示例。它目前宣告的是 `occasion`；#378 時它宣告 `event` 整條跑綠過，README 寫了改回去要動哪幾個檔案。
