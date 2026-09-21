# 曝光紀錄：宣告 `event`、`occasion`，還是兩個都宣告

你的資料一列是「某個 entity 在某一刻被展示了某個 item」，label 是那一次有沒有得到正向回應——例如一次廣告曝光有沒有被點。框架預設的 query group 是 `time` ＋ `entity`，而且假設同一組裡每個 item 只出現一次。這類資料通常不是這個形狀，框架提供兩個**選用**角色讓你說清楚它是什麼形狀：

- **`occasion`**：一次排序的場合，例如一次請求——同一刻、一起被排出來的那幾個候選。宣告之後 query group ＝ `time` ＋ `entity` ＋ `occasion`，名次只在同一次請求裡比。
- **`event`**：同一個 query group 裡、同一個 item 出現不只一列時，用來分辨每一列，例如曝光 ID。它**不**改變 query group。

用比賽來想，兩個角色各回答一個問題：

- **`occasion` 決定誰跟誰同一場比賽。** 同一場的人互相比名次，不同場的不比。一場比賽就是一個 query group：預設是「某個 entity 在某個時段」，宣告 `occasion` 就再切細成「某一次請求」。
- **`event` 是號碼布。** 同一場裡有兩個同名的人——同一個 item 出現不只一列——就靠它分開。

兩個問題互相獨立：想改「誰跟誰比」就宣告 `occasion`；同一組裡同一個 item 有好幾列就宣告 `event`；兩件事都發生就兩個都宣告。

本文先幫你選，再說明宣告 `occasion` 要改什麼、宣告之後什麼會變、這類資料上的離線指標能信到什麼程度。宣告 `event` 的操作細節在 [`one-row-per-event.md`](one-row-per-event.md)。

文中的例子都來自 repo 的廣告示例（[`examples/ad`](../../../examples/ad/README.md)）：`time` 是週，`entity` 是（使用者, 版位），`item` 是廣告素材（12 種），一次請求在同一秒展示 1～6 個互不重複的素材。

## 這類資料跟框架預設的前提差在哪

框架預設的情境是排序，前提是每個 query group 的候選集合是**全網格**：這個 entity 有資格的每一個 item 各一列，由你的 `sample_pool` 來源 SQL 保證。曝光紀錄不是——它是**被展示的子集**，只有舊系統挑出來展示過的 item 才有一列。差別不在每組幾列，在「誰決定哪些 item 進這一組」：

```
全網格（框架預設的前提）                     被展示的子集（本文的資料）
誰在組裡：資格規則決定，跟舊模型無關          誰在組裡：舊系統挑過才擺出來
label 0 ＝「可以選、沒有選」                  label 0 ＝「看到了、沒有回應」
沒在組裡 ＝ 沒資格                            沒在組裡 ＝ 不知道（沒有答案，不是負例）
每組大小通常一樣                              每組大小不一，取決於那次擺了幾個
```

三件事跟著不同，後面各節會展開：

- **指標算得出來，但讀法不同。** 框架每個指標都是「這一組有哪幾列就排哪幾列」，組大小不一不會算錯。但隨便排的 mAP 隨組的大小變（見〈小的 query group 讓 mAP 偏高〉）：全網格下每組一樣大，那條底線是常數，0.6 好不好一看就知道；這裡不是常數，所以 mAP 只能在**同一批資料上**比（兩個模型互比、模型跟熱門度基準線比），不能拿絕對值判斷，也不能跨期直接比——這一期曝光量變了，數字就會動，跟模型無關。
- **每個 item 各自的指標會混進「舊系統愛把它擺給誰」。** 很少被展示的 item 只出現在幾個組裡，數字很抖，而以 item 為單位取平均的指標給它跟大 item 一樣的份量。評估報表這邊有旋鈕：`evaluation.metric.min_positives` 把正例太少的 item 排除在平均之外。**HPO 這邊沒有**：預設目標 `macro_per_item_map` 在 HPO 裡不讀那個設定，每個有正例的 item 一律等權；item 很多、多數很少被展示時，改用 `training.hpo_objective: mean_ap` 比較穩。
- **離線推論只會產生全網格的候選**——每個 entity 配上整份 item 清單。模型是在「被展示過的」配對上學的，離線推論卻要它替從沒見過的配對打分；拿這份結果跑評估的監控模式，沒被展示的 item 會接不到 label、被當成負例。宣告了 `occasion` 或 `event` 時監控模式會在入口被擋下（見〈宣告之後什麼會變〉）。

用 `occasion`、`event` 把資料的形狀說清楚，框架才知道這不是全網格，會跳過不適用的診斷、擋下上面那條路。**資料其實是被展示的子集、卻什麼都沒宣告**（例如先把一週的曝光聚合成「每個 item 一列」再交給框架）時，框架不會報錯，每個數字都照算；上面三件事要你自己記得。

## 先選：兩個問題

**第一個問題：離線的名次要在什麼範圍裡比？**

線上每一次排序是一次請求：同一刻，幾個候選被一起排出來，挑前面的展示。如果你的資料有一欄認得出「這幾列是同一次請求」（例如 `request_id`），而你要離線也只在同一次請求裡比名次，就宣告 `occasion`。`occasion` 可以是多欄：沒有請求 ID、但同一次請求的候選都記在同一秒時，到秒的時間戳就能當 `occasion`（它和 `entity` 合起來認出一次請求）。如果認不出一次請求，或者你要把一個 entity 一整個時段的曝光放在一起比，就不宣告。

**第二個問題：在第一題選定的範圍裡，同一個 item 會出現不只一列嗎？**

會，就宣告 `event`，用它分辨那幾列；不會，就不宣告。

兩題的答案合起來是四種設定：

| | 組內同一 item 只有一列 | 組內同一 item 可以有多列 |
|---|---|---|
| **一組 ＝ entity × 時段**（不宣告 `occasion`） | 什麼都不宣告（框架預設） | 只宣告 `event` |
| **一組 ＝ 一次請求**（宣告 `occasion`） | 只宣告 `occasion` | 兩個都宣告 |

套到廣告示例：

- 以一次請求為一組：一次請求展示的素材互不重複，所以**只宣告 `occasion`**。這是示例目前的設定。
- 以（使用者, 版位, 週）為一組：同一個素材在一週裡會被不同的請求展示好幾次——5,885 組裡有 738 組的曝光數超過素材種數 12，最多的一組 35 列——所以要**只宣告 `event`**。
- 兩個都宣告，只在「同一次請求裡同一個 item 會出現兩次」時才需要，例如資訊流一次載入 6 格，同一個素材出現在第 1 格和第 5 格：`occasion` 讓這一次請求的幾列同一場比，`event` 分開兩個同名的素材。示例沒有這種資料。

## 以時段為一組、以請求為一組：各適合什麼、各自的代價

（ADR-0025 把前者叫「形狀一」、後者叫「形狀二」。）

```
以時段為一組（宣告 event）                  以請求為一組（宣告 occasion）
┌─ u1 在 feed_mid × 第 3 週 ─────────┐      ┌─ … × 請求 r1（週一 09:12）─┐
│ 素材 A（週一 09:12）  label 1      │      │ 素材 A   label 1           │
│ 素材 B（週一 09:12）  label 0      │      │ 素材 B   label 0           │
│ 素材 A（週五 21:40）  label 0      │      └────────────────────────────┘
│ 素材 C（週五 21:40）  label 0      │      ┌─ … × 請求 r2（週五 21:40）─┐
└────────────────────────────────────┘      │ 素材 A   label 0           │
  一組 4 列，同一個素材出現兩次               │ 素材 C   label 0           │
                                             └────────────────────────────┘
```

**以時段為一組**，組比較大，每組有很多列可以比。代價是它把不同時刻的曝光放在一起比：週一早上的素材 A 和週五晚上的素材 C 搶同一個名次，而線上從來不會這樣比。另外，宣告了 `event` 卻沒有逐筆的即時特徵時，同一個素材的多列分數完全相同，名次由同分規則決定（見 [`one-row-per-event.md`](one-row-per-event.md)〈同分〉）。

**以請求為一組**最接近線上真正在做的事：組裡就是那一刻被一起排的候選。代價是組很小。廣告示例平均一次請求 4 個素材，5% 的請求只有 1 個。下一節是這件事對指標的影響。

哪一種對，取決於你的部署線上怎麼用分數，框架不替你選。

## 小的 query group 讓 mAP 偏高

評估只算至少有一個正例的組——沒有正例的組 AP 沒有定義。兩件事因此發生：

- **只有一列的組，只要它是正例，AP 恆為 1**，跟模型排得好不好無關。
- **組越小，隨便排的 mAP 越高。** 一組 n 個候選、其中 1 個正例，隨便排時正例落在第 r 名的機率都是 1/n，AP ＝ 1/r，所以平均 AP ＝ (1 ＋ 1/2 ＋ … ＋ 1/n) ／ n：n ＝ 2 時 0.75，n ＝ 4 時 (1 ＋ 0.5 ＋ 0.333 ＋ 0.25) ／ 4 ≈ 0.521，n ＝ 12 時 ≈ 0.259。

廣告示例 test 週有 445 次請求至少有一個點擊，這 445 組才進評估。其中 7 組（1.6%）只有一列；比全部請求裡單列的 4.9% 少，因為只擺一個素材的請求比較少被點。更大的影響來自組小：把每一組隨便排，mAP 的期望值約 0.559，熱門度基準線的 mAP@12 是 0.578，模型是 0.717。

所以以請求為一組時，**mAP 要跟報表裡的熱門度基準線比，不要跟 0 比**。

對策是只留同時有正例與負例的組再算指標（[#376](https://github.com/curtis-lu/recsys-demo/issues/376) 規劃中的開關，目前還沒有）。在它落地之前，用報表〈規模〉一段估計組有多小：全部列數 ÷ query 數就是平均每組幾列，越接近 1 問題越大。廣告示例 test 週是 1,921 ÷ 445 ≈ 4.3。

### val／test 只留下有正例的組

dataset pipeline 在 val 與 test 只留至少有一個正例的組（`filter_groups_with_positives`）。組內排序指標本來就跳過沒有正例的組，所以 mAP 不受影響；受影響的是「算在過濾之前」的量——`dataset_overview` 的列數、正例率，在 test 上是過濾後母體的數字，比全部曝光的正例率高——廣告示例 test 週全部 3,871 次曝光的點擊率是 0.149，過濾後剩 1,921 列，報表印的正例率是 0.300。要留下一部分沒有正例的組是規劃中的功能（[#429](https://github.com/curtis-lu/recsys-demo/issues/429)）。

## 宣告 `occasion` 要改哪些地方（缺一不可）

### ① 設定：宣告角色

```yaml
# conf/base/parameters.yaml
schema:
  columns:
    time: snap_date
    entity: [user_id, slot_id]
    item: ad_creative
    occasion: request_id      # 一欄；多欄就寫成 list
```

角色之間不得共用同一欄：`occasion` 不能同時是 `event`、`item`，也不能是 `time` 或 `entity` 的任何一欄。寫錯了 CLI 一進來就會說「identity_columns contain duplicates」。

### ② 來源 SQL：`sample_pool` 與 `label_table` 都要帶那些欄

兩張表的每一列都要有 `occasion` 的每一欄，因為 label 是以完整的 identity（`time`、`entity`、`occasion`、`item`）接到候選列上的。少一欄的話，dataset pipeline 的第一個節點就會擋下（B11），訊息會點名哪張表缺哪一欄。`label_table` 照舊可以只放正例。

**`feature_table` 不用帶**：特徵表是 entity 層級，以 `time` ＋ `entity` 接到候選列上（這把鑰匙叫 base key），`occasion` 不加寬它。同一個 entity 同一週的特徵，會一模一樣地接到它每一次請求的列上。

### ③ 來源表的 `primary_key` 跟著加

```yaml
# conf/base/parameters_label_etl.yaml（parameters_sample_pool_etl.yaml 同）
      primary_key: [snap_date, user_id, slot_id, request_id, ad_creative]
      quality_checks:
        max_duplicate_key_ratio: 0.0
```

少列 `request_id`，「同一週同一個素材出現在兩次請求裡」會被判成重複鍵，ETL 停在那裡。列進去還有第二個作用：source_etl 會檢查 `primary_key` 的欄不得是 NULL，而 `occasion` 的欄是 NULL 時框架本身不會報錯（見〈宣告之前先確認〉）。

### ④ catalog：預測表要宣告 `occasion` 的欄

```yaml
# conf/base/catalog.yaml
training_eval_predictions:
  columns:
    - {name: user_id, type: STRING}
    - {name: slot_id, type: STRING}
    - {name: request_id, type: STRING}   # ← 這一行
    ...
```

預測表落地時只寫宣告過的欄，沒宣告的會被靜默丟掉——少了它，評估就分不出兩次請求裡的同一個素材。training 一進來就會擋（A39）。

### ⑤ `occasion` 的欄不得當特徵

把它列進 `dataset.prepare_model_input.categorical_columns` 會報錯（A38）。請求 ID 只是把列分組的標籤，模型拿它當特徵等於背下「哪幾次請求有人點」。想讓模型看到請求的屬性（例如幾點發生），在特徵表裡另外算一欄。

## 宣告之後什麼會變

下表是**只宣告 `occasion`** 的情況；兩個都宣告時有兩列不同，寫在表後。

| 事情 | 只宣告 `occasion` 之後 |
|---|---|
| identity | `time`、`entity`、`occasion`、`item`（再加 `event`，如果也宣告了）。順序是規定：決定性抽樣依這個順序把欄串起來雜湊 |
| query group | `time` ＋ `entity` ＋ `occasion`：LightGBM 的 group、HPO 的評分、`lambdarank` 丟掉無正例的組、評估的每一個指標、比較報表（`--compare`）的「共同 query group 數」，都以一次請求為單位 |
| base key | 不變，仍是 `time` ＋ `entity`：特徵表、分群來源照舊這樣接 |
| 版本號 | `base_dataset_version` 會變：宣告了就進版本雜湊，不會讀回舊形狀的產物 |
| `k_values: "all"` | 仍解析成 item 的種數。組內 item 不重複，所以沒有一組比 item 種數長，這個 K 不會截斷任何一組。`precision@all` 的分母是這個 K，組小時它會很低——看 `map@all`、`recall@all` |
| 評估報表〈完整性檢查〉 | 多兩列「同分列數」「同分列佔比」 |
| 同分規則 | 不變：score 由高到低 → `item` 由小到大（再接 `event`，如果也宣告了） |
| 離線推論 | 照跑，但**忽略** `occasion`：候選是框架替每個 entity 乘上全部 item 產生的，批次評分的當下沒有請求可言。產物沒有 `occasion` 的欄，名次在 `time` ＋ `entity` 裡比 |
| 評估的**監控模式**（不帶 `--post-training`） | **在 CLI 入口被擋下**（A40）。監控模式評估的是離線推論發布的表，它沒有 `occasion` 的欄，而 `label_table` 有——兩邊認列的方式不同，接出來的是錯的答案不是少的答案。用 `--post-training` |
| 比較報表拿 `source: ranked_predictions` 當對手 | **在 CLI 入口被擋下**（A41），理由同上。用 `training_eval_predictions` 或 `enriched_eval_predictions` |
| 診斷 `item_ability` | **自動跳過**，報表寫出原因。它先扣掉每組的平均分數再比較，假設那個平均代表「這個 entity 的整體分數水準」；一組是一次請求時，平均取決於這一次擺了哪幾個素材，只擺一個的請求扣完恆為 0 |
| 診斷 `config_shift`、`suppression`、頭號指標的信賴區間 | 照跑，而且都以一次請求為一組。`suppression` 的共同購買（`cross_purchase`）因此數的是「同一次請求裡兩個素材都被點」：它說的是「這兩個素材常在同一刻一起被點」，不是「點過一個的人也常點另一個」 |

**兩個都宣告時**，同一次請求裡同一個 item 又可以有多列，所以：`k_values: "all"` 改成解析成最大一組的列數；`config_shift`、`item_ability`、`suppression` 與頭號指標的信賴區間**全部**跳過（理由見 [`one-row-per-event.md`](one-row-per-event.md)〈幾件會被擋下、或行為改變的事〉）。其餘各列相同。

## 離線指標在這類資料上的三個限制

離線評估只能用舊系統留下來的紀錄。在曝光紀錄上，這件事有三個具體後果：

1. **曝光偏差。** 紀錄裡只有舊系統選去展示的素材。一個模型只要學會模仿舊系統的偏好，就會把「被選去展示、而且被點」的素材排前面，離線分數好看，線上卻不見得更好。也因此：**沒被展示過的素材不要當負例補進 `sample_pool`**——你不知道它會不會被點，補 0 等於教模型模仿舊系統。
2. **位置偏差。** 同一次請求裡排在前面的位置，本來就比較容易被點，跟素材本身無關。緩解的做法是把位置當成逐筆的即時特徵接進訓練（讓模型把那部分點擊歸給位置），線上評分時把它固定成同一個值（讓每個候選都像在同一個位置上被評）。逐筆的即時特徵要等多張特徵表（[#380](https://github.com/curtis-lu/recsys-demo/issues/380)）。
3. **離線只重排被曝光過的候選，線上是從全部候選裡挑。** 以請求為一組時，每一組是舊系統挑好的 1～6 個素材，離線只問模型「這幾個怎麼排」；線上它要從全部 12 個裡挑。排得好這幾個，不代表挑得好那 12 個——那些沒被展示的素材根本沒有 label 可以評。

前兩者的正解是隨機流量（[#396](https://github.com/curtis-lu/recsys-demo/issues/396)）與傾向分數加權（[#425](https://github.com/curtis-lu/recsys-demo/issues/425)），兩者的前提——部署有隨機流量、拿得到舊系統當時展示每一筆的機率——目前都不成立。

## 宣告之前先確認

框架對下面幾件事**不會報錯**，要你自己確保：

- **`occasion` 的欄不得是 NULL。** 有 NULL 時會安靜地錯兩件事：label 以完整的 identity 接到候選列上，NULL 接不到任何東西，那些列的正例**全部變成 0**；分組時 training 把所有 NULL 的列併進同一組，評估則把它們當成另一個組。兩件都沒有錯誤訊息。擋法是把 `occasion` 的欄列進 `sample_pool` 與 `label_table` 的 `primary_key`（見③）。
- **分群欄如果是「每次請求不同」的屬性**（例如頻道、裝置），預設的分群接法會以 base key 接，每個 entity 每個時段只取一個值，套到它所有的請求上。這種欄要用 `evaluation.segment_sources` 從一張以請求為鍵的表接，`key_columns` 寫 `[snap_date, user_id, slot_id, request_id]` 這類含 `occasion` 的鍵。
- **`config_shift` 的信賴區間會偏窄。** 診斷抽樣以一次請求為單位分層，信賴區間卻以 entity 為單位重抽；同一個 entity 的請求落在不同層時，會被當成兩個獨立的單位。這是 [#389](https://github.com/curtis-lu/recsys-demo/issues/389) 的同一個機制，沒宣告 `occasion` 時要多個評估日期才會發生，宣告之後單一日期也會發生。
- **比較報表的另一邊要是同一組角色宣告跑出來的。** 拿宣告 `occasion` 之前訓練的模型版本來比，它的預測表沒有 `occasion` 的值，兩邊一組也對不上，報表會停在「common_query_groups is empty」。

## 驗收：怎麼確認真的生效了

1. `base_dataset_version` 應該**變了**。沒變代表設定沒被讀到，先確認改的是對的 `conf/` 目錄。
2. `sample_pool` 與 `label_table` 都有 `occasion` 的欄，而且沒有 NULL。
3. 評估報表〈規模〉一段的「全部 query 數」應該是 test 那段時間**有正例的請求數**，不是 entity 數。廣告示例 test 週是 445。
4. 報表〈完整性檢查〉出現「同分列數」「同分列佔比」，診斷入口少了 `item_ability`，並寫著跳過的原因。

## 相關文件

- [ADR-0025](../../adr/0025-query-group-widened-by-occasion-role.md)——為什麼 query group 由 `occasion` 加寬、base key 為什麼不跟著變、離線推論為什麼忽略它。
- [`one-row-per-event.md`](one-row-per-event.md)——宣告 `event` 的操作與它的代價。
- `CONTEXT.md`——**occasion**、**event**、**query group**、**identity**、**base key** 的定義。
- [examples/ad](../../../examples/ad/README.md)——一份宣告了 `occasion` 的設定，可以整條跑起來。
