---
status: accepted
date: 2026-09-24
---

# training 在 test 上算的指標由設定決定；promote 只比計分月份跟現在設定相同的版本

training 在 test 上算出的指標寫進 `evaluation_results.json` 與 MLflow，讀它的有兩支腳本：挑出建議 promote 版本的 `scripts/promote_model.py`，以及 `scripts/model_capacity_diagnosis.py`。本 ADR 決定四件事：test 上算哪些指標、在哪幾個 time 值上算、promote 拿哪一個指標比、這些指標怎麼算才不浪費。另外決定 evaluation 不接手 training 算好的結果。起頭是 #418 項目 4。

## 背景：改之前的三個落差

**一、算了一整包，只用到 4 個值。** `compute_test_mAP_spark`（`pipelines/training/nodes.py`）呼叫 evaluation 用的 `compute_all_metrics`，拿到完整一包：每個 K 的 map、precision、recall，每個 item 的 hit_rate，`dataset_overview`，而且開了 `evaluation.item_categories` 時，整套在大類粒度再算一次。最後只留 `overall_map`、`per_item_map_attr`、`n_queries`、`n_excluded_queries`。以「大類開著、沒宣告選用角色」的設定讀 code 數：14 次 Spark action，其中 9 次的結果整個丟掉，另 1 次只是把一個已經算過的數（item 數）再算一遍。沒有量秒數。

它也讓 training 的數字依賴 evaluation 的設定。兩個值都是從帶 K 的鍵讀出來的（`map@{K}`、`map_attr@{K}`），K 是 `"all"` 解析成的數字；`evaluation.k_values` 裡沒寫 `"all"`（也沒有剛好寫到那個數字；`map_attr@{K}` 另外還會在 `evaluation.metric.k` 剛好是那個數字時存在），這兩個鍵就不存在，`overall_map` 與 `per_item_map_attr` 都會靜默記成 0.0，沒有任何檢查擋得住。這與 #434 修過的是同一種形狀。

**二、挑參數的尺與挑版本的尺不同。** HPO 目標（`training.hpo_objective`）在 val 上挑參數，promote 在 test 上只看 `overall_map`（每個 query group 一票，熱門的 item 佔大宗）。只有 HPO 目標是 `mean_ap` 時兩者才一致：conf 的預設 `macro_per_item_map`（每個 item 一票）在 test 上沒有現成的值，要自己拿 `per_item_map_attr` 去平均；二元預測類的兩個目標（見決定 2）在 test 上完全沒有對應的數字。所以預設設定下，看不出「HPO 挑出來的東西，到沒看過的資料上還成不成立」，promote 用的也不是那把尺。

**三、計分用到哪些月份，沒有人控制。** `test_snap_dates` 不進 model_version（ADR-0001），同一個版本可以陸續被預測更多月份。預測都寫進同一張表 `training_eval_predictions`，它的 catalog 條目只按 model_version 篩。predict 的 node 只預測 `test_snap_dates` 上的月份，計分的 node 卻把表裡這個版本的所有月份都算進去。於是同一個版本的分數會跟著表變，不同版本的分數可能考的是不同的考卷：

```
同一個版本：
  訓練時 test_snap_dates = [2026-01-31]                  → 分數只含 1 月
  加了 2 月的預測，有人重跑計分那一步                     → 分數變成 1、2 月合計
  之後把 2 月移出 test_snap_dates，再重跑                → 2 月照樣算進去（表裡的分區還在）

不同版本：
  版本 A 訓練時 test_snap_dates = [2026-01-31]           → A 的分數只含 1 月
  之後改成 [2026-04-30, 2026-05-31]，改了特徵、訓練出 B   → B 的分數含 4、5 月
  promote_model.py 把兩個分數並排排名，畫面上沒有標示考卷不同
```

## 決定

### 1. 設定決定 test 上另外算哪些指標、在哪幾個 time 值上算

```yaml
test_metrics:            # 頂層區塊，不在 training: 底下，改它不會換 model_version
  snap_date: ...         # 計分月份；寫法同 evaluation.snap_date；沒設＝dataset.test_snap_dates
  metrics: []            # 在排序類之外另外要算的指標
  selection_metric: ...  # 選版指標；沒設＝實際生效的 HPO 目標
```

每次都會算的有三組，開跑時 log 印出實際清單：

- **排序類的兩個指標**（`mean_ap`、`macro_per_item_map`），連同原本那 4 個值，一律算。它們共用最便宜的一趟（見決定 2），讀 `overall_map` 的程式（寫 MLflow 的 `pipelines/training/steps/experiment_log.py`、`model_capacity_diagnosis.py`）因此永遠讀得到，每個版本也永遠有兩個排序類的值可以比。
- **選版指標與 HPO 目標**。少了前者，promote 無從排名；少了後者，看不出訓練目的在 test 上成不成立。例外只有一種：選版指標明寫成排序類、HPO 目標卻是二元預測類，而 test 上算不出來時（設定或用到的 dataset 版本沒留無正例的 query group），HPO 目標那個值不給、寫明原因；見決定 4 最後一段。
- **`metrics` 裡列的指標**。排序類已經一律會算，所以實際上是用來加二元預測類。

「實際生效的 HPO 目標」：conf 寫了 `training.hpo_objective` 就是它；沒寫時是 `tune_hyperparameters` 的預設 `mean_ap`。conf 的預設值（`macro_per_item_map`）與程式的預設值不同，本 ADR 不處理：改程式的預設值不會換 `search_id`，中斷後接續的 HPO 會混用兩種分數。

`evaluation_results.json` 保留原本 4 個鍵，另外記下每個指標的值、實際算到的 time 值，以及選版指標與 HPO 目標的名字。

### 2. 指標登記在一張表，花費按「趟」算

每個指標一列：名字、類別、它要哪一趟計算、val 上的算法（HPO 用）、test 上的算法。HPO 的可選值、CLI 入口的檢查、HPO 的評分、test 的計分、promote 都讀這張表，取代今天分散的兩處：`core/consistency.py` 的 `HPO_OBJECTIVES`，以及 `pipelines/training/steps/hpo_scoring.py` 裡 `_hpo_score` 的手寫 if 鏈（新增目標時漏改它，要到執行時才會報錯）。新增一個指標就是加一列；另外新增一個測試，逐列檢查每個指標都有 test 上的算法。

類別有兩種：

- **排序類**（`mean_ap`、`macro_per_item_map`）：在每個 query group 內比名次，不同 query group 之間的分數不拿來比。沒有正例的 query group 算不出名次好壞，直接略過。
- **二元預測類**（`pooled_average_precision`、`macro_per_item_average_precision`）：每一筆候選當成一題「會不會是正例」的是非題，不分 query group。`pooled_average_precision` 把全部候選一起按分數排；`macro_per_item_average_precision` 讓每個 item 的候選各自按分數排、各算一個值，再對 item 平均。沒有正例的 query group 裡的候選全是負例，照樣算進去。

四個指標在同一份資料上給出不同的答案（用 `evaluation/metrics.py` 的四個函式實算，權重都是 1）：

```
query group 1：item a 0.90 正例、item b 0.20 負例
query group 2：item a 0.15 正例、item b 0.10 負例
query group 3：item a 0.50 負例、item b 0.05 負例      ← 沒有正例

mean_ap、macro_per_item_map：1.0
  每組都把正例排第一；第 3 組沒有正例，略過

pooled_average_precision：0.75
  全部一起排：0.90 正、0.50 負、0.20 負、0.15 正、0.10 負、0.05 負
  第 2 個正例排在第 4 位，前面有 2 筆負例

macro_per_item_average_precision：0.83
  item a 自己排：0.90 正、0.50 負、0.15 正 → 0.83
  item b 沒有正例，不進平均
```

要同一趟計算的指標共用那一趟，多拿一個指標幾乎不花錢：

- **排序類那一趟**：3 次 Spark action，數 query group、算每組的 AP、算每個 item 的歸因。`mean_ap` 就是 `overall_map`，`macro_per_item_map` 是 `per_item_map_attr` 對 item 取平均。「計分月份是不是每個都有資料」的檢查併在數 query group 那一次裡，不另開 action。
- **二元預測類**的兩個各要一趟：`pooled_average_precision` 要把全部候選一起排，`macro_per_item_average_precision` 要按 item 分開排。沒有人要的那一趟不跑。

排序類不再需要 K。AP 不截斷時，每一列「有沒有排進前 K」那一項恆為 1，所以不必先數出 `"all"` 是多少。test 的計分從此不讀任何 `evaluation.*` 設定（`k_values`、`metric.k`、`item_categories`），背景一的 0.0 陷阱跟著消失。

### 3. 選版指標獨立成一個鍵，沒設定時跟 HPO 目標一樣；promote 照現在的設定挑

HPO 目標在 `training:` 底下，是每個 model_version 各自的設定。promote 要把多個版本放在同一把尺上比，這把尺不能取自各版本自己。預設跟著 HPO 目標，訓練目的只要寫一次。

`promote_model.py` 改成讀設定，而且讀的是**執行當下**的設定：

- **尺**＝現在的 `test_metrics.selection_metric`；沒設就是現在實際生效的 HPO 目標。
- **考卷**＝現在的計分月份。

只有「`evaluation_results.json` 裡有這把尺的值、而且記下的 time 值跟這份考卷完全一樣」的版本參加排名（兩邊先正規化成同一種日期寫法再比）。`Recommended` 與不帶參數時的自動挑選，都從參加排名的版本裡挑。其他版本照樣列在畫面上，寫明為什麼沒排、怎麼補。各版本當初自己的選版指標是什麼不重要：每個版本記下了它算過的所有指標的值，promote 只讀這把尺那一個。

### 4. 二元預測類在 test 上用 Spark 精確算；val 照舊用 numpy

原則：Spark 為主，driver 只收非收不可的東西。val 的預測本來就在 driver 上產生（LightGBM 在 driver 上對 val 預測），HPO 在那裡評分不必搬資料。test 的預測已經寫進 Hive 表，沒有理由搬回 driver，而且 driver 的記憶體得跟著 test 的候選列數長。

要精確值，因為 HPO 在 val 上算的是精確值（scikit-learn 的 `average_precision_score`），兩邊要能比。evaluation 的 `pr_auc` 先把分數分進固定數目的箱（預設 1000 箱），箱內的先後已經丟掉，不等於精確值（ADR-0024 的〈更正〉第 1 條），不能拿來頂替。

算法只用 Spark 內建函式，不用 UDF。同分與權重照 scikit-learn 的規則：同一個分數當成同一個門檻；每一列用 `zero_positive_group_weight` 當權重。

- **`macro_per_item_average_precision`**：按 item 分區的 window，每個 item 的候選各自按分數由高到低累加正例與總列數，算出每個門檻的 precision，再對 item 聚合。預計 1 次 action。
- **`pooled_average_precision`**：每個門檻的 precision＝（分數比它高的全部正例 ＋ 同分的正例）÷（分數比它高的全部列 ＋ 同分的列），分子分母都要「從最高分一路累加到這裡」的總和；有權重時，正例數與列數都換成權重和。開一個不分區的 window 可以直接累加，但所有資料會擠進同一個 partition。改成四步：
  1. 算出分數的切段點，把分數由高到低切成若干段；同一個分數一定落在同一段。
  2. 每段算出自己的正例總數與列數，交給 driver。
  3. driver 算出每一段開頭之前的累計（前面所有段的總和）。
  4. 各段在段內累加、加上自己的開頭累計，算出每個門檻的 precision 與它對 AP 的貢獻，最後加總。

  driver 只經手每段的兩個總數與最後的一個數字。預計 3 次 action。

同一個定義因此有兩份程式：val 的 numpy 版、test 的 Spark 版。兩份用對帳測試綁住：同一批資料，兩邊的值在既有對帳測試的容差內相等。`macro_per_item_map` 已經有這種測試（`tests/test_evaluation/test_metrics_spark.py` 的 `test_macro_per_item_map_numpy_matches_spark`），但它刻意避開同分；其他三個要補，二元預測類的兩個要刻意放進同分與不等的權重。

前提：test 必須留下沒有正例的 query group 並帶權重，也就是 `dataset.test_zero_positive_group_ratio` > 0，而且 catalog 的 `training_eval_predictions` 宣告了 `zero_positive_group_weight`（後者 A45 已經在擋）。否則 test 的母體與 val 不同，兩個數字比不了。這個比例在 `base_dataset_version` 裡，改它要重建資料、重新訓練。

這個前提在兩個地方確認：

- **每個指令的入口看現在的設定**，在 dataset 用錯的比例建資料之前就擋下。
- **training 的入口在決定好要用哪個 dataset 版本之後、開跑之前**，讀那個版本的 manifest 記下的 `parameters_dataset`，看建資料時的 test 比例（鍵沒寫時照預設的 0）。這一處擋的是「設定已經調高，用到的卻是比例 0 的舊資料」：改了比例卻沒重跑 dataset、training 照 `data/dataset/latest` 用了舊資料，或是補救舊版本時。放在入口而不是計分的 node 裡，是為了在 HPO 跑完之前就擋下。

兩處的處理一樣：選版指標是二元預測類（不論是明寫的，還是跟著 HPO 目標來的），或 `metrics` 裡有二元預測類，報錯停下；選版指標是排序類時，HPO 目標那個二元預測類不給值、寫明原因。

### 5. evaluation 不讀 training 算好的結果

evaluation 裡只有 `--post-training` 模式讀同一張 `training_eval_predictions`，而且兩邊算的仍然不是同一份：

- **月份**：evaluation 只算 `evaluation.snap_date` 的月份。
- **分群**：evaluation 按 `evaluation.segment_columns` 切片，那些欄從母體表接上來，training 的表沒有。
- **全正的 query group**：evaluation 照 `evaluation.query_filter.drop_all_positive_groups` 決定去留，training 刻意不跟（#376）。

evaluation 為了切分群，最貴的那一步（替每個 query group 排名）一定得自己做。重用只省得下幾次計數，卻要多一道「兩邊資料一樣嗎」的檢查，而且 training 得繼續算完整一包，瘦不下來。

## 不一致的情況怎麼處理

設定面的新檢查都寫在 `core/consistency.py`，掛在哪裡照既有的先例：

- **只影響 training 的**（計分月份、指標名稱）：由 training 指令在入口檢查，不併進每個指令都會跑的 `validate_config_consistency`。先例是 A26、A36：後果只落在 training，擋住其他指令是白擋（同一條規則最早由 #158 定下）。
- **跟 test 的資料有關的**（二元預測類需要 test 留無正例的 query group）：與 A48 一樣由 `validate_config_consistency` 在每個指令的入口檢查。比例在 dataset pipeline 生效，等到 training 才擋，資料已經用錯的比例建好了。
- **`promote_model.py`** 讀設定時跑同一組檢查，不合法就報錯停下，不挑任何版本。

| 情況 | 處理 |
|---|---|
| 計分月份寫成空清單 | training 入口擋下 |
| 計分月份有 time 值不在 `dataset.test_snap_dates` 裡 | training 入口擋下 |
| 計分月份把同一個 time 值寫成兩種寫法 | training 入口擋下，與 A26 對 `test_snap_dates` 的規則相同 |
| 計分月份有 time 值在表裡沒有預測 | 計分的 node 報錯停下，要求先跑 predict；與 evaluation 的 `compute_metrics` 同一個做法 |
| 表裡有計分月份以外的 time 值（舊月份、被移出 `test_snap_dates` 的月份） | 不算；log 印出略過了哪些（從表的分區清單得知） |
| `metrics` 或 `selection_metric` 寫了登記表裡沒有的名字 | training 入口擋下 |
| 選版指標是二元預測類（不論是明寫的，還是跟著 HPO 目標來的），但 test 沒留無正例的 query group | 每個指令的入口擋下。訊息給兩個解法：把 `dataset.test_zero_positive_group_ratio` 調到 > 0（會換 dataset 版本），或把 `test_metrics.selection_metric` 明寫成排序類指標 |
| `metrics` 裡有二元預測類，但 test 沒留無正例的 query group | 每個指令的入口擋下：明確要了，就必須算得出來 |
| 只有 HPO 目標是二元預測類（選版指標明寫成排序類），test 沒留無正例的 query group | 不擋；這個指標不給值，寫明原因。它是自動加進來的，不為它逼使用者改 test 的資料 |
| 設定的 test 比例 > 0，training 用到的 dataset 版本當時卻是 0（改了比例沒重跑 dataset，或補救舊版本時） | training 入口讀那個 dataset 版本的 manifest 判斷，處理跟上面三列相同：選版指標（不論明寫或跟著 HPO 目標）或 `metrics` 要的二元預測類擋下；選版指標是排序類時，HPO 目標那個二元預測類不給值、寫明原因（決定 4 最後一段） |
| 事後改了計分月份，只重跑計分那一步 | `evaluation_results.json` 被覆寫，檔案記著實際算到的 time 值。MLflow 保留舊值，要更新得連 `log_experiment` 一起跑；promote 讀的是 JSON，不受影響 |
| 照 `adding-an-eval-month.md` 加月份 | 見〈後果〉第二條；該文件要加上那一條的對策 |
| 計分月份與 `evaluation.snap_date` 不同 | 允許。一個拿來挑版本，一個拿來出報表 |
| 版本記下的 time 值跟現在的計分月份不同 | promote 不讓它參加排名，列在畫面上並寫明原因 |
| 版本沒有現在這把尺的值（當初沒算，或算不了） | promote 不讓它參加排名，列在畫面上並寫明原因 |
| 舊版本的 `evaluation_results.json` 沒記 time 值 | 視為考卷不明：不參加排名 |

## 考慮過、沒選的做法

**計分月份直接用 `dataset.test_snap_dates`，不加鍵。** 少一個鍵，predict 與計分只認一份清單。沒選，因為兩件事會被綁死：想「預測 6 個月、分數只算最近 1 個月」就得縮短 `test_snap_dates`，而 evaluation 只能評 `test_snap_dates` 裡的月份（A22），一縮連舊月份的報表都出不了。

**選版指標直接用各版本自己的 HPO 目標，不加鍵。** 不同版本的 HPO 目標可以不同，各用各的尺就排不了名。

**選版指標用固定的預設（例如 `macro_per_item_map`），不跟 HPO 目標。** 尺不會因為一次 HPO 實驗就換掉（〈後果〉第四條的風險就不存在）。沒選：訓練目的要寫兩個地方；只要改了 HPO 目標、忘了改選版指標，挑參數與挑版本就又用不同的尺，也就是背景二原本的問題。

**promote 按各版本記下的月份分群，每群各推薦一個。** 結果是好幾個推薦，而且每群用的尺可能不同；使用者要的是「現在該 promote 哪一個」。

**選版指標算不出來時不擋，讓 promote 什麼都不排。** promote 會靜悄悄失效，要到有人想挑版本時才發現。

**有版本被排除時，不帶參數的自動挑選停下來，要使用者指定版本。** 較保守，但自動挑選原本就是給「照規則挑」用的；被排除的版本與原因已經列在畫面上。

**排序類指標另外記下每個 time 值的小計**（`mean_ap` 記每個 time 值的 AP 總和與 query group 數，`macro_per_item_map` 記每個（item, time 值）的歸因總和與正例數）。在同一趟裡順便記，不多花 action；promote 就能在「版本記下的 time 值的任何一部分」上精確重算尺，考卷不必完全相同。沒選：二元預測類拆不開，promote 會有兩套規則；把考卷往前推到新月份時，舊版本沒有新月份的預測，還是得補預測、重算。最常遇到的麻煩（〈後果〉第二條）用固定計分月份就能避開。以後要做「每個 time 值各算一個分數」時，這是第一步。

**二元預測類在 driver 上用 HPO 同一個函式算。** 只有一份程式，val 與 test 天生同一個定義，不用對帳測試。沒選：test 的資料已經在 Hive，搬回 driver 違反決定 4 的原則，driver 記憶體也得跟著 test 的列數長。

**test 上把 4 個 HPO 目標全部算。** 替沒選的目標付錢，而二元預測類的兩趟是最貴的。

**training 算完整一包、附上設定指紋，evaluation 比對得上就直接用。** 見決定 5。

**另寫一支腳本，指定 model_version 就在現在的計分月份上重算。** 只在那個版本已經有這些月份的預測時有用；最常見的情況是新月份還沒預測，它幫不上。

## 後果

- **上線當下，所有既有版本都不參加 promote 排名**：它們的 `evaluation_results.json` 沒記 time 值。這時 `Recommended` 與自動挑選沒有任何候選，promote 只列出全部版本與被排除的原因。

  要讓一個舊版本重新參加，得替它在現在的計分月份上補預測、重跑計分。版本是從設定算出來的（training 沒有 `--model-version`），所以要暫時把設定還原成那個版本的。這是多步驟、容易出錯的手動操作，已知的坑至少有這些：
  - 還原 `training:` 會連帶換掉選版指標（沒明寫時它跟著 HPO 目標）：算出來沒有現在這把尺的值，而且之後若忘了改回來，promote 會用舊的尺排名，畫面上看不出來。
  - 要帶那個版本 manifest 裡的 `--base-dataset-version`、`--train-variant`，算出來才會是同一個 model_version。
  - 那個 dataset 版本沒有計分月份的資料時，要用舊設定重跑 dataset，但 `test_snap_dates` 要保留現在的，否則不會補建這些月份。重跑會把 `data/dataset/latest` 指到舊版本，事後不重跑一次現在設定的 dataset，之後不帶旗標的 training 會靜默用舊資料。
  - 尺是二元預測類、而那個 dataset 版本的 test 比例是 0 時，補不回來（決定 4 最後一段）。先看它的 manifest 再動手。

  操作手冊在實作 promote 那一步時寫進 `docs/operations/user-guides/`，並且實跑一次驗證；本 ADR 不列步驟。
- **計分月份沒設時跟著 `test_snap_dates` 走。** 照 `adding-an-eval-month.md` 把一個月加進 `test_snap_dates` 的當下，考卷就變了，**所有**版本記下的 time 值都對不上，全部退出排名。只想多評估一個月、不想動 promote 的比較範圍，就先把 `test_metrics.snap_date` 明寫下來；那份文件要加上這一步。
- **既有部署若 HPO 目標是二元預測類、而 `dataset.test_zero_positive_group_ratio` 用預設的 0，升級後每個指令都會被擋**：選版指標跟著變成二元預測類，test 卻算不出來。解法是把 `test_metrics.selection_metric` 明寫成排序類指標，或調高 test 的比例（會換 dataset 版本，要重建資料、重新訓練）。
- **尺跟著現在的 HPO 目標走，所以一次 HPO 實驗就可能換掉尺。** 例如版本 A、B 用 `macro_per_item_map` 訓練，實驗 C 把 HPO 目標改成二元預測類：尺跟著換，A、B 沒有這把尺的值，被排除，`Recommended` 與自動挑選只剩 C 可選，不管 C 在排序上是不是比較差。被排除的版本會列在畫面上；要避免，就在實驗 HPO 目標時明寫 `test_metrics.selection_metric`。
- **預設設定下，promote 的排名依據會改變。** conf 的 `training.hpo_objective` 預設 `macro_per_item_map`，選版指標跟著它，所以 promote 從按 `overall_map` 排改成按 `macro_per_item_map` 排。廣告示例的 HPO 目標是 `pooled_average_precision`，它的選版指標也跟著變成這一個（它的 test 有留無正例的 query group，算得出來）。`examples/ad/README.md` 記錄了這個指標在那份資料上挑 trial 近乎擲硬幣（HPO 目標選它是為了讓端到端實跑走到這條程式路徑）；要讓廣告示例的 promote 用比較穩的尺，就明寫 `test_metrics.selection_metric`。
- `promote_model.py` 開始讀設定，要跟 pipeline 一樣指定環境。
- `evaluation_results.json` 原本 4 個鍵保留；計分月份等於表裡的月份時，值與改之前逐值相同。MLflow 原有的指標名不變。`model_capacity_diagnosis.py` 讀到的 `overall_map` 也改成只含計分月份。
- 計分的 node 從 `compute_test_mAP_spark` 改名為 `compute_test_metrics`，`--from-node`／`--only-node` 的寫法跟著改。
- 每個指標在 val、test 各有一份實作；新增指標時兩份都要寫，對帳測試是唯一把它們綁在一起的東西。
- **沒有解決的事**：
  - 「每個 time 值各算一個分數」（promote 該看最新一個月、各月平均，還是各月都要贏）仍然開著。
  - 考卷只認 time 值，不認母體。建在不同 `base_dataset_version` 上的兩個版本（item 清單、`sample_pool`、label 的定義、test 的比例不同），同樣的月份也可能是不同的考卷，promote 看不出來。
  - 表裡已經不在 item 清單上的 item，它的預測分區仍會被算進去，維持只警告（`warn_about_surplus_partitions`）。
  - evaluation 不動。
