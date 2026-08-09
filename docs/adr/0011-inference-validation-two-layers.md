---
status: accepted
date: 2026-08-09
---

# inference 的驗證分兩層、移除 score_range、特徵順序改由模型決定

`validate_predictions` 有六個 sanity check，看起來覆蓋得很完整。本 ADR 記錄三件事：這六個
檢查有一個共同的盲區、其中一條在每一種合法設定下都沒有資訊量、以及「誰決定餵給模型的欄位」
這個問題本來的答案是錯的。

## 一、六個檢查只驗形狀，不驗值

逐條看它們在驗什麼：列數對不對（`nodes_spark.py:315-317`）、分數在不在範圍內（`:324`）、
有沒有 null（`:343`）、每組是不是 22 列（`:362-364`）、名次是不是 1..22 且與分數同序
（`:379`、`:396-399`）、identity 有沒有重複（`:408`）。

**六條全是形狀，沒有一條問「值對不對」。** 兩個具體後果：

### 實例一（已實跑重現）：`prod_name` 寫成整數 code

本機跑一次 `inference --env local`（程式碼未改動）之後，Hive 分區目錄是：

```
ranked_predictions/snap_date=2025-12-31/prod_name=0 … 7        ← inference 三張表
training_eval_predictions/…/prod_name=ccard_bill … fund_stock  ← training 的表，正常

All 6 sanity checks passed (8920 rows)
Pipeline completed in 11.09s
```

成因與修法見 [ADR-0010](0010-inference-chunked-scoring-shape.md) 第六節。這裡的重點是：
**輸出表按名字完全對不上產品，而六個檢查一條都沒紅**——列數對、分數在範圍內、每組 22 列、
名次正確、identity 不重複，形狀全部正確。

### 實例二（設計推論）：`prod_name` 值寫成常數

`(cust, prod)` 與 `(cust, prod')` 的特徵向量**只差 `prod_name` 那一欄**（ADR-0010 §4）。所以
若那一欄的值退化成常數，同一個 entity 的 22 個 product 會拿到**完全相同的分數**，名次純由
`row_number` 的任意順序決定。這種情況下六個檢查同樣全綠——平手時 lag 檢查的條件
`F.col(score_col) > F.col("_prev_score")`（`:398`）為 False，抓不到。

**這個實例在改動前不容易發生，但新設計會把它變成可能。** ADR-0010 §4 把 `prod_name` 的組裝
從 Spark 移到 driver 的一行；那一行寫錯的後果是欄數正確、值錯誤，而 LightGBM 的欄數檢查
（feature count mismatch 會 raise）幫不上忙。

> **一則更正，避免後人誤引**：本節初稿把實例二寫成 issue #63 的實際後果。那是錯的。
> `nodes_spark.py:196-204` 早已優先採用 `model.feature_names()`（commit `a220cb6`，行為由
> `tests/test_pipelines/test_inference/test_nodes_spark.py:223-250` 釘住），#63 描述的 fallback
> 不是預設路徑；即使走上 fallback，欄數不符會被 LightGBM raise 而不是靜默同分。實例二是
> **新設計新開的洞**，不是舊 bug 的重述。

### repo 已經認得這個故障模式，但只守了一半

`require_item_is_a_feature` 的 docstring（`pipelines/dataset/steps/feature_columns.py:131-136`）
逐字寫著：漏掉 item 會「silently makes X miss the item dimension, **collapses predictions to a
constant within each query group**, and produces a flat mAP across every HPO trial」。那條
backstop 守在 **config 層**——它擋得住「設定漏了 item」，擋不住「設定對，但 pipeline 沒把正確的
值餵進去」。**資料層那一半今天沒有人守。**

## 二、`score_range` 在每一條合法路徑上都沒有資訊量

`:324` 斷言分數落在 `[0, 1]`。檢查每一條可達的路徑：

| 路徑 | 分數的界 | 由什麼保證 |
|---|---|---|
| 套用 isotonic 校準 | [0, 1] | `IsotonicRegression(out_of_bounds="clip")` 對 `y_cal ∈ {0,1}` 擬合（`models/calibrated_adapter.py:56-59`） |
| 套用 sigmoid 校準 | (0, 1) | `LogisticRegression().predict_proba(...)[:, 1]`（`:60-64`、`:95`） |
| 未校準、`objective: binary` | (0, 1) | LightGBM 的 `binary` predict 回傳 sigmoid 機率 |
| 未校準、`objective: lambdarank`／`rank_xendcg` | **無界實數** | 原始 booster 輸出 |

前三條路上 `[0, 1]` **由建構方式保證，檢查結構上不可能紅**。第四條路上它是**誤報**，而那條路
是合法的：consistency 的 A7（`core/consistency.py:37-40`、`ranking_objective_conflicts` 於
`:368-400`）只要求 ranking objective 配 ranking metric 與非空 `entity`，**沒有**要求開啟校準；
`inference.use_calibration: false` 也是明文支援的設定（`conf/base/parameters_inference.yaml:8`）。

所以這條檢查**要嘛是裝飾品、要嘛是錯的，沒有第三種情形**。ADR-0008 拒絕用
「`base_dataset_version` 逐字相同」當驗收，用的是同一把尺：那是一個結構上不可能失敗的斷言，
唯一的資訊量是「pipeline 跑完了」。

## 三、決定一：驗證分兩層

關鍵不對稱是——**在 chunk 裡驗，資料已經在 driver 的 pandas frame 上，成本近乎零；在整批驗，
每個檢查都是一次對 220M 列的 Spark 全掃。** 現行成功路徑是 **8 次獨立 Spark action**
（`nodes_spark.py:315, 316, 326, 348, 364, 382, 399, 408`；`:331`／`:354`／`:369` 三個
`.collect()` 都在失敗分支內），其中 3 次帶 shuffle，而 `ranked_staging` 是 Hive 表、每次都重讀；
`:316` 的 `scoring_dataset.count()` 更貴，它重跑 `inference_population ⋈ feature_table` 整條 join。

| 檢查 | 新家 | 理由 |
|---|---|---|
| `no_missing` | **chunk 層**（pandas 斷言） | 分數就是剛算出來的 numpy 陣列 |
| `no_duplicates` | **chunk 層 ＋ 結構保證** | 跨 chunk 不可能重複：bucket 對 entity 是互斥切分；chunk 內不可能重複：`inference_population_features` 的顆粒度就是 `(snap_date, cust_id)` |
| 列數（chunk 版） | **chunk 層** | 「寫出列數 ＝ 讀進列數」 |
| **`item_values_are_known`（新）** | **chunk 層** | 見第四節 |
| `completeness` | **整批層** | 需要同一 entity 的全部 product |
| `rank_consistency` | **整批層** | 同上，且 rank 是下游節點才算的 |
| 列數（整批版） | **整批層，改比分區清單** | 見下 |

整批層的 Spark action 從 8 降到 2–3；下沉的那幾個變成每 chunk 一次 pandas 斷言，成本可忽略。

> **一則更正（#190 實作時）：起點是 7，不是 8。** 上面那個 8 含 `:316` 的
> `scoring_dataset.count()`，而那一條連同它所屬的 `row_count_match` 已經被 #188 換成
> `partition_completeness`（零 action）。所以本票動手前的實測起點是 **7**，落地後是 **2**——
> 一次分組聚合（`completeness` ＋ `score_varies_within_group` ＋ rank 範圍）加一次 window pass
> （score 對 rank 的順序）。這個數字由 `TestBatchLayerActionBudget` 釘住，因為驗證的**結果**
> 看不出它：多加一條整批檢查會通過它自己的測試、不改變任何輸出，只是安靜地多掃一次整張表。

**但主要理由不是成本，是失敗得更早。** 今天分數算錯要等全部 chunk 算完、rank 也跑完才會在
`validate_predictions` 被抓到。下沉之後第一個 chunk 就爆。在一個單月要跑數小時的 pipeline 上，
這是「十分鐘知道」與「四小時後知道」的差別。

### 整批版的列數檢查改成比對分區清單

原本的 `n_ranked != n_scoring`（`:317`）在新結構下必壞：ADR-0010 把第 1 格的顆粒度從 220M 改成
10M，這個比較會恆為真、每次都報 fail。

**也不能改成比對 predict 的 manifest 列數。** manifest 的 `n_rows_written`（照 training 的形狀，
`pipelines/training/nodes.py:1231`）只累加**這次真的寫出去的**列，被 resume 跳過的 chunk 貢獻 0。
220 個 chunk 跑到第 150 個掛掉、重跑只寫 70 個 → manifest ≈70M vs `ranked_staging` 220M →
**每次 resume 必 fail**。

**改成：`unranked_predictions.existing_partition_values()` 在本次 `model_version` ＋ 本月底下的
分區數，必須等於 `len(inference.products) × N_BUCKETS`。** 純 metastore、零掃描
（`io/hive_table_dataset.py:277` 的成本契約），resume 之後仍然正確，而且它同時驗了「該有的分區
都在」與「沒有多餘的」。逐組列數由 `completeness` 從另一個角度守。

> **一則更正（#188 實作時）：乘積形式在小母體上會誤報，實作改成集合比對。**
> `len(products) × N_BUCKETS` 假設每個桶都有 entity。母體小於桶數時（本機合成資料、以及任何
> 小規模試跑）雜湊必然留下空桶，而 `insertInto` 對空 frame 不建立分區——於是這條檢查在每一次
> **正確**的小母體執行上都會紅。
>
> 落地的形式是**集合比對**：評分節點報出「應該存在的分區集合」（這次寫的 ∪ 這次跳過的），
> validate 拿它跟 metastore 說「存在的」比。純 Python、零掃描、resume 後仍正確，而且兩個方向
> 都抓（缺分區＝連續 save 互相覆蓋；多分區＝`entity_buckets` 改過留下的舊桶）。
>
> **代價，以及怎麼補回來。** 集合的「應該存在」把空桶扣掉了，所以「一個**不該**是空的桶回傳
> 零列」原本會從乘積形式的紅變成無聲——而那正是「資料少 1/N_BUCKETS 且零錯誤訊息」的同一類
> 失效，`completeness` 也看不到它（缺席的 entity 不構成任何 query group）。補法不是回到乘積：
> 評分節點改成先問中間表**哪些桶真的有分區**（一次 metastore 級的 distinct，每月一次），
> 桶有分區卻讀回零列就 raise，桶沒有分區才算合法的空桶。這樣「空桶」是有證據的判定而不是假設。
> 殘留的是「中間表本身就是舊的」——那條由 resume 的既有殘留承擔（判準是分區存在、不是分區
> 新鮮，`--rebuild-dates` 是覆寫手段），不是本條新增的。

## 四、決定二：刪 `score_range`，補兩條會紅的

**刪除 `score_range`**，不做條件化。條件化只是把一個裝飾品加上一個開關，還多一條「manifest 要
帶校準旗標」的耦合。

**新增 `item_values_are_known`（chunk 層）**：寫出去的 identity `prod_name` 值必須落在
`inference.products` 裡。pandas 一行、零 Spark。**這是第一節實例一唯一抓得到的檢查**——那個 bug
的分數是正常變動的，所以下面那條也抓不到它。

**新增 `score_varies_within_group`（整批層）**：每個 `(snap_date, cust_id)` 組內
`max(score) > min(score)`。**併進 `completeness` 既有的 `groupBy(snap_date, cust_id)` 聚合，
不增加 shuffle**——它是在一次已經要付的 shuffle 上多算一個 `min` 與一個 `max`。這條守的是第一節
實例二，也就是新設計新開的洞。

兩條加上既有的 config 層 backstop，構成三層：

```
config 層  require_item_is_a_feature   ← 設定漏了 item
chunk 層   item_values_are_known       ← 值不是產品名（已實跑重現）
整批層     score_varies_within_group   ← 值退化成常數（新設計的風險）
```

**刻意不加 `score_finite`（非 NaN／±inf）**：它是 `score_range` 唯一真正在防的東西，但抓不到
上面任何一種故障。

> **三則更正（#190 實作時）。**
>
> **一、`score_varies_within_group` 要排除「組大小 1」，否則單 item 設定必然誤報。** 一組只有
> 一列時 `max == min` 恆真，`len(products) == 1` 的設定於是每一次**正確**執行都會紅——與第三節
> 那個乘積形式在小母體上誤報是同一種形狀（斷言了一個不必成立的前提）。實作的條件因此是
> `_size > 1 AND max <= min`；組大小本來就是 `completeness` 的問題，不是這條的。
>
> **二、`no_missing` 的 identity 那一半，在整批層本來就不可能紅。** 節點寫出去的 entity
> identity 經過 `astype(str)`，NULL 變成字串 `"None"`——所以它在整批層是裝飾品，跟
> `score_range` 同類，只是本 ADR 初稿沒發現。塊層的版本因此讀「進來的」frame 而不是「寫出去的」
> frame，這也是 `validate_scored_chunk` 為什麼收兩個 frame。
>
> **三、`item_values_are_known` 沒有任何設定能讓它紅。** 寫出去的 identity item 就是迴圈變數，
> 而迴圈變數來自 `inference.products`。它是**對那一行賦值的迴歸防護**（那一行這個 repo 已經寫
> 錯過一次，ADR-0010 §6），不是資料層檢查——所以它的證據只能是 mutation 檢查，不會有設定層的
> 測試。這跟第二節刪掉的 `score_range` 不同：`score_range` 是不管**程式碼**怎麼改都不可能紅。

**刻意不加 `feature_importance()["prod_name"] > 0` 的啟動斷言**：它驗的是**模型**會不會用 item
特徵。有它的話 `score_varies_within_group` 的誤報空間會被關掉（一個從不對 item 分裂的模型，
本來就會讓 22 個分數合法地相同）。沒加，所以那個誤報空間是**知情的殘留**：真的遇到時，它報的是
一個應該有人看的模型品質問題，不是假警報。

## 五、決定三：特徵順序與子集由**模型**決定

repo 裡有兩份「特徵欄清單」，而它們**不保證相同**：

| | 內容 | 出處 |
|---|---|---|
| `preprocessor.json` 的 `feature_columns` | **全集** | dataset pipeline fit 時寫下 |
| `model.feature_names()` | **子集**（訓練期特徵選擇後） | booster 內部，來自 `lgb.Dataset(feature_name=...)` |

`models/feature_selection.py:33-35` 的 docstring 逐字寫著：

> This is a *training-stage* subset: the dataset-built `preprocessor.json` keeps the full feature
> set (`base_dataset_version` unchanged). Selection lives in the `training:` block, so it bumps
> `model_version` only.

而 `pipelines/training/pipeline.py:47` 產出 `preprocessor_view`（已扣除
`training.feature_selection.exclude`），**所有碰模型的節點吃的都是 view**，包含 training 自己的
逐分區預測（`:166`）。`exclude` 非空是合法設定，並且有 consistency 的 A14
（`core/consistency.py:78-81`、`:297-306`）守著「item 永遠不得被排除」。

**決定：**

| 問題 | 權威 |
|---|---|
| 取哪些欄、什麼順序 | **`model.feature_names()`** |
| 怎麼編碼（`category_mappings`、`drop_columns`） | **`preprocessor.json`** |

斷言改成：**`model.feature_names()` 必須是 `preprocessor.json` 的 `feature_columns` 的保序
子序列**，否則 raise。不做任何自動對齊。

理由是模型是**唯一記錄「這次訓練實際用了哪個 view」的 artifact**。`preprocessor.json` 是全集，
它不知道有沒有做過特徵選擇；把它當權威、要求兩者逐字相等，會讓一個有 A14 守著的合法設定在
inference 端直接 raise，而後人把斷言放寬成「取交集」之後，就會照全集切 X 餵給只吃子集的
booster。

### 實作形式：view 從模型建，**不要**呼叫 `apply_feature_selection`

`_pdf_to_X` 是照 `preprocessor_metadata["feature_columns"]` 切的（`io/extract.py:304`、`:311`），
所以 chunk 內傳進去的必須是 view 而不是整份 `preprocessor.json`：

```python
view = {**preprocessor, "feature_columns": model.feature_names()}
```

**看起來更「對齊 training」的那條路是錯的**：training 用
`apply_feature_selection(preprocessor_metadata, parameters)` 建 view
（`pipelines/training/pipeline.py:47`），它讀的是**當前 config** 的
`training.feature_selection.exclude`。inference 照抄等於假設「跑推論時的 config 剛好等於訓練
當時的 config」——而 `model_version` 是 `training:` 區塊的函數，指向一個舊模型時那個假設就不
成立，後果是欄位對不上（若欄數不同，LightGBM 會 raise）或**靜默錯位**（若 exclude 換了不同的
欄但數量相同）。

兩條路在 diff 上長得幾乎一樣，這是本節唯一需要 code review 盯的地方。

`inference_population_features` 存的是**全集**（見 [ADR-0010](0010-inference-chunked-scoring-shape.md) §5），
子集只在 chunk 內由這一行切出來——這是那張表能跨 `model_version` 重用的前提。

> **本 ADR 初稿把這個結論寫反了**（`preprocessor.json` 是權威、逐字相等否則 raise）。那不只是
> 選錯一邊，而是**往回退**：`a220cb6`（2026-06-14）已經讓 `predict_scores` 優先採用
> `model.feature_names()`，issue #63 的待辦第 3 點本來就規劃「adapter 暴露 `feature_names()` ＋
> `predict_scores` 選子集」。記在這裡，因為「用正本不用抄本」這個直覺在這個 repo 是錯的，下一個
> 人很可能會再犯一次。

保序子序列的斷言同時保住了原本要的東西：它會抓到 stale 的 `preprocessor.json`（模型有的欄
正本沒有）與不匹配的模型（順序被打亂）。ADR-0008 記錄過，artifact 對 artifact 是 inference
**唯一**能做的自我檢查——「什麼算特徵」的規則隨 `compute_feature_columns` 搬進
`pipelines/dataset/` 之後，inference 讀不到 config 側的定義。這條斷言把那項殘留縮小，但沒有
消掉：它仍然不是 artifact 對 config。

## 考慮過但否決的選項

**條件化 `score_range`**（由執行期是否真的套了校準決定要不要驗）。見第二節：它在可用的地方
結構上不可能紅。

**加一條 A 系列不變量強制「ranking objective 必須開 calibration」**，讓 `[0, 1]` 恆真。這會為了
讓一個沒有資訊量的檢查恆真，去限制一個合法的建模選擇。本專案的目標是排序，分數的絕對尺度沒有
語意；用校準的需求去綁排序的自由度，方向是反的。

**六個檢查全部留在整批層，只做技術優化**（合併 `agg`、對 `ranked_staging` 下 `cache()`）。省得到
成本，省不到第三節那個「早失敗」——而那是主要理由。

**整批列數改比 predict manifest 的 `n_rows_written`。** 見第三節：resume 之後必然對不上。

**驗證下沉到 chunk 層並逐 chunk 發布。** `completeness` 與 `rank_consistency` 需要同一 entity 的
全部 product，predict 的 chunk 只有一個，chunk 層根本驗不了。見 ADR-0010「考慮過但否決的選項」。

## 後果

**`validate_predictions` 不再是唯一的驗證關卡**，驗證邏輯散在兩處。這與「production 只在整批
驗完後才被觸碰」不衝突——下沉的檢查是額外的早期攔截，不是取代閘門。但**文件上必須明說哪些
檢查在哪一層**，否則下一個人加檢查時不知道該加哪邊。第三節那張表就是那份說明。

**六個檢查變成七個**（刪 1、加 2），而整批層的 Spark action 從 8 降到 2–3。

**`no_duplicates` 從「220M 列的 shuffle 去重」降級成「結構保證 ＋ 一行 pandas 斷言」。** 這個
降級依賴 ADR-0010 的分區設計（bucket 對 entity 互斥）——那條 ADR 若被推翻，這條要跟著回頭。

**第五節與第一節實例一合併成同一張實作票**（ADR-0010「後果」的票 A），因為兩者動的是同一段
程式碼：誰決定取哪些欄、誰負責編碼。

## 這條 ADR 沒有解決的事

- **`score_varies_within_group` 的誤報空間沒關掉**，見第四節末。
- **`rank_consistency` 的 min／max 是全域聚合而非逐組**（`nodes_spark.py:379-390`）。一組
  `[1,2,3]` 與另一組 `[1..22]` 並存時全域 min=1、max=22 照樣通過。今天由 `completeness`（組大小）
  補位，兩條合起來大致足夠，但**這個弱點早於本次改動，本次也沒有修**。
- **inference 仍然無法自我檢查「載入的 `preprocessor` 是否與現行 config 相符」**：第五節的斷言
  是 artifact 對 artifact，不是 artifact 對 config。ADR-0008 列的那一項只被縮小，沒有被消掉。
- **`item_values_are_known` 只驗 identity 欄的值域，不驗它與特徵欄那個整數 code 的對應關係。**
  兩者同時錯成一致的（例如編碼查了 `inference.products` 而非 `category_mappings`）仍然全綠。
  那條由 ADR-0010 §4 的實作契約 ＋ code review 守，沒有機械檢查。

## 稽核

`item_values_are_known` 與 `score_varies_within_group` 都必須有 mutation 檢查，而且要確認
**紅在正確的那一條**——第一節整節就是「其他檢查在這些故障下全綠」的證據，所以紅在別條反而代表
測試沒打中：

| 變異 | 應該紅的檢查 |
|---|---|
| chunk 內把 identity `prod_name` 換成整數 code（重現實例一） | `item_values_are_known` |
| chunk 內把特徵欄 `prod_name` 寫成常數（重現實例二） | `score_varies_within_group` |

第五節的斷言也要有測試：給一個 `feature_names()` 回傳子集的 model double，斷言**不 raise**；
給一個回傳「順序被打亂的同一組欄」的 model double，斷言 raise。前者是這條決定存在的理由，
若只測後者，等於沒測到翻案的那一半。

第三節的分層必須有測試釘住「哪些檢查在哪一層」，否則兩層會隨時間互相漂移回單層。
