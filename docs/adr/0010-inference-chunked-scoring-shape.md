---
status: accepted
date: 2026-08-09
---

# inference 逐 chunk 評分：切在哪、落地什麼、分區怎麼排

> **實作狀態（2026-08-19 核對）**：本 ADR 的設計**已全部落地**。下方「後果」列的五張票已全數
> 關閉（#185、#186、#187、#188、#189、#190；票 D 實際拆成 #188 ＋ #189），只剩傘票
> [#183](https://github.com/curtis-lu/recsys-demo/issues/183) 開著。
>
> **讀法**：第一、二、六節寫的是 **2026-08-09 決策當時**的程式碼。當時的
> `pipelines/inference/nodes_spark.py` 已在 #188 拆成 `nodes.py` ＋ `steps/`，本檔保留該檔名與
> 行號只為指認**被改掉的舊碼**，不要拿去對照現在的檔案。指向其他模組的引用則已按現況核對過。

未來讀者打開 `pipelines/inference/` 會問三個問題：為什麼中間表多了一個看似無意義的 hash
bucket 分區欄、為什麼評分用的特徵表不含 product 維度、為什麼 predict 的迴圈是「外層 bucket、
內層 product」而不是反過來。這三個問題是同一個設計的三個面，本 ADR 一起回答。

本 ADR 與 [ADR-0011](0011-inference-validation-two-layers.md) 互相依賴：第四節「節點合併」的
前提是 0011 §3 把 `scoring_dataset` 從 `validate_predictions` 的 input 拿掉，兩者必須一起落地。

## 一、現況：有 chunk 迴圈，但那不是 by-chunk 寫入

`predict_scores`（當時的 `nodes_spark.py:220-256`）確實逐 `(snap_date, prod_name)` 迴圈，但每一
圈的結果進 `all_results.append(...)`，跑完才 `pd.concat` ＋ `spark.createDataFrame(result_pdf)`
一次寫出。**迴圈存在，逐 chunk 寫入不存在**——整批預測結果先在 driver 上完整物化。

同一個節點還有第二個問題：`scoring_dataset` 與 `X_score` 都不在 `conf/base/catalog.yaml`
（grep 零命中），`DataCatalog` 對未宣告的輸出名在 save 端自動建立 `MemoryDataset`
（`core/catalog.py` 的 `save`），裝的是尚未執行的 Spark plan，而整條路徑零 `cache()`／`persist()`。
所以迴圈裡每一次 `.toPandas()` 都是一個獨立的 Spark action，從 `inference_population` ⋈
`feature_table` 從頭重算一次。`feature_table` 沒有 `partition_cols`（`conf/base/catalog.yaml:11-15`），
連分區裁剪都沒有。

## 二、生產規模讓現況跑不起來

生產參數（使用者提供，`inference.snap_dates` 每次一個月）：母體約 **10M entity**、
**22 個 product**、約 **1500 個特徵**。`_cast_feature_floats_to_float32` 已把浮點特徵轉成
float32，所以一列約 6 KB。

現行 chunk ＝ 一個 `(snap_date, prod_name)` ＝ **10M 列 × 1500 欄 ≈ 60 GB 進 driver**。
（`conf/base/parameters_inference.yaml` 的 demo 只有 8 個 product，所以本機從來沒有撞到這個
量級；inference 當時也還沒在公司環境部署過。）

## 三、三個約束把設計夾成一個解

**約束 A — 預測必須在 driver。** 生產禁 UDF，所以 `model.predict` 拿不到 executor，特徵必須
`toPandas()` 回 driver。這是唯一必須進 driver 的資料，也是唯一需要切 chunk 的理由。

**約束 B — 排名的 group 橫跨全部 product。** `rank_predictions` 的
`Window.partitionBy(snap_date, cust_id).orderBy(desc(score))` 與 `validate_predictions` 的
`completeness` 檢查都要求同一個 entity 的全部 22 列同時在場。**所以 chunk 不能按 `prod_name`
切**——按 product 切排名會靜默算出錯的名次。可切的軸只有 **entity**。

**約束 C — 落地邊界必須等於分區邊界。** `HiveTableDataset.save()` 是
`df.write.mode("overwrite").insertInto(...)` ＋ `spark.sql.sources.partitionOverwriteMode=dynamic`
（`io/hive_table_dataset.py`）。dynamic 的語意是「只動這次 DataFrame 裡出現的分區，但對出現的
那些是**整個刪掉重建**」。所以若 chunk 是 entity bucket 而分區只有 `(snap_date, prod_name)`，
第 2 個 bucket 的 `save()` 會把第 1 個 bucket 剛寫進去的 22 個分區全部替換掉，跑完只剩最後一個
bucket，**資料少 90% 且零錯誤訊息**。

training 的逐分區預測節點早就記錄過這條——`predict_and_write_test_predictions` 的 docstring
寫「exactly one partition's rows per save, so dynamic-partition overwrite cleanly overwrites a
single partition and successive saves don't collide」（`pipelines/training/nodes.py:1103-1105`）。
那不是實作偏好，是這個框架的硬約束。

## 四、決定

以 **`1 FT`** 為計量單位 ＝ 讀或寫一個 snap_date 份的完整特徵矩陣（10M × 1500 × 4B ≈ 60 GB）。
Parquet 壓縮比在各方案間是公因數，會在比較中約掉。

| 方案 | 建中間表：讀 | 建中間表：寫 | predict 讀 | **合計** |
|---|---|---|---|---|
| 現況（無 bucket） | – | – | 22 | 22（且單 chunk 60 GB 進 driver） |
| 加 bucket 但不落地中間表 | – | – | **220** | **220** |
| 落地含 product 展開的評分表 | 1 | 22 | 22 | 45 |
| **本案：落地不展開 ＋ bucket 當外層迴圈** | 1 | 1 | 1 | **3** |

（第二列的 220 ＝ 10 桶 × 22 product ＝ 220 個 chunk，每個 chunk 各全掃 1 FT。）

四項決定：

1. **切輸入，不切輸出。** entity 依 crc32 分 `N_BUCKETS = 10` 桶（`utils/hashing.py` 的
   `spark_bucket`）。一桶 1M entity × 1500 欄 ≈ **6 GB 進 driver**。
2. **落地一張不含 product 展開的特徵表** `inference_population_features`，顆粒度
   `(snap_date, cust_id)`。`(cust, prod)` 的特徵向量 ＝ 客戶的 1499 個特徵 ＋ `prod_name` 一個
   類別值；展開就是把同一列客戶特徵抄 22 遍去配 22 個純量，**那 21 份副本零資訊**。
3. **迴圈順序是外層 bucket、內層 product。** 一桶的 6 GB 讀進 driver 一次，內層 22 圈就地
   覆寫 `prod_name` 那一欄重複使用。這一項單獨值 22 倍——它省的不只是磁碟讀，還有
   executor→driver 的 Arrow 序列化搬運，而後者不會因為 Spark 端 `cache()` 而變便宜。
4. **每個 `(bucket, product)` 算完就存**，一次 `save()` 恰好一個分區。

### 節點合併：`build_scoring_dataset` ＋ `apply_preprocessor`

兩者合併成 `build_inference_population_features`。合併後 `scoring_dataset` 只剩一個消費者
（前提見本文開頭的相依宣告）、且沒有任何觀察者（不落地、無測試讀它、log 不報它），一個只被
下一格讀一次的 DAG 節點加了拓樸複雜度卻沒加資訊。

### `prod_name` 在 chunk 內佔兩個位置，不是一個

這一項是實作契約，不是風格偏好，理由見第六節那個實跑重現的 bug：

| 位置 | 值的形態 | 用途 |
|---|---|---|
| **identity 欄** | **原始字串**（`exchange_usd`） | 寫進 `unranked_predictions` 的 `prod_name` 分區欄 |
| **特徵欄** | **整數 code**（`category_mappings["prod_name"].index(p)`） | 餵進模型 |

編碼值必須用 `category_mappings["prod_name"]` 的位置，**不是** `inference.products` 的位置——
兩者內容由 consistency 的 A4 保證相同，**順序不保證**。用錯那一個，22 個 product 的分數會整組
錯位，而所有既有 sanity check 都抓不到（分數仍在合理範圍、名次仍是 1..22、完整性仍成立）。

### 為什麼 `N_BUCKETS` 是 10

分桶用 `spark_bucket`，它的輸出已經是 `crc32(...) % HASH_BUCKETS`（`HASH_BUCKETS = 100_000`）；
再 `% 10` **等價於直接 `crc32(...) % 10`**，因為 `10 | 100000`，二次取模不引入額外失真。crc32
本身在 `2^32 mod 10 = 6` 上有 2.3e-9 的相對偏差，可忽略。

桶數的健康窗口是 **5–20**，由兩端各一個約束夾出來：

- **下界**來自 driver 側「單一 chunk 不該吃掉 driver 的一半以上」（128 GB driver 抓 1/4 →
  `B ≥ 4`）。
- **上界**來自儲存側「分區檔不該遠小於一個 HDFS block」（`unranked_predictions` 每分區 ＝
  `E/B` 列，`B = 50` 時已降到約 6 MB）。

`B = 10` 落在中間，兩端各留約 2 倍餘裕。

## 五、分區設計，以及 #179

| 表 | `partition_filter` | `partition_cols` | 顆粒度 |
|---|---|---|---|
| `inference_population_features`（新） | `base_dataset_version` | `snap_date, entity_bucket` | `(snap_date, cust_id)` |
| `unranked_predictions`（原 `score_table`） | `model_version` | `snap_date, prod_name, entity_bucket` | `(snap_date, cust_id, prod_name)` |
| `ranked_staging` | `model_version` | `snap_date, prod_name` | 同上 |
| `ranked_predictions`（對外） | `model_version` | `snap_date, prod_name` | 同上 |

**`entity_bucket` 只進內部表，不進對外表。** 它是為了滿足約束 C 而存在的機制欄，`rank_predictions`
讀完就丟——一個 bucket 裝的是**完整的客戶**（分桶只依 `cust_id`，而排名 group 是
`(snap_date, cust_id)`，分桶欄 ⊆ group 欄，所以同一 entity 的全部 product 必落同一桶）。

`inference_population_features` 的 `partition_filter` 是 `base_dataset_version` 而不是
`model_version`：它只依賴來源表與 `preprocessor.json`（住在 `data/dataset/${base_dataset_version}/`，
`conf/base/catalog.yaml:83-85`），與模型無關。附帶好處是同一個 base 版本下換模型重跑可以整張
重用。

**因此這張表存的是 `preprocessor.json` 的全集**（扣掉 `prod_name`），不是任何一個模型實際使用的
子集。訓練期特徵選擇（`training.feature_selection.exclude`）產生的子集只在 predict 的 chunk 內
才切出來，依據是 `model.feature_names()`——見
[ADR-0011](0011-inference-validation-two-layers.md) §5。**不要「順手優化」成只存模型要的欄**：
那會讓這張表與 `model_version` 綁死，上面那個重用性質會靜默消失，而且沒有任何檢查會紅。

### `model_version` 從 `partition_cols` 提為 `partition_filter`

**這不是 resume 在技術上的必要條件**——`model_version` 留在 `partition_cols` 時仍會出現在
`existing_partition_values()` 回傳的 spec 裡（`io/hive_table_dataset.py`），計畫器可以自己濾。
提升的理由是另外兩個：

1. **照抄 training 規劃器的形狀會漏掉那一步。** training 的
   `_written_prediction_partitions`（`pipelines/training/nodes.py:1060-1066`）不濾 `model_version`
   ——因為它的表本來就有 `partition_filter: model_version`（`training_eval_predictions` 的 catalog
   條目），scope 由 catalog 保證。inference 若維持現狀又照抄那個形狀，就會**把上個模型版本寫的
   分區算成本次已完成**，跳過一個從未被現行模型評分過的 chunk，而 `completeness` 因為讀到舊分數
   照樣通過。這是 collision，不是誤差。
2. **save 後的分區回報。** 沒有 `partition_filter` 時 `existing_partition_values()` 回傳整張表跨
   所有 run 的累積，before／after 差集分不出「這次覆寫的」與「早就在那的」——這正是
   [ADR-0009](0009-written-partitions-from-the-metastore.md) 把這三張表排除在 metastore 快照路徑
   之外的原因，也是 issue [#179](https://github.com/curtis-lu/recsys-demo/issues/179) 的內容。修完
   之後三張表與 `training_eval_predictions` 結構相同——那不是巧合，是同一個問題已經被解過一次。

### ⚠ 遷移：三張表必須 DROP 重建，不能就地改 catalog

`partition_filter` 的鍵在實體分區順序上**排在 `partition_cols` 之前**
（`io/hive_table_dataset.py` 的 `_insert_column_order` 與建表 DDL）。而
`CREATE ... TABLE IF NOT EXISTS` **不會修改已存在的表**，`insertInto` 又是位置對應。

所以在既有表上直接套用新 catalog 設定的後果是：`model_version` 的值被寫進 `snap_date` 分區、
`snap_date` 寫進 `prod_name`……三個分區欄都是 STRING，**型別檢查不會擋，零錯誤訊息**。

**實作票必須包含 DROP 步驟**，並在文件裡寫明這是一次性的破壞性遷移。（#187 已依此執行，步驟見
`docs/pipelines/inference.md` §6.5。）

### `_filter_current_inference_scope` 刪除，但 snap_date 的限縮要留下

`HiveTableDataset.load()` 在有 `partition_filter` 時發的是 `SELECT * FROM t WHERE
model_version = '...'` 並把該欄從資料欄 drop 掉，所以當時 `nodes_spark.py:35` 的 `model_version`
比對變成不可能執行的死碼。

**但 snap_date 那一半不能跟著消失。** 表跨月份累積，而 `inference.snap_dates` 每次只有一個月；
`rank_predictions` 若不限縮，第二個月起會讀回全部歷史月份、重算、把舊月份無聲重新發布。
修法是把它變成 `rank_predictions` body 裡一句具名的分區裁剪步驟（`restrict_to_snap_dates`），
而不是一個跨節點共用、同時處理兩種不同問題的 helper。

## 六、已重現的 bug：`prod_name` 被寫成整數 code

**本機實跑（`--env local`，程式碼未改動）**：

```
ranked_predictions/snap_date=2025-12-31/prod_name=0 … 7      ← inference 三張表
score_table       /snap_date=2025-12-31/prod_name=0 … 7
ranked_staging    /snap_date=2025-12-31/prod_name=0 … 7

training_eval_predictions   /…/prod_name=ccard_bill … fund_stock   ← 正常
recsys_prod_test_model_input/…/prod_name=ccard_bill … fund_stock   ← 正常

All 6 sanity checks passed (8920 rows)
Pipeline completed in 11.09s
```

成因：當時 `apply_preprocessor:161` 的 `_encode_categoricals(result, categorical_cols, ...)` 吃的是
**全部**類別欄，含 `prod_name`；那一行把 `prod_name` 就地換成整數 code，而 `:167` 的
`select(*identity_cols, *feature_columns)` 讓 identity 位置拿到的也是整數，一路流到分區欄。

dataset 側不會這樣，因為它走 `encodable_categoricals`（現住 `preprocessing.py`）**排除 identity
類別欄**——那些欄稍後從 `keys` 來，在此編碼等於編一個即將被換掉的欄。training 側走 `_pdf_to_X`
的 `deferred_cats`（`io/extract.py:314-316`），同一個語意的 pandas 版。**當時只有 inference 是
第三種做法。**

本 ADR 第四節那張「兩個位置」的表就是這個 bug 的結構性修法：identity 留字串、特徵用 code。
修復本身**不依賴本 ADR 的其他部分**，切在獨立的第一張票（見「後果」）。

## 七、閘門語意：買到的是順序，不是原子性

staging → validate → publish 這道閘的位置不動：production 的 `ranked_predictions` 只在整批
驗證通過後才被觸碰，by-chunk 全部發生在閘門上游。

**但要說清楚它保證什麼、不保證什麼。** `publish_predictions` 的寫入同樣是 `insertInto` ＋
dynamic overwrite，跨分區的 commit 不是全有全無。所以這道閘保證的是**順序**（未通過驗證的
資料不會進 production），**不是原子發布**——「一次 run 要嘛全發要嘛全不發」在今天就不成立，
本次只是第一次把這件事寫下來。逐 chunk 化把失敗視窗從「整條 run」縮到「最後那一次寫」，這是
真實的收益，但它不等於原子性。

## 考慮過但否決的選項

**逐 chunk 直接發布到 production（驗證下沉到 chunk 層）。** 排除的理由不是成本，是約束 B：
`completeness` 與 `rank_consistency` 需要同一 entity 的全部 product，而 predict 的 chunk 只有
一個 product，chunk 層根本驗不了它們。

**落地含 product 展開的評分表**（合計 45 FT）。它唯一的好處是不必新增表——把現有的
`scoring_dataset` 直接落地即可。否決是因為那 22 倍複製沒有任何資訊量，每月要多寫約 1.3 TB
（未壓縮），而換到的只是「DAG 語意一行不動」。

**不落地，改用 `persist(DISK_ONLY)` 頂著。** 22 倍展開仍然存在（只是換到 Spark 的 disk cache），
而且 persist 的生命週期變成節點內的隱性狀態，`--only-node` 切片跑時行為不同。

**`entity_bucket` 進 `ranked_predictions` 的分區欄。** 會把一個純粹的計算層機制寫進對外契約，
所有下游讀取路徑都要跟著改，且不可逆。內部表承擔它、對外表不承擔，是本節前面那條分界線。

**讀的桶數與寫的桶數分開**（讀 40 桶控 driver、每 4 桶存一次維持分區檔大小）。技術上可行且
保留為逃生口，但它讓「一個 chunk」變成兩個不同的東西，spec 與續跑判準都要跟著分裂。目前
`B = 10` 落在健康窗口中間，不需要這個自由度。

**node 自己開 SparkSession 寫表**（不用 `@` handle／`writes=`）。會繞過 `HiveTableDataset` 的
分區報告、schema evolution、`partition_filter` 注入。而 A1 的檢查只掃 `DataCatalog`／
`catalog.load`／`catalog.save`，抓不到裸 `insertInto`——**這是一條稽核看不見的路，正因為看不見
所以更不該走。**

## 後果

**這不是版本中性的改動。** [ADR-0008](0008-dataset-modules-split-by-role.md) 能用「`conf/` 的
diff 為空」當驗收，本次不能：表結構改了、節點集合從 6 個變 5 個、新增一張表，而且第六節那個
修復會改變 `prod_name` 分區欄的值。驗收改由**新舊逐列對照**承擔，差異必須逐條解釋。

**切成五張票，前三張互不依賴：**

| 票 | 內容 | 依賴 | 實作票 |
|---|---|---|---|
| **A** | 特徵向量組裝契約：第六節的 bug ＋ 特徵順序權威（見 [ADR-0011](0011-inference-validation-two-layers.md) §5）＋ issue #63 | 無 | #185 |
| **B** | issue #154：`@` handle → `Node(writes=[...])` | 無 | #186 |
| **C** | 三張表 `model_version` 提為 `partition_filter`、`score_table` → `unranked_predictions`、DROP 重建 | 無 | #187 |
| **D** | 核心：新表 ＋ 節點合併 ＋ bucket 迴圈 ＋ 逐 chunk save ＋ resume | A, B, C | #188 ＋ #189 |
| **E** | 驗證分層（ADR-0011 §3） | D | #190 |

票 A 排最前面且**可以獨立進 main**——它是一個輸出表對不上產品名的正確性 bug，等在四張票的長
路徑後面沒有好處。它與特徵順序權威合併成一張票，因為兩者動的是同一段程式碼（誰決定取哪些欄、
誰負責編碼），分兩次改會動同一批行兩遍。

**`--only-node apply_preprocessor` 的可定址性消失**（節點合併），部分由
`--from-node predict_and_write_scores` 補回——因為 `inference_population_features` 落地了，
從 predict 接續執行不必重跑 join，而這在改動前做不到（它只是 lazy plan）。

**改 `N_BUCKETS` 不再是零成本**：它進了 `unranked_predictions` 的分區欄，改桶數等於該
`model_version` 的該表要重建。因為它是每個模型版本獨立的中間產物，這個代價可以接受，但它是
真的代價。

**identity 類別欄的處理在三條路徑上第一次一致**（票 A 落地後）：dataset 走
`encodable_categoricals`、training 走 `_pdf_to_X` 的 `deferred_cats`、inference 改成同一個語意。

## 這條 ADR 沒有解決的事

- **前處理的編排是兩份，機制是一份。** `_encode_categoricals` 與 `_cast_feature_floats_to_float32`
  （皆在 `preprocessing.py`）兩條 pipeline 共用同一個函式，`preprocessor.json` 的四鍵契約也是同一
  份 artifact。但「編碼哪些欄、在哪一步 cast、輸出哪些欄、警告什麼」的編排各寫一份——第六節那個
  bug 就是這個結構的產物。票 A 把最要緊的一項分歧關掉（inference 現在也走 `encodable_categoricals`
  ＋ `_encode_categoricals`，`pipelines/inference/nodes.py`），**但仍然沒有任何機制防止未來再
  分歧**。便宜的解法不存在：要嘛抽出共用的編排函式，要嘛加一條稽核。
- ~~**inference 側缺 `warn_unknown_encodings`**（dataset 側有，`pipelines/dataset/nodes.py:587`）。
  未知類別靜默變成 `UNKNOWN_CATEGORY_CODE`，沒有 log。推論時尤其要緊——生產母體出現訓練時沒見過
  的類別值是正常的。**併入票 A。**~~ **已解決**（票 A／#185）：`pipelines/inference/nodes.py` 已
  呼叫 `warn_unknown_encodings`。
- ~~**`pipelines/inference/` 的模組佈局沒有照 ADR-0008 切**（`nodes.py` ＋ `steps/`、消 `_spark`
  後綴、inference 版的 S1／S2）。刻意留的缺口：票 D 會把這些函式重寫一遍，形狀該跟著新的 node
  body 長出來，而不是照 dataset 的檔案清單搬。~~ **已解決**（票 D／#188）：現為 `nodes.py` ＋
  `steps/`（`population` / `feature_view` / `chunk_plans` / `partitions` / `scoping` / `validation`）。
- **driver 峰值只有下界，沒有量測。** `_pdf_to_X`（`io/extract.py:328`）的 `X_df.values` 會把 frame
  攤成單一 numpy 陣列，dtype 由所有欄的共同型別決定。特徵全 float32 ＋ `prod_name` 的 int8 →
  float32，不膨脹；但只要有一欄 int32／int64 特徵（`_cast_feature_floats_to_float32` 只轉 Decimal
  與 Double，整數欄刻意保留），共同型別升成 float64，那一步就多吃一倍。**6 GB 是下界不是估計
  值**，實際值取決於生產 `feature_table` 的 dtype 分佈。
- ~~**`scripts/local_e2e.sh` 的註解已過期**：它寫「inference/evaluation gated on #63、卡在模型預測
  層」（`925fa27`，2026-06-09），但 `a220cb6`（2026-06-14）對齊模型特徵契約之後，inference 本機
  已能跑完（本 ADR 第六節那次實跑，11 秒）。**註解更新併入票 A。**~~ **已解決**（票 A／#185）：
  該註解現在自己說明了這段歷史。

## 稽核

三條，前兩條是可機械檢查的成本契約，第三條是本設計唯一「錯了會靜默刪資料」的地方：

1. **`predict` 階段對 `feature_table` 的完整掃描次數 ＝ 0**（改讀已落地的
   `inference_population_features`）。
2. **一個 snap_date 的 `inference_population_features` 分區讀取次數 ＝ `N_BUCKETS`**，不是
   `N_BUCKETS × len(inference.products)`。這條釘住第四節的第 3 項（迴圈順序），而那是唯一
   一個「寫反了功能完全正確、只是慢 22 倍」的決定。
3. **交給 `save()` 的每一個 DataFrame 必須只含一種分區欄組合。** 這是約束 C 的可測形式。

   **刻意不用「寫入前後 `existing_partition_values()` 差集」**：`io/hive_table_dataset.py` 的
   `save()` 註解已經記過，那個差集對「重新發布既有分區」by construction 為空——而約束 C 談的正是
   連續 save 互相覆蓋，覆蓋時前後差集都是 0，斷言看不到它要守的那件事。改成斷言傳入 df 的分區欄
   distinct 組合數 ＝ 1，用 double 測（比照既有的
   `tests/test_io/test_hive_table_dataset.py::TestSaveReportsPartitionsWithoutRecomputing`）。

### 一則更正（#188 實作時）：這條斷言不能放在 `save()` 的入口

上面第 3 條的初稿寫「在 `save()` 的入口斷言」，那是錯的，而且有兩個各自獨立的理由：

1. **這個不變量在 `save()` 這一層是假的。** 第四節新增的 `build_inference_population_features`
   本來就會交出一個橫跨全部 `entity_buckets` 個分區的 frame，那完全正確——它不是逐 chunk 寫的
   節點。放在 `save()` 入口的斷言會擋掉本設計自己的第一個節點。約束 C 約束的是**逐 chunk 寫入的
   呼叫方**，不是 `HiveTableDataset`。
2. **對 Spark frame 而言那個斷言要付一個 action**，正好違反 ADR-0009 的成本契約，並且會弄紅
   `TestSaveReportsPartitionsWithoutRecomputing`（它的 double 對所有 action 一律 raise）。

實作改成：在 `predict_and_write_scores` 交出 frame **之前**對那個 pandas frame 斷言（已在 driver
記憶體裡，免費），並在節點接縫用 dataset double 釘住「每次 save 恰好一種分區欄組合」與 save
次數。io 側另補一條 characterization 測試證明「重寫一個分區是替換而不是追加」——那是這條規則
之所以是正確性規則而非風格偏好的根據。

---

驗收量**次數**不量**秒數**：本機合成資料的規模與生產差好幾個數量級，wall clock 外推不了，
而上面三個計數是規模無關的。ADR-0009 的量測表是同一種思路——那張表是 `insertInto` 本身
（3 jobs／11 tasks）與事後那行 log（**額外** 4 jobs／17 tasks）兩個獨立操作各自的成本，撐住
論證的是「那行 log 自己就要 4 個 job」這個計數，不是任何秒數。
