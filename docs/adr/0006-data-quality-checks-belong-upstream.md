---
status: accepted
date: 2026-08-02
---

# 資料品質檢查歸上游 `source_etl`，dataset 閘門維持零掃描

> **實作狀態（2026-08-19 核對）**：兩項決定都已落地。`conf/base/parameters_feature_etl.yaml`
> 的 `feature_table` 已有 `quality_checks: {max_duplicate_key_ratio: 0.0}`，而且**只掛在終點表**
> （`feature_concat` 與其他四張沒有），與下方的論證一致；D18／D19 在
> `tests/test_pipelines/test_dataset/test_nodes.py`。下文寫的是決策當時的狀態。

兩個候選檢查，兩個不同的歸屬。未來讀者會問「為什麼 `feature_table` 的 PK 檢查在 ETL 設定裡，
而 B1/B5/B6 在 dataset 閘門裡」——這條 ADR 回答它。

## `feature_table` 的 `(time, entity)` 唯一性 → 補在上游

先講事實：**當時沒有任何地方驗證它。**

| ETL 設定檔 | `primary_key` | `quality_checks: max_duplicate_key_ratio` |
|---|---|---|
| `parameters_sample_pool_etl.yaml` | ✅ | ✅ `0.0` |
| `parameters_label_etl.yaml`（含 `label_table`） | ✅ | ✅ `0.0`（4 張表各一） |
| `parameters_feature_etl.yaml`（**全 6 張含 `feature_table`**） | ✅ | ❌ **沒有 `quality_checks` 區塊** |

關鍵在 `pipelines/source_etl/checks.py:343` 的條件：

```python
if "max_duplicate_key_ratio" in qc and table_config.primary_key:
```

**光宣告 `primary_key` 不會跑任何檢查**，兩者要同時具備。所以 `select_train_keys`
（`pipelines/dataset/nodes.py`）裡那句「PK 由 `source_etl` 的 `max_duplicate_key_ratio` 保證」
對 `sample_pool` 為真、**對 `feature_table` 為假**。而 `feature_table` 若有重複
`(snap_date, cust_id)`，`build_model_input` 的 feature join 會靜默把 model_input 列數乘開。

→ **在 `parameters_feature_etl.yaml` 的 `feature_table` 補
`quality_checks: {max_duplicate_key_ratio: 0.0}`**，不在 Layer-2 加閘。

理由：修的是真的洞、用既有機制、在資料還熱的 ETL 時點付一次成本（**每月一次**，而不是
每次 dataset run 一次），而且讓上面那句註解變成真的。

### 為什麼只掛終點表，不掛餵給它的那五張

> 修訂於 2026-08-02（原文為「6 張表都補」）。證據：`conf/sql/etl/feature/feature_table.sql`
> 是 `SELECT * FROM feature_concat`，而 `feature_concat` 由四張來源表 join 而成。

上游任何一張表的重複鍵都會**經由 join fan-out 傳播到 `feature_table`**，所以單一終點檢查
抓得到全部——而 `feature_table` 正是 dataset pipeline 實際讀的那張，也就是這條不變量真正
要守的地方。

代價是**歸因精度**：檢查爆掉時只知道「終點有重複」，不知道是哪一張上游造成的，得自己往
回查。換到的是六次聚合變一次。

這與 `parameters_label_etl.yaml` 的做法不同（那邊四張全掛）。**這個不對稱是本次選擇的結果，
不是有意設計的對照**——label 那份設定早於本次決策，沒有一併重新評估。若日後要統一，方向
應該是把 label 也收斂成只檢查終點的 `label_table`（它是三張來源表的 `UNION ALL`，重複鍵
同樣 1:1 傳到終點），而不是把 feature 補回全掛。

**若日後 `feature_table.sql` 從直通改成含 fan-out 的轉換，這個推論失效**——屆時終點檢查
不再等價於上游檢查，要重新評估是否補回中間層。

## query group 完整性 → 不設閘，只加測試

「每個 `(time, entity)` 的 distinct item 數 == 宣告的 item 數」在**這個 repo 裡是結構保證**的，
三件事各擋一段：

- `conf/sql/etl/sample_pool/sample_pool.sql` 的 `cross_pop` 是字面 cross join
  （`cust_snap LEFT JOIN prod ON 1=1`）→ 母體本身就是完整展開的。
- `select_test_keys`（`pipelines/dataset/nodes.py`）取全母體、不抽樣 → 不會抽掉組內的 item。
- `filter_groups_with_positives`（同檔）只丟整組、不丟組內 item → 過濾不會挖洞。

所以「mAP 的分母不可信」這個風險在現行部署下不存在，加閘是為一個結構上不會發生的狀況付
全掃成本。

**當時真的破著的是另一件事**：`filter_groups_with_positives` 的群鍵定義**零覆蓋**——把群鍵
改成含 item（過濾退化成「只留正例列」，會靜默刪光所有負樣本）仍然 16 passed。這是 code
regression 風險，用測試守得住，零 production 成本。

→ 只加測試（D18/D19），重點是鎖住群鍵定義；閘門等真的遇到非 cross-join 的 `sample_pool`
部署再說。

## 為什麼不順手都加成 Layer-2 閘門

`validate_data_consistency` 當時三條全部是**零掃描**：B5/B6 讀 `feature_table.dtypes`
（metastore metadata），B1 只 collect distinct item 值。加一條 `groupBy` 全掃會改變這個
節點的成本量級，而公司規模下的成本未知。

[ADR-0004](0004-carry-drop-columns-intersection.md) 的 B7 刻意也選在零掃描這一類——
**這個閘門的定位是「設定與資料的矛盾」，不是資料品質稽核**。資料品質有它自己的家
（`source_etl` 的 `quality_checks`）。

## 修訂（2026-09-03，issue #281）：新增了一個需要「資料的事實」的閘門，零掃描仍然成立

上面那段的界線是「**加一條 `groupBy` 全掃會改變這個節點的成本量級**」。#281 加的
不變量 B8（被 cast narrow 的欄撐不撐得過宣告的 `numeric_feature_storage_type`）
需要每欄的 `max(|x|)`——這是**資料的事實**，不是設定的事實，也不在 metastore 裡。
所以它是這條 ADR 寫下後第一個逼問「零掃描到底是不是硬界線」的案子。

**是硬界線。B8 沒有做聚合，它改讀 parquet footer 的 per-column 統計值。**

| | B5／B6／B7 | ADR-0009 的分區回報 | B8 |
|---|---|---|---|
| 事實從哪來 | `feature_table.dtypes` | `SHOW PARTITIONS` | parquet footer 的 min／max |
| 成本 | metastore metadata | metastore | 每個檔一次 seek |
| 與列數的關係 | 無 | 無 | 無 |

三者同一類：**成本由 metadata 的規模決定，不由資料的列數決定**。所以 B8 沒有改變
dataset 閘門的成本量級，這條 ADR 的決定原封不動。

三個實作上的選擇跟著這個結論走，各自都有替代方案被排除掉：

1. **讀的是 `preprocessed_feature_table`（本 repo 自己寫的表），不是 `feature_table`。**
   後者是使用者自備的 `read_only` 表，本框架不規定它的儲存格式；為了一個閘門去要求
   來源表必須是 parquet 且帶統計值，是把框架的適用面縮小。自己寫的表則由
   `HiveTableDataset` 以 `STORED AS PARQUET` 建立，格式是我們自己保證的。
   代價：閘門必須站在那張表**落地之後**——node 自己不寫表，是 Runner 在 node return
   之後 `catalog.save`——所以它是一個獨立的 node，排在 `apply_preprocessor_to_features`
   與 `build_model_input` 之間。它產出 `numeric_precision_report`（每個受檢欄的
   headroom），所以不是零輸出 side-effect node，也就不在 A7／R3 的登記裡。這仍然擋在失真之前：narrowing 發生在下游的
   `build_model_input`，`preprocessed_feature_table` 保留來源 dtype。
2. **footer 透過 Spark 的 JVM Hadoop `FileSystem` 讀，不是 pyarrow**
   （`utils/parquet_stats.py`，與 `utils/hdfs.copy_hdfs_to_local` 同一座橋）。這不是
   偏好：本 repo 從未在 driver 上直接讀過 Hive 表的檔案——training 是先
   `copy_hdfs_to_local` 抄成本機複本再交給 pyarrow（`io/handles.ParquetHandle` 的
   docstring 明寫 *driver-local*）。pyarrow **有** HDFS client
   （`pyarrow.fs.HadoopFileSystem`），排除它不是因為它不存在：它要在 **Python 行程裡**
   自己載入 `libjvm`／`libhdfs`（Spark 之外的第二個 JVM，靠 `JAVA_HOME` 與 Hadoop
   `CLASSPATH` 找），沒設好時連建構都會 raise（本機實測：`OSError: Unable to load
   libjvm`）。Spark 的 JVM 本來就在跑、本來就拿著叢集的 Hadoop 設定，而且不新增依賴
   ——生產環境本來就禁止新增套件。
3. **footer 沒有統計值時不退回掃描，而是 raise**（`numeric_precision_policy: truncate`
   可放行）。退回掃描會讓成本從零掃描**靜默**升級成一次實掃，正是這條 ADR 要避免的
   那件事；而且那條 fallback 幾乎不會被執行到，等於留一段沒人驗證過的程式碼。
   全 null 的欄別到安全邊（`num_nulls == row_count` 說明它沒有值可以失真），不 raise。

> **同日補記（issue #281 的兩軸審查抓到）**：本票原本的判準是「**整數欄** `max(|x|) > 2^24`」，
> 而 `decimal` 被當成「浮點、不受此判準約束」排除。那是事實錯誤——Spark 的 `DecimalType` 是
> **精確定點**，值落在 `10^-scale` 的格點上。而 cast 今天**只轉 Decimal 與 Double**，所以照原判準
> 做出來的閘門檢查集合恆為空：唯一該擋的型別正好被假前提排除。修正後的判準改讀「該欄 dtype 宣告的
> 格點間距」（整數／boolean = 1、`decimal(p,s)` = `10^-s`、float／double 無格點故不設界），
> 界＝`2^(floor(log2(step)) + 尾數位元)`。推導與實測數字在 `core/consistency.py` 的模組 docstring。
> **這一段記在 ADR-0006 是因為它改變了閘門的作用範圍**，不是因為它改變了零掃描的結論——取值方式未變。

## 修訂（2026-09-07，issue #306）：footer 先例從 per-column 統計擴到列數，零掃描結論不變

B8 逼問過一次「零掃描是不是硬界線」，答案是「是，改讀 footer 的 per-column min／max」。
#306 的 B10（`*_model_input` 的列數必須等於它的 `*_keys`）問的是同一個問題的下一格：
**列數也是資料的事實，而且它正是「掃描」這個詞最直觀的對象。**

**仍然是硬界線。B10 沒有做聚合，它把 footer 的 `block.getRowCount()` 加起來。**

| | B8 | B10 |
|---|---|---|
| 事實從哪來 | footer 的 per-column min／max | footer 的 `getRowCount()` |
| 成本 | 每個檔一次 seek | 每個檔一次 seek |
| 與列數的關係 | 無 | 無 |

`getRowCount()` **不是新增的讀取**：`utils/parquet_stats.read_max_abs_stats` 早就在讀它
（用來判斷「全 null」那一格），B10 只是把它加總而不是丟掉。所以這次擴充連新的 JVM
呼叫面都沒有增加，仍走 Spark JVM 的 Hadoop `FileSystem`（生產禁止新增套件）。
守住這條的是測試而不是慣例：`TestB10CostInvariant` 用 AST 讀該路徑上的四個函式，
斷言它們不呼叫 `count`／`groupBy`／`collect` 等任何讀列的方法（**用 AST 不用 grep**——
grep 會被那句解釋「為什麼不用 `df.count()`」的 docstring 自己命中）。

原票把代價寫成「兩個 `count`」並因此擔心撞到成本不變量。那個前提是錯的，換成 footer
之後這條 ADR 的決定原封不動。

三個實作上的選擇跟著這個結論走：

1. **閘門是 build 節點之後的獨立 node**，理由與 B8 的第 1 點同構：node 自己不寫表，
   是 Runner 在 node return 之後 `catalog.save`，所以要有 footer 可讀就必須等表落地。
   放進 `build_model_input` 內部做後置條件，就只能對還沒落地的 frame 呼叫 `count()`
   ——正是本 ADR 要拒絕的成本升級。落地後讀 footer 一樣擋在訓練之前。
   它產出 `model_input_grain_report`，所以不是零輸出 side-effect node，不在 A7／R3
   的登記裡，也不會被切片靜默跳過（架構 F5）。
2. **只讀本 repo 自己寫的表**（`external: false` 的 `HiveTableDataset`，格式由
   `STORED AS PARQUET` 保證）。使用者自備的 `feature_table` 不在檢查範圍——與 B8
   選擇讀 `preprocessed_feature_table` 是同一個理由，也與本 ADR「不規定來源表儲存
   格式」的立場一致。
3. **五個 split 只擋得住三個，這是限制不是疏漏。** train／train_dev／calibration 直接
   落地自 `build_model_input`。val／test 不行：列數會等於 keys 的是
   `val_model_input_unfiltered` / `test_model_input_unfiltered`，而這兩個名字**在任何
   環境的 catalog 裡都沒有條目**（`grep -rn "unfiltered" conf/` 零命中），所以是
   `core/catalog.py` 自動生成的 `MemoryDataset`——惰性 frame，從不落地，沒有 footer。
   真正落地的是 `filter_groups_with_positives` 的輸出，它的列數**本來就該比較小**。
   考慮過用單向界線 `列數(filtered) <= 列數(keys)` 補位，**否決**：本 repo 的
   `sample_pool` 是 entity × item 的稠密展開、`label_table` 稀疏，所以零正例的 group
   佔多數、filter 會砍掉一大片，這條界線能一路撐過 2 倍放大而不報警——一個恆真的閘門
   比沒有閘門更糟。**殘留風險明寫在這裡以免被當成 bug 重新發現**：只出現在 val/test
   月份分區的重複鍵不會被這道閘門看見。

> 這一段記在 ADR-0006 是因為它擴大了 footer 這條路的適用範圍（從 per-column 統計到
> 列數），**不是因為它改變了零掃描的結論**——取值方式沒變。

## 這條 ADR 沒有解決的事

- 框架允許使用者自備 `feature_table`（不經本 repo 的 `source_etl`）。那種部署下上游 PK
  檢查不存在，`feature_table` 的唯一性再度無人保證。**#306 的 B10 補了下游的位、沒補
  上游的洞**：它不檢查 `feature_table` 本身，而是比對三個 split 的 model_input 與 keys
  列數，所以重複鍵造成的放大會被擋下來，但歸因（是哪張表、哪一列）仍要自己往回查，
  且只涵蓋 train／train_dev／calibration 的月份（見上方 2026-09-07 修訂）。
- 非 cross-join 的 `sample_pool` 部署會讓 group 完整性失去結構保證。同上，出現再說。
- 補上 `max_duplicate_key_ratio` 之後，`feature_table` 的重複鍵會在 ETL 階段 raise。
  這個檢查在寫下本 ADR 時從未在生產跑過，**首次啟用可能揭露既有的資料問題**，不該在沒有人
  看著的排程裡首跑。設定已經落地（見頂部狀態），所以這個提醒是現行的——首跑的觀察責任還在。
