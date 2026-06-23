# Round-3 架構／一致性終審 log

審員：Claude Code round-3
日期：2026-06-23
範疇：跨章引用整合性、ch11/ch12 完整讀、能力地圖、敘事弧、index 一致性

---

## 1. 跨章引用整合性（§X.Y 抽查）

### 1a. 章號引用核對

| 引用 | 在哪個檔案 | 指向主題 | 正確？ |
|---|---|---|---|
| 「第 07 章」（排程）| ch04, ch05, ch09, ch10, ch11, ch12 | 07-operating-pipelines.md | ✅ |
| 「第 08 章」（資料產品）| ch09, ch11, ch12 | 08-data-product-correctness.md | ✅ |
| 「第 09 章」（reverse ETL）| index, ch05, ch12 | 09-reverse-etl.md | ✅ |
| 「第 10 章」（PySpark）| ch04, ch05, ch06, ch11, ch12 | 10-pyspark-dataframe-api.md | ✅ |
| 「第 11 章」（場景索引）| ch04, index | 11-scenario-playbooks.md | ✅ |
| 「第 12 章」（速查）| index | 12-cheatsheet-and-glossary.md | ✅ |

結論：所有「第 NN 章」章號引用均正確，無舊番號殘留。

### 1b. §X.Y 節引用抽查

| 引用 | 來源 | 被指章節是否存在且主題符合 |
|---|---|---|
| §2.11「漸進退化」| ch11 §11.2、ch12 §12.3 | ✅ §2.11 存在，內含「進階：作業每週都比上週慢的漸進退化怎麼查」 |
| §3.10（skew）| ch11、ch12 | ✅ §3.10「少搬（六）：處理 skew」存在 |
| §4.2（AQE）| ch12 | ✅ §4.2「AQE 自動幫你做的三件事」存在 |
| §4.4（SQL 旋鈕 shuffle.partitions/autoBroadcast）| ch12 | ✅ §4.4「AQE 之後，還值得手動懂的少數 SQL 旋鈕」存在且涵蓋這三個 config |
| §4.5（記憶體）| ch12 | ✅ §4.5「一個 executor 的記憶體裡裝了什麼」存在 |
| §4.6（executor sizing）| ch12 §12.2（memoryOverheadFactor 列 §4.5、§4.6）| ✅ §4.6「給 executor 配多少」存在；memoryOverheadFactor 主要在 §4.5，§4.6 也有引用 overhead，ref 可接受 |
| §4.7（dynamic allocation）| ch12 | ✅ §4.7「dynamic allocation 與多租戶」存在 |
| §5.3（壓縮）、§5.4（partition 設計）、§5.5（小碎檔）、§5.6（ANALYZE）| ch11、ch12 | ✅ 皆存在 |
| §5.8（managed/external table）| ch12 | ✅ §5.8「營運共用資料表：Hive 3 的 managed／external」存在 |
| §6.2、§6.3（引擎選用）| ch11、ch12 | ✅ §6.2「三個引擎是怎麼跑你的查詢的」、§6.3「怎麼選：一張決策表」存在 |
| §6.7（ACID 跨引擎限制）| ch11、ch12 | ✅ §6.7「CDP 實務三：ACID 表跨引擎的限制」存在 |
| §7.2（冪等）、§7.4（回填）、§7.5（監控）、§7.7（端到端走查）| ch11 | ✅ 皆存在且主題符合 |
| §8.2（品質閘）、§8.3（時間點正確性/training-serving）、§8.4（共用表契約）| ch11 | ✅ 皆存在 |
| §8.5（版本與可重現性）、§8.6（設計模式與回補）| ch09、ch12 | ✅ 皆存在 |
| §9.1（問題定位 pull vs push）、§9.2（推送通道）、§9.3（冪等與重試）、§9.4（就緒閘）、§9.5（PII 稽核）| ch11、ch12 | ✅ 皆存在 |
| §10.2（Catalyst 同一引擎）、§10.5（可測試重用）| ch11、ch12 | ✅ 皆存在 |
| §1.7（executor 大小取捨）| ch12 §12.1 | ✅ §1.7「一個 executor 該多大？」存在且主題符合 |

### 1c. 找到的真缺陷

**❌ [真缺陷-1] ch07 行 5：「前面七章都在教...」應改為「前面六章」**
- 檔案：`07-operating-pipelines.md` 第 5 行
- 現況：「前面七章都在教「怎麼把一條查詢跑得快」」
- 問題：ch07 是第七章，前面只有 01–06 共六章，「前面七章」多算了一章。
- 應改為：「前面六章都在教...」

**❌ [真缺陷-2] ch05 行 150：「出口設計見第 09 章（其餘細節待補）」- TODO 標記殘留**
- 檔案：`05-storage-efficiency.md` 第 150 行
- 現況：「若 reverse ETL 的 entity 過濾需求也很重，出口設計見第 09 章（其餘細節待補）」
- 問題：「（其餘細節待補）」是草稿 TODO 標記，不應出現在 published 章節。
  此外，ch09 實際上並未提供「entity-based partition 設計的 reverse ETL 出口設計」的具體指引——ch09 的重心是 push 通道選擇、冪等、PII、稽核，而非 partition 設計。這個 forward reference 不準確或過度承諾。
- 建議：移除「（其餘細節待補）」並把 forward reference 改為軟指向：「若需要按 entity 過濾後再推出，reverse ETL 通道選擇見第 09 章」；或直接在此補一句關於「entity 過濾留給推送層做」的說明。

---

## 2. ch11 場景索引審查

### 2a. 三情境涵蓋範圍

| 情境 | 主場章 | 所有 §X.Y 引用正確？ | 有無導向不存在的節 |
|---|---|---|---|
| ① ad-hoc 探索 | 02→03→06 | ✅ §2.10、§3.2/3.4、§3.3/5.2、§3.8、§6.2/6.3 全存在 | 無 |
| ② 排程產表/名單 | 07（+03/04/05）| ✅ §7.2、§7.4、§7.5、§7.7、§8.2、§5.5、§2.11 全存在 | 無 |
| ③ 特徵庫 | 08（+05/06/07/09/10）| ✅ §8.3/8.4、§9.3/9.4、§10.5、§5.6、§6.7 全存在 | 無 |

### 2b. 三情境總表（§11.4）

- ✅ 主場章、頭號雷、求快/求穩/求正確欄位均合理
- ✅ 「跨情境共通：第 02 章 + 07、08 章」的說法正確

### 2c. 輕微可加強項

**可加強-1：情境①「選對引擎」說「常常該用 Impala」**
ch11 §11.1 行 26：§6.3 決策表說「秒級互動，常常該用 Impala 而不是 Spark」——這與 ch06 §6.3 的「依工作負載決策」方向一致，屬合理化簡說法而非技術錯誤。無需修改，屬可接受的敘事捷徑。

---

## 3. ch12 速查名詞表審查

### 3a. Config 預設值對齊

| Config | ch12 所列預設 | ch04 所述預設 | 一致？ |
|---|---|---|---|
| adaptive.enabled | true（3.2+）| true（3.2+）| ✅ |
| shuffle.partitions | 200 | 200 | ✅ |
| autoBroadcastJoinThreshold | 10MB | 10MB | ✅ |
| files.maxPartitionBytes | 128MB | 128MB | ✅ |
| advisoryPartitionSizeInBytes | 64MB | 64MB | ✅ |
| memory.fraction | 0.6 | 0.6 | ✅ |
| memory.storageFraction | 0.5 | 0.5 | ✅ |
| executor.memoryOverheadFactor | 0.10 | 0.10（最低 384MB）| ✅ |
| driver.memory | 1g | 1g | ✅ |
| dynamicAllocation.enabled | 開源 false / CDP 預設 true | 開源 false / CDP true | ✅ |
| yarn.queue | default | default | ✅ |
| partitionOverwriteMode | static | static（ch07 §7.2 亦確認）| ✅ |
| parquet.compression.codec | snappy | snappy | ✅ |
| cbo.enabled | false | false | ✅ |
| HDFS block size/副本 | 128MB/3 | 128MB/3（ch05 §5.4）| ✅ |

所有預設值與各章內文一致，無打架。

### 3b. 症狀→章引用

| 症狀 | ch12 指向 | 目標節存在且主題符合 |
|---|---|---|
| 少數 task 特別久 | §3.10 | ✅ |
| spill 非零 | §4.5、§3 | ✅ |
| 該 broadcast 卻是 SortMergeJoin | §3.5、§5.6 | ✅ |
| Scan 無 PartitionFilters | §3.2、§3.4 | ✅ |
| 幾萬個 task 每個 Input 很小 | §5.5 | ✅ |
| join 後列數暴增 | §3.5、§3.7 | ✅ |
| 作業卡在 ACCEPTED | §4.6、§4.7 | ✅（§4.6 有 ACCEPTED 說明）|
| 每週都比上週慢 | §2.11 | ✅（§2.11 含漸進退化子節）|

### 3c. 名詞→章引用

| 名詞 | ch12 指向 | 正確？ |
|---|---|---|
| partition | §1.2 | ✅ |
| shuffle | §1.5、§1.6 | ✅ |
| narrow/wide dependency | §1.5 | ✅ |
| executor/driver | §1.1、§1.4 | ✅ |
| application→job→stage→task | §1.4 | ✅ |
| lazy evaluation | §1.3 | ✅ |
| Catalyst | §1.3、§10.2 | ✅ |
| AQE | §4.2 | ✅ |
| skew | §3.10 | ✅ |
| spill | §1.6、§4.5 | ✅ |
| broadcast join | §3.5 | ✅ |
| predicate pushdown/partition pruning | §3.2、§3.4 | ✅ |
| Metastore | §1.1、§5.8 | ✅ |
| external/managed table | §5.8、§6.7 | ✅ |
| ACID/compaction | §6.7 | ✅ |
| idempotent | §7.2 | ✅ |
| backfill | §7.4 | ✅ |
| feature/label | §8.3 | ✅ |
| snapshot/grain | §8.3 | ✅ |
| data leakage | §8.3 | ✅ |
| training–serving skew | §8.3、§10.5 | ✅ |
| reverse ETL | §9.1 | ✅ |
| upsert | §9.3 | ✅ |
| WAP | §8.6、§9.4 | ✅ |
| PII | §9.5 | ✅ |
| OLTP | §9.2 | ✅ |

ch12 名詞表全部指向正確，無誤指或缺失。

---

## 4. 能力地圖：G1 reverse ETL（§09）、G2 training-serving（§08）覆蓋度

### G1 reverse ETL（§09）

✅ ch09（09-reverse-etl.md）完整覆蓋：
- §9.1 pull vs push 模型定位（有需要才 push）
- §9.2 四條推送通道（JDBC sink、交換區、NiFi、Sqoop），含 numPartitions 打垮 OLTP 的關鍵陷阱
- §9.3 冪等與重試（upsert 業務鍵、全量 vs 增量、partial reject 陷阱）
- §9.4 就緒閘與發佈（只推驗過、已發佈版本）
- §9.5 PII、Ranger 授權、稽核留痕
- §9.6 格式/key 對齊與 schema drift 防禦
- §9.7 端到端走查（算→驗→發佈→遮罩→推→稽核）
- §9.8 取捨（push vs pull、即時 vs 鬆耦合、PII、多目的地）

G1 覆蓋**完整**，達到可操作程度。

### G2 training-serving（§08）

✅ ch08 §8.3「時間點正確性與特徵洩漏：算特徵時，絕不碰未來」完整覆蓋：
- 特徵洩漏定義與危害
- snapshot-partition 模型的時間上界過濾
- training–serving 一致性：同一張快照表、同一段計算、缺值填充一致
- §10.5（ch10）進一步提供 DataFrame API 函式封裝作為「單一真實來源」的工程做法

G2 覆蓋**完整**，達到可操作程度。

---

## 5. 兩條主線 end-to-end 路徑驗證

### 初階分析師（ad-hoc → 排程 → 出名單 → 送達）

| 步驟 | 章節 | 斷點？ |
|---|---|---|
| 建立直覺 | ch01（心智模型） | — |
| 找瓶頸/診斷 | ch02（Spark UI）| — |
| 改寫查詢 | ch03（SQL 優化）| — |
| 調參數 | ch04（Spark config）| — |
| 存得好 | ch05（儲存效率）| — |
| 選引擎 | ch06（引擎選用）| — |
| 跑得可靠 | ch07（可靠排程）| — |
| 資料可信 | ch08（資料產品）| — |
| 送出去 | ch09（reverse ETL）| — |

**✅ 無斷點**，每章到下一章有明確的 forward reference 和 nav link。

### 進階 AE（特徵庫 → 訓練/serving → reverse ETL → 業務系統）

| 步驟 | 章節 | 斷點？ |
|---|---|---|
| 特徵存儲設計 | ch05（partition、external 表、ANALYZE）| — |
| 多引擎共用 | ch06（ACID 限制、REFRESH、多引擎存取）| — |
| 特徵排程可靠 | ch07（冪等、回填、端到端走查）| — |
| 特徵正確性 | ch08（洩漏、training-serving、版本化）| — |
| reverse ETL | ch09（推送通道、冪等、PII）| — |
| DataFrame API 封裝 | ch10（可測試函式、共用特徵邏輯）| — |

**✅ 無斷點**，進階主線完整可走。

---

## 6. 敘事弧／章序

| 段落 | 章 | 合理性 |
|---|---|---|
| 基礎原理 | 01（心智模型）、02（診斷）| ✅ 01 是後面一切的基礎 |
| SQL 優化 | 03（SQL 寫法）| ✅ 緊接診斷工具 |
| 參數調整 | 04（AQE-first config）| ✅ 先 SQL 再 config 的優先序正確 |
| 儲存設計 | 05（儲存效率）| ✅ 建表/存法，在引擎選用前學好 |
| 引擎選用 | 06（Spark/Hive/Impala）| ✅ 有了儲存基礎才能做引擎決策 |
| 營運一 | 07（可靠排程）| ✅ 「能算」→「能可靠跑」 |
| 營運二 | 08（資料產品可信）| ✅ 「能跑」→「能信賴」 |
| 營運三 | 09（送出去）| ✅ 「能信賴」→「能交付」 |
| 進階 API | 10（PySpark DataFrame）| ✅ 進階工程能力，選修位置合理 |
| 索引 | 11（場景索引）| ✅ 讀完前面後的地圖 |
| 速查 | 12（速查名詞表）| ✅ 永久參考工具 |

**✅ 12 章順序合理**，無「前面用到後面才教」的情況。
ch04 在 §4.5 提到 cache()/persist()「第 10 章」—— 這是一個前置提及進階章的 forward reference，不是「前面用到後面才教」，因為 ch04 只說「那是第 10 章的東西」，沒有要求讀者先懂它。

---

## 7. index 與 12 章實況一致性

### 7a. 章節導覽表（index 表格 vs 實際章節）

| index 所列 | 實際章節標題 | 一致？ |
|---|---|---|
| 01 Spark 怎麼跑你的 SQL | 01-how-spark-runs-your-sql.md「Spark 怎麼跑你的 SQL」 | ✅ |
| 02 用 Spark UI 找瓶頸 | 02-diagnose-with-spark-ui.md「用 Spark UI 找瓶頸」 | ✅ |
| 03 SQL 寫法優化 | 03-sql-tuning.md「SQL 寫法優化」 | ✅ |
| 04 Spark 設定（AQE-first）| 04-spark-config.md「Spark 設定（AQE-first）」 | ✅ |
| 05 儲存效率 | 05-storage-efficiency.md「儲存效率」 | ✅ |
| 06 引擎選用 | 06-engine-selection.md「引擎選用」 | ✅ |
| 07 營運（一）：可靠地把排程跑起來 | 07-operating-pipelines.md 標題一致 | ✅ |
| 08 營運（二）：讓資料產品可信 | 08-data-product-correctness.md 標題一致 | ✅ |
| 09 營運（三）：把資料送出去——reverse ETL 回業務系統 | 09-reverse-etl.md 標題一致 | ✅ |
| 10 （進階）何時與如何改用 PySpark DataFrame API | 10-pyspark-dataframe-api.md 標題一致 | ✅ |
| 11 場景對應（索引）| 11-scenario-playbooks.md「場景對應（索引）」 | ✅ |
| 12 速查與名詞表 | 12-cheatsheet-and-glossary.md「速查與名詞表」 | ✅ |

### 7b. 兩條主線描述

index 說：
- 初階：01–06 建立優化能力，07–08 讓排程與資料產品可信賴，最終 → 第 09 章送出去。
- 進階：初階基礎 + 第 10 章 DataFrame API + 第 11 章場景索引 + 第 09 章 reverse ETL。

評估：
- ✅ 初階主線描述：把 ch07/ch08 合稱「讓排程與資料產品可信賴」，ch09 另列為「最終送出去」——這樣分段是合理且清晰的，不是漏章。
- ⚠️ **可加強-2（非真缺陷）**：進階主線描述把 ch11 列為「按場景對號入座」，但 ch11 對初階也有用（它是地圖，兩條線都能用）。目前的描述讓 ch11 看起來是進階專屬，可能讓初階讀者誤以為 ch11 不是給他們的。
  建議（可選）：把 ch11 移到兩條線共用說明，或在初階路徑中也提一句「第 11 章可按情境對號」。

---

## 彙整

### 真缺陷（必修）

| 編號 | 檔案:行 | 問題 | 應改成 |
|---|---|---|---|
| D1 | `07-operating-pipelines.md:5` | 「前面**七**章都在教...」 | 「前面**六**章都在教...」 |
| D2 | `05-storage-efficiency.md:150` | 「見第 09 章（**其餘細節待補**）」 — draft TODO 殘留；且 ch09 未涵蓋 entity 過濾的分區出口設計 | 移除「（其餘細節待補）」；改成「見第 09 章」，或補一句「entity 過濾留推送層做，不必在儲存分區層處理」 |

### 可加強（非阻塞）

| 編號 | 位置 | 說明 |
|---|---|---|
| E1 | `index.md` 兩條主線描述 | ch11 目前被歸為「進階路徑」的步驟，但它是兩條線都有用的地圖章；可在初階路徑也提 ch11 |
| E2 | `09-reverse-etl.md` | ch09 序言說「這是本手冊兩條主線真正的終點」，但初階的「出名單給 PM」可以 pull 而不需要 push；ch09 §9.1 雖有解釋這個差異（pull vs push），但序言的語氣稍強，可能讓初階讀者誤會一定要做 reverse ETL 才算完成。屬措辭可加強，非架構缺陷。 |

### 誤讀（非問題）

- `index.md` 說「07–08 讓排程與資料產品可信賴」而沒列 ch09：這不是遺漏，ch09 在後文明確列為「→ 第09章送出去」，邏輯上正確。
- `ch12 §12.2` `memoryOverheadFactor` 指向 `§4.5、§4.6`：§4.6 確實提到 overhead 計算（「heap × 1.1」），雙引用合理。

---

## 最終判定

**PASS（可進 HTML 轉換階段）**

兩個必修項（D1 數字誤、D2 草稿 TODO 殘留）屬局部單點問題，不影響整體架構和章間邏輯。
兩條主線完整、G1/G2 能力地圖已補齊、跨章 §X.Y 引用全部正確、12 章順序合理。
修完 D1/D2 即可轉換 HTML。
