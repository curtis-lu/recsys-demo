# Spark 優化參考手冊 — 實作計畫

> **For agentic workers:** 用 `superpowers:subagent-driven-development`（推薦）或 `superpowers:executing-plans` 逐 task 執行。步驟用 `- [ ]` checkbox 追蹤。
> 設計來源：`.worktrees/spark-handbook/docs/superpowers/specs/2026-06-14-spark-tuning-handbook-design.md`（spec 為單一真實來源，方向調整先改 spec）。

**Goal:** 產出一份給數據部門 SQL-first 同事自行參考的 Spark 優化手冊（9 章 + index 的 `.md`，全部審定後轉離線 `.html`）。

**Architecture:** 純文件任務，在 worktree `.worktrees/spark-handbook`（分支 `feat/spark-tuning-handbook`）進行，**不跑 python/Spark/pytest**。每章的「測試閘」＝兩個審稿 subagent（A 技術 reviewer 驗真實性、B 目標讀者 reader 驗易讀性）並行審 + 使用者人工審。一章一審、審過才動下一章。

**Tech Stack:** Markdown + Mermaid（概念圖）；HTML 階段內嵌 mermaid.js（離線可看）。權威來源：Spark 3.3 官方文件、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》、Databricks / Cloudera CDP 官方文件。

---

## 路徑與環境（每個 task 都適用）

- 工作根目錄：`/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook`
- 手冊輸出目錄：`docs/handbooks/spark-tuning/`
- 審稿日誌目錄：`docs/handbooks/spark-tuning/.reviews/`
- 寫作規範：`docs/handbooks/handbook-writing-guide.md`（可轉移者：具體數字落地、結論誠實、不洩漏鷹架、流程可操作、§11/§12 審稿清單）
- **給使用者的檔案路徑一律帶 `.worktrees/spark-handbook/` 前綴**（否則他開到主目錄打不開）。
- git 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook ...` 或先 `cd` 該 worktree root。
- **commit 後不必再還原 `graphify-out/GRAPH_REPORT.md`**：它現在是 **untracked**（graphify 已修，commit `61ee9ac`），post-commit hook 重建它不會弄髒 tracked tree、也不會擋後續 checkout/merge。舊版那行 `git checkout -- graphify-out/GRAPH_REPORT.md` 已過時（會報 `pathspec did not match`），別再串進 commit 指令。

## 寫作慣例（每章共用）

- 繁體中文；專有名詞用英文原文（SparkSession 不譯）。
- 範例以 **SQL** 為主（DataFrame API 僅第 07 章）。
- 一節一概念；每個抽象主張用**具體數字 / 銀行資料量**（客戶 ~1000 萬、信用卡帳務 ~3000 萬/月、App ~1000 萬筆/天）落地。
- 每個 config 主張附 **Spark 3.3 預設值 + 出處**；每個「做 X → 變快」先確認因果方向。
- 遇取捨（時間/記憶體/儲存）就地明講。
- 每章結構：H1 標題 → 「本章前提（讀者已讀哪些章）」一句話 → 內文小節 → 章末「上一章 / 下一章」導覽連結。
- 概念圖用 Mermaid code block（` ```mermaid `）。

## 標準「每章撰寫循環」（Task 1–9 共用，每章的 Step A–F）

- **Step A — 查證並記錄關鍵事實**：對該章「須查證重點」清單，用 WebFetch/WebSearch 查 Spark 3.3 官方文件/指定書籍，把確認的預設值/行為/出處記下來（供寫稿與 reviewer 比對）。
- **Step B — 寫 `NN-name.md` 草稿**：依該章內容大綱 + 寫作慣例，含 Mermaid 概念圖、前提註記、章末導覽。
- **Step C — 並行派兩個審稿 subagent**：在同一則訊息內用 Agent tool 同時派 reviewer 與 reader（`superpowers:dispatching-parallel-agents`），各自即時寫日誌到 `.reviews/NN-name__reviewer.md` / `.reviews/NN-name__reader.md`。prompt 用下方「審稿 subagent prompt 模板」，填入 `{CHAPTER}` 與 `{PRIOR_CHAPTERS}`。
- **Step D — triage 並修**：把兩份回報按「真缺陷（必補）/ 可加強（斟酌）/ 誤讀（不改或微調）」分類；真缺陷必修，可加強斟酌，誤讀記錄不改。重大取捨不明才回頭問使用者。
- **Step E — 送使用者審**：把該章（帶 `.worktrees/` 前綴路徑）送使用者，納入回饋；**若使用者調整了方向，append 到本檔末「Direction Log」**。
- **Step F — 標記進度 + commit**：在「Progress Tracker」把該章打勾，commit（訊息 `docs(spark-handbook): 第 NN 章 <主題>`），然後 graphify reset。

### 審稿 subagent prompt 模板

**Reviewer（subagent_type: general-purpose）**
```
你是《Spark 優化參考手冊》的技術審查員。

[背景]
- 讀者：數據部門 SQL-first、無 DE 背景的分析師/科學家。
- 環境：Spark 3.3.x（AQE 預設開）+ Hive 3.1.3（CDP 7.1.9，YARN+HDFS，另有 Impala）；範例以 SQL 為主。
- 這份文件會被同事當行動依據，錯一個預設值或因果方向就會誤導真實調優。
- 寫作規範：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/handbook-writing-guide.md（特別 §12）。

[目標]
逐條檢查本章所有技術主張的正確性，找出不正確/不精確/無權威來源支撐處。

[素材]
- 待審章節：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/{CHAPTER}.md
- 權威來源（限）：Spark 3.3 官方文件（SQL Performance Tuning Guide、Configuration、SQL reference）、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》、Databricks / Cloudera CDP 官方文件。用 WebFetch/WebSearch 查證。

[限制]
- 只查證、不改稿。
- 來源限權威，不引未認證部落格。
- 對齊 Spark 3.3.x、Hive 3.1.3/CDP，不可套別版預設值/行為。
- 每條判定必附出處（URL 或書名+章節）；查不到標「無法查證」、不臆測。
- 不評文筆易讀性（那是 reader 的事）。
- 邊查邊把每條發現「即時 append」到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reviewer.md（每查一條就寫一條，讓人能同步看進度）。

[完成的定義]
日誌檔內：逐條主張 + ✅已驗證(附出處) / ❌錯誤(正確值+出處) / ⚠️無法查證；點出「建議被寫成硬限制」「因果正負號可疑」「引用出處錯」；結尾按 真缺陷(必補)/可加強(斟酌)/誤讀(不改或微調) 三級彙整。回傳一段摘要。
```

**Reader（subagent_type: general-purpose）**
```
你是《Spark 優化參考手冊》的目標讀者審稿人。

[人設＝背景]
你是銀行的資料分析師：會寫 SQL、懂業務資料，但沒學過分散式系統，不知道 shuffle/executor/partition 底層怎麼運作，看到沒解釋的英文術語會卡。嚴格扮演這個讀者，不可因為你其實懂 Spark 就放水。

[目標]
從頭讀到尾，標出讓你讀不懂、卡關、缺脈絡、太抽象、不自明之處。

[素材]
- 待審章節：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/{CHAPTER}.md
- 你「已經讀過、可假設已懂」的前置章：{PRIOR_CHAPTERS}
- 讀者審查清單：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/handbook-writing-guide.md §11。

[限制]
- 只回報讀者視角問題，不查技術對錯（那是 reviewer）。
- 不改稿。
- 任何第一次出現、沒當場解釋的術語都要標（即使你知道那是什麼）。
- 邊讀邊把卡關點「即時 append」到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reader.md。

[完成的定義]
日誌檔內：逐段/逐節卡關點 + 類型（缺脈絡/太抽象＝只有形容詞沒數字例子/術語沒先定義/概念圖不自明/步驟不可操作/鷹架洩漏）+「我會這樣想、我會問什麼」；全章主旨是否一致、範疇有無失衡；結尾按 真缺陷/可加強/誤讀 三級彙整。回傳一段摘要。
```

**Architecture / Completeness（角色 C，subagent_type: general-purpose；里程碑跑、非逐章）**
```
你是《Spark 優化參考手冊》的完整度與架構審查員。所有輸出用繁體中文。這是跨整本手冊的審查，不是單章。

[背景]
- 手冊讀者起點是 SQL-first、無 DE 背景的分析師/科學家，但**終極學習目標**是：讓一個 Spark 新手讀完整本後，具備「長期穩定營運資料排程與特徵庫(feature store)」的能力——不只 ad-hoc，還要懂資源配置與多租戶、可靠/可重跑、schema 演進與維護、時間點正確性等營運課題。
- 手冊骨架(index + 9 章)與各章大綱見 spec：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/superpowers/specs/2026-06-14-spark-tuning-handbook-design.md（讀 §1 目標/讀者/深度、§5 各章大綱、§11 成功標準）。
- 哪些章「已寫成 .md」、哪些「還只有大綱」，看 plan 的 Progress Tracker：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/superpowers/plans/2026-06-14-spark-tuning-handbook-plan.md

[目標]
從整本的角度檢查：邏輯架構是否清楚、章節順序是否合理、深度是否足以達成終極能力目標。找出缺漏主題、順序/依賴問題、深度不足之處。

[素材]
- 已寫章節：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/*.md（實際讀內容）
- 尚未寫的章節：只能評 spec §5 的大綱
- spec 全文、plan 的 Progress Tracker（路徑如上）

[限制]
- 不查單點技術對錯(那是 reviewer A)、不挑逐句易讀性(那是 reader B)——只看整體架構、順序、覆蓋度、深度。
- 不改稿。
- 明確區分「已寫章節的實況」與「尚未寫、只能評 outline」。
- 把發現即時 append 到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/_architecture__round-{N}.md（{N}=本輪輪次）。

[完成的定義]
日誌檔內：(a) 能力地圖——把「長期營運排程/特徵庫」需要的能力逐項列出、對應到「由哪章哪節支撐」，明確標出無人覆蓋的缺口；(b) 章節順序/依賴是否合理(有沒有前面用到後面才教的概念)；(c) 各章深度是否足以支撐營運；(d) 具體補強建議(新增章/節、調順序、加深何處)；按 真缺陷(必補)/可加強/誤讀 三級彙整。回傳一段摘要(最重要的結構缺口與建議)。
```

---

## Progress Tracker

> **▶ 目前進度 / 下一步（/compact 後先讀這裡）**：第 01–06 章已寫完並 commit；**Hive/Parquet 補基礎**（ch01 §1.1 Metastore／兩個 Hive、ch05 §5.2 Parquet/ORC 內部結構進階節、ch03 gloss；雙審＋triage 全修）已 commit（`c90841d`）。**【2026-06-19 調整】**(1) ch07 PySpark **暫緩**，先寫營運線；(2) 原單一營運專章**拆兩章**＋全書 9→11 章重編號（**08 營運一·排程可靠性／09 營運二·資料產品正確性／10 場景索引／11 速查**；spec §5、index、ch01–06 交叉引用、本 plan 皆已改）。第 06 章「引擎選用：Spark vs Hive on Tez vs Impala」（10 節、4 圖、PRIOR=01/02/05）骨架＝§6.1 同一份資料三引擎都能查＋在哪選引擎（看你從哪個入口下 SQL）→§6.2 三引擎跑法（Impala 常駐 daemon/MPP/不容錯、Hive on Tez 非 §1.9 那個老 MapReduce、Spark 你熟那套）＋大量術語鋪墊→§6.3 決策表＋決策樹（界線分明只一條：秒級互動走 Impala、容錯重批次別走 Impala）→§6.4 三引擎驗屍工具（Spark UI / Tez UI / Impala PROFILE，心法同第 02 章）→§6.5 共用表威力與陷阱→§6.6 `REFRESH` vs `INVALIDATE METADATA`＋CDP 事件驅動自動同步→§6.7 ACID 從零講起〔兩個 `###`：先補基礎(交易白話/A·C·I·D/base+delta+compaction/有 vs 沒有/三情境)＋跨引擎限制矩陣〕→§6.8 取捨→§6.9 一天三引擎貫穿→§6.10 帶走；1 輪 reviewer(技術零事實錯、5 點查證全逐字對齊)＋2 輪 reader＋§6.7 補審、triage 全修；修了 3 個 stale Cloudera URL(blog→canonical、runtime/7.1.0→docs-archive；ch05 §5.8 同款一起修)。**第 08 章已寫完＋雙審＋補審＋commit（`672b45a`）**：營運（一）排程可靠性，9 節〔§8.1 三層工具模型 dbt/Airflow/cron→8.2 冪等(含 positional INSERT 欄位錯位 footgun)→8.3 相依→8.4 回填(含 current_date 不可重現 footgun)→8.5 監控(含 cron 靜默失敗 footgun)→8.6 維護→**8.7 常見維運踩雷速查表**→8.8 取捨→8.9 帶走〕＋3 圖、到處具體碼。triage 全修：INSERT OVERWRITE 版本更正(datasource 表只覆寫符合分區是 **Spark 2.1** 非 3.3)、Airflow 全標準化 **2.x**(§8.1 版本提醒 callout：SLA→Deadline Alerts、`dags backfill`→`backfill create`、sensor 匯入移 providers；連結釘 2.10)、Spark 連結改 `/docs/latest/`(3.3.2 主站已 404、version-exact 需 archive.apache.org)、CDP compaction 出處精準化。`BY NAME`＝SPARK-42750/Spark 3.5(3.3.2 無，hedge 保守)。**第 09 章已寫完＋雙審(reader 因 socket 死兩次、§9.4–末由我自審補)＋triage＋commit（`8b5cdbc`）**：營運（二）資料產品正確性，7 節〔§9.1 四面向可信度地圖→9.2 品質驗證(dbt 4 generic test+severity warn/error+純 SQL 品質閘+SQL→shell exit 橋)→9.3 時間點正確性/特徵洩漏(snapshot-partition=讀對分區絕不讀未來、洩漏三類、時間切線圖、as-of 進階旁註)→9.4 多消費者契約(schema 只加不改)→9.5 資料版本(build-version/audit 帳本/雙層(snapshot_date,build_version)分區+current view)→9.6 取捨→9.7 帶走〕＋3 圖、到處具體碼。Step A 查證：dbt 4 generic test/severity error·warn·error_if·warn_if、**Spark 3.3.2 無原生 AS OF/temporal join(對 archive join 頁逐字確認)**全對官方。triage：reviewer 零真缺陷(改 §9.2 accepted_values 補 `quote:false` 整數欄)；reader 補一票術語錨點(cust_feature/特徵/label/entity/snapshot/grain/generic test/宣告式)、補 SQL→exit 可操作橋、as-of 段壓縮成「進階旁註可跳過」、軟化「四件互不重疊」。**ch08 §8.7 末列前指錨點已校(三列→兩列、→§9.3/§9.4 連結可解析)**。

**下一步（待 user 定方向）＝營運線 08+09 已完成。** 剩兩個方向、**由 user 選**：(a) 回頭寫 **Task 7 `07-pyspark-dataframe-api.md`**（原暫緩的 PySpark 章，PRIOR=01,03,04）；(b) 直接寫 **Task 9 `10-scenario-playbooks.md`**（場景索引，PRIOR 含 07–09——但 07 尚未寫，會 forward-ref 未寫章，故 (a) 先寫較順）。ch09 已送 user 審、等回饋。**體例慣例（01–09 已立）**：precision footer 不放 .reviews 指標；深主題/插章中間優先做 `###` 小節避免重編號；能上 mock 面板/貫穿範例就上；環境細節以 user 公司經驗為準；**審稿後若再加料一定要補審**；reader 常抓「術語/縮寫詞第一次出現沒當場定義、無 ML 背景讀者缺特徵/label/training/serving 錨點」→第一次承重就 gloss；commit 後**不必**還原 graphify-out/GRAPH_REPORT.md（已 untracked）；**審稿 subagent 長跑易 socket 死→可分段派或關鍵段自審補**。

- [x] Task 0：scaffold（目錄 + index 骨架 + .reviews/）
- [x] Task 1：`01-how-spark-runs-your-sql.md`（心智模型）— 10 節、三輪雙 subagent 審＋修（含 partition 來源、application/job/stage/task 層級、executor 取捨、shuffle 三麻煩、端到端範例、Spark vs Hive-MR）；待 user 最終 glance
- [x] Task 2：`02-diagnose-with-spark-ui.md`（Spark UI 診斷）— 10 節、雙 subagent 審＋triage 修＋user 回饋修；**§2.2 改為「一律從 History Server 進」（completed/incomplete 清單）**（user 公司經驗：還在跑的 app 在 incomplete 查得到，已對 monitoring.html 逐字查證；同時解掉原 live UI 入口方向錯誤＋RM/AM 術語沒解釋）、升級 AQE `isFinalPlan` 來源（Databricks AQE 文＋SPARK-33850）、**加 mock Summary Metrics 面板＋§2.8「一條慢查詢的驗屍」貫穿範例**、EXPLAIN 示意輸出、percentile 白話；待 user 再 glance
- [x] Task 3：`03-sql-tuning.md`（SQL 寫法）— 12 節、4 圖、雙 subagent 審＋triage 修；骨架＝「少讀（partition 裁剪／projection／pushdown）＋少搬（broadcast vs sort-merge、手動 hint、join 陷阱型別/爆量、聚合 approx_count_distinct、window、skew AQE→salting→分流）」＋§3.11 貫穿範例；triage 修：型別前提矛盾、Definitive Guide 章號 Ch.8→9、cast→NULL 會算錯、HLL++ 直覺、術語白話、§3.10 門檻降為細節；**user 加碼**：§3.5 加「5 種 join 物理模式對照表（含 BNLJ）」過聚焦 reviewer（修掉掛錯的 Databricks KB URL）；02/03 precision footer 的 .reviews 指標已移除對齊 01
- [x] Task 4：`04-spark-config.md`（Spark 設定 AQE-first）— 9 節、3 圖、雙 subagent 審＋triage 修＋user glance；骨架＝風險梯度（§4.1 心法 AQE-first＋兩前提〔改 SQL+喂統計>調 config、旋鈕分 SQL 層/資源層〕→ §4.2 AQE 三件事 → §4.3 確認 AQE＋SET 生效分野＋mock Environment 面板 → §4.4 少數 SQL 旋鈕〔shuffle.partitions/autoBroadcast/maxPartitionBytes〕→ §4.5 記憶體 execution/storage/overhead＋M/R＋spill 救法 → §4.6 core/mem/台數 worked example〔接 §1.7 的 100core/400GB→啟動參數〕→ §4.7 dynamic allocation 與多租戶〔**CDP 預設 true vs 開源 false**〕→ §4.8 排程作業配置貫穿範例 → §4.9〕；Step A 查證：adaptive.enabled true(3.2+)/shuffle.partitions 200/advisoryPartitionSizeInBytes 64MB/autoBroadcast 10MB/maxPartitionBytes 128MB/memory.fraction 0.6(heap−300MB)/storageFraction 0.5/memoryOverheadFactor 0.10/dynamicAllocation 開源 false·CDP true 皆對齊；triage 修：補 heap 定義、重畫記憶體圖（顯示扣 300MB 再分 0.6/0.4 順序、去 §12 禁的 `←`）、解釋 ÷1.1、補資源層「在哪設/Livy/請平台」對 Hue 落地、gloss external shuffle service/SLA、SET 全稱補 static SQL config 例外、「多給 task」改具體、overhead 用途貼官方字、5 core→「至多約 5」貼 Cloudera、20GB 口徑與 §1.7 同步（含 overhead 總額）；小檔分工定案（見 Direction Log）
- [x] Task 5：`05-storage-efficiency.md`（儲存效率）— 10 節、3 圖、**兩輪**雙 subagent 審＋triage 修＋user 兩輪深問加料；骨架＝「§03 花用本錢/§05 存本錢」（§5.2 欄式 Parquet/ORC→§5.3 壓縮 snappy→§5.4 partition 設計＋`###`DataNode/locality→§5.5 小檔成因[含成因四上游就碎+openCost 虛胖]/徵兆/解法[SQL hint REPARTITION/COALESCE]＋`###`進一步＋`###`兩個實戰問題[寫分區表帶分區欄位、CTE hint 放最外層]→§5.6 ANALYZE[非自動/NOSCAN/FOR COLUMNS/CBO opt-in]→§5.7 bucketing[Spark/Hive hash 不相容]→§5.8 Hive3 managed(ACID/ORC/HWC) vs external(Spark CREATE TABLE 預設/Metastore 共用 Hue/Impala 可查/dbt-spark)/schema 只加不改→§5.9 貫穿範例)；Step A 查證：parquet.compression snappy/filterPushdown true/ANALYZE 語法/cbo.enabled·joinReorder·statistics.size.autoUpdate 皆 false(翻 3.3.2 SQLConf 原始碼)/openCostInBytes 4MB/HDFS block 128MB·副本 3/CDP managed=ACID·Spark CREATE TABLE=external·managed 需 HWC/bucketing 不相容/partitioning hints(COALESCE/REPARTITION/RANGE/REBALANCE)/data locality 等級；**誠實更正**：小碎檔對 locality 影響間接偏弱(真正傷的是不可切分大檔)；**全書 列式→欄式**(03 同步)；小檔出處改 Cloudera 官方部落格(原掛錯 HDFS Architecture 頁)；r2 reader 抓過載→§5.4 改結論先行/§5.5 拆兩個 `###`
- [x] Task 6：`06-engine-selection.md`（引擎選用）— 10 節、4 圖、1 輪 reviewer＋2 輪 reader＋§6.7 補審＋triage 修；Step A 查證 5 點全逐字對齊官方（`REFRESH` 增量/同步/輕量/可刷單一 PARTITION vs `INVALIDATE METADATA` 丟快取/非同步/昂貴/不帶表名 flush 全部/官方原話 prefer REFRESH；Impala 讀 full ACID(ORC)·不可寫·insert-only 可讀寫；CDP Hive 只跑 Tez(指定 MapReduce 報錯)；Impala 不容錯；CDP catalogd 輪詢 HMS 自動同步 `hms_event_polling_interval_s`）；triage 修：**3 個 stale Cloudera URL**(blog→`www.cloudera.com/blog/technical/…`、`runtime/7.1.0`→`docs-archive`；**ch05 §5.8 同款一起修**)、補 Impala 官方 MPP/circumvents-MapReduce 出處、Tez UI 出處改正、§6.8「記憶體導向」軟化為「重 join/聚合吃記憶體」(官方:多數查詢 CPU-bound)；reader→大量基礎設施詞就地 gloss(YARN/容器/MPP/HMS/daemon-d 命名/DAG/full ACID/HQL/ad-hoc/OOM/fragment≈stage/catalog vs Metastore)、§6.3 決策表兩判準改可操作、§6.7 散文矩陣改「表種×引擎×讀/寫」對照表；**user 追問→§6.7 改寫成「不預設懂 ACID」**：兩個 `###`(先補基礎從『HDFS 表＝一堆檔案只能整批覆寫』→ACID 補改既有列→A/C/I/D 白話→base+delta+compaction→有 vs 沒有→三情境;跨引擎限制矩陣)；§6.7 補審抓 4 must-fix(交易/transaction 未定義+撞銀行『一筆交易帳』、`commit`、`MERGE` 首見、`full` 懸空)全補
- [ ] Task 7：`07-pyspark-dataframe-api.md`（DataFrame API 進階）— **暫緩**，先寫 08/09（user 2026-06-19 調序）
- [x] Task 8：`08-operating-pipelines.md`（**營運一·排程可靠性**）— 9 節〔+§8.7 維運踩雷速查〕、3 圖、雙審＋補審(加 §8.7)triage 全修、commit `672b45a`；三層落地 dbt/Airflow(2.x)/cron；冪等(含 positional INSERT footgun)/相依/回填(含 current_date footgun)/監控(含 cron 靜默失敗 footgun)/維護(compaction 重寫)；Step A 查證 partitionOverwriteMode static/dynamic·dbt insert_overwrite·Airflow sensor/retries/catchup 全對官方
- [x] Task 8b：`09-data-product-correctness.md`（**營運二·資料產品正確性**）— **8 節、4 圖**（含 user 加碼 §9.6 進階版本化）、reviewer(零真缺陷)＋reader(§9.1–9.3，§9.4–末自審)＋reviewer-r2(§9.6 補審零真缺陷)＋triage、commit `8b5cdbc`＋`0e6d2d4`；品質驗證〔dbt 4 generic test＋severity＋純 SQL 品質閘＋SQL→exit 橋〕/時間點正確性·特徵洩漏〔snapshot-partition=讀對絕不讀未來、時間切線圖、as-of 進階旁註〕/多消費者契約〔schema 只加不改〕/資料版本〔build-version／audit 帳本／雙層分區+current view〕；Step A 查證 dbt tests·severity·**Spark 3.3.2 無原生 AS OF join**全對官方；改 §9.2 補 `quote:false`、補一票術語錨點；ch08 §8.7 末列錨點已校
- [ ] Task 9：`10-scenario-playbooks.md`（場景對應＝索引；PRIOR 含 07–09）
- [ ] Task 10：`11-cheatsheet-and-glossary.md`（速查與名詞表）
- [ ] Task 11：完稿 `index.md`（導覽/連結/如何使用/兩條學習路線；拆章導覽已更新、最終再校）
- [ ] Task 12：轉 HTML（內嵌 mermaid.js、離線檢查、回頂鈕、跨章導覽）
- [ ] Task 13：全書最終 pass（reader 通讀 + architecture C 架構審查）+ 修正
- 架構審查(C)：round-1 已跑（outline+01，產出營運專章決策）；里程碑續跑（每寫完數章、最終 pass）

---

## Direction Log（append-only；每次使用者調整方向就加一行）

- 2026-06-14：骨架定為 9 章（依調優槓桿分層 + 診斷章 + 場景章）。
- 2026-06-14：PySpark DataFrame API 納入（第 07 章），仍不碰 RDD 低階 API。
- 2026-06-14：每章需派 reviewer + reader 兩個 subagent 審，按「目標/背景/素材/限制/DoD」五項給 prompt，並即時寫日誌可同步審核。
- 2026-06-14：離線 HTML 預設內嵌 mermaid.js（單一來源）；要「連 JS 都不依賴」才切 SVG 預渲染。
- 2026-06-14：純文件任務改用 worktree（避免與使用者其他 session 的 code 改動互相干擾）；給使用者的路徑一律帶 `.worktrees/spark-handbook/` 前綴。
- 2026-06-14（審第 01 章後）：**整本提高深度**。讀者雖是分析師/科學家，但要假設他們未來會**自己營運資料排程、經營多人共用的資料產品（如特徵庫/feature store）**，故每章在易懂的基礎上要帶到進階與營運取捨，不止於 ad-hoc。具體點名要有：① application / job / stage / task 的層級關係（第 01 章）；② executor 的 core 數 / instance 台數 / memory size 之間的取捨（第 01 章建立直覺、第 04 章給操作與多租戶/dynamic allocation 細節）。
- 2026-06-15：**新增第三個審稿角色 C（完整度與架構審查員）**，跨整本看邏輯架構/章節順序/深度，確保「Spark 新手讀完能長期穩定營運排程與特徵庫」這個終極能力目標達成。里程碑跑（outline 定/大改、每寫完數章、最終 pass），不逐章。spec §10.4、§11 已更新；模板見下方「審稿 subagent prompt 模板」。round-1 在「只有 outline + 第 01 章」時即跑（早期抓結構缺口）。
- 2026-06-15：**每章必附資料來源**（user 要求）。每個重要概念段落末尾加「📚 來源」footer（代表性出處＋連結）、章末加「資料來源與精確度說明」（列簡化／無逐字出處處＋版本對齊）。目的＝讓讀者自行驗證、看得出哪裡不完全精確。來源限官方/核心開發者/指定書籍，不引未認證部落格；連結用可達頁（工具對 3.3.x 404→用 latest+註明改版本號）。01 章已套用為範本。spec §6 已加此慣例。
- 2026-06-16（第 02 章 user 回饋）：(1) **live UI / port 4040 不提**——user 公司經驗：還在跑的 application 一樣在 History Server 的 **incomplete** 清單查得到，故 §2.2 改成「不分跑中/跑完，一律從 History Server 進，清單分 completed/incomplete」（已對 `monitoring.html` 逐字查證：列 incomplete＋completed、incomplete 含還在跑或崩潰未收尾者、間歇更新預設 10s、需 `spark.eventLog.enabled`）。附帶好處：解掉前一版 reviewer 抓到的「live UI 入口方向寫反」＋ reader 抓到的「ResourceManager/ApplicationMaster 沒給人話」。(2) **要實際範例對照**（純文字難想像）——加 mock Summary Metrics 面板（示意數字）＋ §2.8「一條慢查詢的驗屍」貫穿走查；數字皆示意，章末已標、轉 HTML 時可換公司環境真實截圖。後續各章比照：能上 mock 面板/貫穿範例就上。
- 2026-06-16（第 03 章）：(1) 寫完第 03 章「SQL 寫法優化」，骨架＝「少讀／少搬」兩主軸，每招原理→SQL before/after→EXPLAIN/UI→取捨；雙審 triage 修（§3.11/§3.7 型別前提矛盾、Definitive Guide 章號、cast→NULL 會算錯非只慢、HLL++ 直覺、隱式轉換/笛卡兒積白話、§3.10 AQE skew 門檻數字降為「細節」）。(2) **user 要求補「Spark 各種 join 模式介紹與比較」**（點名 BroadcastNestedLoopJoin）→ 在 §3.5 末尾加 5 種 join 物理模式對照表＋「非等值 join 退化成 BNLJ（O(n×m)）、看到先檢查少不少一個 `=`」；因章內 §3.6–3.12 交叉引用多，**刻意做成 §3.5 內小節而非新節**以免重編號斷鏈（spec §12.6）；過一輪聚焦 technical reviewer，修掉一個掛錯情境的 Databricks KB URL（該頁其實講 NOT IN，改以官方 Perf Tuning「依有無 equi-join key 分流」撐、O(n×m) 標為 nested loop 定義性成本）。(3) **體例微調**：precision footer 不再放「逐條查證記錄見 .reviews/…」指標（對齊 user 先前對 01 章的精簡）；已套用 02、03，後續各章比照。
- 2026-06-15：**採納 architecture round-1 建議，新增營運專章** `08-operating-data-pipelines.md`（手冊 9→10 章）。原因：營運線（終極目標）原散落 08 場景條列、資料品質驗證零覆蓋、特徵洩漏只一句。新章用 01 深度教冪等/回填/排程相依/資料品質驗證/時間點正確性/監控/表維護；原場景章變 09（回歸純索引）、速查變 10。優化線 01–07 不動。spec/index/plan 已同步重編號。
- 2026-06-16（第 04 章）：(1) 寫完第 04 章「Spark 設定（AQE-first）」，骨架＝風險梯度（先 AQE-first→低風險 SQL 層旋鈕→高風險資源層→多租戶）；雙審 triage 修（補 heap 定義、重畫記憶體圖去 §12 禁的 `←`、解釋 ÷1.1、補資源層在哪設/Livy 對 Hue 落地、gloss external shuffle service/SLA、SET 全稱補 static SQL config 例外）。reviewer 證實技術骨幹零事實錯誤。(2) **小檔（HDFS 小碎檔）主題分工定案**（user 問「適合寫在第 04 章嗎」）：完整「成因+解法」（partition 設計/目標檔案大小/compaction/寫出前 repartition·coalesce/bucketing）**歸第 05 章**（spec §5 已排定、全書交叉引用都指向那）；**第 04 章只承載「設定側成因」**——shuffle.partitions 設太大/AQE coalesce → 寫出小檔——並指向第 05 章。已在 §4.4 補一句點明因果。寫第 05 章時務必把小檔寫成主體、勿與 04 重複。
- 2026-06-19（第 06 章）：(1) 寫完第 06 章「引擎選用：Spark vs Hive on Tez vs Impala」（10 節、4 圖、PRIOR=01/02/05）。Step A 5 點查證全逐字對齊官方（見 Task 6 行）。(2) reviewer 判技術零事實錯＋交叉引用零失誤；唯一真缺陷＝**3 個 stale Cloudera URL**（blog 改 canonical `www.cloudera.com/blog/technical/…`、`runtime/7.1.0` 改 `docs-archive`；**ch05 §5.8 同款 7.1.0 連結一起修**）；另補 Impala 官方 MPP/原生執行出處、把『Impala 不容錯』標為 Cloudera 文的「對比隱含」、§6.8『記憶體導向』軟化（官方:多數查詢 CPU-bound、只特定運算 memory-intensive）。(3) reader 主訴：決策樹/取捨頁能照著選、**但一票基礎設施詞沒當場定義**（YARN/容器/MPP/HMS/daemon/DAG/full ACID/HQL/ad-hoc/OOM/fragment）→全部第一次承重就補一句 gloss；§6.3 決策表兩個對不上號的判準（『塞不塞得進記憶體』『複雜度 vs HQL 三欄不同軸』）改可操作；§6.4 fragment≈stage、Tez UI 出處修；§6.7 散文 ACID 矩陣改『表種×引擎×讀/寫』對照表＋表種小地圖；§6.9 去後設旁白『本節沒有新東西』＋修虛線圖例。(4) **user 追問『為什麼要寫 ACID 表、有跟沒有差在哪』→ 要求補基礎、不預設懂 ACID**：§6.7 改寫成兩個 `###`——「先補基礎」從『HDFS 表＝一堆檔案、只能整批覆寫、改不了藏在檔中間的一列』→『ACID 補改既有列的能力』→A/C/I/D 白話(各配畫面)→base+delta+compaction→『有 vs 沒有』對照→三情境(更正/合規刪除被遺忘權/維度表 upsert)→反面:只是整批換掉就不需要；「跨引擎限制」矩陣。(5) **§6.7 補審**（落實『審稿後加料要補審』）：reader-r2 抓 4 must-fix——『交易(transaction)』沒先白話定義(且跟銀行『一筆交易帳』撞名)、`commit` 黑盒、`MERGE` 首見沒交代、`full` 懸空——全補；cheap：C 一致加『帳兜不攏的中間態』畫面、刪無用的 micromanaged、維度表 gloss、拿掉未定義的『重述』。(6) 體例新點：深主題用 `###` 小節（§6.7 兩個）；環境細節『在哪選引擎＝看你從哪個入口下 SQL（Hue Impala/Hive 編輯器、spark-submit/impala-shell/beeline）』以 user 公司經驗補。
- 2026-06-18（第 05 章）：(1) 寫完第 05 章「儲存效率」，小檔成為主體（成因/徵兆/解法，SQL hint 為主、非 DataFrame API）。(2) **user 兩輪深問→大量加料**：repartition vs coalesce 怎麼選＋`coalesce` 的 n＝整段上游平行度上限（repartition 因 shuffle 切 stage 無此問題）、寫分區表要 `REPARTITION(分區欄位)`、CTE hint 放最外層 SELECT、`ANALYZE` 非自動要排程、external 表登記共用 Metastore 故 Hue/Impala 可查、dbt-spark 走 Spark CREATE TABLE 規則、補 DataNode/資料本地性小節。(3) **誠實更正**：原 §5.4 把「小碎檔傷 locality」講過頭，更正為「小碎檔主要傷 NameNode metadata/排程開銷，對 locality 影響間接偏弱；真正傷 locality 的是不可切分大檔」（reviewer 沒抓到是因為該段是審稿後才加的）。(4) **全書 `列式`→`欄式`**：繁中慣例欄=column、列=row，「列式」會被讀成 row（簡中相反），故統一欄式（columnar）；03、05 已改。(5) **體例**：審稿後再加料要補審→本章跑了兩輪雙審；r2 reviewer 翻 3.3.2 SQLConf 原始碼確認 cbo/joinReorder/statistics.size.autoUpdate 預設皆 false；r2 reader 抓出新內容過載→§5.4 改結論先行、§5.5 過載小節拆成「進一步」+「兩個實戰問題」兩個 `###`、補中間算式。(6) 小檔→NameNode 出處原掛錯 HDFS Architecture 頁→改 Cloudera 官方部落格《The Small Files Problem》(已驗證 URL)；引用原則 footer 明列「含 Cloudera 官方工程部落格 blog.cloudera.com」。
- 2026-06-19（補基礎 ＋ 營運線拆兩章）：(0) 寫完 **Hive/Parquet 補基礎**並 commit（`c90841d`）：ch01 §1.1 新 `###` 從零講 Hive Metastore（HDFS 存位元組／Metastore 記「有哪些表」）＋當場拆「Hive 表/Metastore」vs「Hive on Tez 引擎」兩義（接 Hue anchor）；ch05 §5.2 新 `###` Parquet/ORC 內部結構進階（row group→column chunk→page、排序讓 min/max 變窄→跳更多、字典編碼/向量化讀取/`parquet.block.size`↔HDFS block/bloom filter、ORC stripe 對應）；ch03 §3.3 Parquet gloss。雙審（reviewer ACCURATE、reader 三段 LANDS）＋triage 全修（Hue 連兩個 Hive、Impala/Tez/HDFS block/巢狀型別 gloss、ORC「row group」撞名、ORC stripe 改引 `hive-config.html`）。(1) **user 調序**：ch07 PySpark **暫緩**，先寫營運線。(2) **user 要求拆兩章**（brainstorming＋grill-me 後）：原單一營運專章因納入 dbt/Airflow/cron 三層落地＋完整特徵庫＋資料版本＋到處具體程式，**單章 2–3 倍長 → 拆「08 排程可靠性」＋「09 資料產品正確性」**；全書 9→**11 章**重編號（場景→10、速查→11）。spec §5（兩章大綱）/§6（**Airflow/dbt 官方文件納入權威來源**）/index/ch01–06 交叉引用（execution refs 留 §08、混合 refs 改 §08–09、場景 §09→§10、速查 §10→§11）/本 plan 皆已改。(3) **grill-me 鎖定四決策**：①grounding＝抽象→具體、每節「原則→落地」(dbt 為主/Airflow·cron/CDP SQL)，dbt=轉換層、Airflow=排程層觸發 dbt、**dbt 非排程器**；②排程器＝Airflow＋cron 雙軌；③特徵時間模型＝**(entity, snapshot_date) snapshot-partition** → §9.3 as-of＝「讀對分區、絕不讀未來分區」非大 join；④**compaction 表種＝external Parquet/ORC** → 重寫（`INSERT OVERWRITE` 併小檔）非 `ALTER COMPACT`（後者只對 ACID 表）。(4) **user 要點**：到處用具體程式；§9.5 資料版本＝build-version 標記欄／audit 帳本（**通用、不點名 recsys 框架**）／**雙層 `(snapshot_date, build_version)` 分區留歷史**（子分區互不覆蓋、讀時須挑版本＝釘死 or current-build view、取捨吃儲存/須清理呼應 §5.4）。低風險假設已 baking（partitionOverwriteMode 教 static/dynamic＋footgun、警報 generic、回填獨立 queue 指回 §04），保留第二輪 grilling（警報管道/static-dynamic 標準/queue 隔離/manifest 是否已存在）。
- 2026-06-21（第 08 章）：(1) 寫完第 08 章「營運（一）排程可靠性」（9 節、3 圖、commit `672b45a`）。Step A 查證 partitionOverwriteMode static/dynamic、dbt `insert_overwrite`、Airflow sensor/retries/catchup 全對官方。(2) reviewer 判核心 SQL/dbt 正確，抓到真缺陷全修：**D1 事實錯**——datasource 表 `INSERT OVERWRITE … PARTITION` 只覆寫符合分區是 **Spark 2.1** 起的老行為、我誤寫「3.3 起」(migration guide 在 2.0→2.1 段，3.2→3.3 段根本沒這條)；**D2/D3 Airflow 版本漂移**——`sla`／`dags backfill`／`ExternalTaskSensor` 匯入都是 2.x，而 `stable` 文件已 3.x → 全章標準化 **Airflow 2.x**(CDP 經 Cloudera Data Engineering 用 2.x)、§8.1 加版本提醒 callout(SLA→Deadline Alerts、`dags backfill`→`backfill create`、sensor 匯入移 providers)、連結釘 2.10；**D4/D5 死連結**——Spark 3.3.2 主站頁已 404 → Spark 連結改 `/docs/latest/`(對齊 ch01–06 慣例；version-exact 需 `archive.apache.org/dist/spark/docs/3.3.2/`)、修 INSERT 頁改名 slug(`…insert-table.html`)；D6 CDP compaction 出處精準化。(3) reader 全章 LANDS、3 gloss(Jinja/`var`·`is_incremental`/SLA)修。(4) **user 追問「有哪些常見維運錯誤適合放進來」→ 新增 §8.7 維運踩雷速查表**(症狀→成因→修法→去哪節)＋fold 三個頭號 footgun(positional INSERT 欄位錯位/§8.2、`current_date()` 不可重現/§8.4、cron 靜默失敗 `set -euo pipefail`/§8.5)；取捨→§8.8、帶走→§8.9。(5) **§8.7 加完補審**(落實審稿後加料補審)：reviewer-r2 ACCURATE 零真缺陷(positional INSERT 確按位置、`BY NAME`=SPARK-42750/Spark 3.5 故 3.3.2 無、`hdfs -touchz` 對、`set -euo pipefail` 對、每列指節對)、reader-r2 四段 LANDS、3 gloss(`BY NAME`/`logical_date`/`touchz`)補。(6) **遺留**：§8.7 末兩列前指 ch09 §9.3／§9.4，寫完 ch09 要回頭校錨點。
- 2026-06-21（第 09 章）：(1) 寫完第 09 章「營運（二）資料產品正確性」（7 節、3 圖、commit `8b5cdbc`），營運線 08+09 至此完成。Step A 查證 dbt 4 generic test/severity（error 預設·warn·error_if·warn_if 預設 `!=0`）、**Spark 3.3.2 無原生 AS OF/temporal join**（對 archive 3.3.2 join 頁逐字確認，只有 inner/cross/left[outer]/left semi/right[outer]/full[outer]/left anti）全對官方——後者是 §9.3「snapshot 模型只需等值 join、不需 as-of」立論關鍵。(2) reviewer 判零真缺陷、跨章引用全吻合；唯一可加強＝§9.2 `accepted_values: values:[0,1]` 對**整數** label 欄缺 `quote:false`（dbt 預設 `quote:true`→生 `not in ('0','1')`，Spark 隱式轉換多半仍跑對但不精確）→已補。(3) **reader（無 ML 背景人設）主訴＝一票術語第一次出現沒當場定義**：`資料產品`/`exit 0`/`cust_feature`(全章貫穿例沒交代 grain)/`grain`/`generic test`/`宣告式`/`entity`(突冒出來 vs cust_id)/`snapshot`/`特徵`/`label`/`training`/`serving`/`training–serving skew`→全部第一次承重處補白話錨點；另補「純 SQL 品質閘怎麼把『SQL 回了 N 列』變成『作業 exit 1』」的可操作 shell 橋（reader 卡在這步空白）。(4) **§9.3 as-of join 段落 reader 判過抽象、且像在回答讀者不會問的問題**→改寫成「先給結論(snapshot 模型只要一個 `=` 對齊時間)、as-of 細節降級成『對照組·進階旁註·不熟可跳過』blockquote」；保留 Spark 3.3 無原生語法的事實(reviewer 已驗、立論需要)。(5) 軟化 §9.1「四件**互不重疊**的事」→「四個面向、彼此互相支援」(reader 抓到 §9.4 引 §9.2、§9.5 引 §9.4，其實勾連)。(6) **基建教訓**：審稿 subagent 長跑(>13min)接連 socket 死兩次→reader 只審完 §9.1–9.3、§9.4–末由我用同一 reader lens 自審補（§9.4/9.5 概念負擔較低、品質驗證/版本有具體 DDL）；後續長章可考慮分段派 reader。(7) **遺留校正**：ch08 §8.7 收尾註「最後**三列**」實際只 2 列指 ch09(第 3-from-last 是 §8.6 維護題)→改「最後**兩列**」；→§9.3/§9.4 連結指向新建的 09 檔已可解析。
- 2026-06-21（第 09 章 §9.6 加碼）：(1) **user 要求資料版本化多補進階討論＋設計模式＋歷史回補**→先 AskUserQuestion 釐清(structure＝新增 §9.6 進階節 vs 原地擴 §9.5；深度＝全 5 塊 vs 聚焦)，user 選**新增 §9.6＋全 5 塊**(commit `0e6d2d4`)。(2) §9.5 留 baseline 三招＋forward-pointer；§9.6 五塊：①**bitemporal**(snapshot_date=valid time/build_version=transaction time，雙層分區=窮人版 bitemporal，回補=對舊 snapshot 寫新 build)②**回補×版本化走查**(沒版本化→舊版沒了+表模型悄悄不一致 vs 雙層分區新 build 子分區)③**「產出/發佈分離」**=active-version 指標(MAX 盲點:壞回補自動上線→append-only 發佈紀錄表+ROW_NUMBER current view+promote/rollback；資料版 blue/green、promote 人為把關呼應「模型發佈人工觸發」)④**保留清理政策**(留 N 版/pinned/時間制+audit 讓清理安全+回補清理陷阱)⑤**SCD2/Iceberg/MLflow 邊界**。(3) **Step A**:純 Hive/Parquet external 表 Spark 3.3 **不支援 UPDATE/DELETE/MERGE 改單列**(需 v2 SupportsRowLevelOperations=Iceberg/Delta)→promote 用 append-only 或整表 OVERWRITE(反而 §8.2-consistent)。(4) **reviewer-r2 聚焦補審 §9.6**(narrow scope→沒 socket 死)7 主張全對官方、零真缺陷；must-add B1=**回補 INSERT OVERWRITE 兩個分區值都要靜態**(build_version 留動態會清光該 snapshot 所有 build、毀掉要保護的舊版)→已補 ⚠️。(5) **可轉移**:深主題加碼先 AskUserQuestion 釐清 structure+depth 再寫；新節用 `## 9.6` 插 §9.5 後+取捨/帶走順延(重編號成本低、ch08/index 只引章不引子節)；聚焦補審(只審新節)比全章重審快又不易 socket 死。

---

## Task 0：Scaffold

**Files:**
- Create: `docs/handbooks/spark-tuning/index.md`（骨架）
- Create: `docs/handbooks/spark-tuning/.reviews/.gitkeep`

- [ ] **Step 1：建目錄與 .gitkeep**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
mkdir -p docs/handbooks/spark-tuning/.reviews
touch docs/handbooks/spark-tuning/.reviews/.gitkeep
```

- [ ] **Step 2：寫 `index.md` 骨架**（含 9 章導覽連結 placeholder、環境前提摘要、讀者假設、「如何使用本手冊」）。各章連結先指向預定檔名（Task 10 再補一句話地圖與完稿）。

- [ ] **Step 3：commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
git add docs/handbooks/spark-tuning/
git commit -m "docs(spark-handbook): scaffold 目錄與 index 骨架"
git checkout -- graphify-out/GRAPH_REPORT.md
```

---

## Task 1：第 01 章 — 心智模型：Spark 怎麼跑你的 SQL

**File:** Create `docs/handbooks/spark-tuning/01-how-spark-runs-your-sql.md`
**前置章（PRIOR_CHAPTERS）：** 無（第一章；只假設讀者會 SQL）

**內容大綱：**
- 從一條熟悉的 SQL 出發，講它在 Spark 裡發生什麼。
- cluster = driver + executors（在 YARN 上）；資料切成 partitions 平行處理。
- **執行層級：application → job（每 action 一個）→ stage（每 shuffle 切一刀）→ task（一 partition 一個）**；對應第 02 章 Spark UI 頁籤。
- lazy evaluation：transformation 累積、action 才觸發。
- query 生命週期：SQL → logical plan → Catalyst 優化 → physical plan → stages → tasks。
- **executor 的形狀與平行度取捨**：core 數＝同時能跑幾個 task；總平行度＝executors×cores；同 executor 的並行 task 共用其記憶體（core 多→各 task 記憶體少→spill）；fat vs thin executor 工作範例（給定 YARN 額度怎麼切）；instance 台數與多租戶。操作細節 forward 第 04 章。
- 窄依賴（map-like，便宜，不搬資料）vs 寬依賴 = shuffle（貴，跨網路重分佈）。
- shuffle 為什麼是頭號敵人（用 3000 萬筆帳務 `GROUP BY` 客戶舉例）。
- 主軸預告：多數優化＝減少/減輕 shuffle 與掃描量。

**概念圖（Mermaid）：** ① cluster（driver + N executors on YARN）；② application→job→stage→task 層級巢狀；③ SQL→logical→physical→stage→task 流程；④ narrow vs wide（shuffle）對照；⑤（可選）fat vs thin executor 切法對照。

**須查證重點（reviewer 會查）：** application/job/stage/task 層級語意（job 由 action 觸發、stage 由 shuffle 切、task↔partition 一對一）；窄/寬依賴定義；shuffle 對應 physical plan 的 `Exchange`；lazy（transformation vs action）；**一個 core 同時跑一個 task、總平行度＝executors×cores**；`spark.executor.memoryOverhead` 預設；「每 executor ~4–5 core」這類 heuristic 需找權威來源（High Performance Spark / Cloudera 官方文件，非部落格），找不到就改用推理＋hedge。出處：Spark 3.3 官方文件 + Definitive Guide / High Performance Spark。

- [ ] Step A：查證並記錄關鍵事實
- [ ] Step B：寫 `01-how-spark-runs-your-sql.md` 草稿（大綱 + 3 張 Mermaid 圖 + 章末導覽）
- [ ] Step C：並行派 reviewer + reader（`{CHAPTER}=01-how-spark-runs-your-sql`，`{PRIOR_CHAPTERS}=無`）
- [ ] Step D：triage 三級並修
- [ ] Step E：送使用者審，納回饋（必要時記 Direction Log）
- [ ] Step F：Progress Tracker 打勾 + commit + graphify reset

---

## Task 2：第 02 章 — 用 Spark UI 與 EXPLAIN 找瓶頸

**File:** Create `docs/handbooks/spark-tuning/02-diagnose-with-spark-ui.md`
**前置章：** 01

**內容大綱：**
- 心法：先量再調，不憑感覺。
- 在 CDP 上怎麼開 Spark UI（History Server / Cloudera Manager 入口）。
- `EXPLAIN` / `EXPLAIN FORMATTED` 讀重點：找 `Exchange`(=shuffle)、`BroadcastHashJoin` vs `SortMergeJoin`、Scan 的 partition filter 是否生效。
- Spark UI 各頁籤看什麼：Jobs / Stages / SQL / Executors / Storage；SQL-first 的人主看 SQL 頁籤 query plan + Stages 的 task 時間/資料分佈。
- 認症狀：shuffle 過大、skew（少數 task 特別久）、spill（記憶體不足落磁碟）、小檔/掃太多（partition 沒裁到）。
- 「症狀 → 看哪裡 → 翻到哪章」對照表（呼應第 09 章）。

**概念圖（Mermaid）：** ① Spark UI 頁籤地圖；② 一個 stage 內 task 時間分佈（正常 vs skew）示意；③ 症狀→對策決策流。

**須查證重點：** Spark 3.3 web UI 頁籤組成與 SQL tab 的 query plan 視圖；`EXPLAIN FORMATTED`（3.0+）；spill 指標欄位（memory/disk spill）；skew 在 UI 的呈現。出處：Spark 3.3「Web UI」「SQL ref – EXPLAIN」官方文件。

- [ ] Step A–F（同標準循環；`{CHAPTER}=02-diagnose-with-spark-ui`，`{PRIOR_CHAPTERS}=01`）

---

## Task 3：第 03 章 — SQL 寫法優化

**File:** Create `docs/handbooks/spark-tuning/03-sql-tuning.md`
**前置章：** 01, 02

**內容大綱：**
- 只讀需要的：partition 裁剪（`WHERE` 帶 partition column）、projection（別 `SELECT *`）、predicate pushdown。
- join 策略：broadcast（小表，省 shuffle）vs sort-merge（大表×大表）；AQE 自動選，但要喂對統計、別寫法擋住 pushdown。
- 手動 `/*+ BROADCAST(t) */` 何時用、threshold 多少。
- join key 型別一致（型別不符→隱式轉型→pushdown 失效）。
- 避免笛卡兒積 / 一對多爆量 join（連結 `aligning-on-table-joins` 精神）。
- `GROUP BY` / `DISTINCT` / `COUNT(DISTINCT)` 成本與 `approx_count_distinct` 取捨。
- window function 成本（每個 `PARTITION BY` 一次 shuffle）。
- 處理 skew：salting、AQE skew join、熱點 key 分流。
- 每招格式：原理 → SQL before/after → Spark UI 看到什麼變化 → 取捨。

**概念圖（Mermaid）：** ① partition 裁剪（掃全表 vs 只掃命中分區）；② broadcast vs sort-merge join；③ skew 與 salting。

**須查證重點：** `spark.sql.autoBroadcastJoinThreshold` 預設（10MB）；broadcast hint 語法；AQE skew join（`spark.sql.adaptive.skewJoin.*`）；`approx_count_distinct` 為 HyperLogLog 近似；predicate/projection pushdown 條件。出處：Spark 3.3 SQL Performance Tuning + SQL ref（hints / functions）。

- [ ] Step A–F（`{CHAPTER}=03-sql-tuning`，`{PRIOR_CHAPTERS}=01, 02`）

---

## Task 4：第 04 章 — Spark 設定（AQE-first）

**File:** Create `docs/handbooks/spark-tuning/04-spark-config.md`
**前置章：** 01, 02, 03

**內容大綱：**
- 心法：3.3 AQE 預設開，先別亂調靜態旋鈕。AQE 自動做：合併 shuffle 分區、動態切 broadcast、處理 skew join。
- 確認 AQE 開著（`spark.sql.adaptive.enabled`）。
- 仍要懂的少數旋鈕：`spark.sql.shuffle.partitions`（AQE 下角色變了）、`spark.sql.autoBroadcastJoinThreshold`、executor memory/cores/數量（CDP/YARN 上怎麼給）、`spark.sql.files.maxPartitionBytes`、dynamic allocation。
- 怎麼在 Hue/notebook 用 `SET`。
- 記憶體模型一句話：executor memory 分 execution/storage，spill 是不夠的徵兆。
- 取捨：更多記憶體/核心 vs 叢集併發；broadcast threshold 調大 vs driver OOM。
- 強調：對 SQL-first 的人，調 SQL 寫法 + 喂統計多半比硬調 config 有效。

**概念圖（Mermaid）：** ① AQE 自動做的三件事；② executor 記憶體區塊（execution/storage/overhead）。

**須查證重點：** `spark.sql.adaptive.enabled` 預設 true（3.2+）；`spark.sql.shuffle.partitions` 預設 200；`autoBroadcastJoinThreshold` 10MB；`spark.sql.files.maxPartitionBytes` 預設 128MB；AQE coalesce partitions config；dynamic allocation 開關；unified memory（execution/storage）。出處：Spark 3.3 Configuration + Performance Tuning + Tuning guide。

- [ ] Step A–F（`{CHAPTER}=04-spark-config`，`{PRIOR_CHAPTERS}=01, 02, 03`）

---

## Task 5：第 05 章 — 儲存效率

**File:** Create `docs/handbooks/spark-tuning/05-storage-efficiency.md`
**前置章：** 01, 03

**內容大綱：**
- 檔案格式：Parquet/ORC 為何比 text/CSV 快又省（列式、壓縮、謂詞下推、只讀需要的欄）。
- 壓縮：snappy（快）vs zstd/gzip（小）取捨。
- partition 設計：選對 partition column（帳務按 month/date）、不要過度分割（1000 萬客戶別按 `cust_id` 分割→小檔災難）；目標檔案大小 ~128MB–1GB。
- 小檔問題：成因、徵兆、解法（寫出前 `repartition`/`coalesce`、定期 compaction）。
- bucketing：何時有用、Hive 3 注意。
- 統計：`ANALYZE TABLE ... COMPUTE STATISTICS` 為何關鍵（AQE/CBO 靠它）、怎麼跑。
- Hive 3.x ACID/transactional table 提醒（delta 檔、compaction）。
- 取捨：分割細→掃描省但小檔/metadata 爆；壓縮強→省儲存但耗 CPU。

**概念圖（Mermaid）：** ① 列式 vs 列存只讀需要欄；② partition 裁剪在磁碟層；③ 小檔成因（過度分割 / 太多 shuffle 分區寫出）。

**須查證重點：** Parquet/ORC 列式 + 謂詞下推；Spark parquet 預設壓縮 snappy；`ANALYZE TABLE ... COMPUTE STATISTICS` 語法 + `spark.sql.cbo.enabled`（預設 false！需確認）；bucketing 行為；Hive 3 ACID/transactional 預設。出處：Spark 3.3 SQL data sources / Performance Tuning；Cloudera CDP / Hive 3 官方文件。

- [ ] Step A–F（`{CHAPTER}=05-storage-efficiency`，`{PRIOR_CHAPTERS}=01, 03`）

---

## Task 6：第 06 章 — 引擎選用：Spark vs Hive/Tez vs Impala

**File:** Create `docs/handbooks/spark-tuning/06-engine-selection.md`
**前置章：** 01, 02, 05

**內容大綱：**
- 三引擎定位：Spark SQL（大型 ETL/複雜轉換/與 ML 整合）、Hive on Tez（穩定批次/既有 HQL）、Impala（低延遲互動 ad-hoc/BI）。
- 決策表：資料量 / 延遲需求 / 併發 / 查詢複雜度 / 是否寫回大表。
- 各引擎診斷工具：Spark UI / Tez UI(Hue) / Impala query profile。
- CDP 實務：同一 Hive table 三引擎都讀；Impala metadata `INVALIDATE`/`REFRESH`、ACID 表 Impala 支援限制。
- 取捨：Impala 快但吃記憶體、不適合超大 shuffle；Spark 通用但啟動/排程成本高。

**概念圖（Mermaid）：** 引擎決策樹（資料量/延遲/併發/複雜度 → Spark / Hive-Tez / Impala）。

**須查證重點：** Impala `INVALIDATE METADATA` vs `REFRESH` 語意；Impala 對 Hive ACID/transactional 表的支援限制；三引擎定位（vendor 文件）。出處：Cloudera CDP 官方文件（Impala / Hive / Spark on CDP）。

- [ ] Step A–F（`{CHAPTER}=06-engine-selection`，`{PRIOR_CHAPTERS}=01, 02, 05`）

---

## Task 7：第 07 章 — 進階：何時與如何改用 PySpark DataFrame API

**File:** Create `docs/handbooks/spark-tuning/07-pyspark-dataframe-api.md`
**前置章：** 01, 03, 04

**內容大綱：**
- 何時值得從 SQL 改用 DataFrame API：複雜可重用邏輯、要單元測試、動態組查詢、與 ML pipeline/Python 生態整合（本 repo pipeline 即一例）。
- SQL ↔ DataFrame 心智對照（同一 query 兩種寫法並排）；破除「API 比 SQL 快/慢」迷思＝底層同一個 Catalyst、效能等價。
- API 特有效能注意：`cache()`/`persist()` 何時用與記憶體取捨；避免 `collect()`/`toPandas()` 拉回 driver（OOM）；`repartition` vs `coalesce`；UDF 成本（呼應本 repo 生產禁 UDF）；lazy 與 action。
- 取捨：可測試/可維護 vs 純 SQL 簡潔。
- 範圍界線：只到 DataFrame API，不碰 RDD。

**概念圖（Mermaid）：** SQL 與 DataFrame 都進同一個 Catalyst → 同一 physical plan（效能等價）。

**須查證重點：** SQL 與 DataFrame 編譯到相同 logical/physical plan（Catalyst）；`cache`/`persist` storage levels 與預設；`collect`/`toPandas` 把資料拉回 driver；`coalesce` vs `repartition`（是否觸發 shuffle）；Python UDF 序列化成本。出處：Spark 3.3 官方文件 + Definitive Guide。

- [ ] Step A–F（`{CHAPTER}=07-pyspark-dataframe-api`，`{PRIOR_CHAPTERS}=01, 03, 04`）

---

## Task 8：第 08 章 — 營運（一）：可靠地把排程跑起來

**File:** Create `docs/handbooks/spark-tuning/08-operating-pipelines.md`
**前置章：** 01, 03, 04, 05
**完整大綱見 spec §5「08-operating-pipelines.md」**（三層落地模型 dbt/Airflow/cron；§8.1 地圖＋三層模型→§8.2 冪等與可重跑→§8.3 排程相依→§8.4 回填→§8.5 監控與退化→§8.6 檔案與統計維護→§8.7 取捨→§8.8 帶走）。每節「先通用原則 → 再落地（dbt 為主／Airflow·cron／CDP SQL）」分節遞進。

**具體程式（user 要求到處落地）：** `INSERT OVERWRITE … PARTITION` ＋ `partitionOverwriteMode` static/dynamic（講 footgun）；dbt incremental `insert_overwrite` 策略；Airflow `BashOperator(dbt run)` ＋ `ExternalTaskSensor`；cron sentinel 模式；compaction-by-rewrite（external 表用 `INSERT OVERWRITE` 併小檔，**非** `ALTER COMPACT`）。

**概念圖（Mermaid）：** ① 冪等覆寫 vs append（重跑結果對照）；② 排程相依 gate（Airflow sensor vs cron 時間差）。

**須查證重點：** Spark `INSERT OVERWRITE TABLE … PARTITION` 語意、`spark.sql.sources.partitionOverwriteMode`（預設 static / dynamic 行為）、`ANALYZE TABLE`（Spark 3.3）；external 表 compaction＝重寫（無 `ALTER COMPACT`，對 §5.5）；**Airflow 官方**（`BashOperator`、`ExternalTaskSensor`、`retries`/`retry_delay`、`catchup`/backfill）；**dbt 官方**（incremental ＋ dbt-spark `insert_overwrite` 策略、`ref()`/DAG、「dbt 非排程器」）。出處：Spark 3.3 SQL ref/Configuration、Cloudera CDP、airflow.apache.org、docs.getdbt.com。

- [ ] Step A–F（`{CHAPTER}=08-operating-pipelines`，`{PRIOR_CHAPTERS}=01, 03, 04, 05`）

---

## Task 8b：第 09 章 — 營運（二）：讓資料產品可信

**File:** Create `docs/handbooks/spark-tuning/09-data-product-correctness.md`
**前置章：** 01, 03, 05, 08
**完整大綱見 spec §5「09-data-product-correctness.md」**（§9.1 地圖→§9.2 資料品質驗證→§9.3 時間點正確性/特徵洩漏→§9.4 共用特徵庫多消費者契約→§9.5 資料版本與可重現性→§9.6 取捨→§9.7 帶走）。沿用 §08 三層落地模型。

**具體程式：** dbt tests YAML（not_null/unique/accepted_values/relationships ＋ severity warn/error）＋純 Spark SQL 品質檢查；`WHERE snapshot_date=…`（讀對分區）＋誤 join 未來分區反例；§9.5 雙層 `(snapshot_date, build_version)` 分區 DDL ＋ `INSERT OVERWRITE` 子分區 ＋ current-build view（build-version 標記欄、audit 帳本＝**通用模式、不點名框架**）。

**概念圖（Mermaid）：** ③ 時間點正確性切線（snapshot 分區，只能讀左邊／不讀未來）。

**須查證重點：** **dbt 官方** generic tests ＋ test severity（warn/error）；as-of join 在 Spark 3.3 無原生語法（snapshot-partition 模型不需要，只當「若特徵帶任意 effective_date 才需要」補充）；schema 演進「只加不改」（對 §5.8）；雙層分區留歷史的取捨（吃儲存/須挑版本/清理政策呼應 §5.4 高基數分區）。出處：docs.getdbt.com、Spark 3.3 SQL ref、Cloudera CDP。

- [ ] Step A–F（`{CHAPTER}=09-data-product-correctness`，`{PRIOR_CHAPTERS}=01, 03, 05, 08`）

---

## Task 9：第 10 章 — 場景對應（索引）

**File:** Create `docs/handbooks/spark-tuning/10-scenario-playbooks.md`
**前置章：** 01–09

**內容大綱（純索引/指路，不重教概念）：**
- 場景 1 ad-hoc：先 Impala/小樣本、partition 裁剪、`LIMIT`、別 `SELECT *`、別全表 `COUNT(DISTINCT)` → 引 §02/§03/§06。
- 場景 2 排程產表：冪等/可重跑、控檔大小、資源穩 → 引 §08（排程可靠性）+ §09（品質/版本）+ §03/§04/§05。
- 場景 3 特徵運算/特徵庫：寬表多 join/window、易 skew、時間點正確性 → 引 §09（時間點/特徵庫契約）+ §03/§05/§07/§08。
- 每場景：典型流程 → 對應章節清單 → 該情境最常踩的雷。

**概念圖（Mermaid）：** 三場景各一張「典型流程 → 對應章節」對照（或一張總表）。

**須查證重點：** 純綜合，無新技術主張；只需確認跨章引用指對、與各章一致。

- [ ] Step A–F（`{CHAPTER}=10-scenario-playbooks`，`{PRIOR_CHAPTERS}=01, 02, 03, 04, 05, 06, 07, 08, 09`）

---

## Task 10：第 11 章 — 速查與名詞表

**File:** Create `docs/handbooks/spark-tuning/11-cheatsheet-and-glossary.md`
**前置章：** 01–10

**內容大綱：**
- 取捨速查表：時間 ↔ 記憶體 ↔ 儲存（每個手段三維度影響）。
- config 速查表（名稱 / Spark 3.3 預設 / 何時調 / 風險）。
- 症狀→對策速查（呼應第 02 章）。
- 名詞對照表（partition/shuffle/executor/skew/spill/broadcast/AQE/CBO/冪等/backfill/時間點正確性… 中英對照 + 一句話）。

**概念圖（Mermaid）：** 視需要，可不放（本章以表格為主）。

**須查證重點：** config 速查表每一列的預設值要與第 04/03/05 章一致且對齊 Spark 3.3；名詞定義精確。

- [ ] Step A–F（`{CHAPTER}=10-cheatsheet-and-glossary`，`{PRIOR_CHAPTERS}=01–09`）

---

## Task 11：完稿 index.md

**File:** Modify `docs/handbooks/spark-tuning/index.md`

- [ ] **Step 1：** 補各章「一句話地圖」、確認 10 章連結正確、補「如何使用本手冊」（依場景/問題快速導向哪一章）、兩條學習路線、環境前提摘要、讀者假設。
- [ ] **Step 2：** 全書跨章引用檢查（grep「第 NN 章」「見 §」指對沒）。
- [ ] **Step 3：** commit + graphify reset。

---

## Task 12：轉 HTML（離線可看）

**Files:** Create `docs/handbooks/spark-tuning/*.html`（與各 `.md` 成對）；可建一支建置 script（如 `scripts/` 下，若沿用既有手冊 HTML 產法則沿用之）。

- [ ] **Step 1：** 確認既有手冊 HTML 的產法（看 `docs/handbooks/*_offline.html` 怎麼來的；沿用同套樣式/工具，含 anchor 目錄、右下角浮動回頂鈕）。
- [ ] **Step 2：** 把 10 章 + index 的 `.md` 轉 HTML，**內嵌 mermaid.js**（vendored/inline，不靠 CDN），渲染 ` ```mermaid ` 區塊。
- [ ] **Step 3：** 離線驗證：斷網 / `file://` 開啟，確認所有 Mermaid 圖正常渲染、跨章連結可走、回頂鈕可用。
- [ ] **Step 4：** commit + graphify reset。

---

## Task 13：全書最終 pass（reader 通讀 + architecture 架構審查）+ 修正

- [ ] **Step 1：** 並行派兩個 subagent：①reader（人設同 §reader 模板）從 index 一路讀到第 10 章，檢查全書一致性、導覽是否好走、有無跨章脈絡斷裂，即時寫日誌到 `.reviews/whole-book__reader.md`；②architecture（角色 C 模板）做最後一輪完整度/架構/深度稽核（能力地圖是否全覆蓋終極能力目標），寫 `.reviews/_architecture__round-{最終輪次}.md`。
- [ ] **Step 2：** triage 三級並修。
- [ ] **Step 3：** 送使用者最終確認。
- [ ] **Step 4：** commit + graphify reset；（使用者要求時）用 `superpowers:finishing-a-development-branch` 收尾（PR/merge）。

---

## Self-Review（撰計畫後自查，已做）

- **Spec 覆蓋**：spec §5 的 index + 10 章 → Task 0/11 + Task 1–10 全覆蓋；§8 交付流程（先 .md 後 HTML）→ Task 1–11 為 .md、Task 12 為 HTML；§10 審稿 subagent（A/B 逐章、C 里程碑）→ 每章 Step C/D + Task 13；§9 跨 session 機制 → 本檔 Progress Tracker + Direction Log + commit 節奏；§7 離線 mermaid → Task 12。
- **Placeholder 掃描**：每章 task 都有具體內容大綱、概念圖、須查證重點、前置章；subagent prompt 為完整可貼模板（僅 `{CHAPTER}`/`{PRIOR_CHAPTERS}` 為刻意的填空）。
- **一致性**：章節編號 01–10 與檔名、PRIOR_CHAPTERS、index 連結一致；config 預設值集中在 04/03/05 並由 10 速查表彙整，避免各章各說。
