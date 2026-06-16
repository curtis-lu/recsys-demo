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
- **每次 commit 後**：`git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook checkout -- graphify-out/GRAPH_REPORT.md`（post-commit hook 會弄髒它，不還原會擋住後續 checkout/merge）。

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

> **▶ 目前進度 / 下一步（/compact 後先讀這裡）**：第 01、02、03 章已寫完、雙 subagent 審＋triage 修＋user glance。第 03 章另按 user 要求在 §3.5 加「Spark 5 種 join 物理模式對照表（含 BroadcastNestedLoopJoin）」並過一輪聚焦 reviewer。**下一步＝Task 4 寫第 04 章「Spark 設定（AQE-first）」**，照 spec §12 基準（含第 02 章兩個教訓：『能上 mock 面板／貫穿範例就上』『環境細節以 user 公司經驗為準』，見 Direction Log 2026-06-16）＋ §6 來源慣例 ＋ §10 審稿流程（worktree `.worktrees/spark-handbook`、分支 `feat/spark-tuning-handbook`）。**體例微調**：precision footer 不再放「逐條查證記錄見 .reviews/…」指標（對齊 01；02/03 已移除，後續各章比照）。

- [x] Task 0：scaffold（目錄 + index 骨架 + .reviews/）
- [x] Task 1：`01-how-spark-runs-your-sql.md`（心智模型）— 10 節、三輪雙 subagent 審＋修（含 partition 來源、application/job/stage/task 層級、executor 取捨、shuffle 三麻煩、端到端範例、Spark vs Hive-MR）；待 user 最終 glance
- [x] Task 2：`02-diagnose-with-spark-ui.md`（Spark UI 診斷）— 10 節、雙 subagent 審＋triage 修＋user 回饋修；**§2.2 改為「一律從 History Server 進」（completed/incomplete 清單）**（user 公司經驗：還在跑的 app 在 incomplete 查得到，已對 monitoring.html 逐字查證；同時解掉原 live UI 入口方向錯誤＋RM/AM 術語沒解釋）、升級 AQE `isFinalPlan` 來源（Databricks AQE 文＋SPARK-33850）、**加 mock Summary Metrics 面板＋§2.8「一條慢查詢的驗屍」貫穿範例**、EXPLAIN 示意輸出、percentile 白話；待 user 再 glance
- [x] Task 3：`03-sql-tuning.md`（SQL 寫法）— 12 節、4 圖、雙 subagent 審＋triage 修；骨架＝「少讀（partition 裁剪／projection／pushdown）＋少搬（broadcast vs sort-merge、手動 hint、join 陷阱型別/爆量、聚合 approx_count_distinct、window、skew AQE→salting→分流）」＋§3.11 貫穿範例；triage 修：型別前提矛盾、Definitive Guide 章號 Ch.8→9、cast→NULL 會算錯、HLL++ 直覺、術語白話、§3.10 門檻降為細節；**user 加碼**：§3.5 加「5 種 join 物理模式對照表（含 BNLJ）」過聚焦 reviewer（修掉掛錯的 Databricks KB URL）；02/03 precision footer 的 .reviews 指標已移除對齊 01
- [ ] Task 4：`04-spark-config.md`（Spark 設定 AQE-first）
- [ ] Task 5：`05-storage-efficiency.md`（儲存效率）
- [ ] Task 6：`06-engine-selection.md`（引擎選用）
- [ ] Task 7：`07-pyspark-dataframe-api.md`（DataFrame API 進階）
- [ ] Task 8：`08-operating-data-pipelines.md`（**營運專章**：冪等/回填/排程相依/資料品質/時間點正確性/監控/表維護）← architecture C round-1 補
- [ ] Task 9：`09-scenario-playbooks.md`（場景對應＝索引）
- [ ] Task 10：`10-cheatsheet-and-glossary.md`（速查與名詞表）
- [ ] Task 11：完稿 `index.md`（導覽/連結/如何使用/兩條學習路線）
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

## Task 8：第 08 章 — 營運資料排程與資料產品（營運專章）

**File:** Create `docs/handbooks/spark-tuning/08-operating-data-pipelines.md`
**前置章：** 01, 03, 05（尤其 05 儲存）

**內容大綱：**
- 定位：把產出的表/特徵當成**要長期營運的服務**，正確/可靠/可維護優先於快。
- 冪等與可重跑：`INSERT OVERWRITE ... PARTITION` + dynamic partition overwrite（覆寫單一 partition），對照 append 重跑造成重複。
- 回填（backfill）：按 partition 分批、控資源、可中斷續跑。
- 排程相依與資料就緒：上游沒齊不跑下游；partition 存在/列數 gate。
- 資料品質驗證（補 C12，§11 明文要求）：列數量級、null 比例、key 唯一性、值域、對昨日漂移；不過擋下游、發警報。
- 時間點正確性 / 特徵洩漏（C11，特徵庫命門）：只能用 snapshot date 之前資料；常見洩漏（用到未來/label 期間）；as-of join 概念。
- 監控與退化：Spark UI/歷史看時間/資料量/shuffle 隨時間惡化。
- 表生命週期維護：compaction、重算 `ANALYZE`、清過期 partition、schema 演進不打爛下游（呼應 §05）。
- 多人共用資料產品：schema/SLA 契約、版本、文件。
- 取捨：冪等覆寫 vs append；驗證嚴格 vs 誤擋；回填一次到位 vs 分批。

**概念圖（Mermaid）：** ① 冪等覆寫 vs append（重跑後結果對照）；② 排程相依 gate（上游就緒才跑下游）；③ 時間點正確性（snapshot date 切線，只能用左邊資料）。

**須查證重點：** `INSERT OVERWRITE TABLE ... PARTITION` 語意；`spark.sql.sources.partitionOverwriteMode`（預設 static、dynamic 行為）；`ANALYZE TABLE` 重算統計；Hive 3 ACID compaction（major/minor）。對齊 Spark 3.3 / Hive 3.1.3 CDP。出處：Spark 3.3 SQL ref（INSERT OVERWRITE）、Configuration、Cloudera CDP/Hive 文件。

- [ ] Step A–F（`{CHAPTER}=08-operating-data-pipelines`，`{PRIOR_CHAPTERS}=01, 03, 05`）

---

## Task 9：第 09 章 — 場景對應（索引）

**File:** Create `docs/handbooks/spark-tuning/09-scenario-playbooks.md`
**前置章：** 01–08

**內容大綱（純索引/指路，不重教概念）：**
- 場景 1 ad-hoc：先 Impala/小樣本、partition 裁剪、`LIMIT`、別 `SELECT *`、別全表 `COUNT(DISTINCT)` → 引 §02/§03/§06。
- 場景 2 排程產表：冪等/可重跑、控檔大小、資源穩 → 引 §08（營運）+ §03/§04/§05。
- 場景 3 特徵運算/特徵庫：寬表多 join/window、易 skew、時間點正確性 → 引 §08 + §03/§05/§07。
- 每場景：典型流程 → 對應章節清單 → 該情境最常踩的雷。

**概念圖（Mermaid）：** 三場景各一張「典型流程 → 對應章節」對照（或一張總表）。

**須查證重點：** 純綜合，無新技術主張；只需確認跨章引用指對、與各章一致。

- [ ] Step A–F（`{CHAPTER}=09-scenario-playbooks`，`{PRIOR_CHAPTERS}=01, 02, 03, 04, 05, 06, 07, 08`）

---

## Task 10：第 10 章 — 速查與名詞表

**File:** Create `docs/handbooks/spark-tuning/10-cheatsheet-and-glossary.md`
**前置章：** 01–09

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
