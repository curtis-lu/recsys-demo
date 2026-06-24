# 《Spark 優化參考手冊》Round-2 全書重審與修訂 — 實作計畫

> **For agentic workers:** 用 `superpowers:subagent-driven-development`（推薦）或 `superpowers:executing-plans` 逐 task 執行。步驟用 `- [ ]` checkbox 追蹤。
> **設計來源（單一真實來源）**：`docs/superpowers/specs/2026-06-22-spark-handbook-round2-review-design.md`。方向調整先改該 spec。
> **原始內容基準**：`docs/superpowers/specs/2026-06-14-spark-tuning-handbook-design.md`（各章內容大綱仍以它為準）。

**Goal：** 把已成稿的 8 章＋index 依新 12 章骨架重排、補齊 reverse ETL（G1）與 training–serving（G2）缺口、把 ch02 升級為手把手診斷核心（D-UI）、逐章三向深審拉齊五面向，並補完 PySpark/場景/速查三章。

**Architecture：** 純文件任務，在 worktree `.worktrees/spark-handbook`（分支 `feat/spark-tuning-handbook`）進行，**不跑 python/Spark/pytest**。每個「寫/改章」的測試閘＝審稿 subagent（技術 R1／初階 reader R2／進階 reader R3／全書 C／教學 P）＋使用者人工審。**架構先行**：先落結構（Phase 1）再逐章深審（Phase 2+）。

**Tech Stack：** Markdown + Mermaid；HTML 階段內嵌 mermaid.js。權威來源：Spark 3.3 官方文件、Cloudera CDP 官方文件、Apache Airflow / dbt / Iceberg 官方文件、《Learning Spark 2nd》《Spark: The Definitive Guide》《High Performance Spark》。

---

## 路徑與環境（每個 task 都適用）

- 工作根目錄：`/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook`
- 手冊輸出目錄：`docs/handbooks/spark-tuning/`
- 審稿日誌目錄：`docs/handbooks/spark-tuning/.reviews/`
- 寫作規範：`docs/handbooks/handbook-writing-guide.md`（具體數字落地、結論誠實、不洩漏鷹架、§11 reader 清單、§12 reviewer 清單）
- **給使用者的檔案路徑一律帶 `.worktrees/spark-handbook/` 前綴。**
- git 一律 `git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook ...` 或先 `cd` 該 worktree root。
- **commit 後不必還原 `graphify-out/GRAPH_REPORT.md`**（已 untracked，commit `61ee9ac`）；舊版 `git checkout -- graphify-out/GRAPH_REPORT.md` 已過時，別再串。
- 繁體中文；專有名詞英文原文（SparkSession 不譯）；範例以 SQL 為主（DataFrame API 僅新 10 章）。

## 寫作慣例（沿用 01–09 已立）

- 一節一概念；每抽象主張用具體數字/銀行資料量（客戶 ~1000 萬、信用卡帳務 ~3000 萬/月）落地。
- 每 config 主張附 Spark 3.3 預設值＋出處；每「做 X→變快」先確認因果方向。
- 每章：H1 → 「本章前提（讀者已讀哪些章＋可假設懂什麼）」→ 內文 → 章末「一句話帶走」＋「上一章/下一章」導覽。
- 概念圖用 Mermaid；每重要概念段末 `📚 來源` footer、章末「資料來源與精確度說明」。
- 深主題用 `### 進階：`／`### 補充：` 標籤分層（門檻篩讀者），不另開節以免重編號。
- **審稿後若再加料一定要補審。**
- **subagent 長跑（>13 分）易 socket 斷線 → 窄範圍 dispatch（單章×單角色）＋即時 append live log，斷線讀 partial log 接續或自審補。**

---

## 檔案結構（新 12 章骨架；本計畫會動到的檔案與責任）

| 檔案 | 動作 | 責任 |
|---|---|---|
| `index.md` | 改寫 | 12 章導覽表、如何使用、兩條主線（含 reverse ETL 終點）、可選入口圖 |
| `01-how-spark-runs-your-sql.md` | 微修 | §1.6 AQE 縮寫鋪陳節奏（Pedagogy f-4） |
| `02-diagnose-with-spark-ui.md` | **大改（D-UI）** | 手把手診斷核心：逐頁籤 mock 面板＋症狀→面板→章路由＋三張 checklist |
| `03-sql-tuning.md` | 深審微修 | 交叉引用改號；技術/雙 reader triage |
| `04-spark-config.md` | 深審微修 | 同上 |
| `05-storage-efficiency.md` | 深審＋拆節 | §5.5 過載拆節（Pedagogy f-5）；交叉引用改號 |
| `06-engine-selection.md` | 深審微修 | 章末導覽鷹架修；交叉引用改號 |
| `07-operating-pipelines.md` | **git mv（自 08）＋改** | 重編號＋內部 §8.x→§7.x；補 capstone 端到端走查節（Pedagogy f-1）；導覽鷹架修 |
| `08-data-product-correctness.md` | **git mv（自 09）＋改** | 重編號＋內部 §9.x→§8.x；補 training–serving `###`（G2） |
| `09-reverse-etl.md` | **新增** | 營運（三）reverse ETL 回業務系統（G1） |
| `10-pyspark-dataframe-api.md` | **新增** | 現 07 暫緩內容；何時/如何從 SQL 改用 DataFrame API |
| `11-scenario-playbooks.md` | **新增** | 場景索引（三情境串各章＋標註初/進階主線） |
| `12-cheatsheet-and-glossary.md` | **新增** | 速查與名詞表 |

---

## 審稿 subagent prompt 模板

### R1 — Technical reviewer（subagent_type: general-purpose）
沿用原 plan（`2026-06-14-...-plan.md`）的 reviewer 模板：版本釘死 Spark 3.3.2/Hive 3.1.3/CDP 7.1.9、只認官方來源、逐條 ✅/❌/⚠️ 附出處、即時 append `.reviews/{CHAPTER}__reviewer.md`、三級彙整。**填 `{CHAPTER}`。**

### R2 — Reader-初階（subagent_type: general-purpose）
```
你是《Spark 優化參考手冊》的「初階資料分析師」目標讀者審稿人。全程繁體中文（不可簡體）。
[人設] 你會寫 SQL、懂銀行業務資料，但沒學過分散式系統，不知道 shuffle/executor/partition 底層怎麼運作；你的日常＝在 Hue 用 Impala 做 ad-hoc 分析、用 Spark 跑定期排程「出行銷名單給 PM」、建模時產出特徵表。看到沒解釋的英文術語會卡。嚴格扮演，不可因你其實懂 Spark 就放水。
[目標] 從頭讀到尾，標出讀不懂/卡關/缺脈絡/太抽象/不自明處；並回答「我這份工作（ad-hoc→排程→出名單給 PM→產特徵表）照這章做得了嗎」。
[素材] 待審：{CHAPTER_PATH}；你已讀過可假設懂的前置章：{PRIOR}；讀者審查清單：/Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/handbook-writing-guide.md §11。
[限制] 只回報讀者/實用視角，不查技術對錯；不改稿；任何第一次出現、沒當場解釋的術語都標（即使你知道）；邊讀邊即時 append 到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reader-junior.md。
[DoD] 逐節卡關點＋類型（缺脈絡/太抽象/術語沒先定義/概念圖不自明/步驟不可操作/鷹架洩漏）＋「我會這樣想、我會問什麼」；初階 end-to-end 能不能照著走；按 真缺陷/可加強/誤讀 三級彙整；回傳摘要。
```

### R3 — Reader-進階 AE（subagent_type: general-purpose）
```
你是《Spark 優化參考手冊》的「進階 analytics engineer」目標讀者審稿人。全程繁體中文（不可簡體）。
[人設] 你會 SQL 也會一點 Python，負責營運共用特徵庫（feature store）供模型訓練、做 reverse ETL 把資料送回業務端「客戶行銷及管理系統」。你關心：深度夠不夠長期營運、能不能穩定維運、進階課題（資源配置/多租戶、資料版本與發佈、training–serving 一致、出口推送的冪等與合規）有沒有講透。
[目標] 從頭讀到尾，標出「對營運這條主線深度不足/缺進階課題/不可落地」處；不是挑入門易讀性（那是初階 reader 的事）。
[素材] 待審：{CHAPTER_PATH}；前置章：{PRIOR}。
[限制] 只回報深度與營運實用性，不查單點技術對錯；不改稿；邊讀邊即時 append 到 /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook/docs/handbooks/spark-tuning/.reviews/{CHAPTER}__reader-advanced.md。
[DoD] 逐節深度評（夠營運/偏淺/偏深＋為什麼）；進階能力缺口清單；按 真缺陷/可加強/誤讀 三級彙整；回傳摘要。
```

### C / P — 全書 Architecture／Pedagogy
里程碑跑（Phase 1 複核、Phase 5 最終 pass），prompt 同 Phase 0 已用版本（見 `.reviews/_round2-architecture__pass1.md`／`_round2-pedagogy__pass1.md` 開頭）。

---

## Progress Tracker

> **▶ round-2 完成並 merge（PR #87, merge commit `722318e`, 2026-06-23）。下一步（user 2026-06-23 定，/compact 後接續）：**
> 1. **回饋驅動的整體修訂（round-3）**：user 會帶**同事閱讀回饋**進來（文字風格／章節內容搬移／內容補強）。沿用本套方法論（grill→spec→計畫→subagent-driven 分階段審稿→三級 triage→commit）；**動手前先把 branch sync 到最新 main**（落後 32 commit）。章節搬移小心重編號（插節用 `###`；重排用 Task 1.1 的單次掃描 `re.sub`＋連結健檢＋C 複核）。
> 2. **把撰寫流程做成可複用 skill**：方法論藍本在 memory `project_handbook_writing_skill`（5 審稿角色、socket-death 等教訓、模板位置）。
> 3. **已知待修**：`index.md`「如何使用本手冊」01–08 純文字、09–12 是連結 → 統一成全連結（跟章節導覽表一致）。
>
> 審稿 subagent 模板與寫作慣例在本檔上方；風格原則在 `docs/handbooks/handbook-writing-guide.md`。

- [x] **Phase 0**：grill 釐清 → Phase1 全書 C＋P 掃描 → 使用者拍板新骨架（2026-06-22）＋ch02 D-UI 追加（2026-06-23）。spec 已寫。
- [x] **Phase 1**：結構落地完成（commit `1077609` 重排+重編號+導覽軟指標、`3c8197a` index 12 章）；C 複核 `.reviews/_round2-structure-verify.md` 判 **PASS 零不一致**。
- [x] **Phase 2**：逐章深審完成（ch01 `72c9248`／ch02 D-UI `46e6457`+`3089c70`／ch03 `a087904`／ch04 `f959745`+`7d1c664`／ch05 `84c96b3`／ch06 `dc28bf6`／ch07 capstone `673e1c7`／ch08 `6d62db5`）。各章雙/三審＋triage；ch02 升級手把手診斷核心、ch06 補 ACID 多引擎安全（HWC）、ch07 補 capstone 端到端走查。
- [x] **Phase 3**：補缺口完成。G1＝新增第 09 章 reverse ETL（`fa8b1a2` 主迴圈手寫 9 節＋3 圖，`37a2c82` 雙 reader 深審修訂：增量/全量、schema drift、partial-reject、術語 gloss、pre-flight checklist；接 08 導覽＋index 硬連結）。G2＝第 08 章補 training–serving 一致 `###`（`081a28f`）。**教訓：含 WebFetch 的長 subagent 連 3 次 socket 死（一次 hang 100min）→ 新章改主迴圈手寫＋事實預先查好；reader/fix subagent 不帶 WebFetch 則穩定。**
- [x] **Phase 4**：補完三章（皆主迴圈手寫，因 infra 對長 subagent 寫作不穩）。ch10 PySpark `5bde678`、ch11 場景索引＋ch12 速查名詞表 `de535a4`；全書 12 章硬連結、無「撰寫中」殘留。ch11/ch12 為索引/速查（config 值取自 round-1 已驗），技術一致性待 Phase 5 C round-3 複核。
- [~] **Phase 5**：`.md` 部分完成。Task 5.3 全書 C round-3 一致性終審 **PASS**（`.reviews/_round2-architecture__round3.md`：12 章交叉引用/§ref 全對、ch12 16 個 config 值與各章一致、G1/G2 覆蓋、兩主線 end-to-end 無斷點、章序合理）；triage 4 項全修（`ad48e84`：ch07 六章、ch05 去草稿 TODO、ch09 pull 軟化、index ch11 雙線）；全書硬連結健檢無 broken。**待辦（等使用者對 .md 簽核後）**：Task 5.2 轉離線 HTML（內嵌 mermaid.js）；可選 index Diátaxis 入口 mermaid 圖（Pedagogy f-6）；可選全書雙 persona 通讀（per-chapter 已逐章審過，視需要）。

---

## Phase 1 — 結構落地（重排骨架）

> 先把 12 章骨架與全書交叉引用落定，後續逐章深審才在最終編號上進行。**最高風險＝重編號的交叉引用 collision**，故用單次掃描的函式型 re.sub（不會把自己的輸出再次替換），並以 grep + C 複核當閘。

### Task 1.1：重命名兩章檔案 ＋ 全書重編號

**Files:**
- Rename: `08-operating-pipelines.md` → `07-operating-pipelines.md`
- Rename: `09-data-product-correctness.md` → `08-data-product-correctness.md`
- Modify: 全部 `docs/handbooks/spark-tuning/*.md`（除 index.md，index 在 Task 1.2 整體改寫）

- [ ] **Step 1：git mv 兩章**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
git mv docs/handbooks/spark-tuning/08-operating-pipelines.md docs/handbooks/spark-tuning/07-operating-pipelines.md
git mv docs/handbooks/spark-tuning/09-data-product-correctness.md docs/handbooks/spark-tuning/08-data-product-correctness.md
```

- [ ] **Step 2：寫重編號腳本並執行**（單次掃描、函式型替換，避免 7→10/8→7/9→8 連鎖 collision）

建立 `/tmp/renumber.py`：

```python
import re, pathlib
DOCS = pathlib.Path("docs/handbooks/spark-tuning")
# 章參照數字 → 新章號。1–6 不在表內＝不動。
CH = {7: 10, 8: 7, 9: 8, 10: 11, 11: 12}
SLUGS = "operating-pipelines|data-product-correctness|pyspark-dataframe-api|scenario-playbooks|cheatsheet-and-glossary"

def new(n):  # int -> int
    return CH.get(n, n)

def ch_ref(m):       # 第 0?N 章  -> 第 0M 章（zero-pad 2 位）
    return f"第 {new(int(m.group(1))):02d} 章"

def sec_ref(m):      # §N.minor  -> §M.minor（章號不 pad）
    return f"§{new(int(m.group(1)))}.{m.group(2)}"

def heading(m):      # 行首 ##.. N.  -> ##.. M.（僅 N 在 CH 內，即改名兩章的內部小節）
    n = int(m.group(2))
    return f"{m.group(1)} {new(n)}." if n in CH else m.group(0)

def filelink(m):     # NN-slug.md -> MM-slug.md
    return f"{new(int(m.group(1))):02d}-{m.group(2)}.md"

for p in sorted(DOCS.glob("*.md")):
    if p.name == "index.md":      # index 由 Task 1.2 整體改寫
        continue
    t = p.read_text(encoding="utf-8")
    t = re.sub(r"第\s*0*(\d{1,2})\s*章", ch_ref, t)
    t = re.sub(r"§\s*(\d{1,2})\.(\d+)", sec_ref, t)
    t = re.sub(r"(?m)^(#{2,6})\s+0*(\d{1,2})\.", heading, t)
    t = re.sub(rf"\b0*(\d{{1,2}})-({SLUGS})\.md", filelink, t)
    p.write_text(t, encoding="utf-8")
    print("renumbered", p.name)
```

執行：

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
python3 /tmp/renumber.py
```

- [ ] **Step 3：grep 驗證沒有殘留舊檔名連結**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
grep -rn -e "08-operating-pipelines.md" -e "09-data-product-correctness.md" \
         -e "07-pyspark-dataframe-api.md" -e "10-scenario-playbooks.md" \
         -e "11-cheatsheet-and-glossary.md" docs/handbooks/spark-tuning/*.md
```
Expected：**無輸出**（舊號連結都應已改成新號；07-pyspark 應變 10-、10-scenario 應變 11-、11-cheatsheet 應變 12-）。若有輸出→腳本漏網，手動 Edit 修。

- [ ] **Step 4：人工確認兩改名章的 H1／本章前提／章末導覽**

開 `07-operating-pipelines.md` 與 `08-data-product-correctness.md`，確認：H1 章號正確；「本章前提」引用的前置章號正確（07 PRIOR=01,03,04,05；08 PRIOR=01,03,05,07）；章末導覽「上一章/下一章」指向正確新號（07 上一章＝06、下一章＝08；08 上一章＝07、下一章＝09 reverse ETL）。內部小節 §7.x／§8.x 連號無跳號。手動 Edit 修任何 script 沒覆蓋到的非錨定數字（如表格裡裸寫的「8.7」）。

- [ ] **Step 5：commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook
git add docs/handbooks/spark-tuning/
git commit -m "docs(spark-handbook): round-2 重排骨架——營運線 08/09→07/08、全書交叉引用改號"
```

### Task 1.2：改寫 index.md（12 章骨架）

**Files:** Modify `docs/handbooks/spark-tuning/index.md`

- [ ] **Step 1：改章節導覽表**——12 列，依新骨架（spec §5 表）。新增第 09「營運（三）：把資料送出去——reverse ETL 回業務系統」、把 PySpark 移到 10、場景 11、速查 12。每列一句話定位照 spec §5。
- [ ] **Step 2：改「如何使用本手冊」分流清單**——加 reverse ETL（「要把名單/特徵送回業務系統 → 第 09 章」）、PySpark 改指第 10、場景第 11、速查第 12。
- [ ] **Step 3：改「兩條主線」段**——對齊雙 persona（初階分析師／進階 analytics engineer），**把 reverse ETL／出名單明列為兩條線的共同終點**（spec C6）。
- [ ] **Step 4（可選，Pedagogy f-6）：加一張 mermaid「我現在想做什麼 → 哪章」入口圖**（Diátaxis 式分流）。若做，放在「如何使用」段。
- [ ] **Step 5：commit**

```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook add docs/handbooks/spark-tuning/index.md
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/spark-handbook commit -m "docs(spark-handbook): index 改寫為 12 章骨架＋雙主線＋reverse ETL 終點"
```

### Task 1.3：修導覽鷹架洩漏（Pedagogy d-1／必修）

**Files:** Modify `06-engine-selection.md`、`07-operating-pipelines.md`

- [ ] **Step 1：grep 找鷹架字樣**

```bash
grep -rn "撰寫順序暫緩\|撰寫中\|暫緩" docs/handbooks/spark-tuning/*.md
```

- [ ] **Step 2：移除「（撰寫順序暫緩）」等鷹架字樣**，章末導覽箭頭方向改正（`← 上一章` / `下一章 →`）。缺章（10/11/12 尚未寫）期間，導覽**直接指向實際存在的鄰章**，不留「暫緩」字樣；缺章的 forward-ref 暫時就近改指各章自己的速查（如 §2.9/§7.7/§8.x）或標「（規劃中）」純文字、不下會 404 的硬連結。
- [ ] **Step 3：commit**（訊息 `docs(spark-handbook): 移除導覽鷹架字樣＋修箭頭方向`）

### Task 1.4：C 複核交叉引用整合性（Phase 1 閘）

- [ ] **Step 1：派 Architecture-C（窄範圍：只查交叉引用/編號一致性）**，prompt 重點＝「讀 01–08＋index，逐一核對：章節導覽、章首前提、章末導覽、內文 forward-ref 的章號/節號是否都指向正確主題（07＝排程、08＝資料產品、09＝reverse ETL、10＝PySpark、11＝場景、12＝速查）；列出所有不一致」。即時寫 `.reviews/_round2-structure-verify.md`。
- [ ] **Step 2：triage 並修** C 回報的不一致。
- [ ] **Step 3：使用者 glance**（送骨架落地結果）。commit 任何修正。

---

## Phase 2 — 逐章深審（既有 8 章）＋ ch02 D-UI

> 每章標準循環：**派 R1＋R2＋R3（同一訊息並行、窄範圍、即時 live log）→ triage 三級 → 套修（含該章 Pedagogy 可加強項）→ commit → 使用者 glance**。下表給每章新號／PRIOR／要 fold 的已知項。ch02 是重頭戲（D-UI），單獨 Task 2.2 放大。

| Task | 章（新號） | PRIOR | 本章要 fold 的已知項 |
|---|---|---|---|
| 2.1 | 01 心智模型 | 無 | §1.6 AQE 縮寫鋪陳節奏微調（Pedagogy f-4） |
| **2.2** | **02 Spark UI（D-UI 大改）** | 01 | 見下方專節 |
| 2.3 | 03 SQL 寫法 | 01,02 | 交叉引用複查；雙 reader triage |
| 2.4 | 04 Spark 設定 | 01,02,03 | 同上 |
| 2.5 | 05 儲存效率 | 01,03 | §5.5 過載拆節（Pedagogy f-5） |
| 2.6 | 06 引擎選用 | 01,02,05 | 導覽鷹架（已於 1.3 修，複查） |
| 2.7 | 07 營運（一） | 01,03,04,05 | 補 capstone 端到端走查節（Pedagogy f-1／c） |
| 2.8 | 08 營運（二） | 01,03,05,07 | 深度平衡複查（G2 在 Phase 3 補） |

每章（除 2.2）的 Step：

- [ ] **Step A：並行派 R1＋R2＋R3**（一則訊息三個 Agent，填 `{CHAPTER_PATH}`／`{PRIOR}`，live log 各自 `__reviewer`／`__reader-junior`／`__reader-advanced`）。
- [ ] **Step B：triage 三級**（真缺陷必修／可加強斟酌／誤讀記錄不改），併入該章 fold 項。
- [ ] **Step C：套修**；若修後再加料 → 補審該段。
- [ ] **Step D：commit**（`docs(spark-handbook): 第 NN 章 round-2 深審修訂`）＋使用者 glance。

### Task 2.2：第 02 章 Spark UI — D-UI 手把手診斷核心（重點 task）

**Files:** Modify（大改）`02-diagnose-with-spark-ui.md`

**目標**：把 ch02 從「介紹怎麼看 UI」升級成「**照著做就能用 UI 改自己的 SQL／Spark 設定／寫表邏輯**」的操作核心。**邊界＝只診斷與路由，不重教 §03/§04/§05 修法**（守整體性、不重複）。

- [ ] **Step A：查證（Spark 3.3 Web UI 官方文件逐欄位）**——須查證重點：
  - Web UI 各頁籤組成與欄位（Jobs／Stages／**SQL/DataFrame**／Executors／Storage／Environment）：`spark.apache.org/docs/latest/web-ui.html`。
  - Stages 頁的 task metrics（Duration、GC Time、Shuffle Read/Write、**Spill (memory)／Spill (disk)**、Input Size/Records）、Summary Metrics 的 min/25th/median/75th/max percentile 意義。
  - SQL 頁的 query plan 視圖節點（`Exchange`＝shuffle、`BroadcastHashJoin` vs `SortMergeJoin`、Scan 的 `PushedFilters`/`PartitionFilters`、`number of output rows`/`data size`）。
  - Executors 頁（Active/Failed Tasks、Storage Memory、**Shuffle Spill**、GC Time）。
  - AQE 在 SQL 頁的呈現（`AdaptiveSparkPlan isFinalPlan=true`，來源：Databricks AQE 文＋SPARK-33850）。
  - 把每個查證點記到 `.reviews/02__reviewer.md` 上方「Step A 查證」區（供寫稿與 reviewer 比對）。
- [ ] **Step B：改寫 ch02 結構**——目標節（每頁籤一節，控制密度）：
  - §2.1 心法：先量再調（保留/精修）。
  - §2.2 從哪進（History Server completed/incomplete；保留既有 user 公司經驗）。
  - §2.3–§2.8 **逐頁籤 screen-by-screen**：每節＝「**這個頁籤/這一格顯示什麼 → 正常長什麼樣、異常長什麼樣（標註 mock 面板）→ 代表哪個問題 → 去哪一章哪一節修**」。涵蓋 Jobs/Stages/SQL/Executors/Storage/Environment。
  - §2.9 **症狀 → 面板 → 章 路由總表**（升級版）：列 shuffle 過大／skew（task 時間長尾）／spill（memory+disk spill 欄）／掃太多（PartitionFilters 沒生效）／小檔（task 數爆量/Input 很小）／broadcast 沒生效（該 broadcast 卻 SortMergeJoin）等，每列：在哪頁籤哪格看出來 → 翻 §03/§04/§05 哪節。
  - §2.10 **三張 checklist 檢核表**（對齊兩主線工作型態）：
    - **A. 讀 UI 改 SQL**（→ §03）：逐項勾「SQL 頁有沒有非預期 `Exchange`／Scan 有沒有 PartitionFilters／join 是不是退化成 SortMerge/BNLJ……」。
    - **B. 讀 UI 改 Spark 設定**（→ §04）：「Executors 頁 spill 是否頻繁／shuffle partitions 是否過多碎小／broadcast 門檻……」。
    - **C. 讀 UI 改寫表/儲存邏輯**（→ §05）：「Stages Input 是否掃了不該掃的分區／task 數暴增疑似小檔／輸出檔大小……」。
  - §2.11 一條慢查詢的驗屍（沿用 §2.8 貫穿例，串完整三 checklist）＋章末帶走/導覽。
  - mock 面板皆標「示意數字」，章末註明 HTML 階段可換公司真實 History Server 截圖。
- [ ] **Step C：並行派 R1＋R2＋R3**（`{CHAPTER_PATH}=…/02-diagnose-with-spark-ui.md`，`{PRIOR}=01`）。**R1 特別查**：每個「面板某格 → 代表某問題」的對應是否與官方欄位語意一致、沒有把「建議」寫成「硬規則」。**R2 特別查**：checklist 每項初階能不能照著在自己畫面上對號。
- [ ] **Step D：triage＋套修**（screen-by-screen 的對應錯誤屬真缺陷必修）；修後加料補審。
- [ ] **Step E：commit**（`docs(spark-handbook): 第 02 章升級為手把手 UI 診斷核心（D-UI）`）＋使用者審。

---

## Phase 3 — 補缺口新內容

### Task 3.1：新增第 09 章 reverse ETL（營運三；補 G1）

**Files:** Create `09-reverse-etl.md`
**PRIOR：** 05, 06, 07, 08

- [ ] **Step A：查證（只認官方；查不到標「無法查證」、用保守說法、不臆測）**——須查證重點：
  - Spark JDBC 寫出：`spark.apache.org/docs/latest/sql-data-sources-jdbc.html`（`format("jdbc")`、`batchsize`、`isolationLevel`、`numPartitions`、`truncate`、寫入語意）。
  - CDP 上把資料推出 Hadoop 的官方途徑與**現況**：Sqoop 在 CDP 7.1.9 的狀態（是否 legacy/建議替代）、Cloudera Flow Management（NiFi）egress、Cloudera Data Engineering——查 `docs.cloudera.com`。
  - 目標端冪等 upsert 原則（target RDBMS 的 `MERGE`/`ON CONFLICT`，DB-specific，教原則不綁特定 DB）。
  - PII/治理：Apache Ranger 政策/遮罩/稽核於資料離開 Hadoop 邊界（`docs.cloudera.com`）。
  - Airflow 觸發 egress＋retries/sensor（對齊 ch07 §7.x 三層工具）。
  - 記到 `.reviews/09__reviewer.md` Step A 區。
- [ ] **Step B：寫草稿**（內容大綱，對應 spec §5 第 09 章與 architecture C1）：
  - §9.1 問題定位：算好的特徵/名單**只待在 Hadoop 裡沒用**，要送進業務端「客戶行銷及管理系統」——這是兩條主線的最後一哩。pull（下游自己用 Impala 查）vs push（主動推出）兩種模型。
  - §9.2 推送通道對照表：JDBC sink／檔案落交換區（SFTP pickup）／NiFi／Sqoop（標現況）／對接 API；各自適用情境、CDP 上的實務選項與限制。
  - §9.3 推送的冪等與重試（接 ch07 §7.2 同紀律，但目標是**外部系統**不是 Hive 分區）：半推半成怎麼辦、目標端 upsert key、批次 vs 增量、失敗重試。
  - §9.4 就緒閘與發佈（接 ch07 §7.3＋ch08 WAP/promote）：**過了品質閘＋發佈版才推**；推送只讀「已發佈」版本。
  - §9.5 PII／權限／稽核（送出 Hadoop 邊界的合規，Ranger）。
  - §9.6 對齊行銷系統需要的格式/key；§9.7 貫穿範例（把 `cust_feature`／名單一路推到行銷系統）；取捨；帶走；導覽（上一章 08、下一章 10）。
  - Mermaid：① pull vs push；② 推送通道決策；③ 就緒閘→發佈→推送流程。
- [ ] **Step C：並行派 R1＋R2＋R3**（`{PRIOR}=05,06,07,08`）。
- [ ] **Step D：triage＋套修**（reverse ETL 技術主張錯＝真缺陷）；補審加料。
- [ ] **Step E：commit**（`docs(spark-handbook): 新增第 09 章 reverse ETL（營運三·補 G1）`）＋使用者審。

### Task 3.2：補 training–serving 一致 `###`（補 G2）

**Files:** Modify `08-data-product-correctness.md`（時間點正確性節 §8.3 之後）

- [ ] **Step A：查證**——特徵定義單一真實來源（dbt model / 共用 SQL）、批次訓練表與推論共用同一段計算、推論讀哪個 snapshot/新特徵上線對齊、training–serving skew 定義。官方/書籍來源。
- [ ] **Step B：在 §8.3 後加 `### 進階：訓練與推論用同一份特徵——training–serving 一致`**——內容：同一段特徵計算邏輯不要寫兩份（一份訓練、一份推論會 drift）、特徵定義的單一真實來源、推論時讀對 snapshot、與 §8.3 snapshot 切線的關係（snapshot 模型消掉 as-of join、但一致 ≠ 只有 as-of）。具體 SQL/dbt 例。
- [ ] **Step C：聚焦補審**（R1＋R3 審此 `###`，narrow scope）。
- [ ] **Step D：triage＋套修＋commit**（`docs(spark-handbook): 第 08 章補 training–serving 一致（G2）`）。

---

## Phase 4 — 補未寫章

> 沿用標準「每章撰寫循環」Step A–F（查證→寫→並行雙/三審→triage→使用者→commit）。內容大綱以原 spec §5 為準，但章號改為新骨架。

### Task 4.1：第 10 章 PySpark DataFrame API（現 07 後移）

**Files:** Create `10-pyspark-dataframe-api.md`；PRIOR=01,03,(07,08)
- [ ] Step A 查證（DataFrame API 對應 SQL 的等價、何時值得改用、可測試性；不碰 RDD 低階）。
- [ ] Step B 寫（何時從 SQL 改用 API、改用時注意什麼、與營運線的銜接：複雜可重用/要單元測試的特徵邏輯）。
- [ ] Step C–F：三審→triage→使用者→commit。

### Task 4.2：第 11 章 場景對應（索引）

**Files:** Create `11-scenario-playbooks.md`；PRIOR=01–10
- [ ] Step A：**無新技術主張**，只核對跨章引用指對（§02 診斷/§03 SQL/§04 config/§05 storage/§06 引擎/§07 排程/§08 品質·時間點·版本/§09 reverse ETL/§10 PySpark）。
- [ ] Step B 寫：純索引、不重教概念。三情境（① ad-hoc ② 排程產表/出名單 ③ 特徵運算/特徵庫），每情境＝典型流程→對應章節清單→該情境最常踩的雷；**每情境標註主要服務初階/進階主線**（architecture C4）；收口初階 ad-hoc 入口場景（G4）。1 張總表或三張對照圖。
- [ ] Step C–F：reader 雙審（重在導航是否好用）→triage→使用者→commit。

### Task 4.3：第 12 章 速查與名詞表

**Files:** Create `12-cheatsheet-and-glossary.md`；PRIOR=—
- [ ] Step A：彙整既有各章的取捨/config 預設值/症狀速查/中英名詞；**核對與各章內文一致**（C 一致性）。
- [ ] Step B 寫：取捨速查、config 速查（附預設值＋章指引）、症狀→章、中英名詞對照。
- [ ] Step C–F：R1 核對 config 值＋C 核對一致性→triage→使用者→commit。

---

## Phase 5 — 收尾

### Task 5.1：定稿 index
- [ ] 全章寫完後最終校 index（12 章連結、兩主線、入口圖、如何使用）；commit。

### Task 5.2：轉 HTML
- [ ] 內嵌 mermaid.js、離線檢查、回頂鈕、跨章導覽；mock 面板處標可換真實截圖；commit。

### Task 5.3：全書最終 pass
- [ ] 派 **Architecture-C round-3**（能力地圖無缺口、章序/依賴、深度平衡）＋ **reader 通讀**（雙 persona 各跑一次全書）＋ **Pedagogy** 複查（敘事弧、缺章補齊後 forward-ref 全解析）。triage 全修。
- [ ] 使用者最終驗收。

---

## Direction Log（append-only）

- 2026-06-22：round-2 全書重審啟動。grill 鎖定六決策：①完整重構授權 ②雙職涯兩條主線讀者模型 ③教學範本＝名著編排＋Diátaxis＋Databricks（技術仍只認官方）④5 種 subagent（R1 技術／R2 初階 reader／R3 進階 reader／C 架構含一致性／P 教學）⑤架構先行分階段 ⑥先實跑 Phase1 再交 review＋計畫、停在改寫前。Phase1 C＋P 全書掃描完成（無 socket 死）。
- 2026-06-22：Phase1 發現＝現有 8 章強（Pedagogy 達/超名著、Architecture 依賴鏈乾淨）；唯一真缺陷 G1（reverse ETL 零覆蓋，兩主線共同終點）；G2（training–serving 偏薄）；ch08 導覽鷹架洩漏（必修）；Pedagogy 可加強（ch07 capstone、§1.6、§5.5）。使用者拍板新骨架＝**reverse ETL 專章＋完整重排**（12 章；營運 08/09→07/08、新 09 reverse ETL、PySpark→10、場景→11、速查→12）。
- 2026-06-23：使用者追加 **D-UI**——ch02 升級為手把手診斷核心（逐頁籤 mock 面板＋症狀→面板→章路由＋三張 checklist：讀 UI 改 SQL/改 config/改寫表邏輯），邊界＝只診斷與路由、不重教 §03/§04/§05。已納 spec §4.1／§7.4a 與本計畫 Task 2.2。

---

## Self-Review（writing-plans 自檢）

- **Spec 覆蓋**：spec §7 成功標準逐項對應——#1 結構/交叉引用→Phase 1＋Task 1.4＋5.3；#2 G1→Task 3.1；#3 G2→Task 3.2；#4 逐章深審＋Pedagogy 可加強→Phase 2（§1.6=2.1、§5.5=2.5、capstone=2.7、導覽鷹架=1.3）；#4a D-UI→Task 2.2；#5 三章→Phase 4；#6 index/HTML/最終 pass→Phase 5；#7 繁中/體例一致→各 task 慣例＋5.3。無缺口。
- **Placeholder 掃描**：renumber 腳本、grep 指令、reviewer prompt、各章大綱皆具體；無 TBD/「之後再說」。
- **一致性**：章號映射（07=排程、08=資料產品、09=reverse ETL、10=PySpark、11=場景、12=速查）在檔案結構表、renumber CH 字典、Phase 2 表、Direction Log 一致；PRIOR 與 spec §5 一致。
