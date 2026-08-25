# `docs/operations/` 分類清單（2026-08-25）

> **這份是什麼**：把 `docs/operations/` 現有 10 份文件依「讀者」歸類，並判定每份該留在
> operations/ 還是回到別的目錄。**只做分類，不動任何檔案**；刪／併／瘦身是下一步。
>
> **怎麼用**：第 3 節的「歸屬判定」欄是下一步的輸入。第 4 節的事實欄（行數／最後改動／
> 引用數）是取捨時的成本依據。第 5 節是分類過程順手撿到的既成事實，不是建議。

## 1. 分類軸

**主軸＝讀者**，三桶：

| 桶 | 讀者站在哪 | 他在問什麼 |
|---|---|---|
| ① 使用者 | 框架外面，不改 code | 我要拿這個框架做一件事，步驟給我 |
| ② 開發環境 | 這台機器前面 | 我要把環境跑起來 |
| ③ 工作紀律 | 正在改這個 repo（含 agent） | 我該守什麼判準 |

**副軸＝形式**：`runbook`（照著做）／`概念`（要先懂）。同一個桶裡這兩種要分開放，否則
10 行的四步驟會跟 266 行的教學文擠在一起。

## 2. `docs/operations/` 的入場資格

判準來自 `docs/pipelines/*.md` 的固定 10 節模板（1 總覽／2 執行前準備／3 設定方式／
4 使用方式／5 執行流程／6 產物與驗收／7 版本、重跑與恢復／8 常見錯誤與排查／
9 限制與注意事項／10 相關文件）：

- **內容塞得進某「一條」pipeline 的第 3／4／7／8 節** → 屬於那條 pipeline，回 `docs/pipelines/`。
- **跨兩條以上 pipeline 的工作流** → 留 `docs/operations/`。拆進任一條都會把工作流斷成數截。
- **不屬任何 pipeline（環境、框架層機制）** → 留 `docs/operations/`。
- **給 agent 的判準與紀律** → `docs/agents/`。
- **純教學（沒有步驟可照做）** → `docs/handbooks/`。

## 3. 主表：10 份的歸類與歸屬

| 檔案 | 讀者 | 形式 | 歸屬判定 |
|---|---|---|---|
| `adding-an-eval-month.md` | ① 使用者 | runbook | **留 operations**（跨 dataset＋training predict＋evaluation 三條） |
| `sampling-overrides-editor.md` | ① 使用者 | 混：§1–4 概念、§5–6 runbook | **留 operations**（跨 dataset＋training 的設定工具） |
| `training-oom-object-matrix.md` | ① 使用者 | 混 | ✅ 已三分並刪檔，見 §8 進度 |
| `hpo-resume.md` | ① 使用者 | runbook | **回 pipelines**（只屬 training；且見 §5.1 已被涵蓋） |
| `pipeline-slicing.md` | ①＋③ 混合 | 混 | ✅ 已拆三塊，剩餘部分改寫後移入 `user-guides/`，見 §8 進度 |
| `local-spark-setup.md` | ② 開發環境 | runbook | **留 operations** → `dev-setup/` |
| `worktree-venv-setup.md` | ② 開發環境 | runbook | **留 operations** → `dev-setup/`；§62 屬 ③，見下鑽 |
| `spark-connection-architecture.md` | ② 開發環境 | 概念 | **留 operations** → `dev-setup/`（23 行，全篇講「為什麼這樣分層」，無步驟） |
| `diagnosis-report-presentation.md` | ③ 工作紀律 | 判準 | ✅ 已去 `docs/agents/`（寫報表的人才讀，跟跑 pipeline 無關） |
| `known-pitfalls.md` | ③ 工作紀律（全 19 條） | 判準 | **去 `docs/agents/`（整份）**；主題跨多條 pipeline，見下鑽 |

## 4. 事實欄（下一步做取捨時的成本依據）

「活引用」＝扣掉 `docs/superpowers/`（已結案的 plan 存檔）之後，還在指向它的檔案數。

| 檔案 | 行 | 最後改動 | 活引用 | 含存檔 |
|---|---|---|---|---|
| `training-oom-object-matrix.md` | 266 | 2026-08-07 | 3 | 5 |
| `sampling-overrides-editor.md` | 249 | 2026-06-29 | 4 | 4 |
| `known-pitfalls.md` | 213 | 2026-08-09 | 8 | 26 |
| `adding-an-eval-month.md` | 201 | 2026-08-19 | 10 | 11 |
| `worktree-venv-setup.md` | 113 | 2026-06-09 | 3 | 10 |
| `pipeline-slicing.md` | 104 | 2026-08-13 | 7 | 11 |
| `local-spark-setup.md` | 66 | 2026-06-09 | 4 | 9 |
| `diagnosis-report-presentation.md` | 63 | 2026-07-21 | 3 | 3 |
| `hpo-resume.md` | 40 | 2026-06-12 | 4 | 6 |
| `spark-connection-architecture.md` | 23 | 2026-06-09 | 2 | 7 |

合計 1338 行。

## 5. 三份混合檔的章節下鑽

### 5.1 `known-pitfalls.md` — 19 條，讀者全是 ③

**先修正一個初判錯誤。** 第一輪只看標題時，本清單曾把這 19 條判成「①②③ 三桶混合、必須
拆成三份」。逐條讀完內文後推翻：19 條全部採同一格式（症狀／根因／**規則**／驗證方式），
而每一條的「規則」都指名 `src/` 路徑、cache 目錄或 log 行——例如 §8 的規則講
`core/consistency.py::nonnumeric_feature_errors` 與 `io/extract.py` backstop、§17 的規則是
`rm -rf <cache.root>/.../lgb`、§18 的規則是「寫地基公式一律照 code 追一次」。**這些動作只有
正在改這個 repo 的人會做，框架使用者不會。**

結論隨之改變：**known-pitfalls 不需要拆成三份，整份屬 ③、可整份搬進 `docs/agents/`。**

不過 19 條的**主題**確實分兩群，這一層對下一步（哪些條目的內容該併進 pipeline 文件）有用：

| 主題群 | 條目 | 數量 |
|---|---|---|
| 環境與工具鏈（git／venv／worktree／macOS／測試基礎設施／PR 流程） | §1 `.venv` ELOOP、§2 graphify hook、§3 worktree R1/R2/R3、§4 測試效能數字、§5 main 既有測試問題、§5b break-it check 還原、§6 環境 quirk、§7 macOS hostname、§9 本機 evaluation 兩旗標、§10 grep 假陽性、§14 測試會 DROP 本機真表、§16 PR 物件落後 | **12** |
| 產品行為（規則仍寫給改 repo 的人，但主題落在特定 pipeline） | §8 字串欄 OOM（dataset＋training）、§11 `groupby` `dropna`（診斷層）、§12 node inputs 位置綁定（框架層）、§13 隔離環境 hash（部署）、§15 加月份數字沒變（dataset＋training cache）、§17 `sample_weights` `.bin` cache（training）、§18 AP@k 分母（evaluation） | **7** |

### 5.2 `pipeline-slicing.md` — 6 節，兩桶

| 節 | 桶 | 形式 |
|---|---|---|
| §5 先分清楚：模式不是切片 | ① | 概念 |
| §19 使用 | ① | runbook |
| §38 自動擴張補跑 | ① | 概念 |
| §58 使用前提與限制 | ① | 概念 |
| §80 開發守則（改 pipeline 結構的人必讀） | ③ | 判準（`docs/pipelines/` 零覆蓋，瘦身時保留） |
| §93 已知設計決議 | ③ | 設計記錄（ADR 材料） |

### 5.3 `worktree-venv-setup.md` — 8 節，幾乎全是 ②

§7 venv 模型／§24 `.venv` 不進版控／§36 worktree 內跑測試／§57 跨 worktree git／
§78 `data/` 隔離／§95 pre-flight／§104 修復流程 → 全部 ②。

唯一例外：**§62 Known gotcha: graphify hook** → ③。

## 6. 分類過程撿到的既成事實（不是建議，是現況）

1. **`hpo-resume.md` 是孤兒副本（~90%，非 100%）。** `docs/pipelines/training.md` §7.2
   （`model_version` vs `search_id` 對照表）＋ §7.3（HPO 恢復語意 6 條、持久化磁碟前提）
   涵蓋它絕大部分內容且更細，而 `training.md` 對它的引用數為 **0**。
   **更正（2026-08-25 逐行比對後）**：本清單第一版寫「已完整涵蓋」是錯的。有 2 件事只在
   `hpo-resume.md` 有——接續 log 字串 `HPO resume: N completed trial(s) found...`
   （實證仍在 `src/recsys_tfb/pipelines/training/nodes.py:696`）與 `_hpo/` 的保留／清理政策
   （成功後刻意保留、跨 `model_version` 共用、`rm -rf data/models/_hpo/`）。
   兩者已於刪除前併入 `training.md` §4.7 與 §7.3。
2. **同一件事寫兩遍：graphify hook 擋 checkout。** `known-pitfalls.md` §2 與
   `worktree-venv-setup.md` §62 是同一個事故、同一組規則。
3. **同一件事寫兩遍：字串特徵欄 OOM。** `known-pitfalls.md` §8 是
   `training-oom-object-matrix.md`（266 行）的摘要。
4. **`docs/agents/` 的標籤跟內容對不上。** `docs/agents/domain.md` 說它是「本系列 skill 的
   per-repo 設定」，但實際裝著 `architecture-constraints.md`（347 行）與
   `pipeline-node-design.md`（215 行）——那是給 agent 的設計判準。③號桶要搬進去的話，
   `domain.md` 的定義要一起改。
5. **`known-pitfalls.md` 的引用數有 18 筆來自 `docs/superpowers/` 的已結案 plan 存檔**，
   活引用只有 8。動它的成本比 26 這個數字看起來低。

### 6.1 為什麼 `training-oom-object-matrix.md` 不能整份搬去草稿區

早先本清單的第一版曾把它描述成「大半是 B6 閘上線前的歷史」。**逐節讀完後推翻**：它有兩段現役、
且**沒有第二個地方有**的內容——

- **§7「觀測性在這個情況下是瞎的」**：`src/recsys_tfb/io/extract.py:321` 的 `log_data_volume`
  對 `object` 矩陣只算指標、不算 payload，會把 95.9 GiB 報成 22.4 GiB。不知道的人會拿它判斷
  「記憶體夠」。
- **§8「修掉文字欄之後，還是可能不夠」**：移除文字欄後需求仍是 54.7 GiB，而這台機器的上限落在
  48.3 GiB 與致死點之間——**很可能還是會死**。只有這裡有「還不夠時的 5 個後續選項」。

`known-pitfalls.md` §8 的規則只寫「declare categorical 或 drop」。少了上面兩點，它會變成一份
**給假安全感**的條目：照做完以為好了，其實沒有。這就是為什麼那 4 個引用裡，README 與
known-pitfalls 那兩個是**必要**的（`dataset.md` 的兩個則可省，其中 §195 已另有內部指標）。

## 7. 這份沒做的事

- **沒有查證任何一條坑是否還活著。** §1／§2／§8 標題自稱「已修」，本清單照抄，未實證。
- **沒有反向掃 `docs/pipelines/`**（2923 行），所以不知道那邊有沒有東西該搬出來。
- **沒有給刪／併／瘦身的建議**，那是下一步。

## 8. 執行進度

| 日期 | 做了什麼 |
|---|---|
| 2026-08-25 | `pipeline-slicing.md` 的「洞 2」**再修一次**。原寫「那 4 張來源表沒有版本層，回補之後下游 partition 不會知道自己過期」——使用者指出 dataset pipeline 是 snap_date-aware，新增月份必定被處理，這個說法沒抓到重點。查證後改寫：真正的根因是 **`base_dataset_version` 不是內容雜湊**（`design-principles.md`「版本 ID 不代表來源資料內容完全相同」明列排除「同一 partition 被回補後的資料差異」），而月份判準只問 partition 在不在（`month_plans.py::plan_incremental_snap_dates`，該 docstring 自稱 `--rebuild-dates` 是 "the escape hatch for upstream backfill"）。並明說**新增月份不受影響**，出事的只有「既有月份、內容變了」。教訓：照抄一個來源（這次是程式碼的警語字串）而不追一次 code path，錯誤會被文件放大。 |
| 2026-08-25 | `pipeline-slicing.md` **修一處錯誤事實**並移入 `user-guides/`（72 行）。錯誤：原文寫「`recsys_prod_train_keys` 這類覆寫式表沒有版本層」——實查 `conf/base/catalog.yaml:104-115`，該 dataset 的 `partition_filter` 同時濾 `base_dataset_version` 與 `train_variant_id`；掃全 catalog 得 **22 個 `HiveTableDataset` 中 18 個 pipeline 產物全部帶版本 filter**，沒帶的 4 個是 source_etl 唯讀來源表。catalog 開頭明文「Hive 表為固定名稱，版本以 partition_filter 的 partition column 區分」，所以「表名沒版本 ⇒ 沒有版本層」的推論本身錯。**根源是程式碼**：`src/recsys_tfb/__main__.py:190` 無條件印的警語括號寫 `overwrite-style Hive tables are not version-stamped`，文件照抄了它。改寫為「版本擋得住什麼、擋不住什麼」，真正擋不住的兩件＝(a) 版本 hash 涵蓋 config 不涵蓋 code、(b) 4 張無版本來源表被回補。移入 `user-guides/` 後與 `adding-an-eval-month.md` 同層，互相連結變短；anchor `#先分清楚模式不是切片` 仍成立。 |
| 2026-08-25 | **兩份「尷尬檔」處理完**。①`pipeline-slicing.md` 79→55 行：診斷出它變成「殘料檔」（5 節裡 2 節用「別處沒有」命名），且 `CLAUDE.md` 指標承諾了文件明說沒有的旗標用法。拆法——§開發守則 → `docs/agents/pipeline-node-design.md` 新增 **G7**（跟 G1 是同一個決定的兩面，原本散在兩檔；順便補進節三 checklist，那裡原本只問「撈不撈得出來」、沒問接續成本）；§已知設計決議 2 條 ＋ manifest artifacts → `training.md` §7.4／§6.1；剩 3 節改名為正向主題「模式與切片」，並重寫 `CLAUDE.md` 指標。anchor `#先分清楚模式不是切片` 保持不變。②`training-oom-object-matrix.md` **三分後刪檔**：現役排錯 → `dataset.md` §8 表補一列 ＋ 新增 §8.1（B6 是 dataset 的閘，但那張排查表原本沒有這條）與 `training.md` 新增 §9.1（觀測陷阱＋降峰值選項表）；事故調查過程 → `docs/notes/2026-07-11-training-oom-investigation.md`（147 行）。刪除前跑 30 個關鍵字對照確認每一塊都有落腳處。4 個引用改指新位置。 |
| 2026-08-25 | `diagnosis-report-presentation.md` → `docs/agents/`。同時**收掉一組重複**：`deliberate-non-goals.md:66` 原本既是指標又抄了一份禁用字清單（`severity`／`verdict`／「該先查誰」／「偏高低」），現在收成單一指標＋正向敘述（「給數字、對照點與範圍說明，把角度攤開讓讀者自己選」），禁用字的唯一真實來源回到被指的那份。搬進同目錄後指標從 `docs/operations/…` 縮成 `diagnosis-report-presentation.md`。另修 `domain.md` 的目錄樹（`agents/` 原寫「本系列 skill 的 per-repo 設定」與實況不符——它裝著 `architecture-constraints.md` 347 行與 `pipeline-node-design.md` 215 行的設計判準；`operations/` 補上兩個新子目錄）。`_render.py:7` 用裸檔名引用，不受影響。 |
| 2026-08-25 | **修掉全部既有斷連**（22 處編輯，與本次重整無關的舊帳）。兩個根因都在 commit `203459f`（2026-06-14）：① `docs/behavior-diagrams.html` 被刻意刪除（「移除過時的」）但 2 個引用沒清；② `metrics.html` 從 `docs/` 純搬進 `docs/metrics/`（diffstat 顯示 `0` 行變動），裡面 6 個相對連結全部少一層 `../`。另修：README 的 6 個 gbdt 連結（檔案在 `handbooks/gbdt/` 子目錄）、`evaluation.md` 的裸相對連結、4 份 `*_offline.html` 互連改指同目錄的 offline 版（原本指向上一層的 `.md`，違背「無網路直接開啟」的用途）。驗收：全 repo `.md` ＋ `.html` 斷連 **0**。 |
| 2026-08-25 | ②開發環境三份（`local-spark-setup` / `worktree-venv-setup` / `spark-connection-architecture`）移入 `docs/operations/dev-setup/`。修 9 處引用：README ×4、CLAUDE.md ×2、known-pitfalls ×1、user-guides/sampling-overrides-editor ×1、`conf/spark-local/spark-defaults.conf` ×1。三份之間的互連是同層、一起搬所以未動。**repo 外**還有一處：`~/.claude/skills/local-spark/SKILL.md:8`（不在版控內，需另外同步）。 |
| 2026-08-25 | `pipeline-slicing.md` 評估後**不刪、瘦身**（104→79 行）。逐節查證：§使用完全重複（5 份 pipeline 文件的 §4 都有）、§自動擴張前半重複（`dataset.md:301`）、§限制 5 條中 3 條重複；砍掉。留下 4 塊 `docs/pipelines/` **零覆蓋**的內容——§模式 vs 切片（ADR-0013 在 pipelines/ 零命中）、dataset 增量產物的月份判準（ADR-0012 零命中）、§開發守則（`RESUME_CONTRACTS` 跨 pipeline 判準）、以及 2 條限制＋2 條設計決議。順帶解掉一個因瘦身而產生的循環引用（`design-principles.md:316` ↔ 本檔）。`#先分清楚模式不是切片` 這個 anchor 的標題一字未動。 |
| 2026-08-25 | `training-oom-object-matrix.md` **拆兩份**。教學部分（numpy 一格一種大小、置物櫃比喻、每格成本對帳、實測方法、`nbytes` 說謊、pandas 轉換暫態、降峰值手法光譜）抽成 `docs/handbooks/spark-tuning/_drafts/spark-to-pandas-numpy-memory.md`（187 行草稿，含〔待補〕清單）。原檔留在 operations/、266→217 行，只留現役排錯；§3 濃縮成 4 段但保留節號，內部交叉引用（§1→§6、§4→§7、§6→§5）全部沒壞。4 個現役引用一個都沒動。 |
| 2026-08-25 | `hpo-resume.md` 刪除（`git rm`）。刪除前先把 2 件 `training.md` 沒有的內容併進去（§4.7 接續 log、§7.3 `_hpo/` 清理政策）。5 處引用改指 `training.md`：README、CLAUDE.md、design-principles、pipeline-slicing、`conf/base/parameters_training.yaml` 註解。`docs/superpowers/` 的存檔引用刻意不動。 |
| 2026-08-25 | `adding-an-eval-month.md`、`sampling-overrides-editor.md` 移入 `docs/operations/user-guides/`（`git mv`，歷史保留）。連帶修 26 處 inbound 連結（含 `scripts/sampling_overrides_editor.py` 內嵌在產出 HTML 的路徑、`scripts/rebuild_eval_month.sh`、`conf/base/parameters_training.yaml` 註解）＋ 20 處 outbound 相對路徑。 |

子目錄命名慣例：`user-guides/` ＝ ①使用者，`dev-setup/` ＝ ②開發環境。③工作紀律不設子目錄，整批去 `docs/agents/`。

**`docs/superpowers/` 的存檔一律不改路徑。** 那些是已結案的 plan／spec，記錄的是「當時做了什麼」；把裡面的路徑更新成今天的位置會讓歷史紀錄說謊（例如某 plan 寫「Create: `docs/operations/local-spark-setup.md`」——那在當時是對的）。因此本次搬檔造成的存檔內連結失效是**刻意接受**的。
