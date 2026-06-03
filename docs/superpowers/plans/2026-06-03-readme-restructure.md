# README 重構 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重寫 `README.md` 為薄入口,並建立 `docs/` 主題檔集,讓不熟排序問題的資料科學家能看懂並照著跑。

**Architecture:** 薄 `README.md`(看懂 + 跑起來)+ `docs/` 各檔單一職責(深入);重圖用自包式 HTML(A)、README 輕量流程用 ASCII(C);排序概念靠連結到既有 `gbdt_*` 手冊與 `metrics.html`。

**Tech Stack:** Markdown、自包式 HTML/SVG;內容一律從 `src/recsys_tfb/`、`conf/` 與實際資料推導。

**設計依據:** `docs/superpowers/specs/2026-06-03-readme-restructure-design.md`(尤其 §0 核心原則、§4 架構、§8 流程)。

---

## 通用協定(每個 Task 都適用)

### P0 — 正確性(§0 核心原則,凌駕一切)
撰寫任何技術敘述前,**先讀對應原始碼**;必要時用 venv python 讀實際 parquet/Hive 確認欄位、型別、範例值。**絕不從註解/命名/舊文件臆測。** 衝突時以程式碼/資料為準。

讀 parquet 的標準作法(worktree 內):
```bash
/Users/curtislu/projects/recsys_tfb/.venv/bin/python -c "import pandas as pd; df=pd.read_parquet('/Users/curtislu/projects/recsys_tfb/data/<t>.parquet'); print(df.dtypes); print(df.head())"
```

### P1 — 生產 vs 合成資料的分野(每個 Task 都要守)
- **生產規模數字**(~10M 客戶、22 產品、~1500 特徵、每週批次):來自專案知識/`CLAUDE.md`,**非**合成資料可推得。敘述生產情境用這些數字,並標明「範例以合成資料示意」。
- **合成資料**(repo 內 `data/*.parquet`):8 個 `prod_name`、~22 特徵欄。所有**範例列**取自此,且明確標示為合成。
- 兩者不可混淆(例:別說「本 repo 有 22 產品」——實際合成只有 8)。

### P2 — 文件 TDD:三角色 subagent 驗證(取代單元測試)
每個 Task 草稿完成後,**依序**派三個 subagent(general-purpose,worktree 內),收集回饋後修訂,必要時重跑:

1. **目標讀者 persona** — prompt 要點:「你是熟 SQL/Python、做過二元分類產品回應模型、但**不熟排序問題**的資料科學家。只讀這份 `<file>`(不准看原始碼)。逐段回報:(a) 哪句看不懂/太行話;(b) 哪裡邏輯跳步;(c) 哪裡不夠白話。具體引用句子,不要籠統好評。」
2. **照做執行者** — prompt 要點:「**只照 `<file>` 寫的**去做,不准腦補。實際驗證每個指令/路徑/設定是否存在且一致(可跑的輕量檢查就跑,重量級 Spark 全跑不要跑、改靜態核對命令與產物路徑)。回報:哪一步卡住、哪裡有文件沒寫卻必須知道的隱性步驟。」
3. **正確性稽核** — prompt 要點:「對照 `<source files>` 與實際資料,逐條查 `<file>` 的技術敘述是否與**程式碼/資料**相符。落實 §0:不接受以註解/命名為依據的敘述。列出每個不符點 + 程式碼出處(file:line)。」

### P3 — 視覺審核
每個 Task 修訂後,把成品(HTML 直接開檔;Markdown 渲染重點或截要)透過視覺 companion 呈現給使用者審核,通過再 commit。

### P4 — Commit(在 feat 分支)
```bash
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/readme-restructure add <files>
git -C /Users/curtislu/projects/recsys_tfb/.worktrees/readme-restructure commit -m "<msg>"
```
路徑一律用 worktree 絕對路徑(`.worktrees/readme-restructure/...`)。

---

## Task 1: README 骨架 + §0 這是什麼 + §1 應用情境

**Files:**
- Create: `.worktrees/readme-restructure/README.md`(先建骨架 + §0/§1)

**Source of truth(先讀,P0):**
- `src/recsys_tfb/__main__.py`(CLI 入口、有哪些 pipeline 指令、`--env`/`--target-dates`)
- `src/recsys_tfb/pipelines/`(各 pipeline 目錄)與 pipeline registry
- `conf/base/catalog.yaml`(輸出 `score_table`/`ranked_predictions` 的 schema = 輸出長相)
- `src/recsys_tfb/core/schema.py` + `data/{feature_table,label_table,sample_pool}.parquet`(輸入長相)

- [ ] **Step 1:** 讀上述來源,列出事實清單(pipeline 清單、輸入/輸出 schema、產品數;區分生產 22 vs 合成 8,守 P1)。
- [ ] **Step 2:** 寫 README 骨架(§0–§5 標題 + 一句話佔位)與 §0「這是什麼」(定位濃縮一段)。
- [ ] **Step 3:** 寫 §1 應用情境:`要解決的問題`(排序/推薦優先順序,排序概念連到 `gbdt_learning_to_rank`)、`限制條件`(no UDF/no network/CPU-only,源自 `CLAUDE.md` 與 spark conf)、`輸入/輸出資料長相`(摘要表 + 連 `docs/data-lineage.html` 佔位連結)。
- [ ] **Step 4:** 跑 P2 三角色驗證(`<file>`=README,§0/§1 範圍);修訂。
- [ ] **Step 5:** P3 視覺審核。
- [ ] **Step 6:** P4 commit:`docs(readme): 骨架 + §0 定位 + §1 應用情境`。

---

## Task 2: README §2 快速上手

**Files:**
- Modify: `.worktrees/readme-restructure/README.md`(§2)

**Source of truth(P0):**
- `src/recsys_tfb/__main__.py`(每個 pipeline 的指令名、flag;**逐字**核對,別憑記憶)
- `src/recsys_tfb/pipelines/{source_etl,dataset,training,evaluation}/pipeline.py`(各 pipeline 做什麼)
- `conf/base/catalog.yaml` + dataset/training nodes(data lineage 總覽)

- [ ] **Step 1:** 從 `__main__.py` 抽出**真實可複製指令**(含 `--env`、`--target-dates` 等;確認無 `run` 子指令、無 `--pipeline`)。
- [ ] **Step 2:** 寫 `Pipeline 總覽`(C ASCII 流程圖:source_etl → dataset → training → evaluation;inference 標示略過)。
- [ ] **Step 3:** 寫 `Data lineage 總覽`(C ASCII 精簡版 + 連 `docs/data-lineage.html`)。
- [ ] **Step 4:** 寫 `各 pipeline:一句話 + 指令`(四條,每條一句用途 + 指令 + 連 `docs/pipelines/<name>.md` 佔位)。
- [ ] **Step 5:** 寫 `執行 commands 速查表`。
- [ ] **Step 6:** P2 驗證——**照做執行者**此處特別重要(逐一驗證每個指令字面正確、pipeline 名存在);修訂。
- [ ] **Step 7:** P3 視覺審核 → P4 commit:`docs(readme): §2 快速上手(pipeline 總覽 + 指令)`。

---

## Task 3: README §3 FAQ + §4 常見錯誤 + §5 文件地圖

**Files:**
- Modify: `.worktrees/readme-restructure/README.md`(§3/§4/§5)

**Source of truth(P0):**
- `src/recsys_tfb/core/consistency.py`(A1–A6 一致性閘 → 常見設定錯誤訊息)、`preprocessing/_spark.py`(B1 資料閘 `DataConsistencyError`)
- `CLAUDE.md`、`docs/spark-connection-architecture.md`、`docs/worktree-venv-setup.md`(常見環境錯誤;但敘述以程式碼/實際錯誤為準)

- [ ] **Step 1:** 蒐集真實會 fail-loud 的錯誤(一致性閘訊息、schema 錯誤、env 連線錯誤),每條附「症狀→原因→解法」。
- [ ] **Step 2:** 寫 §3 FAQ(讀者最可能問的:這跟分類模型差在哪→連手冊;為何要分 train/cal/val/test;model promote 怎麼做)。
- [ ] **Step 3:** 寫 §4 常見錯誤(取真實錯誤訊息字串,可被搜尋)。
- [ ] **Step 4:** 寫 §5 文件地圖 + 建議閱讀順序(導覽 docs/ 各檔 + 既有 `gbdt_*` 手冊)。
- [ ] **Step 5:** P2 驗證 → 修訂 → P3 視覺審核 → P4 commit:`docs(readme): §3 FAQ + §4 常見錯誤 + §5 文件地圖`。

---

## Task 4: docs/data-lineage.html(★A 自包式 HTML)

> 本 Task 開工前,先與使用者敲定 spec §7 延後的兩件事:**表卡深度**與**涵蓋範圍**(完整表卡 vs 分層詳略)。已有 mockup 可複用(`.superpowers/brainstorm/.../data-lineage-doc.html`)。

**Files:**
- Create: `.worktrees/readme-restructure/docs/data-lineage.html`

**Source of truth(P0):**
- `conf/base/catalog.yaml`(全表清單、partition、三層版本)
- `src/recsys_tfb/core/schema.py`(角色合約)
- `src/recsys_tfb/pipelines/dataset/nodes*.py`、`training/nodes.py`(各表如何產生、欄位)
- `data/*.parquet` 實際讀取(每張表的範例列;守 P1 標示合成)

- [ ] **Step 1:** 敲定深度/範圍(見上方 note)。
- [ ] **Step 2:** 從 catalog + nodes 建完整表清單與 schema(逐欄型別/角色,從程式碼與 parquet dtypes 核對)。
- [ ] **Step 3:** 讀實際 parquet 取範例列。
- [ ] **Step 4:** 寫自包式 HTML:分階段+版本層總覽圖(可錨點)+ 各表表卡。
- [ ] **Step 5:** P2 驗證——**正確性稽核**逐欄比對 catalog/parquet;修訂。
- [ ] **Step 6:** P3 視覺審核(直接開 HTML)→ P4 commit:`docs(lineage): 資料流 + schema + 範例(自包式 HTML)`。

---

## Task 5: docs/pipelines/{source_etl,dataset,training,evaluation}.md(4 個獨立檔)

> 四個子單位,各自獨立草稿+驗證+commit。每檔結構一致:用途 → 輸入/輸出表 → 節點流程 → 關鍵設定(parameters_*.yaml)→ 重跑語意 → 連結。

**Source of truth(P0,每檔對應):**
- 5a `source_etl`:`pipelines/source_etl/`、`conf/sql/etl/`、`conf/base/parameters_*_etl.yaml`、`HiveTableDataset`/`SQLRunner`
- 5b `dataset`:`pipelines/dataset/nodes*.py`、`select_keys`/前處理/`build_model_input`、三層 versioning(`core/versioning.py`)
- 5c `training`:`pipelines/training/nodes.py`(LightGBM adapter、calibration、cache、`training_eval_predictions`、diagnostics)
- 5d `evaluation`:`pipelines/evaluation/`(metrics_spark、baseline、report、segment、compare 模式)

- [ ] **5a Step 1–4:** 讀來源 → 草稿 `docs/pipelines/source_etl.md` → P2 驗證 → P3 → P4 commit。
- [ ] **5b Step 1–4:** 同上 `dataset.md`(特別交代三層版本與抽樣)。
- [ ] **5c Step 1–4:** 同上 `training.md`(driver-local artifact、cache 來源、SPARK_CONF_DIR 對應)。
- [ ] **5d Step 1–4:** 同上 `evaluation.md`(post-training vs monitoring vs compare 模式)。

---

## Task 6: docs/design-principles.md + docs/change-guide.md

**Files:**
- Create: `.worktrees/readme-restructure/docs/design-principles.md`
- Create: `.worktrees/readme-restructure/docs/change-guide.md`

**Source of truth(P0):**
- design-principles:`core/consistency.py`(A1–A6 + B1 不變量;唯一真實來源)、`core/versioning.py`、`models/`(ModelAdapter 抽象)、`io/handles.py`(ParquetHandle)、`core/catalog.py`
- change-guide:增加 feature(`conf` + source_etl SQL)、product(`schema.categorical_values` + consistency 連動)、schema、training 設定(`parameters_training.yaml`、HPO search space)各自要動哪些檔——從 consistency 不變量回推

- [ ] **6a:** 讀來源 → 草稿 `design-principles.md`(定位深入 + 設計原則 + 功能特色)→ P2 → P3 → P4 commit。
- [ ] **6b:** 讀來源 → 草稿 `change-guide.md`(逐情境 SOP,每步指出要改的檔與會觸發的閘)→ P2(**照做執行者**實走一個情境如「加一個 product」的靜態核對)→ P3 → P4 commit。

---

## Task 7: docs/behavior-diagrams.html + docs/metrics.html(★A 自包式 HTML)

**Files:**
- Create: `.worktrees/readme-restructure/docs/behavior-diagrams.html`
- Create: `.worktrees/readme-restructure/docs/metrics.html`

**Source of truth(P0):**
- behavior-diagrams:挑最反直覺的行為畫圖——分群抽樣、前處理 fit/transform 解耦、group-positive 過濾、calibration、三層 versioning;各自對照 `nodes*.py`
- metrics:`evaluation/metrics_spark.py`、`evaluation/metrics.py`、`report_builder`、`comparison/`;mAP/NDCG/per-item 的**程式實算**(逐行核對公式),概念連既有 `metrics_concept_map.html`/手冊

- [ ] **7a:** 讀 nodes → 草稿 `behavior-diagrams.html` → P2 → P3 → P4 commit:`docs(diagrams): 程式行為圖解(自包式 HTML)`。
- [ ] **7b:** 讀 metrics 程式 → 草稿 `metrics.html`(每個指標:程式實算什麼 + 輸出格式 + 報表分段 + 釋疑)→ P2(**正確性稽核**逐行對公式)→ P3 → P4 commit:`docs(metrics): metrics 說明與釋疑(自包式 HTML)`。

---

## Task 8: 收尾

**Files:**
- Modify: README + docs/*(交叉連結)
- 可能 Delete:舊重疊 docs(`metrics.md`、`metrics_concept_map.html`、`pipeline-runbook.md`、`change-sop.md`、`config-and-versioning.md`)——**需使用者拍板**

- [ ] **Step 1:** 校對所有交叉連結可達(README ↔ docs ↔ 手冊);**照做執行者**驗證連結與閱讀順序走得通。
- [ ] **Step 2:** 與使用者確認舊重疊 docs 去留(spec §7 延後項);決定後 `git rm` 或保留。
- [ ] **Step 3:** 全文件最終 persona 通讀(端到端「看懂 + 跑起來」)。
- [ ] **Step 4:** P4 commit:`docs: 收尾交叉連結 + 舊檔處理`;準備開 PR。

---

## Self-Review(對照 spec)

- **Spec coverage:** §4.1 README §0–§5 → Task 1–3;§4.2 各 docs 檔 → Task 4(lineage)/5(pipelines)/6(design+change)/7(diagrams+metrics);§4.3 既有資產連結 → Task 3 §5 + Task 8;§8.1 順序 → Task 1→8;§8.2 驗證 → 通用協定 P2;§8.3 視覺 → P3。涵蓋完整。
- **延後項對齊:** data-lineage 深度/範圍(spec §7)→ Task 4 開工 note;inference → 全程標示略過;舊檔去留 → Task 8 Step 2。
- **正確性:** §0 → P0/P1 + 每個 Task 的 Source of truth 清單 + P2 正確性稽核。
- **Placeholder scan:** 無 TBD;延後項都有明確 owner-step。
