# README 重構設計(整體架構)

> 狀態:架構定案,內容待逐節建置。
> 日期:2026-06-03
> 範圍:重寫 `README.md` 與配套 `docs/` 文件集的「整體架構」。各文件的細部內容於建置該節時再逐一設計(尤其 data-lineage)。inference pipeline 相關內容本輪先略過。

---

## 0. 核心原則(凌駕本文件其餘所有便利性考量)

**正確性一律以程式碼為準。** 撰寫任何技術敘述前,先讀對應原始碼(`src/recsys_tfb/`、`conf/`);必要時直接檢視實際資料(parquet / Hive 表)確認欄位、型別、範例值。**絕不從註解、變數/函式命名、或舊文件描述臆測行為。** 凡敘述與程式碼/資料牴觸,以程式碼/資料為準。

## 0.1 編寫原則(隨使用者回饋累積,避免重犯)

> 每當使用者糾正,就把該意見**抽象成通用原則**追加於此,並回頭套用到已寫內容。

- **WP1 用語對齊 schema 角色抽象。** 欄位 / 概念優先用 schema 角色(`time / entity / item / label / score / rank`)表述;要用具體情境欄名(如 `prod_name`)時必須明示對應(「以產品推薦為例,`item = prod_name`」),不可把具體欄名當成框架固有。
- **WP2 可配置選項不可講死成單一立場。** 陳述設計前先查 config / 程式碼,確認該點是「固定設計」還是「可配置選項」。可配置的(如訓練 `objective`:pointwise `binary` / `lambdarank` / `rank_xendcg`)要呈現為「可選、預設 X」,不可宣稱專案「就是 X、不是 Y」。(本案首犯:把 objective 猜成 LTR——正是 §0 禁止的「從名稱臆測」。)
- **WP3 定位對齊真實適用範圍。** 本專案是「**通用排序問題**的架構與方法」,產品推薦只是**示例情境**;敘述不可把專案窄化成單一應用。
- **WP-meta 先抽象再具體。** 先講框架 / 抽象(排序問題、schema 角色),再給具體例子(銀行產品推薦)。呼應 `feedback_general_before_specific`。
- **WP4 README 要一目瞭然,結構化而非散文牆。** 善用標題、列點、表格、圖示、連結,讓人快速掃讀;深入細節連到其他文件,不在 README 展開。用最適合該內容的形式:schema / 對應關係用**表格**、流程用**圖**、並列要點用**列點**。兩個都要避免的失敗模式:(1)名詞短句 / 關鍵詞**堆疊**——讀起來卡、要自己腦補;(2)大段**散文牆**——README 不該是論文。每個列點或表格欄位都要是清楚、完整到能讀懂的短陳述(見 WP6)。
- **WP5 節制括號與旁白。** 盡量不用括號夾註;必要的補充併進句子裡。刪除對話殘留 / 自言自語式註記(例如「(守 decision C)」「(見上方)」「(離線:…)」這類寫給自己看的話)。
- **WP6 簡要 ≠ 簡略;精準要兼顧描述性。** 不要為了精簡而抽象到讀者得自行補語境。把概念用具體、完整的文字交代清楚;簡短的同時仍要讓人一讀就懂。
- **WP7 指向使用者實際會動的配置檔。** 面向使用者的「在哪裡設定」一律指向 `conf/.../parameters*.yaml` 等配置檔(使用者改配置、不動 code);只有在說明內部實作時才指程式碼模組。例:schema 角色說「在 `conf/base/parameters.yaml` 的 `schema` 區塊設定」,而非「定義於 `core/schema.py`」。
- **WP8 示例慣例 ≠ 框架固有。** 示例情境的具體慣例或取值(月底、每月、特定欄名)不可寫成框架的固有定義。通用描述保持中性(如 `snap_date` 是「快照日」,不預設頻率),特定慣例只在明確標示的示例脈絡下出現。呼應 WP1 / WP3。
- **WP9 中文敘述用全形標點。** 中文句子的標點(,。、:;「」()等)一律全形;程式碼、檔名、英文與數字內維持半形。
- **WP10 只提對使用者有操作意義的細節;內部代號用意義描述。** 省略非必要的內部工具 / 實作名詞(例:DAG 不必特別點名是「Ploomber 編排」)。**內部代號(如一致性不變量 A1–A6、B1)一律用其意義描述,不直接寫代號**(例:「B1 資料閘」→「資料一致性檢查」)。判準 —— 這個細節會影響使用者怎麼設定或執行嗎?不會就不放進面向使用者的段落。
- **WP11 結構 / 流程 / lineage 圖優先用圖形,而非 ASCII。** 凡是有結構的圖(pipeline 總覽、資料 lineage),用 draw.io / excalidraw / dbt 風格的**圖形 SVG**,比 ASCII 好讀好看;README 可內嵌 SVG。ASCII 只留給最簡單的行內示意。

## 1. 背景與目標

現有 `README.md` 與 `docs/` 已能用,但要從零重新規劃結構與敘事。本設計只鎖定**整體資訊架構**:哪些內容放 README、哪些拆成 docs/ 主題檔、各檔單一職責、格式慣例與內容來源規則。各檔的細部內容(章節措辭、圖的細節、data-lineage 的 schema/範例)刻意延後到建置該部分時再談,以符合「先定架構、再一部分一部分動手」的節奏。

**成功標準**:一位熟 SQL/Python、做過產品回應(二元分類)模型、但不熟排序問題的資料科學家,能靠這套文件(1)看懂這專案在解什麼問題、(2)把標準週期的 pipeline 跑起來、(3)知道要深入某主題時該讀哪一份文件。

## 2. 目標讀者

- 資料科學家,熟 SQL、會用 Python 做資料分析。
- 過去有產品回應模型建置經驗(熟二元分類)。
- 可能略懂電商推薦系統概念。
- **對排序(ranking / learning-to-rank)問題不熟**——這是讀者最關鍵的知識落差,但本套文件**不自寫排序教學**,概念與數學交給既有 GBDT 手冊系列 + metrics 文件,以連結帶過(見 §6)。

## 3. 已定案的決策

| # | 決策 | 結論 |
|---|---|---|
| IA | 資訊架構 | **薄 README 入口 + docs/ 主題檔**(README 只放「看懂 + 跑起來」;深入主題各自成檔;README 末尾用「建議閱讀順序」當導覽) |
| 教學 | 排序概念怎麼處理 | **不自寫教學,靠連結帶**。README「應用情境」只說明這是排序模型;概念/數學連到根目錄的 `gbdt_*` 手冊與 `docs/metrics.html` |
| 舊檔 | 與新結構重疊的現有 docs | **全新從程式/設定重寫**(不信舊文字描述,實際對照 `src/`、`conf/`);舊 docs 暫留,日後驗證過再決定刪除 |
| 圖 | 渲染風格 | **混用**:重圖(data-lineage、behavior-diagrams、metrics)用 **A 自包式 HTML/SVG**(離線可開、樣式可控,呼應現有 `metrics_concept_map.html`);README 內輕量流程用 **C ASCII 方框**(隨檔可讀、好維護) |
| D1 | FAQ + 常見錯誤放哪 | **留在 README**(可見性高) |
| D2 | per-pipeline 深入文件 | **4 個獨立檔** `docs/pipelines/*.md` |
| D3 | 定位 / 設計原則 / 功能特色 | **併成 `docs/design-principles.md`** |

## 4. 整體架構

### 4.1 `README.md` 章節大綱(薄入口)

```
§0 這是什麼            ← 定位濃縮成一段
§1 應用情境
   • 要解決的問題
   • 限制條件
   • 輸入 / 輸出資料長相   (摘要;完整 schema 連到 docs/data-lineage.html)
§2 快速上手
   • Pipeline 總覽         (C ASCII 圖)
   • Data lineage 總覽     (C ASCII;細節連到 docs/data-lineage.html)
   • 各 pipeline:一句話 + 指令
       source_etl · dataset · training · evaluation
       (每條一句話講用途;深入連到 docs/pipelines/<name>.md)
   • 執行 commands 速查表
§3 FAQ
§4 常見錯誤
§5 文件地圖 + 建議閱讀順序   (導覽 docs/)
```

註:`§2 各 pipeline` 在 README 只給「一句話用途 + 可複製指令」;行為/設定細節在對應的 `docs/pipelines/<name>.md`。

### 4.2 `docs/` 檔案集(各檔單一職責)

| 檔案 | 職責 | 格式 | 對應原始大綱 |
|---|---|---|---|
| `docs/data-lineage.html` | 完整資料流圖 + 每張表的 schema + 範例列 | **A** 自包式 HTML | 快速上手 · data lineage 總覽(深入) |
| `docs/pipelines/source_etl.md` | source_etl 行為/設定深入 | Markdown | 快速上手 · source_etl(深入) |
| `docs/pipelines/dataset.md` | dataset 行為/設定深入 | Markdown | 快速上手 · dataset(深入) |
| `docs/pipelines/training.md` | training 行為/設定深入 | Markdown | 快速上手 · training(深入) |
| `docs/pipelines/evaluation.md` | evaluation 行為/設定深入 | Markdown | 快速上手 · evaluation(深入) |
| `docs/design-principles.md` | 定位深入 · 設計原則 · 功能特色 | Markdown | 專案細節 · 定位/原則/特色 |
| `docs/change-guide.md` | 增加 feature / product / schema / training 設定的修改 SOP | Markdown | 專案細節 · 修改指引 |
| `docs/behavior-diagrams.html` | 程式行為說明與圖解 | **A** 自包式 HTML | 專案細節 · 程式行為圖解 |
| `docs/metrics.html` | metrics 說明與釋疑(概念圖 + 程式實算什麼) | **A** 自包式 HTML | 專案細節 · metrics |

「建議閱讀順序」不另立檔,放在 README §5 作為 docs/ 導覽。

### 4.3 保留並從新文件連結的既有資產(不重寫)

- **排序教學連結目標**(repo 根目錄):`gbdt_binary_classification`、`gbdt_class_imbalance`、`gbdt_multiitem_imbalance`、`gbdt_learning_to_rank`(各有 `.md` + `_offline.html`)。
- **開發/環境 SOP**(與「專案在做什麼/怎麼跑」正交,被 `CLAUDE.md` 大量引用):`docs/worktree-venv-setup.md`、`docs/spark-connection-architecture.md`。
- **手冊寫作風格指南**(維護手冊時參考,非本套讀者主線):`docs/handbook-writing-guide.md`。

## 5. 格式與撰寫慣例

- **重圖一律 A 自包式 HTML/SVG**:單檔、無外部 JS 相依、離線點兩下即開。理由:呼應現有 `metrics_concept_map.html` 與手冊 `_offline.html`;離線可讀勝過 Mermaid(離線只剩原始碼)。
- **結構 / 流程 / lineage 圖優先用圖形 SVG**(draw.io / excalidraw 風格的 pipeline 圖、dbt 風格的 lineage 圖),README 可內嵌;ASCII 只留給最簡單的行內示意(見 WP11)。
- **散文用 Markdown**。
- 語言:繁體中文(對齊 repo 既有文件與使用者偏好)。

## 6. 內容來源與正確性規則

- **落實 §0 核心原則**:每條技術敘述都從原始碼判斷,必要時看實際資料(parquet / Hive)佐證;不從註解/命名/舊文件臆測。
- 與新結構重疊的內容**一律從程式/設定重新推導**,實際對照 `src/recsys_tfb/`、`conf/`,不沿用舊 docs 的文字描述(舊描述可能 stale)。
- 排序概念/數學**不在本套文件重寫**,以連結指向 §4.3 的 `gbdt_*` 手冊與 `docs/metrics.html`。
- 舊 docs/ 重疊檔(`metrics.md`、`metrics_concept_map.html`、`pipeline-runbook.md`、`change-sop.md`、`config-and-versioning.md`)**暫時保留不動**;待新文件逐節驗證通過後,再於收尾時決定刪除/取代。

## 7. 範圍邊界與延後項目

- **inference pipeline**:本輪略過(使用者尚未 review)。README §2 與 data-lineage 圖會標示 inference 階段但留待後續補。
- **各文件細部內容**:本設計只定架構。每份文件的章節措辭、圖的細節、`docs/data-lineage.html` 的表卡深度/涵蓋範圍(完整表卡 vs 分層詳略),於建置該檔時再逐一設計與確認。
- **舊檔刪除**:延到全部新文件驗證通過後的收尾步驟。

## 8. 建置、驗證與審核流程(一部分一部分動手)

### 8.1 漸進式建置順序(每一步各自成可獨立 review 的單位)

1. `README.md` 骨架 + §0 這是什麼 + §1 應用情境(輸入/輸出先給摘要)。
2. README §2 快速上手(Pipeline 總覽 ASCII、各 pipeline 一句話 + 指令、commands 速查)。
3. README §3 FAQ + §4 常見錯誤 + §5 文件地圖。
4. `docs/data-lineage.html`(屆時再細談表卡深度與涵蓋範圍)。
5. `docs/pipelines/{source_etl,dataset,training,evaluation}.md`。
6. `docs/design-principles.md` + `docs/change-guide.md`。
7. `docs/behavior-diagrams.html` + `docs/metrics.html`。
8. 收尾:校對交叉連結、決定舊 docs 去留。

### 8.2 文件驗證:以 subagent 扮演評審,驅動修訂

每完成一個建置單位後,派 subagent 從不同角色檢視,依回饋修訂,反覆到通過:

1. **目標讀者 persona** — 以 §2 讀者身份(熟 SQL/Python、做過二元分類產品回應模型、不熟排序)通讀,只問:好不好懂?哪裡太行話 / 跳步 / 不夠白話?要求**具體指出**卡住的句子,而非籠統好評。
2. **照做執行者(follow-the-doc executor)** — 嚴格「只照文件寫的」去做,不准腦補。**起始狀態 = 已配置好的本機 dev-cluster + 合成來源表**(`scripts/setup_hive_dev.py` 把 `data/*.parquet` 載入成 Hive `ml_recsys.*`)。**從 `conf/.../parameters.yaml` 起,設自己的 `hive.db` 與情境,依 README 實跑**:`dataset → training → evaluation` 三條(dev 跑得動)真的跑過合成資料;`source_etl` 因 dev 跳過(合成資料已是 feature/label 粒度)改做**設定級核對**並標出 dev/prod 落差。回報:哪一步卡住、哪裡有文件沒寫卻必須知道的隱性步驟、指令 / 路徑 / 設定是否一致。Spark 連線依 `CLAUDE.md` 的 SPARK_CONF_DIR 對應表。
3. **正確性稽核** — 對照原始碼 / 實際資料,逐條查文件技術敘述是否與實作相符(落實 §0 核心原則)。

修訂迴圈:產出 → 三角色檢視 → 修訂 → 必要時再檢視。

**同步審核機制(使用者要求)**:驗證 subagent 一律以**獨立 subagent** 執行(保留不被既有程式知識汙染的視角);但 subagent 內部步驟不會直接顯示給使用者,故每個驗證 subagent 被要求**每一步即時 append 到執行日誌檔** `/<repo-root>/.superpowers/exec-journal-<task>.md`(絕對路徑),使用者可即時 `tail -f` 同步審核;主控在檢查點把日誌 render 到視覺 companion。此日誌是同步可視的唯一管道,不可省略。

### 8.3 計畫與審核的呈現

- 後續 writing-plans 產出的實作計畫,在適合處用 **HTML / 圖(視覺 companion)** 呈現以便審核,而非純文字牆。
- 各建置單位的成品(尤其 ★A 自包式 HTML 圖)也透過視覺 companion 或直接開檔讓使用者審核。
- **驗證過程**透過 §8.2 的即時執行日誌讓使用者同步審核(`tail -f` + 檢查點 render 到 companion)。

## 9. 附錄:資產盤點(建置時的事實依據)

### 9.1 來源層真實 schema(取自合成資料,欄名即資料合約)

- **`feature_table`**(每客戶每月特徵寬表,主鍵 `snap_date, cust_id`):`snap_date`(date/time)、`cust_id`(string/entity)、加 ~22 個特徵欄(`total_aum`、`fund_aum`、`ccard_*`、`age`、`gender`、`risk_attr`、`education_level`、`marital_status`、`channel_preference`、`income_level`、`tenure_months` …)。
- **`label_table`**(ground truth,主鍵 `snap_date, cust_id, prod_name`):`snap_date`、`cust_id`、`prod_name`(item)、`label`(0/1)、`apply_start_date`、`apply_end_date`。
- **`sample_pool`**(抽樣母體):`snap_date`、`cust_id`、`prod_name`、`label`、加分群欄 `cust_segment_typ`、`tenure_months`、`channel_preference`(`sample_group_keys`)。

### 9.2 Schema 角色合約(`src/recsys_tfb/core/schema.py`)

`time=snap_date`、`entity=[cust_id]`、`item=prod_name`、`label=label`、`score=score`、`rank=rank`;`identity_columns = [time] + entity + [item]`。`schema.categorical_values.prod_name` 是 item 宇集的單一真實來源(現含 8 個產品代號:`ccard_bill/ccard_cash/ccard_ins/exchange_fx/exchange_usd/fund_bond/fund_mix/fund_stock`)。

### 9.3 資料表清單與三層版本(`conf/base/catalog.yaml`)

- **來源層(source_etl 維護,唯讀)**:`feature_table`、`label_table`、`sample_pool`。
- **dataset · base 層**(`base_dataset_version`):`preprocessor`、`category_mappings`、`preprocessed_feature_table`、`val_keys`、`test_keys`、`val_model_input`、`test_model_input`。
- **dataset · train-variant 層**(`train_variant_id`):`sample_keys`、`train_keys`、`train_dev_keys`、`train_model_input`、`train_dev_model_input`。
- **dataset · calibration 層**(`calibration_variant_id`):`calibration_keys`、`calibration_model_input`。
- **training**(`model_version`):`model.txt`(+ calibrator)、`best_params`、`evaluation_results`、`diagnostics/*`、`training_eval_predictions`(Hive)。
- **evaluation**:`enriched_eval_predictions`(Hive)、`report.html`。
- **inference(本輪略過)**:`score_table`、`ranked_predictions`。
