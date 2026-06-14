# README 概念圖解（concept diagrams）設計

- 日期：2026-06-14
- 狀態：設計已與使用者確認，待 spec review
- 主題：為 README 製作少而精的英雄圖，讓新進使用者一目了然，並可截圖做報告

## 背景與問題

README 文字密度集中在 §0（資料模型）、§1（設計理念）、§2（五條 pipeline）。新進使用者很難從純文字「一看就懂」。先前一版提了 9 張小圖，使用者回饋兩個問題：**抽象層級不對**、**看不懂/不直觀**。經 brainstorming 釐清後重新定錨。

## 受眾與目標

- **受眾**：第一次讀 README 的新進使用者（不是主管摘要、不是接線圖）。
- **目標**：用少數幾張精緻圖，把 README 最核心、最難用文字「叮一聲就懂」的概念視覺化；同一份檔案可直接截圖做報告。
- **非目標**：不逐節覆蓋全 README；不取代文字；不畫 §4/§5 的設定/FAQ 內容。

## 已確認的設計決策（brainstorming 結論）

1. **抽象風格＝資料模型式（精確、結構化）**：帶 schema 角色、術語 OK；不是淡化術語的商業隱喻、也不是 before/after 敘事。（使用者於風格 A/B/C 選 B）
2. **少而精＝4 張英雄圖**，每張磨乾淨，其餘留給文字。
3. **交付＝獨立 house-style HTML**：新增 `docs/concept-diagrams.html`，對齊既有 `docs/behavior-diagrams.html`、`docs/metrics/metrics_concept_map.html` 的手刻 HTML/CSS 風格（**不用 Mermaid**——B 風格的巢狀框＋角色色塊 Mermaid 做不出來，且 repo 既有視覺文件都非 Mermaid）。README 從 §0 / §1 連結過去。
4. **硬規則：嚴格對齊 README，不外推、不杜撰**。每張圖在本 spec 附「來源對照」，每個文字/標註元素列出對應 README 的出處。凡對不回 README 原文者，只能是純結構示意（如 score 的示意數值），且需在圖上明確標註。

> 此規則來自使用者修正：先前我把「（與 C001 不互相比較）」「具體產品名」「只聯繫前 3 名」等 README 未提及的內容夾帶進圖中。

## 共用視覺系統（4 張一致）

- **角色色票**（沿用 `metrics_concept_map.html` 的色系基調）：
  - `time` `#1f6feb`、`entity` `#0f766e`、`item` `#ea580c`、`score` `#db2777`、`rank` `#334155`
  - pipeline/gate 補色：人工關卡（amber `#d97706`）、fail-fast/閘門（red `#dc2626`），僅 #2 使用。
- **版型**：house style——`-apple-system / PingFang TC` 字體、淺底、圓角卡片、左邊框 callout；自包含、離線可開、無外部相依、無 CDN。
- **每張圖固定含**：(a) 標題；(b) 一行「怎麼讀」；(c) 圖本體；(d) 角色色票圖例（如適用）；(e) 必要時的「示意/非 README」小字標註。
- **頁面結構**：單頁、4 個 `<section>`，每節錨點 `#fig-data-model` 等，供 README 連結。

## 4 張英雄圖（含來源對照）

### #1 核心資料模型（放 README §0 資料模型）
- **要呈現**：time × entity × item 顆粒度；query group = time+entity，是「一次排名的範圍」；組內比 score、由高到低給 rank；下游依名次決定優先順序。以一個展開的 query group（score 長條＋rank 名次）＋一個收合的「同一 time、不同 entity」示意組成。
- **來源對照**：
  - 「time × entity × item 顆粒度」← §0「基本資料顆粒度為 time × entity × item」
  - 「query group 由 time 與 entity 組成 / 一次排名與評估的範圍」← §0 該句＋角色表「query group ＝ 一次排名與評估的範圍 / 同一快照日的同一位客戶」
  - 「比較組內所有候選 item 的 score，再產生 rank」← §0「框架會比較該群組內所有候選 item 的 score，再產生 rank」
  - 「依分數由高到低」← §0「依分數由高到低產生排序結果」
  - 角色名 time=snap_date / entity=cust_id / item=prod_name / score / rank ← §0 schema 角色表
  - 「同一 time、不同 entity ＝ 另一個 query group」← 由「time × entity × item」與角色表「同一快照日的同一位客戶」結構導出（不含跨組比較的外推）
  - 下游 callout「供下游在有限資源下，依名次決定優先處理順序」← §0「供下游在有限資源下決定優先處理順序」
  - **示意（標註）**：item 用通用 `prod_A/B/C`、score 數值 0.92/0.85/0.10 為概念示意，非 README 既有資料。

### #2 五條 pipeline + 產物流（放 README §1 設計理念）
- **要呈現**：source ETL → dataset → training →〔人工 promote 關卡〕→ inference → evaluation 的巨觀流；標出三張來源表、各 pipeline 主要產物、以及「訓練不自動上線、需人工 promote 為 best」。
- **來源對照**：
  - 五個 pipeline 名稱 ← §1「拆分為 source ETL、dataset、training、evaluation 與 inference 五個 pipeline」
  - source ETL → 三張來源表 ← §2「整理成…三張來源表：feature_table、label_table、sample_pool」
  - dataset → `*_model_input` ← §2 dataset 段
  - training → `model_version`、不自動上線 ← §0「訓練完成不會自動將模型發布為 inference 預設版本」、§1「training 只產生版本化模型，不會自動發布…設為 best」
  - 人工 promote 關卡 ← §1「透過 scripts/promote_model.py 將核准版本設為 best，inference 才會預設使用該版本」
  - inference → `ranked_predictions`（經 validate→publish）← §0「發布至 Hive 表 ranked_predictions…通過 validate_predictions…才會由 publish_predictions 發布」
  - evaluation 讀預測＋label 出報表 ← §2 evaluation 段
- **示意（標註）**：無杜撰；箭頭僅表 README 已描述的依賴/產物關係。

### #3 版本化 4-hash 依賴鏈（放 README §1 設計理念）
- **要呈現**：base_dataset_version → train_variant_id →（可選）calibration_variant_id → model_version → best 的層疊依賴；各層由什麼決定；「改下層不必重建上層」；latest/best 語意。
- **來源對照**：
  - 「8 碼 hash、純執行/logging 不改版本」← §1「計算 8 碼 hash…純執行環境、logging 或監控類設定不會改變產物版本」
  - base_dataset_version 決定因素 ← §1 該 bullet（資料日期範圍、前處理、schema、feature_table 欄位名稱/型別/順序）
  - train_variant_id ← §1 該 bullet（抽樣比例、分層 override、train_dev_ratio）
  - calibration_variant_id ← §1 該 bullet
  - model_version ← §1 該 bullet（資料版本＋演算法參數、HPO、特徵選擇、機率校準、樣本權重）
  - 「只調整 train 抽樣時不必重建前處理器與 val/test」← §1 該句
  - latest / best 語意 ← §1「latest 代表最近成功產生的資料版本，best 則代表經人工核准…」

### #4 Kedro 風格 node 設計（放 README §1 設計理念，第一小節）
- **要呈現**：單一 DAG pipeline 內部——多個職責單一 node；node 的 in/out 宣告於 `pipeline.py`、依資料依賴決定順序；`catalog.yaml` 統一 I/O＝邏輯與 I/O 解耦；未入 catalog 的中間結果用 MemoryDataset 暫存、用完釋放；需重用/部分重跑者持久化。
- **來源對照**：
  - 「拆成 node、職責單一」← §1「每個 DAG pipeline 由多個職責單一的 node 組成」
  - 「in/out 宣告於 pipeline.py、依資料依賴決定執行順序」← §1 該句
  - 「catalog.yaml 統一 I/O、資料處理邏輯與 I/O 解耦」← §1「資料的讀寫方式、儲存位置與格式則統一設定於 catalog.yaml，使資料處理邏輯與 I/O 解耦」
  - 「MemoryDataset 暫存、最後一個下游 node 用完釋放」← §1「未在 catalog.yaml 設定的中間結果會以 MemoryDataset 暫存…釋放」
  - 「需跨次重用/部分重跑→catalog 持久化」← §1 該句
  - 「source ETL 由 SQL 驅動、其餘 DAG」← §1「source ETL 由 SQL 流程驅動，其餘 pipeline 採用 Kedro-inspired 的 DAG 設計」

## 檔案與整合

- 新增 `docs/concept-diagrams.html`（單檔、自包含）。
- README 連結：
  - §0「資料模型」附近加一行指向 `concept-diagrams.html#fig-data-model`。
  - §1「主要設計理念」開頭加一行指向 `concept-diagrams.html`（涵蓋 #2/#3/#4）。
  - §6 文件表新增一列「核心概念圖解 → concept-diagrams.html」（待 review 決定是否加）。
- 不動 README 既有文字內容，只加連結行（最小侵入）。

## 驗收標準

- 4 張圖在離線（無網路、無 CDN、`file://`）下可正常開啟與渲染。
- 視覺風格與 `behavior-diagrams.html` / `metrics_concept_map.html` 一致（字體、配色、卡片、callout）。
- **每個文字/標註元素都能對回本 spec 的「來源對照」**；純結構示意（item 佔位名、score 數值）已在圖上標註。
- README 連結可正確跳到對應錨點。
- 角色色票在 4 張圖一致。

## 未決事項

- **分支/commit 策略**：目前 `main` 有未提交變更（README.md、docs/pipelines/* 等）。需與使用者確認：在 `main` 直接做、或開 `feat/` worktree（依使用者既有 worktree workflow 偏好）。本 spec 暫不 commit，待此決定。
- §6 文件表是否新增一列（最小侵入 vs 完整索引）。
