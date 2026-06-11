# Design: 各 pipeline 文件加「指令與選項」逐情境段

日期：2026-06-11
分支：`feat/docs-pipeline-cli-options`
類型：純文件（docs-only，無程式碼／測試改動）

## 1. 問題

CLI 各子指令的選項目前**散落且不完整**，使用者無法在單一 pipeline 頁查到「什麼情境用什麼指令＋選項」：

- `docs/operations/pipeline-slicing.md` 只講切片四旗標（`--from-node` / `--only-node` / `--dry-run` / `--list-nodes`），且獨立於 pipeline 頁之外。
- `README.md §2`「常用選項」是刻意精選的總覽，不求全。
- `docs/pipelines/{dataset,training,evaluation}.md` 各自的「重跑語意／版本語意」只覆蓋一半旗標，**完全沒提切片四旗標**；`training.md` 漏 `--calibration-variant`。
- `docs/pipelines/inference.md` **根本不存在**——inference 的 `--model-version` ＋切片四旗標在任何 pipeline 頁都查不到。
- `docs/pipelines/dataset.md` 有**事實錯誤**：宣稱 `dataset` 指令吃 `--base-dataset-version` / `--train-variant`，但 CLI 簽章中 `dataset` 沒有這兩個旗標（它「always recomputes versions from parameters」）。

`source_etl.md` 是反例：它已有完整的逐情境「重跑語意」＋「指令」段（含 `--target-dates` / `--restart-from` / `--source-check` preflight 工作流），可當模板參照。

## 2. 目標

讓每份 pipeline 文件**自給自足**地回答「什麼情境用什麼指令＋選項」，以該 pipeline 的**實際 CLI 簽章**為唯一真實來源。

非目標：
- 不新增一份集中式 CLI reference 總表（刻意分散在各 pipeline 頁，貼近使用情境）。
- 不改 `README.md §2`「常用選項」與其旗標敘述（維持精選總覽，逐情境完整版交給 pipeline 頁）。
  - **唯一例外**：新建 `inference.md` 後，`README.md §5` 文件地圖把 `inference` 補進
    `docs/pipelines/{...}` 清單，避免新檔 orphan。此為一行連結補綴、非「常用選項」改寫。
    （此例外仍待使用者於 spec review 時確認；若不要動 README，改以其他既有 pipeline 頁的
    「接下來」互連 inference.md。）
- 不改任何程式碼／設定／測試。

## 3. CLI 簽章（權威來源，取自 `src/recsys_tfb/__main__.py`）

| 指令 | 選項 |
|---|---|
| `feature_etl` / `label_etl` / `sample_pool_etl` | `--env/-e`、`--target-dates`、`--restart-from`、`--source-check` |
| `dataset` | `--env/-e`、`--from-node`、`--only-node`、`--dry-run`、`--list-nodes`（**無版本旗標**，每跑依 parameters 重算版本） |
| `training` | `--env/-e`、`--base-dataset-version`（預設 latest）、`--train-variant`（預設 latest）、`--calibration-variant`（預設 latest，僅 `training.calibration.enabled=true` 生效）、切片四旗標 |
| `inference` | `--env/-e`、`--model-version`（預設 best symlink）、切片四旗標 |
| `evaluation` | `--env/-e`、`--model-version`、`--post-training`、`--compare <key>`、`--compare-only <key>`（與 `--compare` 互斥，`<key>` 取自 `compare_sources`）、切片四旗標 |

切片四旗標 = `--from-node` / `--only-node`（互斥）/ `--dry-run` / `--list-nodes`，四個 DAG pipeline 共用，機制詳見 `docs/operations/pipeline-slicing.md`。

## 4. 設計

### 4.1 統一模板

每份 pipeline 文件新增一個 **`## 指令與選項`** 段，放在**開頭 intro blockquote 之後、作為文件第一個 `##` 段**（在「用途」之前）。內容為：

- 一個 ```bash``` 區塊，**每個情境一行 `#` 註解 ＋ 一行指令**，窮舉該 pipeline 的全部旗標。
- 區塊後以一行 `>` 註記互斥／限制，切片機制統一連 `docs/operations/pipeline-slicing.md`（不在五份各抄一遍）。

呈現格式範例（以 training 為準）：

```bash
# 一般訓練（取 latest base/train）
python -m recsys_tfb training --env local

# 指定上游資料版本
python -m recsys_tfb training --base-dataset-version V --train-variant W

# 啟用校準時挑 calibration 版本（需 training.calibration.enabled=true）
python -m recsys_tfb training --calibration-variant C

# 改了下游 node、跳過昂貴 HPO 接續（缺料自動補跑上游）
python -m recsys_tfb training --from-node finalize_model

# 只 debug 單一 node
python -m recsys_tfb training --only-node calibrate_model

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run
python -m recsys_tfb training --list-nodes
```

> 預設取 latest；`--from-node` / `--only-node` 互斥；切片四旗標機制與限制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

### 4.2 與既有「重跑語意／版本語意」段的關係

- **保留概念**：版本層（dataset 三層）、為什麼分層、compare 模式語意等概念敘述留在原段。
- **旗標收斂進新段**：原段裡「該用哪個旗標」的清單（如 dataset.md `--base-dataset-version` 那行、training.md「指定上游」那行）移除，統一由新「指令與選項」段承載，避免兩處重複。

### 4.3 各 pipeline 的情境覆蓋

- **dataset**：`--env` ＋切片四旗標。**順手修 dataset.md 的事實錯誤**——移除「dataset 吃 `--base-dataset-version` / `--train-variant`」的說法，改寫成「dataset 版本由 parameters 決定、每跑必重算；要選既有版本是在下游 `training` 用 `--base-dataset-version`」。
- **training**：`--env`、`--base-dataset-version`、`--train-variant`、`--calibration-variant`（註明僅校準開啟時生效）、切片四旗標。
- **inference（新建整份 `docs/pipelines/inference.md`）**：比照其他 pipeline 頁結構補齊——
  - intro blockquote → `## 指令與選項`（`--env`、`--model-version`〔預設 best〕、切片四旗標）→ 用途 → 節點流程（`build_scoring_dataset` → `apply_preprocessor` → `predict_scores` → `rank_predictions` → `validate_predictions`〔6 項 sanity check〕→ `publish_predictions`〔驗證通過才寫 production 表〕）→ 關鍵設定（`parameters_inference.yaml` 的 `snap_dates` / `products` / `use_calibration`）→ 接下來。
  - 節點與輸入／輸出沿用 `README.md §2`「inference」表的既有描述，不自創。
- **evaluation**：`--env`、`--model-version`、`--post-training`（情境1，註解標清楚 vs 情境2 預設讀 `ranked_predictions`）、`--compare <key>`、`--compare-only <key>`（互斥、前提：須先跑過標準／`--compare` 並 persist 過 `enriched_eval_predictions`）、切片四旗標。
- **source_etl**：內容已達標，僅做**結構對齊**——在 intro blockquote 之後新增一個**精簡版** `## 指令與選項`（作為第一個 `##` 段，與其他四份位置一致；情境含三個 `*_etl` 指令、`--target-dates` / `--restart-from` / `--source-check`，並標 `--source-check` 與 `--restart-from` 互斥），其下保留**既有的詳細「重跑語意」＋ preflight 工作流敘述**不動。新段是「快速情境入口」，既有段是「nuance 深寫」，分工同 §4.2。

### 4.4 連動／一致性

- 各 pipeline 頁新段與 `README.md §2`「常用選項」分工：README 精選總覽不動，pipeline 頁逐情境完整版。
- 新建 `inference.md` 後，更新指向 pipeline 文件的索引（`README.md §5` 文件地圖把 `inference` 補進 `docs/pipelines/{...}` 清單）——此為例外的最小 README 觸碰，僅補連結、不改「常用選項」。

## 5. 驗證

純文件，無自動化測試。驗證方式：

1. **準確性**：每份新段的旗標與 `src/recsys_tfb/__main__.py` 的 typer 簽章逐一對照（旗標名、預設值、互斥關係、help 語意），不得出現不存在的旗標（特別是 dataset 版本旗標 bug 已修）。
2. **連結**：新增的 `../operations/pipeline-slicing.md`、新建 `inference.md` 的相對連結可解析；README §5 索引含 inference。
3. **格式一致**：五份 pipeline 頁的 `## 指令與選項` 段位置（intro 後第一段）、bash＋情境註解、`>` 註記樣式一致。
4. 改完文件後依 CLAUDE.md 規則重建 graphify code graph（`_rebuild_code`）——本變更為 docs，視需要。

## 6. 風險／取捨

- **重複維護**：旗標說明同時存在於 pipeline 頁與 `pipeline-slicing.md`／README。緩解：切片機制只在 `pipeline-slicing.md` 深寫，pipeline 頁只列情境並連出；README 不碰。
- **未來 CLI 變動**：新增旗標時需同步更新對應 pipeline 頁。此為文件固有成本，本設計不引入自動生成（避免 over-engineering）。
