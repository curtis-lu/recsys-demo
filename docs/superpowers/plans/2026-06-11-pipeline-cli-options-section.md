# 各 pipeline 頁「指令與選項」逐情境段 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在每份 pipeline 文件開頭加一個「指令與選項」逐情境段（窮舉該 pipeline 的 CLI 旗標），並新建 `inference.md`、修掉 `dataset.md` 的版本旗標事實錯誤。

**Architecture:** 純文件（docs-only）。每份 `docs/pipelines/*.md` 在 intro blockquote 之後、作為第一個 `##` 段插入 `## 指令與選項`（bash 區塊＋情境註解＋一行 `>` 註記，切片機制連 `operations/pipeline-slicing.md`）。既有「重跑語意／版本語意」保留概念、旗標清單收進新段避免重複。新建 `inference.md` 比照其他 pipeline 頁結構。README §5 文件地圖補 inference 連結。

**Tech Stack:** Markdown。權威來源 = `src/recsys_tfb/__main__.py`（Typer CLI 簽章）。無自動化測試；驗證 = 旗標對照 CLI 簽章 ＋ 相對連結可解析 ＋ 格式一致。

**Spec:** `docs/superpowers/specs/2026-06-11-pipeline-cli-options-section-design.md`

**工作守則：** 全程在 worktree `.worktrees/docs-pipeline-cli-options`（branch `feat/docs-pipeline-cli-options`），git 一律 `git -C` 或先 `cd` 到 worktree root。Edit/Write 用含 `.worktrees/docs-pipeline-cli-options` 的絕對路徑。

---

### Task 1: 新建 `inference.md`

**Files:**
- Create: `docs/pipelines/inference.md`

- [ ] **Step 1: 寫整份 `docs/pipelines/inference.md`**

完整內容（一字不差）：

````markdown
# inference pipeline

> 用上線模型對評分母體評分、每個 query group 內排名，通過驗證閘後發布到 production 表。
> DAG pipeline；節點接線與每張表的 schema 見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 用上線模型（best）對 parameters_inference.yaml 的 snap_dates 評分、排名、發布
python -m recsys_tfb inference --env local

# 指定模型版本（預設 best symlink）
python -m recsys_tfb inference --model-version <model_version>

# 改了下游 node、從某 node 接續（缺料自動補跑上游）
python -m recsys_tfb inference --from-node rank_predictions

# 只重跑單一 node（如驗證閘）
python -m recsys_tfb inference --only-node validate_predictions

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb inference --from-node predict_scores --dry-run
python -m recsys_tfb inference --list-nodes
```

> 評分母體（`snap_dates` / `products`）在 `parameters_inference.yaml`；`validate_predictions` 的 6 項 sanity check 通過後才 `publish_predictions` 寫 production 表。`--from-node` / `--only-node` 互斥；切片四旗標機制與限制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途

`inference` 載入上線模型（預設 `best`），組出評分母體（每個 (time, entity) 的候選 `item`），用訓練時的前處理編碼後評分，於每個 query group 內依 `score` 排名，最後經驗證閘把已驗證結果發布到 production 表（示例名 `ranked_predictions`）。模型版本決定要回溯哪個 dataset／前處理版本，由模型 manifest 自動對齊。

> 預設讀 `best` 那一版（人工 `scripts/promote_model.py` 升上來的）；`evaluation`（情境 2）之後讀回此 production 表做上線監控。

## 節點流程

| node | 輸入 | 主要功能 | 產出 |
|---|---|---|---|
| `build_scoring_dataset` | `feature_table` | 組出要評分的 (time, entity) × 候選 `item` 母體 | `scoring_dataset` |
| `apply_preprocessor` | `scoring_dataset`、`preprocessor` | 用訓練時的前處理編碼 | `X_score` |
| `predict_scores` | `model`、`X_score` | 模型評分 | `score_table` |
| `rank_predictions` | `score_table` | 每個 query group 內依 `score` 由高到低排名 | `ranked_staging` |
| `validate_predictions` | `ranked_staging`、`scoring_dataset` | 6 項 sanity check（筆數／分數範圍／完整性／排名一致…），失敗即中止整批 | `validated_predictions`（中間態） |
| `publish_predictions` | `validated_predictions` | 驗證通過後才把結果發布到 production 表（**唯一一次 production 寫入**） | `ranked_predictions` |

> **query group** ＝ 同一個 (time, entity) 下所有候選 `item`（見 README §0）；排名在組內進行。`validate_predictions` → `publish_predictions` 是 staging gate：未通過驗證不發布，避免半截／異常結果污染下游。

## 關鍵設定（`conf/base/parameters_inference.yaml`）

- `snap_dates`：要評分的時間切點清單；輸出以 `model_version` ＋ `snap_date` 分區。
- `products`：評分母體納入的 `item` 集合（須與 `schema.categorical_values.<item>` 一致，否則被一致性閘擋，見 README §4）。
- `use_calibration`：是否套用校準後模型輸出。

## 重跑語意

- `--model-version`：指定要用哪一版模型評分（預設 `best` symlink）；該版的 dataset／前處理版本由模型 manifest 回溯對齊。
- `publish_predictions` 對每個 `model_version` ＋ `snap_date` partition 整個覆寫——重跑同一版同一天 ＝ 覆寫，不是 append。
- 切片四旗標（`--from-node` / `--only-node` / `--dry-run` / `--list-nodes`）語意與限制見 [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 接下來

- 各表 schema / 版本層 / 範例 → [`../data-lineage.html`](../data-lineage.html)
- 發布後怎麼監控評估 → [`evaluation.md`](evaluation.md)
- 一致性閘的所有錯誤訊息 → README §4
````

- [ ] **Step 2: 驗證旗標對齊 CLI 簽章與連結可解析**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -nE "model-version|from-node|only-node|dry-run|list-nodes" docs/pipelines/inference.md
test -f docs/operations/pipeline-slicing.md && echo "slicing link OK"
test -f docs/pipelines/evaluation.md && echo "evaluation link OK"
```
Expected: 顯示 5 個旗標行；兩個 link OK。inference 旗標應恰為 `--env`/`--model-version`/切片四旗標（無 `--base-dataset-version` 等 training 專屬旗標）。

- [ ] **Step 3: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add docs/pipelines/inference.md
git commit -m "docs(inference): 新建 inference.md（含指令與選項段）"
```

---

### Task 2: `dataset.md` — 加指令段 ＋ 修版本旗標錯誤

**Files:**
- Modify: `docs/pipelines/dataset.md`（在 `## 用途` 前插段；改 line 65）

- [ ] **Step 1: 在 intro blockquote 後、`## 用途` 前插入 `## 指令與選項`**

把開頭

```markdown
> 把三張來源表變成各 split 的訓練輸入：抽樣 → 前處理 → 組 `*_model_input`。
> DAG pipeline；節點接線與每張表的 schema 見 [`../data-lineage.html`](../data-lineage.html)。

## 用途
```

改成

````markdown
> 把三張來源表變成各 split 的訓練輸入：抽樣 → 前處理 → 組 `*_model_input`。
> DAG pipeline；節點接線與每張表的 schema 見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 建資料集（每次都依 parameters 重算三層版本）
python -m recsys_tfb dataset --env local

# 改了下游 node、從某 node 接續（缺料自動補跑上游）
python -m recsys_tfb dataset --from-node build_model_input

# 只重跑單一 node
python -m recsys_tfb dataset --only-node fit_preprocessor_metadata

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb dataset --from-node build_model_input --dry-run
python -m recsys_tfb dataset --list-nodes
```

> dataset **沒有版本旗標**：三層版本由 `parameters_dataset.yaml` 決定、每跑必重算（見「三層資料版本」）；要選既有版本是在下游 `training` 用 `--base-dataset-version` / `--train-variant`。`--from-node` / `--only-node` 互斥；切片機制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途
````

- [ ] **Step 2: 移除 `## 用途` 段內現有的單行範例指令（已收進新段）**

把

````markdown
`dataset` 從 `sample_pool` 抽樣挑出各 split 的 key，對 `feature_table` 做一次前處理（編碼），再把 key ⋈ 特徵 ⋈ label 組成模型輸入。輸出供 `training` 讀。

```bash
python -m recsys_tfb dataset --env local
```

## 三層資料版本（`core/versioning.py`）
````

改成

```markdown
`dataset` 從 `sample_pool` 抽樣挑出各 split 的 key，對 `feature_table` 做一次前處理（編碼），再把 key ⋈ 特徵 ⋈ label 組成模型輸入。輸出供 `training` 讀。

## 三層資料版本（`core/versioning.py`）
```

- [ ] **Step 3: 修「重跑語意」段的版本旗標事實錯誤（原 line 65）**

把

```markdown
- **怎麼指定要用哪個版本**：`--base-dataset-version` / `--train-variant`（預設取最新）。各層的版本對齊由框架自動處理（manifest ＋ `latest` symlink）。
```

改成

```markdown
- **dataset 不接受版本旗標**：版本恆由 `parameters_dataset.yaml` 重算（見開頭「指令與選項」）。下游 `training` 才用 `--base-dataset-version` / `--train-variant` 指定要吃哪個既有版本（預設取最新）；各層版本對齊由框架自動處理（manifest ＋ `latest` symlink）。
```

- [ ] **Step 4: 驗證**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -n "## 指令與選項\|沒有版本旗標\|dataset 不接受版本旗標" docs/pipelines/dataset.md
grep -c "python -m recsys_tfb dataset --env local" docs/pipelines/dataset.md   # 應為 1（只剩新段那行）
```
Expected: 顯示新段標題與兩處更正語句；基本指令只出現 1 次（用途段那行已移除）。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add docs/pipelines/dataset.md
git commit -m "docs(dataset): 加指令與選項段；修正不存在的版本旗標說法"
```

---

### Task 3: `training.md` — 加指令段 ＋ 收斂版本語意旗標

**Files:**
- Modify: `docs/pipelines/training.md`（在 `## 用途` 前插段；移用途段範例；改 line 63）

- [ ] **Step 1: 在 intro blockquote 後、`## 用途` 前插入 `## 指令與選項`**

把開頭

```markdown
> 用各 split 的 `*_model_input` 訓練**一個共用** LightGBM 模型：cache → 調參 →（校準）→ 寫 test 預測 ＋ 診斷。
> DAG pipeline；節點接線與產物見 [`../data-lineage.html`](../data-lineage.html)。

## 用途
```

改成

````markdown
> 用各 split 的 `*_model_input` 訓練**一個共用** LightGBM 模型：cache → 調參 →（校準）→ 寫 test 預測 ＋ 診斷。
> DAG pipeline；節點接線與產物見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 一般訓練（取 latest base/train 版本）
python -m recsys_tfb training --env local

# 指定上游資料版本
python -m recsys_tfb training --base-dataset-version <base_v> --train-variant <train_v>

# 啟用校準時挑 calibration 版本（需 training.calibration.enabled=true）
python -m recsys_tfb training --calibration-variant <cal_v>

# 改了下游 node、跳過昂貴 HPO 接續（缺料自動補跑上游）
python -m recsys_tfb training --from-node finalize_model

# 只重跑單一 node（如校準）
python -m recsys_tfb training --only-node calibrate_model

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb training --from-node finalize_model --dry-run
python -m recsys_tfb training --list-nodes
```

> 版本旗標省略則取 latest；`--calibration-variant` 僅在 `training.calibration.enabled=true` 時生效。`--from-node` / `--only-node` 互斥；切片機制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途
````

- [ ] **Step 2: 移除 `## 用途` 段內現有的單行範例指令（已收進新段）**

把

````markdown
`training` 讀 dataset 產的 `*_model_input`，訓出單一模型（pointwise 或 learning-to-rank），可選做機率校準，並對 test set 評分供 evaluation 情境 1 使用。

```bash
python -m recsys_tfb training --env local
```

> 訓練是 **driver 上的單機 LightGBM**，不靠分散式 cluster——所以模型與快取都駐留 driver 本機檔案系統（見「產物」）。
````

改成

```markdown
`training` 讀 dataset 產的 `*_model_input`，訓出單一模型（pointwise 或 learning-to-rank），可選做機率校準，並對 test set 評分供 evaluation 情境 1 使用。

> 訓練是 **driver 上的單機 LightGBM**，不靠分散式 cluster——所以模型與快取都駐留 driver 本機檔案系統（見「產物」）。
```

- [ ] **Step 3: 收斂「版本語意」段的旗標行（原 line 63）為指向新段的指標**

把

```markdown
- 指定上游：`--base-dataset-version`、`--train-variant`（預設取最新）。
```

改成

```markdown
- 指定上游版本：見開頭「指令與選項」（`--base-dataset-version` / `--train-variant` / `--calibration-variant`，預設取最新）。
```

- [ ] **Step 4: 驗證**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -nE "## 指令與選項|--calibration-variant|--base-dataset-version" docs/pipelines/training.md
grep -c "python -m recsys_tfb training --env local" docs/pipelines/training.md   # 應為 1
```
Expected: 新段含 `--calibration-variant`；基本指令只出現 1 次。

- [ ] **Step 5: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add docs/pipelines/training.md
git commit -m "docs(training): 加指令與選項段（補 --calibration-variant）"
```

---

### Task 4: `evaluation.md` — 加指令段 ＋ 移除兩個情境段的冗餘 bash

**Files:**
- Modify: `docs/pipelines/evaluation.md`（在 `## 用途` 前插段；移除 `## 兩個情境` 段內 bash）

- [ ] **Step 1: 在 intro blockquote 後、`## 用途` 前插入 `## 指令與選項`**

把開頭

```markdown
> 把預測 ⋈ label 算排序指標、產報表。兩個情境（訓練後 / 上線監控）、三個模式（標準 / 比較 / 只比較）。
> DAG pipeline；節點接線見 [`../data-lineage.html`](../data-lineage.html)。

## 用途
```

改成

````markdown
> 把預測 ⋈ label 算排序指標、產報表。兩個情境（訓練後 / 上線監控）、三個模式（標準 / 比較 / 只比較）。
> DAG pipeline；節點接線見 [`../data-lineage.html`](../data-lineage.html)。

## 指令與選項

```bash
# 情境1：訓練後評估（讀 training_eval_predictions）
python -m recsys_tfb evaluation --env local --post-training

# 情境2：上線監控（讀 ranked_predictions，預設）
python -m recsys_tfb evaluation --env local

# 指定要評估哪一版（Model A；省略依解析規則取對應版本）
python -m recsys_tfb evaluation --model-version <model_version>

# 加比較（同時產標準報表與 report_comparison.html）
python -m recsys_tfb evaluation --post-training --compare <key>

# 只出比較報表（前提：該版已用標準/--compare 跑過、persist 過 enriched_eval_predictions）
python -m recsys_tfb evaluation --compare-only <key>

# 先看執行計畫不跑 / 列 node 名與接續成本
python -m recsys_tfb evaluation --from-node compute_metrics --dry-run
python -m recsys_tfb evaluation --list-nodes
```

> `<key>` 取自 `parameters_evaluation.yaml` 的 `compare_sources`；`--compare` / `--compare-only` 互斥（只能給一個）。`--from-node` / `--only-node` 互斥；切片機制 → [`../operations/pipeline-slicing.md`](../operations/pipeline-slicing.md)。

## 用途
````

- [ ] **Step 2: 移除 `## 兩個情境` 段內的 bash 區塊（與情境表的「指令」欄、新段重複）**

把

````markdown
| 上線後監控 | `evaluation`（預設） | `ranked_predictions`（inference 發布的已驗證結果） | 模型上線後定期追蹤排名品質 |

```bash
python -m recsys_tfb evaluation --env local --post-training   # 情境 1
python -m recsys_tfb evaluation --env local                   # 情境 2
```

> 兩情境都靠 `label_table` 提供 ground truth
````

改成

```markdown
| 上線後監控 | `evaluation`（預設） | `ranked_predictions`（inference 發布的已驗證結果） | 模型上線後定期追蹤排名品質 |

> 兩情境都靠 `label_table` 提供 ground truth
```

- [ ] **Step 3: 驗證**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -nE "## 指令與選項|--compare-only|--post-training" docs/pipelines/evaluation.md
grep -c "python -m recsys_tfb evaluation --env local --post-training" docs/pipelines/evaluation.md   # 應為 1
```
Expected: 新段含全部 evaluation 旗標；`--post-training` 範例指令只出現 1 次（兩個情境段那組已移除）。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add docs/pipelines/evaluation.md
git commit -m "docs(evaluation): 加指令與選項段；去除重複 bash"
```

---

### Task 5: `source_etl.md` — 加精簡指令段 ＋ 改名既有「指令」段

**Files:**
- Modify: `docs/pipelines/source_etl.md`（在 `## 用途` 前插段；既有 `## 指令` 改名並去重）

- [ ] **Step 1: 在 intro blockquote 後、`## 用途` 前插入精簡 `## 指令與選項`**

把開頭

```markdown
> 把上游原始表整理成框架要讀的三張**來源表**：`feature_table`、`label_table`、`sample_pool`。
> 這是唯一用 **SQL** 跑、不是 DAG node 的階段。

## 用途
```

改成

````markdown
> 把上游原始表整理成框架要讀的三張**來源表**：`feature_table`、`label_table`、`sample_pool`。
> 這是唯一用 **SQL** 跑、不是 DAG node 的階段。

## 指令與選項

```bash
# 三條獨立 ETL，各產一張來源表（--target-dates 逗號分隔多個；未給讀 config）
python -m recsys_tfb feature_etl     --env local --target-dates 2025-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2025-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2025-01-31

# 先 preflight 驗上游（唯讀、不寫表；有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31

# 從某張表續跑（跳過它之前已寫的表；接續失敗的長流程）
python -m recsys_tfb feature_etl --restart-from <table_name> --target-dates 2025-01-31
```

> `source_etl` 非 DAG pipeline、無切片旗標。`--source-check` 不可與 `--restart-from` 併用（檢查不寫表，無從續跑）；dry-run／preflight／覆寫的完整語意見下方「重跑語意」與「preflight 工作流」。

## 用途
````

- [ ] **Step 2: 既有 `## 指令` 段改名為 `## preflight 工作流（建議）`，並移除已在新段出現的三行基本指令**

把

````markdown
## 指令

```bash
python -m recsys_tfb feature_etl     --env local --target-dates 2025-01-31
python -m recsys_tfb label_etl       --env local --target-dates 2025-01-31
python -m recsys_tfb sample_pool_etl --env local --target-dates 2025-01-31   # 此 stage local 也會實寫
```

**先 preflight 再正式跑**（建議工作流）：

```bash
# 1) 先驗上游（唯讀、不寫表；全部跑完有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31
# 2) 通過後再實際寫表
python -m recsys_tfb feature_etl              --target-dates 2025-01-31
```

`--source-check` 不可與 `--restart-from` 併用（檢查不寫表，無從續跑）。
````

改成

````markdown
## preflight 工作流（建議）

對來源資料新鮮度沒把握時，**先 preflight 再正式跑**——先唯讀驗上游、通過後再寫表：

```bash
# 1) 先驗上游（唯讀、不寫表；全部跑完有失敗即 exit 1 並印修復指引）
python -m recsys_tfb feature_etl --source-check --target-dates 2025-01-31
# 2) 通過後再實際寫表
python -m recsys_tfb feature_etl              --target-dates 2025-01-31
```

> 三條 ETL 的完整指令見開頭「指令與選項」。`sample_pool_etl` 在 `--env local` 也會實寫（設了 `dry_run: false`）；`feature_etl` / `label_etl` 本機預設 dry-run（見「重跑語意」）。`--source-check` 不可與 `--restart-from` 併用。
````

- [ ] **Step 3: 驗證**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -nE "^## 指令與選項|^## preflight 工作流|^## 指令$" docs/pipelines/source_etl.md
grep -c "python -m recsys_tfb feature_etl     --env local --target-dates" docs/pipelines/source_etl.md   # 應為 1
```
Expected: 出現 `## 指令與選項` 與 `## preflight 工作流（建議）`，**不再有**裸 `## 指令` 段；feature_etl 基本指令只出現 1 次。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add docs/pipelines/source_etl.md
git commit -m "docs(source_etl): 加精簡指令與選項段；既有指令段改名去重"
```

---

### Task 6: README §5 文件地圖補 inference 連結

**Files:**
- Modify: `README.md`（§5 兩處 pipeline 清單）

- [ ] **Step 1: 「我想做什麼」表的 pipeline 行補 inference**

把

```markdown
| 深入某一個 pipeline | [`docs/pipelines/`](docs/pipelines)（`source_etl` / `dataset` / `training` / `evaluation`） |
```

改成

```markdown
| 深入某一個 pipeline | [`docs/pipelines/`](docs/pipelines)（`source_etl` / `dataset` / `training` / `inference` / `evaluation`） |
```

- [ ] **Step 2: 「完整文件地圖」表的 pipeline 行補 inference 並補述「指令與選項」**

把

```markdown
| pipeline | `docs/pipelines/{source_etl,dataset,training,evaluation}.md` | 各 pipeline 的節點、設定、重跑語意 |
```

改成

```markdown
| pipeline | `docs/pipelines/{source_etl,dataset,training,inference,evaluation}.md` | 各 pipeline 的節點、設定、指令與選項、重跑語意 |
```

- [ ] **Step 3: 驗證（README §2「常用選項」未被動到）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -n "source_etl,dataset,training,inference,evaluation\|training / .inference. / .evaluation" README.md
git diff README.md | grep -E "^\+|^-" | grep -i "常用選項\|calibration-variant"   # 應無輸出（沒碰常用選項）
```
Expected: 兩處 §5 清單含 inference；diff 不含「常用選項」相關改動。

- [ ] **Step 4: Commit**

```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git add README.md
git commit -m "docs(readme): §5 文件地圖補 inference.md 連結"
```

---

### Task 7: 全域一致性驗證 ＋ graphify 重建

**Files:**
- 無（驗證 ＋ graphify graph）

- [ ] **Step 1: 五份 pipeline 頁的 `## 指令與選項` 位置與格式一致**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
for f in source_etl dataset training inference evaluation; do
  echo "== $f =="; grep -n "^## " docs/pipelines/$f.md | head -3
done
```
Expected: 每份的**第一個 `##`** 都是 `## 指令與選項`（intro blockquote 之後）。

- [ ] **Step 2: 旗標對齊 CLI 簽章（無捏造旗標）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
echo "--- dataset 不應有版本旗標 ---"
grep -nE "dataset --(base-dataset-version|train-variant)" docs/pipelines/dataset.md || echo "OK: dataset 無版本旗標誤用"
echo "--- training 應有三個版本旗標 + 切片 ---"
grep -cE "base-dataset-version|train-variant|calibration-variant|from-node|only-node|dry-run|list-nodes" docs/pipelines/training.md
echo "--- inference 不應有 training 專屬旗標 ---"
grep -nE "inference --(base-dataset-version|train-variant|calibration-variant)" docs/pipelines/inference.md || echo "OK: inference 無 training 旗標"
```
Expected: dataset / inference 兩個 OK 行都印出；training 計數 ≥7。

- [ ] **Step 3: 相對連結可解析**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
grep -ho "(\.\./[a-zA-Z0-9_/-]*\.md\|[a-z_]*\.md)" docs/pipelines/*.md | tr -d '()' | sort -u | while read p; do
  base=docs/pipelines; tgt="$base/$p"; [ -f "$tgt" ] && echo "OK $p" || echo "MISSING $p"
done | grep MISSING || echo "所有相對 .md 連結可解析"
```
Expected: 印「所有相對 .md 連結可解析」（特別是各頁的 `../operations/pipeline-slicing.md`、inference.md ↔ evaluation.md）。

- [ ] **Step 4: graphify code graph 重建（docs 變更，保持圖新鮮）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))" 2>&1 | tail -2
```
Expected: 重建完成輸出（非錯誤）。

- [ ] **Step 5: 最終 diff 檢視（確認純文件、無程式碼改動）**

Run:
```bash
cd /Users/curtislu/projects/recsys_tfb/.worktrees/docs-pipeline-cli-options
git diff main --stat
git diff main --name-only | grep -vE "^(docs/|README.md|graphify-out/)" && echo "⚠️ 有非文件改動" || echo "OK: 純文件改動"
```
Expected: 變動只含 `docs/`、`README.md`、`graphify-out/`；印「OK: 純文件改動」。

---

## Self-Review 紀錄

- **Spec coverage**：§4.1 模板→Task1-5；§4.2 舊段分工→Task2 Step3 / Task3 Step3 /（evaluation、source_etl 概念段保留不動）；§4.3 各 pipeline→Task1-5；dataset bug→Task2 Step3；inference 新建→Task1；source_etl 對齊→Task5；§4.4 README §5→Task6；§5 驗證→Task7。全覆蓋。
- **Placeholder scan**：bash 內 `<model_version>` / `<base_v>` / `<key>` / `<table_name>` 為使用者代入的佔位，非計畫 placeholder；無 TBD/TODO。
- **Type/識別字一致**：node 名（`build_model_input` / `fit_preprocessor_metadata` / `finalize_model` / `calibrate_model` / `compute_metrics` / `rank_predictions` / `validate_predictions` / `predict_scores`）與 README §2 節點表一致；旗標名與 `__main__.py` 簽章一致。
