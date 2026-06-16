# Crash-safe provenance：training / dataset manifest 兩階段寫入 + 版本防呆

- 日期：2026-06-16
- 分支：`feat/crash-safe-manifest-stub`
- 範圍：`training` / `dataset` CLI 的 manifest 寫入時機；`training` 的版本防呆 advisory
- 狀態：設計定稿（待轉 implementation plan）

## 1. 背景與問題

`training` / `dataset` 兩個 pipeline 在開跑前就把版本身分算好（`model_version` /
`base_dataset_version` / `train_variant_id` / `calibration_variant_id`），版本目錄
`data/models/<mv>/` 或 `data/dataset/<base_v>/...` 隨即被 catalog 模板化使用。

但 `manifest.json`（含完整 `parameters` 快照與已解析的版本身分）是 **pipeline 全部
跑完、且 `runner.run` 沒拋例外時** 才在 post-run 階段寫入。一旦中途崩潰：版本目錄裡
可能已有部分產物（cache、甚至 finalize 一半的模型），卻 **沒有任何一份記錄說明「這是
哪一套 `parameters` 產生的」**——provenance 跟著 crash 一起蒸發。

`model_version` 是 model-defining params 的內容雜湊（content hash），版本目錄名 **就是**
那串 params 的標籤。把「定義這個目錄的 params 快照」寫進去的最自然時機，是目錄誕生
那一刻，而非整條 pipeline 跑完。

此外，使用者**不一定理解 content-addressed 版本機制**，會踩到具體的坑（見 §5）。
本次一併補上低風險的防呆 advisory——而它之所以便宜，正是因為 stub 讓每個 `<mv>/` 從
誕生就帶著自己的 `parameters` / `status`，advisory 可直接讀來組訊息。

## 2. 目標 / 非目標

### 目標
- 讓 `training` / `dataset` 的 provenance **在 crash 後仍可還原**：版本目錄一誕生就帶
  完整 `parameters` 快照與版本身分。
- 讓**不熟版本機制的使用者**在「以為重跑 predict、實際悄悄重訓另一個模型」這類情境下
  **不再沉默**：大聲、可操作的 WARN。
- 維持最小、低風險：不改 pipeline 節點、不改版本雜湊邏輯、不改既有 post-run 行為
  （除多蓋一個 `status` 欄位）、**不新增會破壞既有 `--from-node` 契約的失敗模式**。

### 非目標（明確排除，列為已知限制 / follow-up）
- **不**做硬擋（fail-and-exit）或新增 `--allow-retrain` 旗標。Tier 1 防呆採「大聲 WARN
  但仍照跑」，向後相容既有「缺料自動補跑」契約（決策 Q5）。
- **不**修「post-run 無條件覆寫既有 manifest」這個既有行為（見 §8）。
- **不**讓 `promote_model` 依 `status` 拒絕 crash 半成品（Tier 2，列 follow-up）。
- **不**新增 `--model-version` 之類覆寫旗標（與內容定址設計衝突，先前已否決）。
- **不**保證 SIGKILL / OOM-kill 下寫出 failure marker（見 §8）。

## 3. 設計決策（brainstorming 結論）

| # | 決策 | 選定 |
|---|------|------|
| Q1 | 早期寫入內容 | **manifest stub**（完整 manifest，含 `parameters` + 版本身分 + `run_id` + `git_commit` + `created_at`，`status="running"`）。不另寫 params sidecar——stub 已內嵌完整 `parameters`。 |
| Q2 | 早期寫入遇 `manifest.json` 已存在 | **skip-if-present**（first-writer-wins）。`--from-node predict` 切片落進既有 finalized 目錄時，早期階段不動它的 manifest。 |
| Q3 | crash 時 `status` 處理 | **被動**：stub 寫 `running`，成功時 post-run 覆寫成 `completed`；crash 就停在 `running`（不做 error-path 寫入）。 |
| Q4 | stub 套用範圍 | **training + dataset 都做**。dataset 的 `latest` symlink 仍只在 post-run / 成功時更新（stub 絕不寫 symlink）。 |
| Q5 | 版本防呆強度 | **大聲 WARN，不擋**。sliced run 的切片 auto-include 了 model 生產節點時，印 prominent 警告（含 `mv`、既有版本、diff 提示）但仍照跑。advisory **僅限 training**。 |

## 4. 架構：兩階段 manifest 生命週期

每個版本目錄的 `manifest.json` 寫兩次：

- **Phase 1（pre-run stub）**：版本身分算完、呼叫 `_execute_pipeline` 之前，寫入
  `status="running"` 與所有「開跑前已知」的欄位（含完整 `parameters`）。skip-if-present；
  **不**寫 `latest` symlink；**不**寫 params sidecar。
- **Phase 2（post-run，僅成功時）**：沿用既有 post-run 寫入，額外蓋上 `status="completed"`，
  並補 `artifacts` + `sample_weight` + sidecar + symlink。除 `status` 外行為不變。

兩階段之間崩潰 ⇒ `manifest.json` 停在 `status="running"`、`parameters` 完整 ⇒
provenance 可還原。

開跑前已知 vs 只能 post-run 取得：
- 早期可寫：`version` / `pipeline` / `created_at` / `git_commit` / `parameters` /
  `base_dataset_version` / `train_variant_id` / `calibration_variant_id` / `model_version` /
  `parent_version` / `variant_kind` / `feature_table_fingerprint` / `run_id` / `status`。
- 只能 post-run：`artifacts`（需掃目錄）、`sample_weight`（讀 pipeline 中途產出的
  `sample_weight_report.json`）。

## 5. 使用者防呆機制

### 具體會出事的情境（取代抽象描述）

**情境 1（最危險：以為重跑 predict，其實悄悄重訓了另一個模型）**
使用者 finalize 了 `<mv_old>`，改了 `parameters_training.yaml` 裡 `training:` 下某鍵
（可能自以為無害），跑 `training --from-node predict_and_write_test_predictions`。
→ model_version 重算成 `<mv_new>`，`data/models/<mv_new>/` 是空的 → slice 把
`finalize_model` + `tune_hyperparameters` + 各 `cache_*` 全 auto-include → **重訓出一個
不同的模型**並寫出 predictions。使用者以為評估的是 finalized 模型，**實際評估了新訓的**。
沉默 + 昂貴 + 結論錯誤。

**情境 2（不知道哪個 hash 是哪個）**
`data/models/` 下一堆 8 碼 hash 目錄，不讀 manifest 分不出來；唯一指標是手動 promote 的
`best` symlink。容易對錯版本做事。

**情境 3（crash 半成品被誤用）**
中途崩潰留下 `status=running` 的 `<mv>/`（可能含半截模型）。若有人手動 `promote_model`
它、或誤認成完成品，會拿到壞模型。

### 防呆分層

**Tier 0 — 觀測（generic，含 dataset；近零成本）**
- 既有 `logger.info("Model version: %s", mv)` 保留。
- slice plan 輸出（`_format_slice_plan`）在「model 生產節點被 auto-include」時，明講
  「將（重）訓：finalize_model, …」，讓 `--dry-run` 與正常 run 都看得到。此為 generic、
  dataset 同享。

**Tier 1 — advisory（僅 training；讀本次新增的 stub）**
- 觸發條件（pipeline-aware、精確）：**sliced run（`--from-node`/`--only-node`）的 slice
  plan auto-include 了「`model` 的生產節點」**（無 calibration 為 `finalize_model`；有
  calibration 為 `calibrate_model`）。等價於「使用者想接續使用既有模型，但 `model` 產物
  不存在、得先重建」。用 slice plan 的 `auto_included`（其依據是 `can_load("model")` 的
  真實產物存在性）為準，**不**用 manifest `status` 當觸發（避免「模型已寫好但 manifest
  仍 running」的偽陽性）。
- 行為：印 prominent 多行 `WARN`，**仍照跑**（不擋）。訊息含三樣具體東西：
  1. 算出的 `<mv_new>` 與「將被（重）訓的節點」；
  2. **既有 model 版本清單**：掃 `data/models/*/manifest.json`，挑最近的 `completed`
     版本（讀 `created_at` / `status`）；
  3. **diff 提示**：比對當前 params 與該既有版本 `manifest.json` 的 `parameters`，直接
     看出哪個鍵改了 hash。
- 範圍註記：advisory 僅 training（版本混淆的危害集中在此）；crash-safe stub 才是
  training + dataset（Q4）。

**Tier 2 — follow-up（不進本次）**
- `scripts/promote_model.py` 拒絕 promote `status=running`（crash 半成品）目錄。屬另一個
  檔 / 另一道關卡，放本次會擴大測試面，獨立做。

## 6. 元件與變更（file-by-file）

### `src/recsys_tfb/core/versioning.py`
- `build_manifest_metadata(...)`：新增 `status: str | None = None`。非 `None` 時寫入
  `metadata["status"]`；`None` ⇒ 省略 ⇒ 既有呼叫端與測試不受影響。慣例：缺 `status` 的
  舊 manifest 視為「completed」（legacy = 已完成）。
- 新增純函式 `find_latest_completed_model_version(models_dir) -> tuple | None`：掃
  `models_dir/*/manifest.json`，回最近 `created_at` 且 `status` 為 completed（或缺 status
  的 legacy）的 `(version, created_at)`，供 advisory 組「最接近的既有版本」。無則回 `None`。

### `src/recsys_tfb/__main__.py`
- 新增 `_write_manifest_stub(version_dir, metadata_kwargs, run_id)`：
  ```python
  if (version_dir / "manifest.json").exists():
      return  # skip-if-present (Q2)
  metadata = build_manifest_metadata(**metadata_kwargs, status="running")
  metadata["run_id"] = run_id
  write_manifest(version_dir, metadata)  # 無 symlink、無 sidecar
  ```
- `_write_pipeline_manifest(...)`：以 `status="completed"` 建 metadata（此函式只在 post-run
  成功時被呼叫，恆為已完成）。
- `_format_slice_plan(...)`（Tier 0）：當 plan 的 `auto_included` 含 model 生產節點時，輸出
  明確的「將（重）訓：…」字句。
- 新增 advisory helper（Tier 1，training-only），於切片完成後、有 `from_node`/`only_node`
  且 slice plan auto-include 了 model 生產節點時呼叫：組並 `logger.warning(...)` 上述三段
  訊息（用 `find_latest_completed_model_version` 取最接近版本）。**不**改變執行流程。
- `training` command：
  - 算完 `mv`/`base_v`/`train_v`/`cal_v` 後、`_execute_pipeline` 之前，**以
    `if not dry_run and not list_nodes:` 為閘**，對 `data/models/<mv>/` 呼叫一次
    `_write_manifest_stub`。
  - 切片後觸發 Tier 1 advisory（需要 slice plan；實作上由 `_execute_pipeline` 回傳 plan
    或抽出 pre-flight，細節留 plan 階段；本 spec 釘行為與觸發條件）。
- `dataset` command：同一個閘，對 `base_dir`、`train_variant_dir`、（啟用 calibration 時）
  `cal_variant_dir` 各呼叫一次 `_write_manifest_stub`，**任何 stub 都不帶 symlink**。各 stub
  的 `metadata_kwargs` 對齊既有 post-run 對應 manifest（base 帶 `feature_table_fingerprint`；
  variant 帶 `parent_version` + `variant_kind`）。dataset **不**做 Tier 1 advisory。

## 7. 資料流（manifest 生命週期 + advisory）

| 情境 | Pre-run | Post-run | 結果 / 行為 |
|---|---|---|---|
| 全新完整 run | 寫 stub `running` | 覆寫 `completed`（+artifacts +sidecar +symlink） | `completed`，provenance 完整 |
| 中途 crash | 寫 stub `running` | 不會到達 | `running` + 完整 params（可還原）；`latest` **未**前進 |
| `--from-node predict` 落進既有 `<mv>/`（model 在） | 見 manifest → **skip** | 覆寫（既有行為） | `completed`；無 advisory（model 在、不重訓） |
| `--from-node predict`、model_version 漂移（model 不在） | 寫新 `<mv_new>` stub `running` | 重訓後覆寫 `completed` | **Tier 1 WARN**（含既有版本 + diff 提示）後照跑重訓 |
| `--dry-run` / `--list-nodes` | **閘擋掉**，不寫 | 不適用 | 不寫任何東西；plan 仍顯示「將（重）訓」 |

## 8. 邊界情況與已知限制

### 邊界情況（本設計處理）
- **dry-run / list-nodes**：`if not dry_run and not list_nodes` 閘確保 stub 不寫；advisory
  在 `--dry-run` 下仍可透過 plan 輸出看到「將（重）訓」（Tier 0）。
- **dataset `latest` symlink 安全**：stub 絕不寫 symlink，crash 不會讓 `latest` 指向不完整
  dataset。
- **crash 後重跑（同版本）**：stub 因 manifest 已存在（`running`）被 skip；post-run 成功時
  修正成 `completed`。stub 的 `run_id`/`created_at` 會停在第一次嘗試直到成功——可自癒。
- **advisory 偽陽性防範**：觸發用 slice plan（`can_load("model")` 的真實產物存在性），非
  manifest `status`；故「模型已寫好但 manifest 仍 running」不會誤觸發重訓警告。
- **`version_dir` 在 pre-run 尚不存在**：`write_manifest` 內含 `mkdir(parents=True,
  exist_ok=True)`。
- **`find_latest_completed_model_version` 容錯**：壞掉 / 缺欄位的 manifest 跳過，回 `None`
  時 advisory 省略「最接近版本」那段、仍印其餘警告。

### 已知限制（沿用既有行為，本次不修）
- **post-run 仍無條件覆寫既有 `manifest.json`**：`--from-node predict` 帶 *非 model-defining*
  編輯重跑時，post-run 仍會用當下 params 覆寫 finalized manifest 的 `parameters`。
  skip-if-present 只保護早期階段。修 post-run 覆寫屬另一 follow-up。
- **Tier 1 是 WARN 非硬擋**：決心忽略 log 的 batch script 仍可能略過。硬擋 + `--allow-retrain`
  已於 Q5 否決（向後相容優先）。
- **Tier 2（promote 拒絕 running）未做**：crash 半成品仍可能被手動 promote；列 follow-up。
- **SIGKILL / OOM**：stub 停在 `running`（無法寫 failure marker）——依 Q3 接受，與 etl_audit
  硬殺取捨同型。

## 9. 測試（全部純函式 / `tmp_path`，無 Spark，秒級）

- **`build_manifest_metadata`**：`status="running"`/`"completed"` 寫入；`status=None` 省略該鍵
  （向後相容）。
- **`_write_manifest_stub`**：無 manifest 時寫 `running`；已存在時 **不覆寫**；**不**建 symlink；
  內嵌完整 `parameters`。
- **`find_latest_completed_model_version`**：多版本取最近 completed；忽略 `running`/壞 manifest；
  全空回 `None`。
- **Tier 0 plan 輸出**：`_format_slice_plan` 在 model 生產節點被 auto-include 時含「將（重）訓」
  字句。
- **Tier 1 advisory**：給定 (sliced + model 生產節點在 auto_included) → 產生含 `mv`、既有版本、
  diff 提示的 WARN 文字；model 在（未 auto-include）→ 不產生；非 sliced full run → 不產生。
  測 advisory 的訊息組裝（純函式），不需跑 pipeline。
- **生命週期回歸**：stub（`running` + 完整 params）→ `_write_pipeline_manifest` 翻成 `completed`
  + 補 `artifacts`。
- **既有 manifest 形狀測試**：更新為預期含 `status: "completed"`。

## 10. 驗收標準

1. `training`/`dataset` 在 `_execute_pipeline` 之前對相應版本目錄寫出 `status="running"` 的
   `manifest.json`（`--dry-run`/`--list-nodes` 不寫）。
2. 成功跑完後 manifest 為 `status="completed"` 且含 `artifacts`（行為與今日一致、多 status）。
3. 早期 stub 對既有 `manifest.json` skip-if-present；dataset stub 不更新任何 `latest`。
4. sliced training run 觸發非預期重訓（model 生產節點被 auto-include）時，印含 `mv`/既有版本/
   diff 提示的 WARN 並**照跑**；model 在或 full run 不印。
5. 新增測試全綠；既有 manifest 形狀測試更新後全綠。
