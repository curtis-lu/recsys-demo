# Crash-safe provenance：training / dataset manifest 兩階段寫入

- 日期：2026-06-16
- 分支：`feat/crash-safe-manifest-stub`
- 範圍：`training` 與 `dataset` CLI command 的 manifest 寫入時機
- 狀態：設計定稿（待轉 implementation plan）

## 1. 背景與問題

`training` / `dataset` 兩個 pipeline 在開跑前就把版本身分算好（`model_version` /
`base_dataset_version` / `train_variant_id` / `calibration_variant_id`，
`__main__.py` 的 `training`、`dataset` command），版本目錄 `data/models/<mv>/` 或
`data/dataset/<base_v>/...` 隨即被 catalog 模板化使用。

但 `manifest.json`（含完整 `parameters` 快照與已解析的版本身分）是 **pipeline 全部
跑完、且 `runner.run` 沒拋例外時** 才在 post-run 階段寫入
（`_write_pipeline_manifest`）。一旦 pipeline 中途崩潰：

- 版本目錄裡可能已經有部分產物（cache、甚至 finalize 一半的模型），
- 卻 **沒有任何一份記錄說明「這是哪一套 `parameters` 產生的」**，

provenance 跟著 crash 一起蒸發。使用者無法事後還原「產生這個 `model_version` 當下的
`parameters_training.yaml` 狀態」，也就無法乾淨地對既有 finalized 模型重跑下游
（例如 `--from-node predict_and_write_test_predictions`）。

`model_version` 是 model-defining params 的內容雜湊（content hash），版本目錄名
**就是** 那串 params 的標籤。把「定義這個目錄的 params 快照」寫進去的最自然時機，
是目錄誕生那一刻，而非整條 pipeline 跑完。

## 2. 目標 / 非目標

### 目標
- 讓 `training` 與 `dataset` 的 provenance **在 crash 後仍可還原**：版本目錄一誕生
  就帶著完整 `parameters` 快照與版本身分。
- 維持最小、低風險：不改 pipeline 節點、不改版本雜湊邏輯、不動既有 post-run 行為
  （除了多蓋一個 `status` 欄位）。

### 非目標（明確排除，列為已知限制）
- **不**修「post-run 無條件覆寫既有 manifest」這個既有行為（見 §7）。
- **不**新增任何依 `status` 分流的 reader（例如「hash 漂移時 fail-loud」）；本次
  `status` 純資訊性。
- **不**新增 `--model-version` 之類的覆寫旗標（與內容定址設計衝突，先前討論已否決）。
- **不**保證 SIGKILL / OOM-kill 下能寫出 failure marker（見 §6）。

## 3. 設計決策（brainstorming 結論）

| # | 決策 | 選定 |
|---|------|------|
| Q1 | 早期寫入的內容 | **manifest stub**（完整 manifest，含 `parameters` + 版本身分 + `run_id` + `git_commit` + `created_at`，`status="running"`）。不另寫 params sidecar——stub 已內嵌完整 `parameters`。 |
| Q2 | 早期寫入遇到 `manifest.json` 已存在 | **skip-if-present**（first-writer-wins）。`--from-node predict` 切片落進既有 finalized 目錄時，早期階段不動它的 manifest。 |
| Q3 | crash 時的 `status` 處理 | **被動**：stub 寫 `running`，成功時 post-run 覆寫成 `completed`；crash 就停在 `running`（不做 error-path 寫入）。 |
| Q4 | 套用範圍 | **training + dataset 都做**。dataset 的 `latest` symlink 仍只在 post-run / 成功時更新（stub 絕不寫 symlink）。 |

## 4. 架構：兩階段 manifest 生命週期

每個版本目錄的 `manifest.json` 寫兩次：

- **Phase 1（pre-run stub）**：版本身分算完、呼叫 `_execute_pipeline` 之前，寫入
  `status="running"` 與所有「開跑前已知」的欄位（含完整 `parameters`）。
  skip-if-present；**不**寫 `latest` symlink；**不**寫 params sidecar。
- **Phase 2（post-run，僅成功時）**：沿用既有 post-run 寫入，額外蓋上
  `status="completed"`，並補 `artifacts` + `sample_weight` + sidecar + symlink。
  除 `status` 外行為不變。

兩階段之間崩潰 ⇒ `manifest.json` 停在 `status="running"`、`parameters` 完整 ⇒
provenance 可還原。

開跑前已知 vs 只能 post-run 取得（依 `build_manifest_metadata` 與既有 post-run 碼）：

- 早期可寫：`version` / `pipeline` / `created_at` / `git_commit` / `parameters` /
  `base_dataset_version` / `train_variant_id` / `calibration_variant_id` /
  `model_version` / `parent_version` / `variant_kind` /
  `feature_table_fingerprint` / `run_id` / `status`。
- 只能 post-run：`artifacts`（需掃目錄）、`sample_weight`（讀 pipeline 中途節點產出的
  `sample_weight_report.json`）。

## 5. 元件與變更（file-by-file）

### `src/recsys_tfb/core/versioning.py`
- `build_manifest_metadata(...)`：新增 `status: str | None = None`。非 `None` 時寫入
  `metadata["status"]`；`None` ⇒ 省略該鍵 ⇒ 既有呼叫端與測試不受影響。
  慣例：缺 `status` 的舊 manifest 視為「completed」（legacy = 已完成）。

### `src/recsys_tfb/__main__.py`
- 新增 helper：

  ```python
  def _write_manifest_stub(version_dir, metadata_kwargs, run_id):
      if (version_dir / "manifest.json").exists():
          return  # skip-if-present (Q2)
      metadata = build_manifest_metadata(**metadata_kwargs, status="running")
      metadata["run_id"] = run_id
      write_manifest(version_dir, metadata)  # 無 symlink、無 sidecar
  ```

- `_write_pipeline_manifest(...)`：以 `status="completed"` 建 metadata（此函式只在
  post-run 成功時被呼叫，恆為已完成）。
- `training` command：算完 `mv` / `base_v` / `train_v` / `cal_v` 後、`_execute_pipeline`
  之前，**以 `if not dry_run and not list_nodes:` 為閘**，對 `data/models/<mv>/` 呼叫一次
  `_write_manifest_stub`。stub 的 `metadata_kwargs`：`version=mv`、`pipeline="training"`、
  `parameters=params_training`、`base_dataset_version=base_v`、`train_variant_id=train_v`、
  （calibration 啟用時）`calibration_variant_id=cal_v`。
- `dataset` command：同一個閘，對 `base_dir`、`train_variant_dir`、（啟用 calibration 時）
  `cal_variant_dir` 各呼叫一次 `_write_manifest_stub`，**任何 stub 都不帶 symlink**。
  各 stub 的 `metadata_kwargs` 對齊既有 post-run 的對應 manifest（base 帶
  `feature_table_fingerprint`；variant 帶 `parent_version` + `variant_kind`）。

## 6. 資料流（manifest 生命週期）

| 情境 | Pre-run | Post-run | 結果 `manifest.json` |
|---|---|---|---|
| 全新完整 run | 寫 stub `running` | 覆寫 `completed`（+artifacts +sidecar +symlink） | `completed`，provenance 完整 |
| 中途 crash | 寫 stub `running` | 不會到達 | `running` + 完整 params（可還原）；`latest` **未**前進 |
| `--from-node predict` 落進既有 `<mv>/` | 見 manifest → **skip** | 覆寫（既有行為） | `completed` |
| `--dry-run` / `--list-nodes` | **閘擋掉**，不寫 | 不適用 | 不寫任何東西 |

## 7. 邊界情況與已知限制

### 邊界情況（本設計處理）
- **dry-run / list-nodes**：`if not dry_run and not list_nodes` 閘確保不寫任何東西，
  對齊現行「nothing executed, nothing written」。
- **dataset `latest` symlink 安全**：stub 絕不寫 symlink，故 crash 不會讓 `latest`
  指向不完整的 dataset。
- **crash 後重跑（同版本）**：stub 因 manifest 已存在（`running`）被 skip；post-run
  成功時修正成 `completed`。小代價：stub 的 `run_id` / `created_at` 會停在第一次嘗試，
  直到成功才被覆寫——可自癒，可接受。
- **`version_dir` 在 pre-run 尚不存在**：`write_manifest` 內含
  `mkdir(parents=True, exist_ok=True)`。

### 已知限制（沿用既有行為，本次不修）
- **post-run 仍無條件覆寫既有 `manifest.json`**：因此 `--from-node predict` 帶著
  *非 model-defining* 編輯重跑時，post-run 仍會用當下 params 覆寫掉 finalized manifest 的
  `parameters`。skip-if-present 只保護早期階段。修 post-run 覆寫屬另一個 follow-up。
- **尚無 consumer 依 `status` 分流**：`status` 純資訊性。先前討論的「hash 漂移
  fail-loud」reader 不在本次範圍。
- **SIGKILL / OOM**：stub 停在 `running`（無法寫 failure marker）——依 Q3 接受，
  與 repo 既有 etl_audit 在硬殺下的取捨同型。

## 8. 測試（全部純函式 / `tmp_path`，無 Spark，秒級）

- **`build_manifest_metadata`**：`status="running"` / `"completed"` 會寫入；
  `status=None` 省略該鍵（向後相容）。
- **`_write_manifest_stub`**：目錄無 manifest 時寫出 `running` manifest；已存在時
  **不覆寫**；**不**建立 `latest` symlink；內嵌完整 `parameters`。
- **生命週期回歸**：stub（`running` + 完整 params）→ `_write_pipeline_manifest`
  翻成 `completed` 並補 `artifacts`。
- **既有 manifest 形狀測試**：把斷言 post-run manifest 精確鍵集合的測試，更新成預期
  含 `status: "completed"`。

## 9. 驗收標準

1. `training` / `dataset` 在 `_execute_pipeline` 之前對相應版本目錄寫出
   `status="running"` 的 `manifest.json`（`--dry-run` / `--list-nodes` 不寫）。
2. 成功跑完後，manifest 為 `status="completed"` 且含 `artifacts`（行為與今日一致、
   多一個 status 欄位）。
3. 早期 stub 對既有 `manifest.json` skip-if-present；dataset stub 不更新任何 `latest`。
4. 新增測試全綠；既有 manifest 形狀測試更新後全綠。
