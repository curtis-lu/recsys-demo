# Pipeline 切片：--from-node / --only-node 設計

日期：2026-06-10
狀態：設計核准，待實作
分支：`feat/pipeline-node-slicing`

## 1. 背景與目標

目前 `python -m recsys_tfb <pipeline>` 永遠執行完整 pipeline。四個實際需求要求部分執行能力：

1. **失敗接續**：pipeline 跑到一半掛掉（如 Spark OOM），參數沒變，想從失敗的 node 接續，不重跑已成功的昂貴上游。
2. **開發迭代**：改了某個下游 node 的程式碼，上游產物不變，只想重跑下游驗證。
3. **跳過昂貴步驟**：例如跳過 HPO，用既有 `best_params` 直接重訓 final model。
4. **單獨 debug 某 node**：只跑一個 node 看輸出，輸入全部從落地產物讀。

適用 `dataset` / `training` / `inference` / `evaluation` 四個 CLI 指令。`source_etl` 已有自己的 `--restart-from`（SQL 步驟層級），不在範圍內。**不給任何新 flag 時，行為與現狀完全相同。**

## 2. 核心語意

### 2.1 兩種切片

- `--from-node <name>`：**拓撲位置語意**——指定 node 與拓撲序在它之後的全部 node 構成必跑集合。選此語意而非 kedro 式「下游閉包」（只含依賴指定 node 輸出者），因為 Runner 是循序執行：失敗接續時「X 之後的 node 全都沒跑過」，包括與 X 平行、不依賴它的分支。代價是偶爾多跑平行分支的便宜 node，執行計畫中可見。
- `--only-node <name>`：必跑集合只有該 node。

### 2.2 自動擴張補跑

必跑集合決定後，反向擴張解決「輸入從哪來」：

1. 計算必跑集合 K 的外部輸入（K 內 node 要吃、但不由 K 內 node 產出的 dataset）。
2. 對每個外部輸入問 `can_load(name)`（定義見 §2.3）：
   - 可載入 → 直接從 catalog 讀，生產者維持跳過（**擴張停止條件**）。
   - 不可載入 → 把生產者 node 拉回 K，對其輸入遞迴同樣判斷。
3. 收斂後得到最小閉包。最壞情況擴張回整條 pipeline（等於 full run），任何起點都合法，不會死循環。

**判斷單位是 dataset，拉回單位是 node**：多輸出 node 只要下游實際需要的那個輸出可載入，就不會被拉回；需要的恰是不可載入的那個才拉回（整個 node 重跑）。

### 2.3 可載入判準：`catalog.exists()`（設計依據）

`can_load(name) = catalog.exists(name)`，catalog 以真實 runtime_params 實例化（`${base_dataset_version}` 等模板已解析，檢查的是本次 run 實際讀寫的路徑/Hive 表）。

刻意**不採用**「名字是否定義於 catalog.yaml」這個靜態判準，理由：

- memory-only dataset 不在 catalog config 中，`exists()` 回 `False`，自然落入「拉回生產者」分支，無需特殊標記。
- 「有定義但磁碟上尚未產出」（上次跑掛在半路）同樣回 `False` → 自動補跑，一個判準涵蓋兩種缺料。
- 對 dataset type 無假設：`MemoryDataset` 目前不在 `_DATASET_REGISTRY`（YAML 寫 `type: MemoryDataset` 會在 catalog 建構時 `ValueError`），但即使未來放行，新 process 的 MemoryDataset 必為空、`exists()` 回 `False`，行為仍正確。靜態判準在此情境會誤判為可載入、下游 load 到 `None`。
- `parameters` 是 CLI 注入的帶值 MemoryDataset，`exists()` 恆真，不觸發擴張。

**誠實的限制**：`exists()` 只回答存在與否，不回答「是否由當前參數產生」。版本化路徑產物天然防呆（參數變 → hash 變 → 路徑變 → `exists()` 假 → 自動補跑）；但**不帶版本的覆寫式 Hive 表**（`recsys_prod_train_keys` 等）存在 ≠ 新鮮，此風險由使用者承擔，執行計畫尾端印固定警語（接續的前提是參數未變）。

## 3. 元件與改動面

| 元件 | 改動 |
|---|---|
| `src/recsys_tfb/core/pipeline.py` | 新增 `slice_from(start_node, can_load)`、`slice_only(node_name, can_load)`，共用反向擴張 helper；回傳 `(Pipeline, SlicePlan)`。與既有 `only_nodes_with_outputs`（反向切片：砍下游留上游、純 DAG 不看 catalog）成對，後者不動 |
| `src/recsys_tfb/__main__.py` | 四個指令各加 `--from-node` / `--only-node` / `--dry-run` / `--list-nodes`；統一在 `_execute_pipeline` 處理：組好 catalog 後切片、印執行計畫、`--dry-run` 印完即退。`--list-nodes` 對每個 node 試算切片、印接續成本表後即退 |
| `src/recsys_tfb/core/runner.py` | **零改動**。切片結果是合法 Pipeline；Runner 既有輸入驗證（input 不由前序 node 產出時 fallback 到 `catalog.exists`）與執行邏輯原樣可用 |
| `conf/base/catalog.yaml` | 補落地兩個 HPO 產物（§4） |

### 3.1 SlicePlan

小 dataclass，承載執行計畫與測試斷言：

- `requested`：使用者指定的起點與其後 node（或 only-node）。
- `auto_included`：自動補入的 node，各附觸發原因（缺哪個不可載入的 dataset）。
- `skipped`：被跳過的 node，各附「已存在的輸出」清單。
- `skipped_side_effect`：被跳過的 zero-output node（§5.3）。

### 3.2 執行計畫輸出

切片完成、開跑前印出（並走 structured logging）：

```
[plan] requested start : finalize_model
[plan] skipped (output exists in catalog):
[plan]   tune_hyperparameters        best_params ✓ best_iteration ✓ hpo_best_model ✓
[plan] auto-included (missing input → producer re-run):
[plan]   select_features             ← preprocessor_view (memory-only)
[plan]   cache_train_model_input     ← train_parquet_handle (memory-only)
[plan]   cache_train_dev_model_input ← train_dev_parquet_handle (memory-only)
[plan]   cache_test_model_input      ← test_parquet_handle (memory-only)
[plan] skipped side-effect nodes (outputs=None, not re-validated):
[plan]   (none in this pipeline)
[plan] WARNING: resume assumes parameters unchanged since the artifacts were produced.
[plan] running 11 of 15 nodes
```

這是「自動擴張」不變成「靜默重跑昂貴 node」的關鍵防線：昂貴 node 若被拉回，開跑前就看得到。

## 4. 補落地：HPO 產物

`tune_hyperparameters` 三個輸出中 `best_params` 已落地，`best_iteration` 與 `hpo_best_model` 是 memory-only——這恰好讓「跳過 HPO 重訓 final model」（場景 3）失效：`finalize_model` 吃這兩個輸入，純擴張會把整個 HPO 拉回來。修法是**只加 catalog 條目、不動 node 簽名**（輸出名字已存在，加定義即自動落地）：

```yaml
best_iteration:
  type: JSONDataset
  filepath: data/models/${model_version}/best_iteration.json

hpo_best_model:
  type: ModelAdapterDataset
  filepath: data/models/${model_version}/hpo_best_model.txt
```

實作時必須驗證：

- (a) `hpo_best_model` 的實際型別與 `ModelAdapterDataset.save/load` 相容。
- (b) 某些策略下（如 `refit_on_full`）它是否可能為 `None`；若是，需決議由 dataset 容忍 None 或 node 端保證非 None，並以測試釘住決議。

**其他 pipeline 的 memory-only 產物刻意不落地**（`preprocessor_view`、各 parquet/lgb handle、`eval_predictions`、`evaluation_metrics`、`scoring_dataset`、`X_score` 等）：重算便宜或有既有 cache（lgb `.bin` cache、parquet cache），擴張補跑即可。YAGNI。

## 5. 邊界情況與錯誤處理

1. **未知 node 名**：報錯並列出該 pipeline 全部 node 名（拓撲序，等同 `--list-nodes` 名單）。
2. **flag 互斥**：`--from-node` 與 `--only-node` 同時給 → 直接報錯。
3. **side-effect node（`outputs=None`）**：無輸出 → 反向擴張永遠不會拉回。位於起點前即被跳過（dataset 的 `validate_data_consistency` B1 資料閘在接續跑時**不重驗**——接續場景資料未變、前次已驗過）。計畫輸出單獨列出讓使用者知情，此為明文行為。
4. **`@` handle 輸入**（如 `@training_eval_predictions`）：拿 catalog handle 而非資料，與一般輸入同用 `exists()` 判斷，無特殊處理。
5. **manifest**：post-run manifest 照常寫（接續成功後內容與 full run 相同——參數沒變才該接續）；metadata 加 `resumed_from: <node>` 留痕。`--dry-run` 與 `--list-nodes` 不寫任何東西、不執行任何 node。
6. **覆寫式 Hive 表新鮮度**：見 §2.3 限制；計畫輸出固定警語。

## 6. 三道防線（防未來退化）

切片可用性不是寫一次就永久成立的性質：新增一個 memory-only 中間產物，就可能讓原本便宜的接續點默默變貴。Node 的 `inputs`/`outputs` 是描述性事實（決定切片「實際會」補跑什麼），需要另一層規範性承諾（宣告「應該只」補跑什麼）守護：

1. **`--list-nodes` 標註接續成本**（使用面）：對每個 node 試算切片，列出 auto-included 集合。使用者選起點前可見代價；開發者改完 pipeline 肉眼可驗。
2. **`RESUME_CONTRACTS` 契約測試**（開發面，最有牙齒）：每個 pipeline 在測試中宣告承諾支援的接續點與允許的 auto-included 上限集合，例如：

   ```python
   RESUME_CONTRACTS = {
       "training": {
           "finalize_model": {"select_features", "cache_train_model_input",
                              "cache_train_dev_model_input", "cache_test_model_input"},
       },
       "dataset": {
           "fit_preprocessor_metadata": set(),
       },
   }
   ```

   測試用 stub `can_load`（假設所有 catalog 定義產物存在）做純 DAG 切片，斷言 auto-included 不超出宣告。破壞時紅燈，錯誤訊息指明兩條路：給新產物補 catalog 落地，或有意識修改契約（在 PR review 中可見）。毫秒級、無 Spark。仿 `core/consistency.py` 哲學（不變量集中宣告），差別在執行時機：consistency 是 runtime config 閘擋使用者配置錯誤，契約測試是測試閘擋開發者結構退化，故放 `tests/` 不放 `src/`。
3. **文件**（解釋面）：docs/ 撰寫使用說明與開發守則——node 輸出要不要進 catalog 落地的判準（「是否為某宣告接續點的必要輸入」×「重算貴不貴」）；改 pipeline 結構後跑 `--list-nodes` 與契約測試確認沒破壞接續點。CLAUDE.md 加一行指回。

## 7. 測試策略

1. **切片演算法單元測試**（純 DAG + fake `can_load`，無 Spark）：基本切片、擴張一層／遞迴多層、停止於可載入產物、per-dataset 判斷（多輸出 node 部分落地）、side-effect node 不被拉回、`slice_only`、未知 node 名報錯、最壞退化成 full pipeline、SlicePlan 內容正確。
2. **契約測試**：§6-2 本身，對四個 pipeline 的宣告接續點逐一驗證。
3. **CLI 測試**：`--dry-run` 印計畫不執行、`--list-nodes` 輸出、flag 互斥報錯、不給 flag 行為不變。
4. **補落地 round-trip**：`best_iteration` / `hpo_best_model` 經 catalog save→load 還原；§4(b) None 案例的決議行為。
5. **本機 Spark 整合 smoke**（`--env local`）：`dataset` full run 後 `--from-node fit_preprocessor_metadata` 接續成功；`training` full run 後 `--from-node finalize_model` 驗證 HPO 被跳過（斷言計畫輸出）。

## 8. 明確不做

- **Runner 內建 make-style 自動跳過**（執行前檢查輸出存在即跳過、無需 flag）：已評估否決。覆寫式 Hive 表跑過一次永遠 `exists()=True`，會讓每次 run 靜默跳過、參數改了也不重算——「存在 = 不用重跑」前提在本 repo 不成立，且違反「不給 flag 零行為改變」。
- `--to-node` / `--to-outputs`：`only_nodes_with_outputs` 留在 `Pipeline` 上但不接 CLI。
- `source_etl` 的 node 切片。
- 為切片落地 §4 以外的任何中間產物。

## 9. 已拍板的決策紀錄

| 決策 | 選擇 | 備案（否決） |
|---|---|---|
| memory-only 輸入處理 | 自動擴張補跑＋補落地關鍵產物 | fail-fast 報錯；純補落地不擴張 |
| 架構層次 | CLI 顯式切片（Pipeline 方法 + flag，Runner 零改動） | Runner make-style 增量；node 級子指令重構 |
| `--from-node` 語意 | 拓撲位置（含平行分支） | 下游閉包（kedro 式） |
| 守門 node（outputs=None） | 跳過、計畫明示 | 一律保留；flag 控制 |
| 可載入判準 | runtime `catalog.exists()` | 靜態「是否定義於 catalog.yaml」 |
| CLI 介面 | `--from-node` / `--only-node` / `--dry-run` / `--list-nodes` 全做 | — |
