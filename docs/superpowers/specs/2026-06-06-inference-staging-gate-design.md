# Inference staging → validate → publish gate

**Date:** 2026-06-06
**Branch:** `feat/inference-staging-gate`
**Status:** Design approved, pending spec review

## 背景與動機

目前 inference pipeline 尾段的 `validate_predictions` 是一個**會寫資料的「驗證」節點**,造成三個問題:

1. **名實不符**:節點叫 `validate`,卻把資料覆寫回 production 表。看 DAG 的人不會預期「跑驗證」會改 production 資料。
2. **self-overwrite 反模式**:`validated_predictions` 與 `ranked_predictions` 是 catalog 兩個別名指向**同一張** Hive 表。Runner 對 validate 的 input 走 `catalog.load()`(從 Hive 重讀)、對 output 走 `catalog.save()`(寫回同一張表),等於 `INSERT OVERWRITE T SELECT * FROM T`,在 dynamic partition overwrite 的 staging→swap 語義下可能 `AnalysisException` 或半寫/清空。
3. **先發布後驗證(gate 沒 gate 到)**:`rank_predictions` 在 validate **之前**就把 `ranked_predictions` 寫進 Hive(write #1)。驗證失敗時壞資料**早已 published**,gate 只能讓「run 失敗」,不能讓「publish 失敗」。`validated_predictions` 的第二次寫(相同 bytes)對「已發布什麼」零保護,且**沒有任何 reader**。

## 目標

把驗證變成**真正的發布閘門**:已驗證的資料**只在通過時**才落到 production `ranked_predictions`;驗證失敗則 production 完全不被寫入。

## 限制與非目標

- **無 backward-compat 包袱**:inference pipeline 尚未在公司環境部署/開發,不影響既有資料 → 可乾淨切換,不保留 `validated_predictions`、不做 deprecation shim、不需資料遷移。
- **不加 cleanup node**:`ranked_staging` 靠 `model_version` 分區於下次 run 自我覆寫;刻意保留作為驗證失敗時的 forensic artifact。
- **evaluation 程式碼不改**(見下節分析,介面不變)。
- **dev-cluster e2e 實跑不在本次 scope**:以 unit / structure 測試 + `git diff` 驗證;需要時另跑。

## 設計

### 資料流(改後)

```
predict_scores       → score_table          (Hive, 不變)
rank_predictions     → ranked_staging        (Hive 新表, write #1)
validate_predictions(ranked_staging, scoring_dataset)
                     → validated_predictions (intermediate / auto-MemoryDataset; 失敗即 raise ValidationError)
publish_predictions(validated_predictions)
                     → ranked_predictions     (Hive production, write #2 = 唯一 production 寫入)
```

DAG topological order:`score_table → ranked_staging → validated_predictions → ranked_predictions`,publish 嚴格晚於 validate。寫入次數仍為 2(與現狀相同),但 staging≠production 兩張不同表 → **無 self-overwrite**。

### Gate 語義

- validate `raise ValidationError` → Runner 中止 → **publish 不執行 → production `ranked_predictions` 本次不寫**。
- 失敗批次留在 `ranked_staging`(供事後排查);`score_table` / `ranked_staging` 是 intermediate,下次 run 依 `model_version` 分區自我覆寫。
- 跨 run:dynamic partition overwrite by `model_version`,失敗 run **不會**清掉先前已驗證 OK 的舊 `model_version` 分區(因 publish 沒跑)。

### catalog.yaml

- **新增** `ranked_staging`:managed `HiveTableDataset`,欄位 `cust_id/score/rank` + 分區 `snap_date/prod_name/model_version`,`write_mode=overwrite`(dynamic)。
- **移除** `validated_predictions` entry → 變 auto-`MemoryDataset`(produced→consumed,Runner 於最後 consumer 後釋放)。
- **保留** `ranked_predictions`(production 輸出 + standalone evaluation 讀取入口),更新註解為「由 `publish_predictions` 寫入」。

### 程式碼

- `src/recsys_tfb/pipelines/inference/pipeline.py`:`rank_predictions` output 改 `ranked_staging`;`validate_predictions` input 改 `ranked_staging`;新增 `publish_predictions` node(input `validated_predictions`、output `ranked_predictions`)。
- `src/recsys_tfb/pipelines/inference/nodes_spark.py`:**新增** `publish_predictions(validated_predictions, parameters)` —— 純 pass-through(回傳原物)+ log `model_version`;production 寫入由 Runner 對 `ranked_predictions` output 的 `catalog.save()` 完成。`rank_predictions` / `validate_predictions` **函式本體不動**(只改接線),validate 的 6 個 sanity check 與 raise 行為原樣保留。

## evaluation 互動分析

evaluation 三種模式讀的來源:監控預設 `ranked_predictions`、`--post-training` `training_eval_predictions`、`--compare-only` `enriched_eval_predictions`。

1. **介面不變**:保留 `ranked_predictions` 為已宣告 HiveTableDataset(改由 publish 寫)。evaluation 端三張表都不碰 `ranked_staging` / `validated_predictions` → **evaluation 程式碼零改動**。
2. **正確性提升**:現況 `rank_predictions` 在 validate 前就寫 `ranked_predictions`,驗證失敗時壞資料已躺在表裡,事後 standalone 監控 evaluation 會把壞資料當正常算指標。Option B 補掉此洞:publish 只在驗證通過後寫 production → 驗證失敗該 `model_version` 無分區 → evaluation 觸發既有 loud `No predictions found`(列出可用 snap_dates),而非默默評估未驗證資料。
3. **standalone evaluation 仍受保護**:`ranked_predictions` 顯式宣告的理由(MemoryDataset fallback footgun,design-principles §9)依舊成立。

## 測試(TDD)

- 既有 `tests/test_pipelines/test_inference/test_validation.py`:**不變**(positional 呼叫、validate signature/body 不變)→ 仍 green,作防退步護欄。
- `tests/test_pipelines/test_inference/test_pipeline.py`:**RED** → 6 nodes / 新 outputs set(含 `ranked_staging`、`validated_predictions`、`ranked_predictions`)/ `publish_predictions` 在 node names / 新增「staging→validate→publish 串接 + topological 順序 `rank < validate < publish`」斷言(直接證明 production 寫入在驗證之後)。
- 新 `tests/test_pipelines/test_inference/test_publish.py`:**RED** → `publish_predictions` 回傳原物 + 容忍缺 `model_version`。
- 執行:worktree 絕對 venv python + `PYTHONPATH=<wt>/src`。

## docs scope(只動 live 文件)

無 `docs/pipelines/inference.md`(inference 寫在 README + `data-lineage.html`)。

| 檔案 | 改什麼 |
|---|---|
| `README.md` | inference node 表(L137–142,目前連 `validate_predictions` 都沒列)補成 `rank_predictions → ranked_staging`、新增 `validate_predictions`(gate)、`publish_predictions → ranked_predictions`;L153 catalog 清單加 `ranked_staging` |
| `docs/design-principles.md` | §9(L72–77)catalog footgun 段:`ranked_predictions` 改述為 publish 輸出 + 讀取入口;補「驗證閘門 gate 的是發布、不只是中止 run」的設計原則 |
| `docs/data-lineage.html` | inference lineage 行(L197)、導覽標籤(L252)、`ranked_predictions` 卡片(L516–522):把「= validated_predictions 同一張表」與兩-entry 說明改寫為 `ranked_staging → validate → publish → ranked_predictions` 的 gate |
| `docs/pipelines/evaluation.md` | L17 註明監控讀的是「已驗證/已發布」的 `ranked_predictions`(輕量) |
| `docs/diagrams/pipeline-overview.svg` | 檢視 L53 一帶;若畫了 node 串接則補 staging 節點,只是 output label 就不動 |

**排除** `docs/superpowers/plans/*` 與 `specs/*`(過去 PR 的時間點紀錄,不竄改歷史)。

## 收尾

- 改完 code 後依 CLAUDE.md 重建 graphify code graph。
- commit / push 由使用者人工觸發。
