# HPO 崩潰復原 + 接續搜尋

`training` pipeline 的 `tune_hyperparameters` 預設持久化 HPO 狀態，crash 後重跑只補跑剩餘
trial、零重訓直接拿回最佳模型。

## 機制

- Optuna study 用 `JournalStorage` 落地 `data/models/_hpo/<search_id>/study_journal.log`；
  每次刷新最佳就原子寫 `checkpoint/model.txt` + `checkpoint/best_meta.json`。
- `search_id` = `model_version` 的 model-defining 輸入去掉 `n_trials`。故：
  - 改 `search_space` / `hpo_objective` / `num_iterations` / `early_stopping_rounds` / 資料身分
    → search_id 變 → 自動開新 study。
  - 只改 `n_trials` → search_id 不變 → 接續（同值補跑）或延長（調高補跑差額）。
- config 的 `n_trials` 當「目標總數」：接續只跑 `max(0, n_trials − 已完成 trial 數)`。

## 操作

- **自動接續**：crash 後用同一份 config 重跑 `training` 即可，log 會印
  `HPO resume: N completed trial(s) found ... running M more`。
- **延長搜尋**：把 `training.n_trials` 調高再跑（最佳模型落新的 model_version 目錄，但共用
  同一 search_id study）。
- **強制重來**：`python -m recsys_tfb training --fresh-hpo` —— 清除當前 search_id 的
  `_hpo/<search_id>/`、從 trial 0 重搜（log 明列丟棄幾個已完成 trial）。
- **關閉持久化**：頂層 `hpo_checkpointing: false`（純記憶體、不落地）。

## 清理

`data/models/_hpo/<search_id>/` 成功後刻意保留（很小、可稽核、重跑秒收）。它**跨 model_version
共用**、不隨任一 model_version 目錄刪除而連帶清。要清：

- 單一搜尋：`--fresh-hpo`（下次該 search_id 執行時清）或手動刪該子目錄。
- 全部：`rm -rf data/models/_hpo/`。

## 限制

- 接續依賴 `data/models/` 在兩次執行間持續存在（與既有 `--from-node finalize_model` 同一保證）；
  driver 本機碟若被清空則無法接續。
- resumed run 非單次不中斷執行的位元級複製：已完成 trial 完全重用，但 TPE sampler 接續時以同
  seed 重建、RNG 不還原，故剩餘 trial 取樣參數可能與不中斷版不同（結果仍有效）。
- 單行程假設：同一 search_id 不應同時兩個訓練在跑。
