# `tune_hyperparameters` observability

**Date**: 2026-05-12
**Status**: Draft

## 背景

公司環境跑 `tune_hyperparameters` 時，log 在 `Step started: optuna_optimize` 之後就一片寂靜直到 study 結束。50 trials × 每個 trial 5-15 分鐘 = 多達 8 小時 silence，沒有任何 progress 指標。使用者無法判斷：

- HPO 是否還在跑（vs hang / crash）
- 目前進到第幾個 trial
- 哪個 sub-step 在花時間（dataset load / training / predict / score）
- 累計 best ap 是多少

跟 PR #9 [`extract_Xy` 子步驟可觀測性](2026-05-12-extract-xy-observability-design.md) / PR #10 [pre-read metadata](2026-05-12-extract-xy-pre-read-metadata-design.md) 是同一個觀測性系列；同樣的 pattern 套用到 `tune_hyperparameters`。

## 設計

只動 `src/recsys_tfb/pipelines/training/nodes.py` 中的 `tune_hyperparameters`（lines 254-368）。Optuna verbosity 維持 WARNING（避免跟我們的 per-trial 摘要 double-log）；外層 `with log_step(logger, "optuna_optimize"):` 包裹保留。

### `objective(trial)` 改動

加 entry/exit INFO summary，加 4 個內部 `log_step` 包：

```python
def objective(trial: optuna.Trial) -> float:
    trial_idx = trial.number
    trial_params = {...}  # 既有
    params = {...}        # 既有

    logger.info(
        "tune_hyperparameters: trial=%d/%d start params=%s",
        trial_idx, n_trials, trial_params,
    )
    t0 = time.monotonic()

    adapter = get_adapter(algorithm)

    with log_step(logger, "prepare_datasets"):
        ds_train = train_lgb_handle.load()
        ds_dev = train_dev_lgb_handle.load(reference=ds_train)

    with log_step(logger, "train"):
        adapter.train(
            X_train=None, y_train=None, X_val=None, y_val=None,
            params=params,
            train_dataset=ds_train, val_dataset=ds_dev,
        )

    with log_step(logger, "predict"):
        y_pred = adapter.predict(X_v)

    with log_step(logger, "score"):
        ap = compute_ap(y_v, y_pred)
        ap = ap if ap is not None else 0.0

    if ap > best_state["ap"]:
        best_state["ap"] = ap
        best_state["model"] = adapter
        best_state["iteration"] = adapter.booster.best_iteration

    duration = time.monotonic() - t0
    logger.info(
        "tune_hyperparameters: trial=%d/%d completed ap=%.4f "
        "best_iteration=%d duration=%.1fs best_so_far=%.4f",
        trial_idx, n_trials, ap,
        adapter.booster.best_iteration, duration, best_state["ap"],
    )
    return ap
```

需要 `import time`（module-level）。

### 關鍵設計決策

- **Step names 用 generic 名稱** (`prepare_datasets` / `train` / `predict` / `score`)，不包成 `trial_0_train` 那樣。跟 PR #9 一致；trial 識別走 INFO start 那行（在每組 step events 之前）。要找特定 trial 的 step duration 靠 timestamp 對齊 INFO 行。
- **`time.monotonic()`** 而非 `time.time()` — 不受系統鐘錶調整影響。
- **`adapter.booster.best_iteration`** 從當前 trial-local `adapter` 拿，**不**用 `best_state` 那份 — 避免摘要行記到不是當前 trial 的 iteration。
- **`start` 行印 `trial_params`**（搜索維度），**不**印展開後完整 `params`（含 `algorithm_params` 會冗很多）。
- **`duration` 用秒、1 位小數**：簡單 grep-able；要分鐘自己除 60。

### 完整 log 樣態

```
INFO  Step started: extract_features                                  ← 既有
INFO  Step completed: extract_features
INFO  Step started: optuna_optimize                                   ← 既有
INFO  tune_hyperparameters: trial=0/50 start params={'learning_rate': 0.012, ...}  ← 新
INFO  Step started: prepare_datasets                                  ← 新
INFO  Step completed: prepare_datasets
INFO  Step started: train                                             ← 新
INFO  Step completed: train
INFO  Step started: predict                                           ← 新
INFO  Step completed: predict
INFO  Step started: score                                             ← 新
INFO  Step completed: score
INFO  tune_hyperparameters: trial=0/50 completed ap=0.3421 best_iteration=215 duration=487.3s best_so_far=0.3421  ← 新
INFO  tune_hyperparameters: trial=1/50 start ...
... (repeat per trial)
INFO  tune_hyperparameters: trial=49/50 completed ap=... ...
INFO  Step completed: optuna_optimize                                 ← 既有
INFO  Best trial mAP: ..., best_iteration: ..., params: ...           ← 既有 (line 365)
```

每個 trial 產出 10 條 log（2 INFO summary + 4 step pairs）。50 trials = 500 條，相對於 8 小時 run 是健康量。

## 驗證

### 單元測試

加在 `tests/test_pipelines/test_training/test_nodes.py` 既有 `TestTuneHyperparameters` class 內（lines 207-258）。Fixtures `lgb_handles` / `synthetic_model_inputs` / `preprocessor_metadata` / `training_parameters` 已跑真正的 mini Optuna study；全部用 `caplog`：

1. **`test_emits_trial_start_and_completed_info_lines`**
   - assert `caplog` 中有 `n_trials` 條 `trial=N/total start params=...`
   - assert `n_trials` 條 `trial=N/total completed ap=... best_iteration=... duration=...s best_so_far=...`
   - `trial_idx` 從 0 連續到 `n_trials-1`

2. **`test_emits_inner_step_events_per_trial`**
   - assert step_started/step_completed 各 `4 × n_trials` 次（透過 `r.event` / `r.step` 屬性）
   - step 名稱為 `prepare_datasets`、`train`、`predict`、`score`

3. **`test_completed_line_has_correct_best_so_far`**
   - `best_so_far` 單調遞增 — assert 每條 completed 的 `best_so_far` ≥ 前一條的 `best_so_far`
   - 最後一條 `best_so_far` == `study.best_value`（即 return 的 best result）

4. **`test_start_line_params_contains_only_search_dimensions`**
   - assert `start` 行的 `params={...}` 字串包含 `learning_rate`、`num_leaves` 等 search space key
   - **不**包含 `algorithm_params` 中的 key（如 `objective`、`metric`）— 確保印的是 `trial_params`、不是展開後完整 `params`

### 公司環境驗證

PR merge 後使用者重跑 `tune_hyperparameters`，預期看到：

- 每個 trial 前後各一條 `tune_hyperparameters: trial=N/total ...` INFO
- 4 對 `Step started:` / `Step completed:` 在每個 trial 內
- 任何 trial 卡 > 5 分鐘可以從最近一條 `Step started:` 直接看出卡在哪個 sub-step

## 不做的事

- **不改 `optuna.logging.set_verbosity`**：維持 WARNING。我們的 INFO 摘要已涵蓋 trial 完成資訊；改成 INFO 會 double-log。
- **不加 fail-fast / threshold**：純記錄。某個 trial 跑很久不會主動殺 study；要停就 Ctrl-C / kill。
- **不改 `tune_hyperparameters` 簽名或 return values**：純 observability，外部 contract 不動。
- **不動 `finalize_model` / `calibrate_model` / `evaluate_model` 的 observability**：它們各自已有 `extract_features` / `predict` / `fit_calibrator` 等 step events，且不是 long-running loop，本 PR 不擴張 scope。
- **不加 Optuna pruning / 中途報告**：另一個獨立 feature。
- **不改現有 `Best trial mAP: ...` summary line**（line 364-367 既有）。
- **不在 INFO 行加 MLflow integration**：純 stdout/stderr log。
