# HPO 崩潰復原 + 接續搜尋設計

- 日期：2026-06-11
- 分支：`feat/hpo-resume-checkpoint`
- 範圍：`tune_hyperparameters` 節點（training pipeline）內部
- 狀態：設計定稿，待轉實作計畫

## 1. 動機與目標

Training pipeline 跑 HPO 時若中途 crash（OOM、被 kill、trial 內例外），目前所有已完成
trial 與當前最佳模型全部白費，必須從 trial 0 重跑。

**目標**：HPO 進行中 crash 時，已完成 trial 的歷史與當前最佳模型不白費；重跑只補跑剩餘
trial、零重訓直接拿回最佳模型。

### 1.1 現況盤點

`tune_hyperparameters`（`src/recsys_tfb/pipelines/training/nodes.py`）目前的持久化邊界：

| 項目 | 現況 | 缺口 |
|---|---|---|
| Optuna study | 純記憶體（`create_study()` 無 `storage=`） | crash 時所有已完成 trials 全失 |
| 最佳模型 | 只在 `best_state["model"]` 記憶體 | crash 時連目前最佳也失 |
| 節點輸出 `best_params` / `best_iteration` / `hpo_best_model` | 已落地 catalog（`data/models/${model_version}/...`），且可 `--from-node finalize_model` 接續 | 只在「整個 HPO 跑完」才寫，跑到一半 crash 沒幫助 |

「HPO 全部跑完、但下游 node 掛掉」這條已有解（node slicing 從 `finalize_model` 接續，
PR#76）。本設計補的是**唯一未覆蓋的缺口：HPO 跑到一半 crash**。

### 1.2 為什麼兩件事必須一起做

Optuna 的 storage **只存每個 trial 的「參數 + 分數」純量，不存訓練好的模型**。所以光持久化
study（接續 trials）不夠：接續後要拿最佳模型，仍得用 `best_params` 重訓一次。因此必須同時
做「每次刷新最佳就 checkpoint 模型」，才能在接續後零重訓直接拿回最佳模型。

## 2. 範圍

**範圍內**

1. Optuna study 持久化（`JournalStorage` + `JournalFileBackend`），支援接續 trials。
2. 每次刷新最佳就 checkpoint 模型 + meta。
3. 自動接續（idempotent）+ 顯著 log。
4. `--fresh-hpo` runtime flag（強制重來的逃生口）。
5. `search_id` keying —— 順帶讓「調高 `n_trials` 就延長搜尋」自然可用。

**範圍外（保留、不做）**

- per-trial MLflow nested logging（MLflow 為 best-effort sink，非復原機制依賴；觀測性 nice-to-have）。
- resumed-vs-uninterrupted 的位元級重現（需 pickle/還原 sampler 狀態）。
- HDFS 持久化 study（driver-local 即可，與既有產物同一保證）。
- 並行 / 分散式 HPO worker（單行程假設）。
- `--extend-hpo N` CLI 糖（延長已可用「改 config 的 `n_trials`」達成）。
- 把 `num_iterations` / `early_stopping_rounds` 當超參數搜尋（移入 `search_space` 的路徑）。

## 3. 架構總覽（什麼變、什麼不變）

- **只改 `tune_hyperparameters` 節點內部**。節點三個輸出（`best_params` / `best_iteration` /
  `hpo_best_model`）的型別、catalog 落地位置、下游節點與 `--from-node finalize_model` **全部不變**。
- 新增「搜尋工作狀態」目錄 `data/models/_hpo/${search_id}/`，放 Optuna journal 與 checkpoint。
  這是**內部可接續狀態**，與 model_version-keyed 的最終產物分離。
- HPO 迴圈本身是 **driver-CPU、無 Spark**（讀預建 lgb binaries + val parquet via pyarrow），
  故 journal / checkpoint 都是純本機檔 I/O，即使 Spark 已死也安全。
- **持久化可行性**：`ModelAdapterDataset` / `JSONDataset` 都是寫 driver-local 的
  `data/models/${model_version}/...`（相對路徑、本機 fs），模型產物不走 HDFS。新放的 study /
  checkpoint 落在同一個 `data/models/` 樹下，存活保證與「現有那個讓
  `--from-node finalize_model` 能接續的 `hpo/model.txt`」**完全一致**。同樣的限制：若整個
  driver 本機碟在兩次執行間被清空，則無法接續（與現有節點輸出限制一致）。

## 4. 檔案佈局

```
data/models/_hpo/${search_id}/         # 搜尋工作狀態（跨 model_version 共用）
  study_journal.log                    # Optuna JournalFileBackend（append-only）
  checkpoint/
    model.txt                          # adapter.save()：raw hpo 最佳 booster
    best_meta.json                     # {score, best_iteration, best_params,
                                        #  trial_number, search_id}
data/models/${model_version}/          # 最終產物（位置不變）
  best_params.json  best_iteration.json
  hpo/model.txt (+ model_meta.json)    # hpo_best_model（節點結束時照舊寫）
```

`_hpo` 前綴底線避免與真實 model_version（8-hex hash）目錄衝突。

## 5. storage backend

- `optuna.storages.journal.JournalStorage(JournalFileBackend(study_journal.log))`（Optuna 4.5.0
  現行 API；append-only、crash 安全、內建零額外套件 —— 符合「無額外套件」生產限制）。
- `create_study(storage=…, study_name=search_id, load_if_exists=True,
  sampler=TPESampler(seed=random_seed), direction="maximize")`。
- 選 Journal 而非 sqlite `RDBStorage`：append-only 對 crash 中途寫入更安全（末筆半寫紀錄被忽略），
  且無 sqlite 在某些檔案系統的 locking 疑慮。

## 6. `search_id` 計算

```
search_id = _hash8( _model_version_payload(params)  去掉 training.n_trials )
```

- 重用現有 `src/recsys_tfb/core/versioning.py` 的 `_model_version_payload`（已排除
  `verbosity` / `log_period` / `num_threads`），再 `pop` 掉 `n_trials`。
- 精確欄位以 `versioning.py` 為**單一真實來源**（新增 `compute_search_id`），不在 HPO 節點散落。

### 6.1 納入準則（為何排除 `n_trials`、但保留 `num_iterations` / `early_stopping_rounds`）

study 累積的 trial 是一組 `(params → score)` 量測。只有當某 knob **不改變「一個 trial 的 score
代表什麼意思」** 時，跨它的變動重用舊 trial 才合理：

| 參數 | 在 model_version | 在 search_id | 理由 |
|---|---|---|---|
| `n_trials` | ✅ 留 | ❌ **排除** | 只改「抽樣幾次」、不改 score 意義 → 排除即可接續/延長 |
| `num_iterations` | ✅ 留 | ✅ 留 | 改它會讓舊 trial 的 score 失真（不同訓練預算下測得），TPE surrogate 失準、`study.best_value` 不可比 → 一改就該開新 study |
| `early_stopping_rounds` | ✅ 留 | ✅ 留 | 同上，改變停止點＝改變每個 trial 的 score 意義 |

行為結論：改 `search_space` / `hpo_objective` / `num_iterations` / `early_stopping_rounds` /
資料身分 → `search_id` 變 → 自動開新 study（正確失效）；**只改 `n_trials`** → `search_id` 不變
→ 接續（同值補跑）或延長（調高補跑差額）。延長到 30 後最佳模型會落在新的 `model_version`
目錄（20-trial 與 30-trial 是不同模型身分），但兩者共用同一個 `search_id` study/checkpoint，
此為刻意且合理的設計。

## 7. 接續控制流（核心演算法）

```
search_id = compute_search_id(parameters)
study_dir = data/models/_hpo/<search_id>/

# §9 逃生口
if fresh_hpo:
    n = count_completed(study_dir)              # 讀現有 journal（若有）
    log.warning("--fresh-hpo: 清除 %s，丟棄 %d 個已完成 trial（前最佳 score=%.4f）",
                study_dir, n, prev_best_or_nan)
    rmtree(study_dir)

study = create_study(storage=Journal(study_dir/study_journal.log),
                     study_name=search_id, load_if_exists=True,
                     sampler=TPESampler(seed=random_seed), direction="maximize")
done   = #{trial in study if state == COMPLETE}
target = training.n_trials
remaining = max(0, target - done)

# 種回 best_state（checkpoint 是 best-so-far 的耐久鏡像）
if checkpoint 存在且可讀:
    best_state.score, best_state.iteration, best_state.params = 讀 best_meta.json
    best_state.model = get_adapter(algorithm).load(checkpoint/model.txt)
    log.info("接續：偵測到 %d 個已完成 trial，目前最佳 score=%.4f（trial #%d），補跑 %d 個",
             done, best_state.score, best_state.trial_number, remaining)
else:
    best_state = {score:-1.0, model:None, iteration:0}   # 全新或極少數 checkpoint 損毀

if remaining > 0:
    study.optimize(objective, n_trials=remaining)        # 只跑剩餘
else:
    log.info("done(%d) >= target(%d)，跳過搜尋，直接用 checkpoint 收尾", done, target)

# 收尾保證 best_state.model 有值
if best_state.model is None:        # 防衛：study 有 trial 但 checkpoint 不可用
    log.warning("checkpoint 不可用，以 study.best_params 重訓一次（last-resort）")
    best_state.model = refit_once(study.best_params)
    best_state.iteration, best_state.params = best_state.model.booster.best_iteration, study.best_params

return best_state.params, best_state.iteration, best_state.model
```

- `objective(trial)` 內部維持現狀（建 lgb.Dataset → train → predict → `_hpo_score`），只在
  「`score > best_state["score"]`」分支多做 §8 的 checkpoint 寫入。
- `n_trials` 語意：Optuna 的 `optimize(n_trials=N)` 是「這次呼叫跑 N 個新 trial」，**非目標總數**。
  本設計把 config 的 `n_trials` 當目標總數，由上面 `remaining = target - done` 換算，故接續只補跑
  差額（crash 在 15/20 → 接續只跑 5、總共仍 20）。

## 8. 每次刷新最佳的 checkpoint

- **時機**：`objective` 內 `score > best_state["score"]`（沿用現有判斷）。因 best_state 初始
  score = -1.0，第一個完成的 trial 必觸發 → 「有完成 trial 就一定有 checkpoint」。
- **動作（原子寫入：先寫 temp 再 rename，避免半寫）**：
  - `checkpoint/model.txt` ← `adapter.save()`（raw hpo booster，未校準）
  - `checkpoint/best_meta.json` ← `{score, best_iteration: adapter.booster.best_iteration,
    best_params: trial_params, trial_number: trial.number, search_id}`
- **`best_iteration` 以 meta 為準**：LightGBM `Booster(model_file=…)` 重載後 `best_iteration`
  不保證保留，故獨立寫進 meta、收尾與接續都讀 meta，不依賴重載 booster。

## 9. CLI / config 介面

- **預設自動接續**：無需任何 flag，重跑同 config 即接續。
- `training.hpo_checkpointing: true`（預設 `true`）：設 `false` 可退回舊「純記憶體、不落地」
  行為（debug / 逃生彈性）。此時 `--fresh-hpo` 形同 no-op。
- `--fresh-hpo`（runtime flag，僅 `training` pipeline 接受；其他 pipeline 帶它視為用法錯誤、
  fail loud）：plumbing 沿用 node-slicing PR 既有的 runtime-flag 注入模式（`__main__` 解析後注入
  parameters，節點讀取）。
- **顯著 log**：每次載入舊 study 都明列「done 個已完成、目前最佳 score / trial、補跑 remaining 個」。

### 9.1 `--fresh-hpo` 行為規格

定義：**這次執行忽略並丟棄目前 `search_id` 已累積的 study/checkpoint，從 trial 0 重新搜尋**。

| 對象 | 是否受影響 |
|---|---|
| `data/models/_hpo/${search_id}/`（**當前** search_id 的 journal+checkpoint） | ✅ 刪除（rmtree，發生在載入 study 之前） |
| **其他** search_id 的目錄 | ❌ 不碰（只清當前這把） |
| `data/models/${model_version}/`（最終產物） | ❌ 不刪，照常在節點結束被新結果覆寫 |
| 下游節點 / `--from-node` | ❌ 不影響 |

邊界與互動：

- 目錄不存在 → no-op（永遠可安全附帶）。
- `done ≥ target`：不帶 flag 會 remaining=0 秒收（重用）；帶 `--fresh-hpo` 正是強制重搜的方法。
- 與延長並用：`n_trials` 20→30 又帶 `--fresh-hpo` → 先清空 → 跑滿 30（非接 10）。`--fresh-hpo`
  一律壓過接續/延長語意。
- 同 config + 同 seed 重搜 → trial 序列與最初首跑完全一致（決定論）。真正用途：懷疑
  study/checkpoint 損毀或想丟棄被污染的搜尋重來。
- 刪除前必在 log 明列「丟棄 N 個已完成 trial」作為誤觸的可見性保護。
- 非並行安全（rmtree 假設單行程，對齊 §11 假設）。
- 不持久化到 config，僅本次執行有效。

清掉所有實驗的可接續狀態 = 手動 `rm -rf data/models/_hpo/`；`--fresh-hpo` 只針對當前 search_id。

## 10. 向後相容與決定論注意事項

- **首跑（無既有檔）行為與現狀等價**：仍跑滿 `n_trials`，只是多寫了 journal/checkpoint。無回歸。
- **決定論誠實說明**：resumed run **不是**單次不中斷執行的位元級複製。已完成 trial 完全重用；
  但 TPE sampler 在接續時以同 seed **重新建立**，其 RNG 不還原到中斷前位置，故**剩餘 trial
  取樣的參數可能與不中斷版不同**。結果仍有效、不浪費。要位元級重現需 pickle/還原 sampler
  狀態 —— 刻意不做（YAGNI）。

## 11. 失敗邊界處理

| 情況 | 處理 |
|---|---|
| crash 當下 RUNNING 的 trial | 留 RUNNING 狀態、不算 done、不佔 target；接續重新探索補足 |
| trial 訓練中 raise（FAIL） | 只算 COMPLETE，FAIL 不佔 target → 仍補足到 target 個成功 trial |
| study 有 trial 但 checkpoint 損毀/缺 | §7 last-resort：用 `study.best_params` 重訓一次 |
| journal 末筆半寫 | JournalFileBackend append-only 可容忍，最後一筆不完整紀錄被忽略 |
| `done ≥ target` | 跳過 `optimize`，直接 checkpoint 收尾 |

（單行程假設：同一 `search_id` 不應同時有兩個訓練在跑。）

## 12. 清理生命週期

- 預設**成功後保留** `_hpo/${search_id}/`（journal + checkpoint 很小：純量 + 單一 booster）。
  好處：重跑 idempotent（remaining=0 秒收）、可稽核全部 trial。
- 清理由使用者主動：`--fresh-hpo`（單一 search_id）或手動刪 `data/models/_hpo/`。
- **不**隨任一 model_version 目錄刪除而連帶清（跨 model_version 共用）。文件需明列此目錄存在與清法。
- worktree 隔離：落在 `data/` 下，沿用「每個 worktree 自己的真 `data/` 樹」隔離，不額外處理。

## 13. 測試策略（對齊「測試跑快、不略過」）

- **純函式快測**：
  - `compute_search_id`：排除 `n_trials`（改它 search_id 不變）；含 `num_iterations` /
    `early_stopping_rounds` / `search_space` / 資料身分（改它們 search_id 變）。
  - `remaining = max(0, target - done)`；`done ≥ target` 短路。
- **checkpoint round-trip**：寫 → 載 best_meta + model；`best_iteration` 以 meta 為準。
- **接續整合測（不需殺 process）**：對同一 Journal storage 分兩次 `optimize`（先 k 個模擬 crash，
  再開新 study 物件接續），斷言總 COMPLETE == target、且接續那次只跑 `target - k` 個；小 lgb 資料
  + `n_trials=2~3`、`num_iterations` 小，driver-local 秒級。
- **fallback / 逃生**：checkpoint 缺 → 觸發 last-resort refit；`--fresh-hpo` → 目錄被清、從 0 跑、
  log 明列丟棄數；`hpo_checkpointing=false` → `--fresh-hpo` no-op、無持久化檔產生。

## 14. 公司環境驗證

本設計在 local（`--env local`、`SPARK_CONF_DIR=conf/spark-local`）可完整測（HPO 為 driver-local、
無需 docker）。公司環境真實訓練的接續驗證屬部署後事項，列為已知未驗項，不阻擋本次 local 驗收。
