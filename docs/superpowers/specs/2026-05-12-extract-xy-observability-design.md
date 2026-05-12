# `extract_Xy` 內部子步驟可觀測性

**Date**: 2026-05-12
**Status**: Draft

## 背景與動機

公司環境執行 training pipeline 的 `tune_hyperparameters` 時，外層 `with log_step(logger, "extract_features"):` 之內的 `extract_Xy` 卡了幾分鐘後被 kill（OS signal kill，無 Python traceback）。

現狀問題：
- 外層 `log_step("extract_features")` 只能告訴你「整次 extract 卡住了」
- `extract_Xy` 內部其實有 4 個明顯不同 cost profile 的子步驟（讀 parquet / DataFrame slice / categorical 編碼 / numpy cast），但沒有任何 sub-step 級的 timing 或 size 訊號
- 被 OS kill 時沒有 traceback，只能靠最後一條 log 反推卡點 — 缺少 sub-step `step_started` 事件就完全無從推斷

目標：讓任何一次 `extract_Xy` 呼叫被 kill 後，從 log 即可定位卡在哪一步，並判斷是否為記憶體暴增所致。

## 設計原則

- **單點修改**：5 個呼叫點（val / train / train_dev / calibration / test）都走同一個 `extract_Xy`，只改 `src/recsys_tfb/io/extract.py` 一處全部受益
- **沿用既有 `log_step` framework**：不引入新的 logging 機制，事件 schema 與既有 JSON line logging 一致
- **觀測本身不能變成下一個 OOM 來源**：避免在 string-heavy DataFrame 上呼叫 `memory_usage(deep=True)`；只取 shape、`numpy.ndarray.nbytes`、以及 int-coded 後的 `memory_usage(deep=False)`
- **YAGNI**：只做 timing + 資料大小（user 選擇的 Option B），不加 per-column cardinality、不加 heartbeat、不改外層 `log_step("extract_features")`

## 設計

### 改動範圍

只動 `src/recsys_tfb/io/extract.py`。`nodes.py` 中 5 處的外層 `log_step("extract_features")` 保留 — 它仍然提供「整次 extract 呼叫的總時長」這個高階訊號，與內部 sub-step 不衝突。

### 4 個子步驟 + size 訊號

每段用 `log_step` 包，緊接一行 INFO 摘要 size：

| step name | 包住的操作 | 緊接的 INFO log |
|---|---|---|
| `read_parquet` | `handle.to_pandas()` | `path=…  rows=N  cols=M` |
| `slice_features` | `pdf[feature_cols].copy()` | `rows=N  n_features=M  mem=X.XMB`（`memory_usage(deep=False)`） |
| `encode_categoricals` | for loop 套 `pd.Categorical(...).codes` | `deferred_cats=[…]  count=K`（若 `deferred_cats` 空，**整個 step + log 都跳過**，避免噪音） |
| `to_numpy` | `X_df.values` + `pdf[label_col].values` | `X shape=(…) dtype=… nbytes=X.XMB; y len=… dtype=…` |

額外於 function 入口加一行 INFO：

```
extract_Xy start path=...  n_feature_cols=N  label=...  identity_cols=[...]
```

讓使用者立刻知道是在處理哪個 parquet handle。

### 為什麼可以診斷卡住 + 被 kill

- **卡在 `read_parquet`**：parquet IO / 從 cache.root 讀取慢 → 看到 `step_started step=read_parquet` 後沒有對應 `step_completed`
- **卡在 `slice_features`**：DataFrame copy 撐爆 RAM（128GB，但 val 還在 + 之前的 lgb.Dataset binaries 還活著）→ slice 完的 `mem=` 數字可看出來
- **卡在 `encode_categoricals`**：偶發但可能 — string col 太大、known categories 過長
- **卡在 `to_numpy`**：1500 個 features 是 int + float 混合 → 若有 object dtype fallback 會慢且暴量 → `X dtype=` 是 `object` 就是它
- **OOM kill (signal 9，無 traceback)**：最後一個 `step_started` 之後沒 `step_completed` 就是凶手；前段 `mem=` 趨勢也能反推哪段把記憶體推爆

### log_step 巢狀行為

`log_step` 是純 context manager + module-level `_current_context`，沒有 stack 結構。內外層同時用是合法的：兩層各自發 `step_started`/`step_completed`，JSON 上以 `step` 欄位區別（外 `extract_features`、內 `read_parquet` 等）。不衝突、不需要改 `core/logging.py`。

### 不做的事（YAGNI）

- **不加 per-categorical-col cardinality logging**（user 選 Option B，不是 C）
- **不加 periodic heartbeat**：`to_pandas` 是單一 blocking 呼叫，無法插 progress；用「`step_started` 沒對應 `step_completed`」反推已足夠
- **不改 `nodes.py` 外層 `log_step("extract_features")`**：保留高階總時長訊號
- **不改 log level**：沿用 INFO；extract 呼叫頻率不高（5 個呼叫點，HPO 中 val 也只進入一次）
- **不擴展 `core/logging.py`**：`log_step` 既有形態足夠

## 驗證

`extract.py` 既有的 unit test（如有）應該繼續 pass — 函數簽章與回傳值不變。

實際公司環境再跑一次 `tune_hyperparameters`，驗收標準：
1. console / JSON log 應出現 `step=read_parquet`、`step=slice_features`、`step=encode_categoricals`（若有 deferred cats）、`step=to_numpy` 的 `step_started` / `step_completed` 事件
2. 每段之間應有對應的 size summary INFO log
3. 再次被 kill 時，最後一條 log 應能指出在哪一個 sub-step

## 不在範圍

- 進一步診斷出原因後的修法（例如改成 chunked read、或預先 cast dtype）— 等觀測資料拿到後再開新 spec
- 其他 node 的 sub-step 觀測性增強（如 `prepare_lgb_train_inputs` 內部、`evaluate_model` 內 `predict` 之外的部分）
