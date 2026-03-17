## Context

Pipeline 已完成 dataset building、training、inference 三階段，但缺乏端對端的情境測試。現有 `tests/` 以 unit/integration test 為主，不涵蓋「調整設定後重新執行整條 pipeline」的營運情境。

關鍵限制：
- `__main__.py` 使用 `Path.cwd() / "conf"` 和 `Path.cwd() / "data"` 定位設定與資料
- `ConfigLoader` 支援 `conf/base/` + `conf/{env}/` 兩層 deep merge
- 訓練需要 Optuna + LightGBM，即使縮減 trials 仍需數十秒
- `promote_model.py` 的 `REQUIRED_ARTIFACTS` 含 `model.pkl`（已改為 `model.txt` 格式）

## Goals / Non-Goals

**Goals:**
- 建立 4 個可重複執行的端對端情境測試（pytest）
- 每個情境完全隔離，不影響現有 `conf/`、`data/`、其他測試
- 產出人工可檢視的繁體中文驗證報告
- 自動化 assertions 確保 pass/fail 明確

**Non-Goals:**
- 不測試 Spark backend（僅驗證 pandas backend）
- 不做效能壓測或大資料量測試
- 不修改既有 pipeline 邏輯（除 promote_model.py bug fix）
- 不建立 CI/CD 整合（未來可加）

## Decisions

### 1. 使用 pytest 而非 shell script

**選擇**：pytest 框架
**理由**：自動化 assertions + fixture 管理 setup/teardown + 和現有 test suite 一致
**替代方案**：shell script + validate.py — 但缺乏自動化判定，且 shell script 難維護

### 2. 工作目錄隔離策略

**選擇**：每個情境在 `tests/scenarios/output/scenario_N/` 下建立獨立工作目錄，內含 `conf/` 和 `data/` 的完整結構
**理由**：`__main__.py` 硬編碼使用 `Path.cwd()` 定位 conf 和 data，唯一的隔離方式是控制工作目錄
**做法**：
  - fixture 將 `conf/base/*.yaml` 複製到工作目錄的 `conf/base/`
  - 情境覆蓋設定寫入 `conf/{scenario_name}/`
  - 情境資料寫入 `data/feature_table.parquet` 和 `data/label_table.parquet`
  - 用 `subprocess.run(cwd=work_dir)` 執行 CLI

**替代方案**：monkeypatch `_find_conf_dir()` / `_find_data_dir()` — 侵入性更強且 subprocess 中無法生效

### 3. 資料產生方式

**選擇**：Python 模組函式（`data_generator.py`），由 fixture 呼叫，不產生持久化預製資料
**理由**：每次測試都產生新資料確保可重複性；函式化便於各情境傳入不同參數
**替代方案**：預製 parquet 檔 check in 到 repo — 但 parquet 是二進位檔，不好做 code review

### 4. 設定覆蓋方式

**選擇**：fixture 動態寫入情境專用 YAML 到工作目錄的 `conf/{env}/`，利用 ConfigLoader 的 deep merge
**理由**：只需寫差異設定，base 設定自動繼承；不碰專案 `conf/`
**具體做法**：每個 test function 定義自己的 parameter overrides dict，fixture 將其序列化為 YAML

### 5. promote_model 整合

**選擇**：情境測試中需要 inference 的情境（1, 3, 4），在 training 後自動呼叫 `promote_model.py` 建立 `best` symlink
**理由**：inference pipeline 的 `__main__.py` 從 `best` symlink 讀取 model，無 promote 則 inference 會失敗
**注意**：promote 腳本的 `--models-dir` 需指向工作目錄的 `data/models`

### 6. 驗證報告格式

**選擇**：每個 test function 內部呼叫 `generate_report()` helper，產出 `report.txt` 到 `tests/scenarios/output/scenario_N/`
**理由**：report 和 pytest 結果互補 — pytest 判 pass/fail，report 提供人工檢視的細節
**內容**：各 split 統計、preprocessor 資訊、模型指標、推論結果摘要、前 N 筆樣本

## Risks / Trade-offs

**[風險] 測試執行時間較長（每個情境含 Optuna tuning）**
→ 緩解：`n_trials=3`, `num_iterations=100` 將單一情境壓縮到約 1-2 分鐘

**[風險] 工作目錄隔離依賴 `Path.cwd()` 行為**
→ 緩解：這是 `__main__.py` 的明確設計，不太可能變動。如果未來改為參數化，fixture 只需小幅調整

**[風險] `promote_model.py` 的 `model.pkl` bug 阻擋情境 1/3/4**
→ 緩解：作為前置修正處理（改為 `model.txt`）

**[權衡] 使用 subprocess 而非直接呼叫 Python 函式**
→ subprocess 更貼近實際使用方式，但錯誤訊息較不直觀。透過 `check=True` + capture output 緩解
