## Context

目前 data catalog 使用 PickleDataset 儲存 model（`lgb.Booster`）和 preprocessed arrays（`X_train`/`y_train` 等）。Pickle 格式不可讀、跨 Python 版本脆弱，且與其他資料類 artifacts（已用 parquet）不一致。Pipeline parameters 雖嵌入 manifest.json，但沒有獨立檔案方便直接檢視比較。

現有 I/O adapter 架構（`AbstractDataset` → registry in `DataCatalog`）已提供良好的擴展點，只需新增 adapter class 並註冊即可。

## Goals / Non-Goals

**Goals:**
- Model 使用 LightGBM 原生文字格式儲存（人類可讀、跨版本相容）
- 資料類 artifacts（X/y）統一使用 ParquetDataset
- 每次 pipeline 執行自動儲存 parameters JSON snapshot
- 維持既有 I/O adapter 架構的一致性

**Non-Goals:**
- 不處理 preprocessor（dict of sklearn objects）的格式變更 — PickleDataset 仍是合理選擇
- 不處理舊版本 artifacts 的自動遷移 — 需重新執行 pipeline
- 不新增通用的「模型格式」抽象層 — 目前只有 LightGBM 一種模型

## Decisions

### Decision 1：LightGBMDataset 使用原生文字格式

使用 `lgb.Booster.save_model()` / `lgb.Booster(model_file=...)` 原生 API。

**替代方案**：
- Pickle（現狀）：二進位、不可讀、跨版本脆弱
- Joblib：同 pickle 問題
- ONNX：增加依賴，inference 路徑需大改

原生格式是純文字、自帶結構描述、LightGBM 版本間高度相容，且 `lgb.Booster` 已有原生 API 支援。

### Decision 2：y_train 從 ndarray 改為 DataFrame

`y_train` 改為 `pd.DataFrame({"label": ...})`，training nodes 在需要 ndarray 的地方加 `["label"].values`。

**替代方案**：
- 讓 ParquetDataset 自動處理 ndarray↔DataFrame 轉換：增加 adapter 複雜度，違反「adapter 只做 I/O」原則
- 保持 ndarray 但用自訂 NumpyParquetDataset：過度工程

DataFrame 方案最簡潔：資料型別在整個 pipeline 中一致（DataFrame → Parquet → DataFrame），轉換只在 training nodes 的 LightGBM 接口處發生。

### Decision 3：Parameter snapshot 在 framework 層（`__main__.py`）處理

在 `__main__.py` post-run 區塊中寫入，不透過 catalog/pipeline node。

**替代方案**：
- 做為 catalog entry + pipeline node 輸出：parameters 是輸入不是輸出，語意不正確
- 在 versioning.py 的 `write_manifest()` 中附帶：耦合兩個不同關注點

post-run 區塊已經有 manifest 和 symlink 的邏輯，parameter snapshot 是同類操作。

## Risks / Trade-offs

- **Breaking change**：既有 `.pkl` model 和 X/y 檔案不再相容 → **Mitigation**: 版本系統是 hash-based，重新執行 pipeline 即產生新格式檔案。在 commit message 中明確標註。
- **y_train 型別變更影響面**：training nodes 所有接收 y 的函式都需修改 → **Mitigation**: 變更是機械式的（加 `["label"].values`），且有完整測試覆蓋。
- **LightGBM import 在 adapter 中**：若環境未安裝 lightgbm 會 import 失敗 → **Mitigation**: lightgbm 已是專案必要依賴，不需額外處理。
