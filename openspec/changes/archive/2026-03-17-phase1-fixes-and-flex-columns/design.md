## Context

recsys_tfb 是 Kedro-inspired 的推薦排序模型專案，已完成 Strategy 1 MVP。在進入後續 Phase（結構化日誌、版本管理增強）前，需修正三個已知問題：README 文件不一致、inference output 路徑使用 `"best"` 而非實際 hash、dataset pipeline 欄位 hard-coded。

目前 inference pipeline 在 `__main__.py` 中將 `runtime_params["model_version"]` 設為 `"best"`，導致所有 catalog 路徑中的 `${model_version}` 都被替換為 `"best"`。這包括 model 讀取路徑（合理）和 inference output 路徑（不合理——多次推論會覆蓋同一目錄）。

## Goals / Non-Goals

**Goals:**
- README 與程式碼行為一致
- Inference output 路徑使用實際 model hash，支援多版本 output 並存
- dataset pipeline 的欄位設定可透過 YAML 調整，無需改程式碼
- 所有修改向後相容

**Non-Goals:**
- 不改變 model 讀取機制（仍透過 `best` symlink）
- 不改變 dataset_version / model_version 的 hash 計算邏輯
- 不新增 CLI 參數
- 不涉及 結構化日誌（Phase 2）

## Decisions

### 1. Inference model 讀取與 output 路徑分離

**決定**：在 catalog 解析前，對 model 相關 entry 使用 `best` symlink 路徑，對 inference output entry 使用實際 hash。

**做法**：`runtime_params["model_version"]` 設為實際 hash（`mv`）。但 model 和 preprocessor 的 catalog entry 需要透過 `best` symlink 讀取，因此在 catalog config 解析後，針對 model/preprocessor entry 的 filepath 做 post-processing：將其中的 `${model_version}` 替換為 `best`。

**替代方案考慮**：
- A) 新增 `${model_version_or_best}` 模板變數 → 過度工程化，增加概念負擔
- B) 在 catalog.yaml 中 model/preprocessor 路徑直接寫 `best` → 破壞模板化設計
- C) 不改動，保持 `"best"` → 無法追溯 inference output 對應的模型版本

**選擇 post-processing 方案**：最小侵入性，不需改 catalog 模板系統，也不需改 catalog.yaml。

### 2. 欄位彈性化的預設值策略

**決定**：`parameters` dict 中用 `.get()` 取值，程式碼內保留現有 hard-coded 值作為 fallback。

**理由**：向後相容。未更新 `parameters_dataset.yaml` 的使用者不受影響。同時 preprocessor dict 會記錄實際使用的設定值（不論來自 YAML 或預設值），確保 inference 時能正確重現。

### 3. Inference output latest symlink

**決定**：inference pipeline 完成後，在 `data/inference/` 下維護 `latest` symlink 指向最近的 `<model_hash>/<snap_date>/` 目錄。

**理由**：與 dataset/models 目錄的 symlink 機制一致，方便快速存取最新推論結果。

## Risks / Trade-offs

- **路徑結構改變**：inference output 從 `data/inference/best/` 變為 `data/inference/<hash>/`。既有 output 不會自動遷移。→ 低風險，dev 環境可重跑，production 尚未上線。
- **test_cli.py 需更新**：`test_inference_uses_best_model_version` 中 model filepath 驗證邏輯需調整。→ 直接修改測試。
- **Post-processing 耦合**：model/preprocessor entry 名稱需在 `__main__.py` 中 hard-code。→ 可接受，因為這些 entry 名稱本身就是 pipeline 設計的一部分，且數量固定。
